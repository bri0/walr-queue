use bytes::Bytes;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use ulid::Ulid;

use walrq::cluster::raft::RaftNode;
use walrq::engine::queue::{QueueEngine, QueueOptions};
use walrq::server::tcp_service::WalrServer;
use walrq_client::{AckItem, ClientConfig, ClientError, WalrClient};

struct TestCluster {
    _dir1: tempfile::TempDir,
    _dir2: tempfile::TempDir,
    _dir3: tempfile::TempDir,
    pub engine1: Arc<QueueEngine>,
    pub engine2: Arc<QueueEngine>,
    pub engine3: Arc<QueueEngine>,
    pub raft1: Arc<RaftNode>,
    pub raft2: Arc<RaftNode>,
    pub raft3: Arc<RaftNode>,
    pub addr1: SocketAddr,
    pub addr2: SocketAddr,
    pub addr3: SocketAddr,
    _shutdown_tx1: broadcast::Sender<()>,
    _shutdown_tx2: broadcast::Sender<()>,
    _shutdown_tx3: broadcast::Sender<()>,
}

async fn start_test_cluster(base_port: u16, max_hot_messages_in_ram: usize) -> TestCluster {
    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();
    let dir3 = tempdir().unwrap();

    let addr1: SocketAddr = format!("127.0.0.1:{}", base_port).parse().unwrap();
    let addr2: SocketAddr = format!("127.0.0.1:{}", base_port + 1).parse().unwrap();
    let addr3: SocketAddr = format!("127.0.0.1:{}", base_port + 2).parse().unwrap();

    let opts = QueueOptions {
        default_visibility_timeout_sec: 2,
        max_delivery_count: 3,
        max_hot_messages_in_ram,
        max_wal_segment_size: 16 * 1024 * 1024,
    };

    let engine1 = Arc::new(QueueEngine::open(dir1.path(), opts.clone()).unwrap());
    let engine2 = Arc::new(QueueEngine::open(dir2.path(), opts.clone()).unwrap());
    let engine3 = Arc::new(QueueEngine::open(dir3.path(), opts.clone()).unwrap());

    let raft1 = Arc::new(RaftNode::with_threshold(
        addr1.to_string(),
        vec![addr2.to_string(), addr3.to_string()],
        Arc::clone(&engine1),
        1000,
    ));
    let raft2 = Arc::new(RaftNode::with_threshold(
        addr2.to_string(),
        vec![addr1.to_string(), addr3.to_string()],
        Arc::clone(&engine2),
        1000,
    ));
    let raft3 = Arc::new(RaftNode::with_threshold(
        addr3.to_string(),
        vec![addr1.to_string(), addr2.to_string()],
        Arc::clone(&engine3),
        1000,
    ));

    raft1.become_leader_for_test().await;

    let server1 = Arc::new(WalrServer::new_raft(Arc::clone(&engine1), Arc::clone(&raft1), addr1.to_string()));
    let server2 = Arc::new(WalrServer::new_raft(Arc::clone(&engine2), Arc::clone(&raft2), addr2.to_string()));
    let server3 = Arc::new(WalrServer::new_raft(Arc::clone(&engine3), Arc::clone(&raft3), addr3.to_string()));

    let listener1 = TcpListener::bind(addr1).await.unwrap();
    let listener2 = TcpListener::bind(addr2).await.unwrap();
    let listener3 = TcpListener::bind(addr3).await.unwrap();

    let (shutdown_tx1, rx1) = broadcast::channel(1);
    let (shutdown_tx2, rx2) = broadcast::channel(1);
    let (shutdown_tx3, rx3) = broadcast::channel(1);

    tokio::spawn(async move { server1.run(listener1, rx1).await; });
    tokio::spawn(async move { server2.run(listener2, rx2).await; });
    tokio::spawn(async move { server3.run(listener3, rx3).await; });

    tokio::time::sleep(Duration::from_millis(50)).await;

    TestCluster {
        _dir1: dir1,
        _dir2: dir2,
        _dir3: dir3,
        engine1,
        engine2,
        engine3,
        raft1,
        raft2,
        raft3,
        addr1,
        addr2,
        addr3,
        _shutdown_tx1: shutdown_tx1,
        _shutdown_tx2: shutdown_tx2,
        _shutdown_tx3: shutdown_tx3,
    }
}

fn create_client(addrs: Vec<SocketAddr>) -> WalrClient {
    WalrClient::with_config(
        addrs.into_iter().map(|a| a.to_string()).collect(),
        ClientConfig {
            buffer_window_ms: 1,
            max_batch_size: 100,
            max_redirects: 5,
        },
    )
}

/// 1. Visibility Timeout Redelivery & Max Delivery Count (DLQ)
#[tokio::test]
async fn test_correctness_visibility_timeout_and_max_deliveries() {
    let cluster = start_test_cluster(58110, 100_000).await;
    let client = create_client(vec![cluster.addr1]);
    let q = "dlq-test-queue";

    let msg_id = client.push(q, Bytes::from("payload-dlq"), 0).await.unwrap();

    // 1st delivery
    let polled1 = client.poll(q, 1, 10).await.unwrap();
    assert_eq!(polled1.len(), 1);
    assert_eq!(polled1[0].message_id, msg_id);
    assert_eq!(polled1[0].delivery_count, 1);

    // Immediate poll should yield nothing (in-flight)
    let empty_poll = client.poll(q, 1, 10).await.unwrap();
    assert!(empty_poll.is_empty());

    // Wait >1s for visibility timeout expiration
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // 2nd delivery
    let polled2 = client.poll(q, 1, 10).await.unwrap();
    assert_eq!(polled2.len(), 1);
    assert_eq!(polled2[0].message_id, msg_id);
    assert_eq!(polled2[0].delivery_count, 2);

    tokio::time::sleep(Duration::from_millis(1500)).await;

    // 3rd delivery (max_delivery_count = 3)
    let polled3 = client.poll(q, 1, 10).await.unwrap();
    assert_eq!(polled3.len(), 1);
    assert_eq!(polled3[0].message_id, msg_id);
    assert_eq!(polled3[0].delivery_count, 3);

    tokio::time::sleep(Duration::from_millis(1500)).await;

    // 4th poll -> exceeded max_delivery_count, dropped / dead-lettered!
    let polled4 = client.poll(q, 1, 10).await.unwrap();
    assert!(polled4.is_empty(), "Message beyond max deliveries should not be redelivered");
}

/// 2. Receipt Handle Guard: Stale or forged receipts must be rejected
#[tokio::test]
async fn test_correctness_stale_and_forged_receipt_handles() {
    let cluster = start_test_cluster(58120, 100_000).await;
    let client = create_client(vec![cluster.addr1]);
    let q = "receipt-guard-test";

    let id = client.push(q, Bytes::from("guarded-data"), 0).await.unwrap();

    // Claim 1
    let p1 = client.poll(q, 1, 10).await.unwrap();
    assert_eq!(p1.len(), 1);
    let receipt1 = p1[0].receipt_handle.clone();

    // Try acknowledging with forged fake receipt
    let forged_ack = client.ack(q, &id, "forged-fake-receipt-token").await.unwrap();
    assert!(!forged_ack, "Forged receipt must be rejected");

    // Let visibility expire
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // Claim 2 -> new receipt issued
    let p2 = client.poll(q, 2, 10).await.unwrap();
    assert_eq!(p2.len(), 1);
    let receipt2 = p2[0].receipt_handle.clone();
    assert_ne!(receipt1, receipt2, "New poll claim must generate distinct receipt");

    // Stale receipt1 must fail now!
    let stale_ack = client.ack(q, &id, &receipt1).await.unwrap();
    assert!(!stale_ack, "Stale receipt from prior visibility window must be rejected");

    // Fresh receipt2 must succeed
    let valid_ack = client.ack(q, &id, &receipt2).await.unwrap();
    assert!(valid_ack, "Valid fresh receipt must succeed");

    // Re-ack after delete must fail cleanly
    let re_ack = client.ack(q, &id, &receipt2).await.unwrap();
    assert!(!re_ack, "Re-acknowledging already deleted message must return false");
}

/// 3. Delayed Messages: visible_at timing contract
#[tokio::test]
async fn test_correctness_delayed_message_visibility() {
    let cluster = start_test_cluster(58130, 100_000).await;
    let client = create_client(vec![cluster.addr1]);
    let q = "delay-test-queue";

    // Push with 2 second delay
    let id = client.push(q, Bytes::from("delayed-payload"), 2).await.unwrap();

    // Immediately poll -> must not be visible
    let immediate = client.poll(q, 5, 10).await.unwrap();
    assert!(immediate.is_empty(), "Delayed message should not be visible immediately");

    // Wait 1 second -> still not visible
    tokio::time::sleep(Duration::from_millis(1000)).await;
    let intermediate = client.poll(q, 5, 10).await.unwrap();
    assert!(intermediate.is_empty(), "Delayed message should not be visible at 1s");

    // Wait another 1.2 seconds -> now visible
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let final_poll = client.poll(q, 5, 10).await.unwrap();
    assert_eq!(final_poll.len(), 1);
    assert_eq!(final_poll[0].message_id, id);

    // Ack cleanly
    let acked = client.ack(q, &id, &final_poll[0].receipt_handle).await.unwrap();
    assert!(acked);
}

/// 4. Custom ULID Idempotent Deduplication
#[tokio::test]
async fn test_correctness_custom_ulid_deduplication() {
    let cluster = start_test_cluster(58140, 100_000).await;
    let client = create_client(vec![cluster.addr1]);
    let q = "dedup-test-queue";

    let custom_id = Ulid::new().to_string();

    // Push first time
    let id1 = client.push_with_id(q, Bytes::from("first-attempt"), 0, &custom_id).await.unwrap();
    assert_eq!(id1, custom_id);

    // Push exact duplicate ULID -> must be safely deduplicated
    let id2 = client.push_with_id(q, Bytes::from("second-attempt-duplicate"), 0, &custom_id).await.unwrap();
    assert_eq!(id2, custom_id);

    // Only ONE message exists in queue!
    let polled = client.poll(q, 10, 10).await.unwrap();
    assert_eq!(polled.len(), 1, "Duplicate ULID must not create duplicate queue entries");
    assert_eq!(polled[0].message_id, custom_id);
    assert_eq!(polled[0].payload, Bytes::from("first-attempt"));

    let empty = client.poll(q, 10, 10).await.unwrap();
    assert!(empty.is_empty());
}

/// 5. Disk Spill Instead of Ram Limit Error
#[tokio::test]
async fn test_correctness_ram_limit_backpressure() {
    // Set RAM cap strictly to 10 messages
    let cluster = start_test_cluster(58150, 10).await;
    let client = create_client(vec![cluster.addr1]);
    let q = "backpressure-queue";

    // Push 25 messages into queue capped at 10 in RAM
    for i in 0..25 {
        let res = client.push_immediate(q, Bytes::from(format!("item-{}", i)), 0).await;
        assert!(res.is_ok(), "Pushes must succeed by spilling to disk instead of failing!");
    }

    // Messages in RAM must remain bounded at 10
    assert!(cluster.engine1.total_messages_in_ram() <= 10);

    // Poll and ACK all 25 messages via sequential disk spill paging
    let mut total_drained = 0;
    for _ in 0..10 {
        let polled = client.poll(q, 10, 10).await.unwrap();
        for m in polled {
            let acked = client.ack(q, &m.message_id, &m.receipt_handle).await.unwrap();
            assert!(acked);
            total_drained += 1;
        }
        if total_drained >= 25 {
            break;
        }
    }
    assert_eq!(total_drained, 25);
}

/// 6. Strict Batch Operations (Batch Push & Batch Ack)
#[tokio::test]
async fn test_correctness_batch_push_and_batch_ack_isolation() {
    let cluster = start_test_cluster(58160, 100_000).await;
    let client = create_client(vec![cluster.addr1]);
    let q = "batch-correctness-queue";

    let mut payloads = Vec::new();
    for i in 0..50 {
        payloads.push(Bytes::from(format!("batch-item-{}", i)));
    }

    let ids = client.push_batch(q, payloads).await.unwrap();
    assert_eq!(ids.len(), 50);

    // Poll all 50 items
    let polled = client.poll(q, 30, 100).await.unwrap();
    assert_eq!(polled.len(), 50);

    // Ack first 25 with batch ack
    let ack_group_1: Vec<AckItem> = polled[..25]
        .iter()
        .map(|m| AckItem {
            message_id: m.message_id.clone(),
            receipt_handle: m.receipt_handle.clone(),
        })
        .collect();

    let acked_count = client.ack_batch_items(q, ack_group_1).await.unwrap();
    assert_eq!(acked_count, 25);

    // Attempting to re-ack that same batch must yield 0
    let re_ack_group_1: Vec<AckItem> = polled[..25]
        .iter()
        .map(|m| AckItem {
            message_id: m.message_id.clone(),
            receipt_handle: m.receipt_handle.clone(),
        })
        .collect();
    let re_acked_count = client.ack_batch_items(q, re_ack_group_1).await.unwrap();
    assert_eq!(re_acked_count, 0);

    // Ack remaining 25
    let ack_group_2: Vec<AckItem> = polled[25..]
        .iter()
        .map(|m| AckItem {
            message_id: m.message_id.clone(),
            receipt_handle: m.receipt_handle.clone(),
        })
        .collect();
    let acked_count_2 = client.ack_batch_items(q, ack_group_2).await.unwrap();
    assert_eq!(acked_count_2, 25);

    // Queue is now completely empty
    let empty = client.poll(q, 30, 10).await.unwrap();
    assert!(empty.is_empty());
}

/// 7. Cross-Node Replication & Consensus Consistency
#[tokio::test]
async fn test_correctness_quorum_replication_across_nodes() {
    let cluster = start_test_cluster(58170, 100_000).await;
    let client = create_client(vec![cluster.addr1]);
    let q = "replication-test-queue";

    let mut pushed_ids = Vec::new();
    for i in 0..20 {
        let id = client.push(q, Bytes::from(format!("repl-item-{}", i)), 0).await.unwrap();
        pushed_ids.push(id);
    }

    // Allow commit index synchronization
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Verify engines of followers replicated exact messages
    let q_name = q.to_string();
    let count1 = cluster.engine1.total_messages_in_ram();
    let count2 = cluster.engine2.total_messages_in_ram();
    let count3 = cluster.engine3.total_messages_in_ram();

    assert_eq!(count1, 20);
    assert_eq!(count2, 20, "Node 2 must have replicated all 20 messages");
    assert_eq!(count3, 20, "Node 3 must have replicated all 20 messages");

    // Client connected directly to follower addr2: sending push should automatically redirect to leader addr1!
    let follower_client = create_client(vec![cluster.addr2]);
    let redirected_id = follower_client.push(&q_name, Bytes::from("redirect-test"), 0).await.unwrap();
    assert!(!redirected_id.is_empty());

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(cluster.engine1.total_messages_in_ram(), 21);
    assert_eq!(cluster.engine2.total_messages_in_ram(), 21);
    assert_eq!(cluster.engine3.total_messages_in_ram(), 21);
}
