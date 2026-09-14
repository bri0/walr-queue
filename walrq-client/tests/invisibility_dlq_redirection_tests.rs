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
use walrq_client::{AckItem, ClientConfig, WalrClient};

struct TestCluster {
    _dir1: tempfile::TempDir,
    _dir2: tempfile::TempDir,
    _dir3: tempfile::TempDir,
    pub addr1: SocketAddr,
    pub addr2: SocketAddr,
    pub addr3: SocketAddr,
    _shutdown_tx1: broadcast::Sender<()>,
    _shutdown_tx2: broadcast::Sender<()>,
    _shutdown_tx3: broadcast::Sender<()>,
}

async fn start_test_cluster(base_port: u16, max_delivery_count: u32) -> TestCluster {
    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();
    let dir3 = tempdir().unwrap();

    let addr1: SocketAddr = format!("127.0.0.1:{}", base_port).parse().unwrap();
    let addr2: SocketAddr = format!("127.0.0.1:{}", base_port + 1).parse().unwrap();
    let addr3: SocketAddr = format!("127.0.0.1:{}", base_port + 2).parse().unwrap();

    let opts = QueueOptions {
        default_visibility_timeout_sec: 1,
        max_delivery_count,
        max_hot_messages_in_ram: 100_000,
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

    // Establish leader knowledge on followers
    raft1.send_heartbeat().await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    TestCluster {
        _dir1: dir1,
        _dir2: dir2,
        _dir3: dir3,
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

/// 1. Push & Poll Redirection: Client hitting follower gets transparently redirected on BOTH push and poll
#[tokio::test]
async fn test_push_and_poll_follower_redirection() {
    let cluster = start_test_cluster(58310, 3).await;
    // Client ONLY connects to follower addr2
    let follower_client = create_client(vec![cluster.addr2]);
    let q = "redirect-poll-push-q";

    // Follower client pushes -> must be redirected to leader addr1 and succeed
    let id = follower_client.push(q, Bytes::from("redirect-payload"), 0).await.unwrap();
    assert!(!id.is_empty());

    // Follower client polls -> must be redirected to leader addr1 and succeed
    let polled = follower_client.poll(q, 10, 10).await.unwrap();
    assert_eq!(polled.len(), 1);
    assert_eq!(polled[0].message_id, id);
    assert_eq!(polled[0].payload, Bytes::from("redirect-payload"));

    // Follower client acks -> must be redirected and succeed
    let acked = follower_client.ack(q, &id, &polled[0].receipt_handle).await.unwrap();
    assert!(acked);
}

/// 2. Invisibility Window Verification: In-flight messages are invisible until timeout
#[tokio::test]
async fn test_invisibility_window_enforcement() {
    let cluster = start_test_cluster(58320, 3).await;
    let client = create_client(vec![cluster.addr1]);
    let q = "invis-window-q";

    let id = client.push(q, Bytes::from("invis-data"), 0).await.unwrap();

    // Consumer A claims with 2-second visibility window
    let p1 = client.poll(q, 2, 10).await.unwrap();
    assert_eq!(p1.len(), 1);
    assert_eq!(p1[0].message_id, id);

    // Consumer B polls immediately -> MUST BE INVISIBLE (returns 0 msgs)
    let p_immediate = client.poll(q, 2, 10).await.unwrap();
    assert!(p_immediate.is_empty(), "Claimed message must be completely invisible to other consumers");

    // Sleep 1 second -> still inside the 2s lease
    tokio::time::sleep(Duration::from_millis(1000)).await;
    let p_mid = client.poll(q, 2, 10).await.unwrap();
    assert!(p_mid.is_empty(), "Message must remain invisible at 1.0s of a 2.0s window");

    // Sleep another 1.2 seconds -> 2.2s total, lease expired!
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let p_expired = client.poll(q, 2, 10).await.unwrap();
    assert_eq!(p_expired.len(), 1, "Expired message must become visible again");
    assert_eq!(p_expired[0].message_id, id);
    assert_eq!(p_expired[0].delivery_count, 2);

    let acked = client.ack(q, &id, &p_expired[0].receipt_handle).await.unwrap();
    assert!(acked);
}

/// 3. Dead Letter Queue (DLQ): Poison messages automatically route to `<queue>.dlq`
#[tokio::test]
async fn test_dead_letter_queue_routing() {
    let max_deliveries = 2;
    let cluster = start_test_cluster(58330, max_deliveries).await;
    let client = create_client(vec![cluster.addr1]);
    let q = "work-queue";
    let dlq = format!("{}.dlq", q);

    let id = client.push(q, Bytes::from("poison-pill-task"), 0).await.unwrap();

    // Delivery 1
    let p1 = client.poll(q, 1, 1).await.unwrap();
    assert_eq!(p1.len(), 1);
    assert_eq!(p1[0].delivery_count, 1);
    // Don't ack -> let visibility expire (1s lease)
    tokio::time::sleep(Duration::from_millis(1200)).await;

    // Delivery 2 (Max Delivery reached)
    let p2 = client.poll(q, 1, 1).await.unwrap();
    assert_eq!(p2.len(), 1);
    assert_eq!(p2[0].delivery_count, 2);
    // Don't ack -> let visibility expire again
    tokio::time::sleep(Duration::from_millis(1200)).await;

    // Next poll on original queue triggers expiration routing
    let p3 = client.poll(q, 1, 1).await.unwrap();
    assert!(p3.is_empty(), "Original queue must now be empty after DLQ routing");

    // Poll the DLQ queue!
    let dlq_polled = client.poll(&dlq, 10, 1).await.unwrap();
    assert_eq!(dlq_polled.len(), 1, "Poison message must appear in the .dlq queue");
    assert_eq!(dlq_polled[0].message_id, id);
    assert_eq!(dlq_polled[0].payload, Bytes::from("poison-pill-task"));

    // DLQ messages can be inspected and acknowledged
    let acked = client.ack(&dlq, &id, &dlq_polled[0].receipt_handle).await.unwrap();
    assert!(acked, "DLQ item must be acknowledgeable");

    let empty_dlq = client.poll(&dlq, 10, 1).await.unwrap();
    assert!(empty_dlq.is_empty());
}

/// 4. Concurrent Multi-Consumer Race: Zero duplicate deliveries under contention
#[tokio::test]
async fn test_concurrent_poll_no_duplicate_deliveries() {
    let cluster = start_test_cluster(58340, 3).await;
    let client = Arc::new(create_client(vec![cluster.addr1]));
    let q = "race-poll-q";

    let count = 200;
    let mut payloads = Vec::with_capacity(count);
    for i in 0..count {
        payloads.push(Bytes::from(format!("race-item-{}", i)));
    }
    client.push_batch(q, payloads).await.unwrap();

    // Spawn 5 concurrent consumers racing to poll items
    let mut consumer_handles = Vec::new();
    for _ in 0..5 {
        let cl = Arc::clone(&client);
        let q_name = q.to_string();
        consumer_handles.push(tokio::spawn(async move {
            let mut claimed = Vec::new();
            for _ in 0..50 {
                if let Ok(msgs) = cl.poll(&q_name, 10, 10).await {
                    if !msgs.is_empty() {
                        for m in msgs {
                            let _ = cl.ack(&q_name, &m.message_id, &m.receipt_handle).await;
                            claimed.push(m.message_id);
                        }
                    }
                }
                tokio::task::yield_now().await;
            }
            claimed
        }));
    }

    let mut all_claimed = Vec::new();
    for h in consumer_handles {
        let items = h.await.unwrap();
        all_claimed.extend(items);
    }

    assert_eq!(all_claimed.len(), count, "Every single message must be claimed exactly once");
    all_claimed.sort();
    all_claimed.dedup();
    assert_eq!(all_claimed.len(), count, "Zero duplicate claims permitted among concurrent consumers");
}
