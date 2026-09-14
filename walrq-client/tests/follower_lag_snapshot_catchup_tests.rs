use bytes::Bytes;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
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

fn create_client(addrs: Vec<SocketAddr>) -> WalrClient {
    WalrClient::with_config(
        addrs.into_iter().map(|a| a.to_string()).collect(),
        ClientConfig {
            buffer_window_ms: 1,
            max_batch_size: 50,
            max_redirects: 5,
        },
    )
}

/// Attack: Follower Isolation Lag and High-Watermark Catchup Under Compaction
/// 1. Boot 3-node cluster with small compaction threshold (20 entries).
/// 2. Disconnect Node 3 completely.
/// 3. Ingest 150 items through Leader Node 1 and Follower Node 2 (exceeding compaction threshold multiple times).
/// 4. Leader Node 1 prunes its in-memory Raft log via snapshots.
/// 5. Reconnect Node 3 after log truncation.
/// 6. Node 3 must receive InstallSnapshot RPC, recover entire queue state machine, and match state.
/// 7. Client polls from Node 3 (via follower redirect) and verifies 100% data integrity.
#[tokio::test]
async fn test_follower_lag_and_snapshot_catchup_recovery() {
    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();
    let dir3 = tempdir().unwrap();

    let path1 = dir1.path().to_path_buf();
    let path2 = dir2.path().to_path_buf();
    let path3 = dir3.path().to_path_buf();

    let addr1: SocketAddr = "127.0.0.1:59811".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:59812".parse().unwrap();
    let addr3: SocketAddr = "127.0.0.1:59813".parse().unwrap();

    let opts = QueueOptions {
        default_visibility_timeout_sec: 10,
        max_delivery_count: 3,
        max_hot_messages_in_ram: 50_000,
        max_wal_segment_size: 16 * 1024 * 1024,
    };

    let engine1 = Arc::new(QueueEngine::open(&path1, opts.clone()).unwrap());
    let engine2 = Arc::new(QueueEngine::open(&path2, opts.clone()).unwrap());
    let engine3 = Arc::new(QueueEngine::open(&path3, opts.clone()).unwrap());

    // Low compaction threshold = 20 entries to force snapshot truncation
    let raft1 = Arc::new(RaftNode::with_threshold(addr1.to_string(), vec![addr2.to_string(), addr3.to_string()], Arc::clone(&engine1), 20));
    let raft2 = Arc::new(RaftNode::with_threshold(addr2.to_string(), vec![addr1.to_string(), addr3.to_string()], Arc::clone(&engine2), 20));
    let raft3 = Arc::new(RaftNode::with_threshold(addr3.to_string(), vec![addr1.to_string(), addr2.to_string()], Arc::clone(&engine3), 20));

    raft1.become_leader_for_test().await;

    let server1 = Arc::new(WalrServer::new_raft(Arc::clone(&engine1), Arc::clone(&raft1), addr1.to_string()));
    let server2 = Arc::new(WalrServer::new_raft(Arc::clone(&engine2), Arc::clone(&raft2), addr2.to_string()));
    let server3 = Arc::new(WalrServer::new_raft(Arc::clone(&engine3), Arc::clone(&raft3), addr3.to_string()));

    let listener1 = TcpListener::bind(addr1).await.unwrap();
    let listener2 = TcpListener::bind(addr2).await.unwrap();
    let listener3 = TcpListener::bind(addr3).await.unwrap();

    let (_tx1, rx1) = broadcast::channel(1);
    let (_tx2, rx2) = broadcast::channel(1);
    let (tx3, rx3) = broadcast::channel(1);

    tokio::spawn(async move { server1.run(listener1, rx1).await; });
    tokio::spawn(async move { server2.run(listener2, rx2).await; });
    tokio::spawn(async move { server3.run(listener3, rx3).await; });

    raft1.send_heartbeat().await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = create_client(vec![addr1, addr2]);
    let q = "snapshot-lag-q";

    // Step 1: Initial 10 items committed across all 3
    for i in 0..10 {
        client.push(q, Bytes::from(format!("initial-data-{}", i)), 0).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert_eq!(engine1.total_messages_in_ram(), 10);
    assert_eq!(engine3.total_messages_in_ram(), 10);

    // Step 2: Sever Node 3 completely (stop listener and kill node)
    raft3.is_running.store(false, Ordering::Relaxed);
    let _ = tx3.send(());
    // Also drop cached socket on leader
    raft1.drop_peer_connection(&addr3.to_string()).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Step 3: Ingest 140 more items across Node 1 and Node 2
    // With threshold = 20, this forces MULTIPLE snapshot compactions on Leader Node 1!
    for i in 10..150 {
        client.push(q, Bytes::from(format!("high-watermark-{}", i)), 0).await.unwrap();
    }

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(engine1.total_messages_in_ram(), 150);
    assert_eq!(engine2.total_messages_in_ram(), 150);
    assert_eq!(engine3.total_messages_in_ram(), 10, "Node 3 lagged behind while offline");

    // Verify Leader compacted its log and advanced snapshot index
    let snap_idx = raft1.snapshot_index.load(Ordering::SeqCst);
    assert!(snap_idx > 0, "Leader must have compacted and created snapshots: got {}", snap_idx);

    // Step 4: Reconnect Node 3!
    let listener3_new = TcpListener::bind(addr3).await.unwrap();
    let server3_new = Arc::new(WalrServer::new_raft(Arc::clone(&engine3), Arc::clone(&raft3), addr3.to_string()));
    let (_tx3_new, rx3_new) = broadcast::channel(1);
    tokio::spawn(async move { server3_new.run(listener3_new, rx3_new).await; });

    // Step 5: Trigger Leader catchup sync (Leader detects Node 3 is far behind log and sends snapshot)
    raft1.sync_all_entries_to_peer(&addr3.to_string()).await;
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Node 3 state machine must now be fully caught up to 150 messages!
    assert_eq!(engine3.total_messages_in_ram(), 150, "Node 3 must catch up to all 150 messages via snapshot");

    // Step 6: Client connects EXCLUSIVELY to caught-up Node 3 and drains all 150 items
    let client_node3 = create_client(vec![addr3]);
    let mut total_drained = 0;
    for _ in 0..10 {
        if let Ok(msgs) = client_node3.poll(q, 10, 50).await {
            if !msgs.is_empty() {
                let ack_items: Vec<AckItem> = msgs.into_iter().map(|m| {
                    total_drained += 1;
                    AckItem {
                        message_id: m.message_id,
                        receipt_handle: m.receipt_handle,
                    }
                }).collect();
                let _ = client_node3.ack_batch_items(q, ack_items).await;
            }
        }
        if total_drained >= 150 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert_eq!(total_drained, 150, "Must drain all 150 messages through resurrected follower node 3");
}
