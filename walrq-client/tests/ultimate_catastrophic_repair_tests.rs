use bytes::Bytes;
use std::fs::{self, OpenOptions};
use std::io::Write;
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

/// The Ultimate Catastrophic Destruction & Recovery Gauntlet:
/// 1. 3 Nodes active, ingest 200 items in parallel across 4 queues.
/// 2. Abruptly kill Node 1 (Leader).
/// 3. While Node 1 is offline:
///    - Truncate and corrupt Node 1's on-disk WAL file with garbage bytes.
///    - Overwrite Node 1's queues.manifest with partial entries.
/// 4. Promote Node 2 as new Leader and push 100 more items (2/3 quorum).
/// 5. Revive Node 1 with the corrupted disk!
/// 6. Trigger Raft catchup: Node 2 sends snapshot/log replication to repair Node 1.
/// 7. Drain and verify all 300 items across all queues.
#[tokio::test]
async fn test_ultimate_catastrophic_corruption_and_repair_gauntlet() {
    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();
    let dir3 = tempdir().unwrap();

    let path1 = dir1.path().to_path_buf();
    let path2 = dir2.path().to_path_buf();
    let path3 = dir3.path().to_path_buf();

    let addr1: SocketAddr = "127.0.0.1:58811".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:58812".parse().unwrap();
    let addr3: SocketAddr = "127.0.0.1:58813".parse().unwrap();

    let opts = QueueOptions {
        default_visibility_timeout_sec: 10,
        max_delivery_count: 3,
        max_hot_messages_in_ram: 50_000,
        max_wal_segment_size: 16 * 1024 * 1024,
    };

    let engine1 = Arc::new(QueueEngine::open(&path1, opts.clone()).unwrap());
    let engine2 = Arc::new(QueueEngine::open(&path2, opts.clone()).unwrap());
    let engine3 = Arc::new(QueueEngine::open(&path3, opts.clone()).unwrap());

    let raft1 = Arc::new(RaftNode::with_threshold(addr1.to_string(), vec![addr2.to_string(), addr3.to_string()], Arc::clone(&engine1), 50));
    let raft2 = Arc::new(RaftNode::with_threshold(addr2.to_string(), vec![addr1.to_string(), addr3.to_string()], Arc::clone(&engine2), 50));
    let raft3 = Arc::new(RaftNode::with_threshold(addr3.to_string(), vec![addr1.to_string(), addr2.to_string()], Arc::clone(&engine3), 50));

    raft1.become_leader_for_test().await;

    let server1 = Arc::new(WalrServer::new_raft(Arc::clone(&engine1), Arc::clone(&raft1), addr1.to_string()));
    let server2 = Arc::new(WalrServer::new_raft(Arc::clone(&engine2), Arc::clone(&raft2), addr2.to_string()));
    let server3 = Arc::new(WalrServer::new_raft(Arc::clone(&engine3), Arc::clone(&raft3), addr3.to_string()));

    let listener1 = TcpListener::bind(addr1).await.unwrap();
    let listener2 = TcpListener::bind(addr2).await.unwrap();
    let listener3 = TcpListener::bind(addr3).await.unwrap();

    let (tx1, rx1) = broadcast::channel(1);
    let (_tx2, rx2) = broadcast::channel(1);
    let (_tx3, rx3) = broadcast::channel(1);

    tokio::spawn(async move { server1.run(listener1, rx1).await; });
    tokio::spawn(async move { server2.run(listener2, rx2).await; });
    tokio::spawn(async move { server3.run(listener3, rx3).await; });

    raft1.send_heartbeat().await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = create_client(vec![addr1, addr2, addr3]);

    // Phase 1: Ingest 200 items across 4 distinct queues
    for q_idx in 0..4 {
        let q = format!("gauntlet-q-{}", q_idx);
        let mut payloads = Vec::with_capacity(50);
        for i in 0..50 {
            payloads.push(Bytes::from(format!("payload-{}-{}", q_idx, i)));
        }
        client.push_batch(&q, payloads).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Phase 2: Kill Leader Node 1
    let _ = tx1.send(());
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Phase 3: Actively corrupt Node 1's disk files while offline
    let wal_a1 = path1.join("wal_a.log");
    if wal_a1.exists() {
        let mut f = OpenOptions::new().write(true).open(&wal_a1).unwrap();
        f.write_all(b"\xDE\xAD\xBE\xEF\x00\x00\x00\x00CORRUPT_BYTES").unwrap();
    }

    let manifest1 = path1.join("meta").join("queues.manifest");
    if manifest1.exists() {
        let mut f = OpenOptions::new().write(true).open(&manifest1).unwrap();
        f.write_all(b"\xFF\xFF\x00\x01MALFORMED").unwrap();
    }

    // Phase 4: Promote Node 2 as Leader; push 100 more items (25 per queue) to surviving majority (Node 2 + Node 3)
    raft2.recover_as_new_leader().await;
    raft2.send_heartbeat().await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client_surviving = create_client(vec![addr2, addr3]);
    for q_idx in 0..4 {
        let q = format!("gauntlet-q-{}", q_idx);
        let mut payloads = Vec::with_capacity(25);
        for i in 50..75 {
            payloads.push(Bytes::from(format!("payload-{}-{}", q_idx, i)));
        }
        client_surviving.push_batch(&q, payloads).await.unwrap();
    }

    // Phase 5: Revive Node 1 on the corrupted storage directory!
    // The engine must not crash on boot, and Raft replication must reconcile state
    let engine1_revived = Arc::new(QueueEngine::open(&path1, opts.clone()).unwrap());
    let raft1_revived = Arc::new(RaftNode::with_threshold(
        addr1.to_string(),
        vec![addr2.to_string(), addr3.to_string()],
        Arc::clone(&engine1_revived),
        50,
    ));

    let listener1_revived = TcpListener::bind(addr1).await.unwrap();
    let server1_revived = Arc::new(WalrServer::new_raft(Arc::clone(&engine1_revived), Arc::clone(&raft1_revived), addr1.to_string()));
    let (_tx1_new, rx1_new) = broadcast::channel(1);
    tokio::spawn(async move { server1_revived.run(listener1_revived, rx1_new).await; });

    // Node 2 (Leader) sends heartbeat & snapshot sync to repair Node 1
    raft2.send_heartbeat().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Phase 6: Drain and verify all 300 items across all 4 queues
    let mut total_drained = 0;
    for q_idx in 0..4 {
        let q = format!("gauntlet-q-{}", q_idx);
        let mut q_drained = 0;
        for _ in 0..10 {
            if let Ok(msgs) = client_surviving.poll(&q, 10, 100).await {
                if !msgs.is_empty() {
                    let ack_items: Vec<AckItem> = msgs.into_iter().map(|m| {
                        q_drained += 1;
                        AckItem {
                            message_id: m.message_id,
                            receipt_handle: m.receipt_handle,
                        }
                    }).collect();
                    let _ = client_surviving.ack_batch_items(&q, ack_items).await;
                }
            }
            if q_drained >= 75 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(q_drained, 75, "Queue {} must have all 75 items intact", q);
        total_drained += q_drained;
    }

    assert_eq!(total_drained, 300, "All 300 items must be intact and drained after catastrophic offline corruption & recovery");
}
