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

/// Attack: Active Log Segment Rotation Flapping Under Continuous Ingestion
/// Set max_wal_segment_size to an ultra-small threshold (256 bytes) so that EVERY batch
/// or single push forces an active segment flip-flop rotation (wal_a <-> wal_b) and
/// hole-reclamation compaction under continuous load across all 3 nodes.
#[tokio::test]
async fn test_extreme_wal_segment_flipflop_flapping() {
    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();
    let dir3 = tempdir().unwrap();

    let path1 = dir1.path().to_path_buf();
    let path2 = dir2.path().to_path_buf();
    let path3 = dir3.path().to_path_buf();

    let addr1: SocketAddr = "127.0.0.1:59311".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:59312".parse().unwrap();
    let addr3: SocketAddr = "127.0.0.1:59313".parse().unwrap();

    // 256-byte ultra-small segment size: triggers instant WAL rotation on almost every write!
    let opts = QueueOptions {
        default_visibility_timeout_sec: 10,
        max_delivery_count: 3,
        max_hot_messages_in_ram: 10_000,
        max_wal_segment_size: 256,
    };

    let engine1 = Arc::new(QueueEngine::open(&path1, opts.clone()).unwrap());
    let engine2 = Arc::new(QueueEngine::open(&path2, opts.clone()).unwrap());
    let engine3 = Arc::new(QueueEngine::open(&path3, opts.clone()).unwrap());

    let raft1 = Arc::new(RaftNode::with_threshold(addr1.to_string(), vec![addr2.to_string(), addr3.to_string()], Arc::clone(&engine1), 10));
    let raft2 = Arc::new(RaftNode::with_threshold(addr2.to_string(), vec![addr1.to_string(), addr3.to_string()], Arc::clone(&engine2), 10));
    let raft3 = Arc::new(RaftNode::with_threshold(addr3.to_string(), vec![addr1.to_string(), addr2.to_string()], Arc::clone(&engine3), 10));

    raft1.become_leader_for_test().await;

    let server1 = Arc::new(WalrServer::new_raft(Arc::clone(&engine1), Arc::clone(&raft1), addr1.to_string()));
    let server2 = Arc::new(WalrServer::new_raft(Arc::clone(&engine2), Arc::clone(&raft2), addr2.to_string()));
    let server3 = Arc::new(WalrServer::new_raft(Arc::clone(&engine3), Arc::clone(&raft3), addr3.to_string()));

    let listener1 = TcpListener::bind(addr1).await.unwrap();
    let listener2 = TcpListener::bind(addr2).await.unwrap();
    let listener3 = TcpListener::bind(addr3).await.unwrap();

    let (tx1, rx1) = broadcast::channel(1);
    let (tx2, rx2) = broadcast::channel(1);
    let (tx3, rx3) = broadcast::channel(1);

    tokio::spawn(async move { server1.run(listener1, rx1).await; });
    tokio::spawn(async move { server2.run(listener2, rx2).await; });
    tokio::spawn(async move { server3.run(listener3, rx3).await; });

    raft1.send_heartbeat().await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = create_client(vec![addr1, addr2, addr3]);
    let q = "flapping-wal-q";

    // 1. Ingest 100 items with interleaved acks to constantly trigger rotation + compaction
    let mut pushed_ids = Vec::new();
    for i in 0..100 {
        let id = client.push(q, Bytes::from(format!("flapping-wal-data-item-{:04}", i)), 0).await.unwrap();
        pushed_ids.push(id);

        // Every 5 pushes, ack an earlier message to leave holes and trigger active/inactive compaction flips
        if i >= 10 && i % 5 == 0 {
            let target_idx = i - 10;
            if let Ok(msgs) = client.poll(q, 10, 1).await {
                if !msgs.is_empty() {
                    let _ = client.ack(q, &msgs[0].message_id, &msgs[0].receipt_handle).await;
                }
            }
        }
    }

    tokio::time::sleep(Duration::from_millis(100)).await;

    // 2. Kill all nodes cleanly and reboot to verify on-disk integrity across rapid flip-flop rotations
    let _ = tx1.send(());
    let _ = tx2.send(());
    let _ = tx3.send(());
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Reboot engine instances from the flapping disk state
    let engine1_reboot = Arc::new(QueueEngine::open(&path1, opts.clone()).unwrap());
    let engine2_reboot = Arc::new(QueueEngine::open(&path2, opts.clone()).unwrap());
    let engine3_reboot = Arc::new(QueueEngine::open(&path3, opts.clone()).unwrap());

    let c1 = engine1_reboot.total_messages_in_ram();
    let c2 = engine2_reboot.total_messages_in_ram();
    let c3 = engine3_reboot.total_messages_in_ram();

    assert_eq!(c1, c2, "Rebooted Node 1 and Node 2 must match message count");
    assert_eq!(c1, c3, "Rebooted Node 1 and Node 3 must match message count");
    assert!(c1 > 70 && c1 <= 100, "Must recover remaining non-acked messages safely: got {}", c1);

    // Drain all surviving messages to verify zero payload corruption
    let raft1_reboot = Arc::new(RaftNode::with_threshold(addr1.to_string(), vec![addr2.to_string(), addr3.to_string()], Arc::clone(&engine1_reboot), 10));
    raft1_reboot.become_leader_for_test().await;

    let server1_reboot = Arc::new(WalrServer::new_raft(Arc::clone(&engine1_reboot), Arc::clone(&raft1_reboot), addr1.to_string()));
    let listener1_reboot = TcpListener::bind(addr1).await.unwrap();
    let (_t1, r1) = broadcast::channel(1);
    tokio::spawn(async move { server1_reboot.run(listener1_reboot, r1).await; });

    let client_reboot = create_client(vec![addr1]);
    let mut total_drained = 0;
    while total_drained < c1 {
        let msgs = client_reboot.poll(q, 10, 50).await.unwrap();
        if msgs.is_empty() {
            break;
        }
        for m in msgs {
            assert!(m.payload.starts_with(b"flapping-wal-data-item-"), "Payload corruption detected in recovered WAL");
            let acked = client_reboot.ack(q, &m.message_id, &m.receipt_handle).await.unwrap();
            assert!(acked);
            total_drained += 1;
        }
    }

    assert_eq!(total_drained, c1, "All surviving messages must be drained without corruption");
}
