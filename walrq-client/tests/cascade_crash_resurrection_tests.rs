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
use walrq_client::{ClientConfig, WalrClient};

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

/// Attack: Simultaneous Two-Node Cascade Crash under Active Client Ingestion
/// 1. Cluster running with 3 nodes.
/// 2. Abruptly kill Node 1 (Leader) and Node 2 simultaneously.
/// 3. Verify client requests stall/fail cleanly (no quorum possible with 1/3 node).
/// 4. Revive Node 2.
/// 5. Verify quorum restores (2/3 majority), cluster elects leader, and ingestion recovers without corruption.
#[tokio::test]
async fn test_cascade_crash_and_cluster_resurrection() {
    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();
    let dir3 = tempdir().unwrap();

    let path2 = dir2.path().to_path_buf();

    let addr1: SocketAddr = "127.0.0.1:58711".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:58712".parse().unwrap();
    let addr3: SocketAddr = "127.0.0.1:58713".parse().unwrap();

    let opts = QueueOptions {
        default_visibility_timeout_sec: 10,
        max_delivery_count: 3,
        max_hot_messages_in_ram: 50_000,
        max_wal_segment_size: 16 * 1024 * 1024,
    };

    let engine1 = Arc::new(QueueEngine::open(dir1.path(), opts.clone()).unwrap());
    let engine2 = Arc::new(QueueEngine::open(&path2, opts.clone()).unwrap());
    let engine3 = Arc::new(QueueEngine::open(dir3.path(), opts.clone()).unwrap());

    let raft1 = Arc::new(RaftNode::with_threshold(addr1.to_string(), vec![addr2.to_string(), addr3.to_string()], Arc::clone(&engine1), 1000));
    let raft2 = Arc::new(RaftNode::with_threshold(addr2.to_string(), vec![addr1.to_string(), addr3.to_string()], Arc::clone(&engine2), 1000));
    let raft3 = Arc::new(RaftNode::with_threshold(addr3.to_string(), vec![addr1.to_string(), addr2.to_string()], Arc::clone(&engine3), 1000));

    raft1.become_leader_for_test().await;

    let server1 = Arc::new(WalrServer::new_raft(Arc::clone(&engine1), Arc::clone(&raft1), addr1.to_string()));
    let server2 = Arc::new(WalrServer::new_raft(Arc::clone(&engine2), Arc::clone(&raft2), addr2.to_string()));
    let server3 = Arc::new(WalrServer::new_raft(Arc::clone(&engine3), Arc::clone(&raft3), addr3.to_string()));

    let listener1 = TcpListener::bind(addr1).await.unwrap();
    let listener2 = TcpListener::bind(addr2).await.unwrap();
    let listener3 = TcpListener::bind(addr3).await.unwrap();

    let (tx1, rx1) = broadcast::channel(1);
    let (tx2, rx2) = broadcast::channel(1);
    let (_tx3, rx3) = broadcast::channel(1);

    tokio::spawn(async move { server1.run(listener1, rx1).await; });
    tokio::spawn(async move { server2.run(listener2, rx2).await; });
    tokio::spawn(async move { server3.run(listener3, rx3).await; });

    raft1.send_heartbeat().await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = create_client(vec![addr1, addr2, addr3]);
    let q = "resurrection-q";

    // 1. Push 25 messages to 3-node cluster
    for i in 0..25 {
        client.push(q, Bytes::from(format!("pre-cascade-{}", i)), 0).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(50)).await;

    // 2. Cascade Crash: Abruptly kill Node 1 AND Node 2 simultaneously!
    let _ = tx1.send(());
    let _ = tx2.send(());
    tokio::time::sleep(Duration::from_millis(50)).await;

    // 3. Cluster is now in minority state (1/3 nodes). Pushes should fail or reject without hang.
    let minority_client = create_client(vec![addr3]);
    let fail_push = minority_client.push(q, Bytes::from("should-fail-minority"), 0).await;
    assert!(fail_push.is_err(), "Writes must fail when quorum is impossible (1/3)");

    // 4. Resurrection: Revive Node 2 on the same persisted directory!
    let engine2_revived = Arc::new(QueueEngine::open(&path2, opts.clone()).unwrap());
    let raft2_revived = Arc::new(RaftNode::with_threshold(
        addr2.to_string(),
        vec![addr1.to_string(), addr3.to_string()],
        Arc::clone(&engine2_revived),
        1000,
    ));

    let listener2_revived = TcpListener::bind(addr2).await.unwrap();
    let server2_revived = Arc::new(WalrServer::new_raft(Arc::clone(&engine2_revived), Arc::clone(&raft2_revived), addr2.to_string()));
    let (_tx2_new, rx2_new) = broadcast::channel(1);
    tokio::spawn(async move { server2_revived.run(listener2_revived, rx2_new).await; });

    // Promote Node 3 as leader with Node 2 back online (2/3 quorum restored)
    raft3.recover_as_new_leader().await;
    raft3.send_heartbeat().await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // 5. Ingestion resumes! Client connects to revived cluster (Node 2 + Node 3)
    let restored_client = create_client(vec![addr2, addr3]);
    for i in 25..50 {
        restored_client.push(q, Bytes::from(format!("post-cascade-{}", i)), 0).await.unwrap();
    }

    // 6. Poll and verify all 50 messages
    let mut total_polled = 0;
    for _ in 0..10 {
        let msgs = restored_client.poll(q, 10, 50).await.unwrap();
        total_polled += msgs.len();
        for m in msgs {
            let _ = restored_client.ack(q, &m.message_id, &m.receipt_handle).await;
        }
        if total_polled >= 50 {
            break;
        }
    }
    assert_eq!(total_polled, 50, "All 50 messages must be retrieved after dual-node cascade crash & resurrection");
}
