use bytes::Bytes;
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;
use tokio::net::TcpListener;
use tokio::sync::broadcast;

use walrq::cluster::raft::RaftNode;
use walrq::engine::queue::{QueueEngine, QueueOptions};
use walrq::server::tcp_service::WalrServer;
use walrq_client::WalrClient;

#[tokio::test]
async fn test_raft_snapshot_compaction_and_bounded_memory() {
    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();

    let addr1: SocketAddr = "127.0.0.1:49351".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:49352".parse().unwrap();

    let engine1 = Arc::new(QueueEngine::open(dir1.path(), QueueOptions::default()).unwrap());
    let engine2 = Arc::new(QueueEngine::open(dir2.path(), QueueOptions::default()).unwrap());

    let raft1 = Arc::new(RaftNode::new(addr1.to_string(), vec![addr2.to_string()], Arc::clone(&engine1)));
    let raft2 = Arc::new(RaftNode::new(addr2.to_string(), vec![addr1.to_string()], Arc::clone(&engine2)));

    raft1.become_leader_for_test().await;

    let server1 = Arc::new(WalrServer::new_raft(engine1, Arc::clone(&raft1), addr1.to_string()));
    let server2 = Arc::new(WalrServer::new_raft(engine2, Arc::clone(&raft2), addr2.to_string()));

    let listener1 = TcpListener::bind(addr1).await.unwrap();
    let listener2 = TcpListener::bind(addr2).await.unwrap();

    let (_tx1, rx1) = broadcast::channel(1);
    let (_tx2, rx2) = broadcast::channel(1);

    tokio::spawn(async move { server1.run(listener1, rx1).await; });
    tokio::spawn(async move { server2.run(listener2, rx2).await; });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = WalrClient::new(vec![addr1.to_string()]);

    // 1. Push 2,000 items
    let mut pushed_ids = Vec::new();
    for i in 0..2_000 {
        let id = client.push_immediate("snap-q", Bytes::from(format!("payload-{}", i)), 0).await.unwrap();
        pushed_ids.push(id);
    }
    assert_eq!(raft1.log.read().await.len(), 2_000);

    // 2. Ack first 1,000 items
    let polled = client.poll("snap-q", 30, 1000).await.unwrap();
    assert_eq!(polled.len(), 1000);
    for m in polled {
        client.ack("snap-q", &m.message_id, &m.receipt_handle).await.unwrap();
    }

    // 3. Trigger snapshot compaction on Leader
    raft1.compact_log_snapshot().await;

    // In-memory log on leader is truncated!
    let remaining_log_len = raft1.log.read().await.len();
    assert!(
        remaining_log_len < 100,
        "Log should be truncated after snapshot compaction, was: {}",
        remaining_log_len
    );
    assert!(raft1.snapshot_index.load(Ordering::SeqCst) >= 2000);

    // 4. Spin up blank replacement node and sync via snapshot
    let dir3 = tempdir().unwrap();
    let addr3: SocketAddr = "127.0.0.1:49353".parse().unwrap();
    let engine3 = Arc::new(QueueEngine::open(dir3.path(), QueueOptions::default()).unwrap());
    let raft3 = Arc::new(RaftNode::new(addr3.to_string(), vec![addr1.to_string()], Arc::clone(&engine3)));

    let server3 = Arc::new(WalrServer::new_raft(engine3, Arc::clone(&raft3), addr3.to_string()));
    let listener3 = TcpListener::bind(addr3).await.unwrap();
    let (_tx3, rx3) = broadcast::channel(1);
    tokio::spawn(async move { server3.run(listener3, rx3).await; });

    raft1.add_peer(&addr3.to_string()).await;
    raft1.sync_all_entries_to_peer(&addr3.to_string()).await;

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Node 3 now holds exactly the 1,000 surviving unacked messages from the snapshot
    raft3.become_leader_for_test().await;
    let client3 = WalrClient::new(vec![addr3.to_string()]);

    let remaining_msgs = client3.poll("snap-q", 30, 1000).await.unwrap();
    eprintln!("DEBUG: Node 3 recovered {} remaining messages (expected 1000)", remaining_msgs.len());
    assert_eq!(remaining_msgs.len(), 1000, "Node 3 should recover exactly 1,000 unacked messages from snapshot");
}
