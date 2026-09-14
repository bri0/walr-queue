use bytes::Bytes;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;
use tokio::net::TcpListener;
use tokio::sync::broadcast;

use walrq::cluster::raft::RaftNode;
use walrq::engine::queue::{QueueEngine, QueueOptions};
use walrq::server::tcp_service::WalrServer;
use walrq_client::{ClientConfig, WalrClient};

#[tokio::test]
async fn test_chaos_cluster_failover() {
    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();

    let addr1: SocketAddr = "127.0.0.1:65051".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:65052".parse().unwrap();

    let engine1 = Arc::new(QueueEngine::open(dir1.path(), QueueOptions::default()).unwrap());
    let engine2 = Arc::new(QueueEngine::open(dir2.path(), QueueOptions::default()).unwrap());

    let raft1 = Arc::new(RaftNode::new(addr1.to_string(), vec![addr2.to_string()], Arc::clone(&engine1)));
    let raft2 = Arc::new(RaftNode::new(addr2.to_string(), vec![addr1.to_string()], Arc::clone(&engine2)));

    raft1.become_leader_for_test().await;

    let server1 = Arc::new(WalrServer::new_raft(engine1, Arc::clone(&raft1), addr1.to_string()));
    let server2 = Arc::new(WalrServer::new_raft(engine2, Arc::clone(&raft2), addr2.to_string()));

    let listener1 = TcpListener::bind(addr1).await.unwrap();
    let listener2 = TcpListener::bind(addr2).await.unwrap();

    let (tx1, rx1) = broadcast::channel(1);
    let (_tx2, rx2) = broadcast::channel(1);

    tokio::spawn(async move {
        server1.run(listener1, rx1).await;
    });
    tokio::spawn(async move {
        server2.run(listener2, rx2).await;
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = WalrClient::new(vec![addr1.to_string(), addr2.to_string()]);

    let id = client.push_immediate("chaos-q", Bytes::from("pre-failover"), 0).await.unwrap();
    assert!(!id.is_empty());

    // Kill Node 1
    let _ = tx1.send(());
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Failover Node 2 to standalone leader
    {
        let mut p = raft2.peers.write().await;
        p.clear();
    }
    raft2.become_leader_for_test().await;

    // Client connects to Node 2 (Node 1 down, auto seed retry)
    let id2 = client.push_immediate("chaos-q", Bytes::from("post-failover"), 0).await.unwrap();
    assert!(!id2.is_empty());
}
