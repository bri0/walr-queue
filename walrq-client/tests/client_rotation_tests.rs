use bytes::Bytes;
use std::net::SocketAddr;
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
async fn test_client_leader_redirect() {
    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();

    let addr1: SocketAddr = "127.0.0.1:62051".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:62052".parse().unwrap();

    let engine1 = Arc::new(QueueEngine::open(dir1.path(), QueueOptions::default()).unwrap());
    let engine2 = Arc::new(QueueEngine::open(dir2.path(), QueueOptions::default()).unwrap());

    let raft1 = Arc::new(RaftNode::new(addr1.to_string(), vec![addr2.to_string()], Arc::clone(&engine1)));
    let raft2 = Arc::new(RaftNode::new(addr2.to_string(), vec![addr1.to_string()], Arc::clone(&engine2)));

    // Node 2 is the leader, Node 1 points to Node 2 as leader
    raft2.become_leader_for_test().await;
    {
        let mut l1 = raft1.current_leader.write().await;
        *l1 = Some(addr2.to_string());
    }

    let server1 = Arc::new(WalrServer::new_raft(engine1, raft1, addr1.to_string()));
    let server2 = Arc::new(WalrServer::new_raft(engine2, raft2, addr2.to_string()));

    let listener1 = TcpListener::bind(addr1).await.unwrap();
    let listener2 = TcpListener::bind(addr2).await.unwrap();

    let (_tx1, rx1) = broadcast::channel(1);
    let (_tx2, rx2) = broadcast::channel(1);

    tokio::spawn(async move {
        server1.run(listener1, rx1).await;
    });
    tokio::spawn(async move {
        server2.run(listener2, rx2).await;
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Connect client pointing only to Node 1 initially (follower)
    let config = ClientConfig {
        buffer_window_ms: 10,
        max_batch_size: 10,
        max_redirects: 5,
    };
    let client = WalrClient::with_config(vec![addr1.to_string()], config);

    // Client should get redirected to Node 2 and succeed!
    let msg_id = client.push_immediate("rot-q", Bytes::from("rot-data"), 0).await.unwrap();
    assert!(!msg_id.is_empty());

    let msgs = client.poll("rot-q", 30, 5).await.unwrap();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].message_id, msg_id);
}
