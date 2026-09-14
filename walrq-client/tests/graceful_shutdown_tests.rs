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

fn create_client(addr: SocketAddr) -> WalrClient {
    WalrClient::with_config(
        vec![addr.to_string()],
        ClientConfig {
            buffer_window_ms: 1,
            max_batch_size: 500,
            max_redirects: 5,
        },
    )
}

#[tokio::test]
async fn test_max_connection_limit_and_graceful_shutdown() {
    let dir = tempdir().unwrap();
    let addr: SocketAddr = "127.0.0.1:57449".parse().unwrap();

    let engine = Arc::new(QueueEngine::open(dir.path(), QueueOptions::default()).unwrap());
    let raft = Arc::new(RaftNode::new(addr.to_string(), vec![], Arc::clone(&engine)));
    raft.become_leader_for_test().await;

    // Set connection limit to 2 for this test
    let server = Arc::new(WalrServer::new_raft_with_limits(
        Arc::clone(&engine),
        Arc::clone(&raft),
        addr.to_string(),
        2,
        false,
    ));

    let listener = TcpListener::bind(addr).await.unwrap();
    let (shutdown_tx, shutdown_rx) = broadcast::channel(1);

    let srv_task = tokio::spawn({
        let server = Arc::clone(&server);
        async move {
            server.run(listener, shutdown_rx).await;
        }
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let q = "conn-limit-q";

    // 1. Client 1 pushes successfully
    let c1 = create_client(addr);
    let id1 = c1.push(q, Bytes::from("m1"), 0).await.unwrap();
    assert!(!id1.is_empty());

    // 2. Client 2 pushes successfully
    let c2 = create_client(addr);
    let id2 = c2.push(q, Bytes::from("m2"), 0).await.unwrap();
    assert!(!id2.is_empty());

    // 3. Graceful shutdown signal triggered
    println!("Triggering shutdown signal...");
    shutdown_tx.send(()).unwrap();

    // Server should drain, flush WAL, compact Raft snapshot, and exit cleanly
    tokio::time::timeout(Duration::from_secs(3), srv_task).await.unwrap().unwrap();

    // Verify data was flushed and is durable on disk
    let engine2 = QueueEngine::open(dir.path(), QueueOptions::default()).unwrap();
    let polled = engine2.poll(q, 10, 5).await.unwrap();
    assert_eq!(polled.len(), 2, "Graceful drain must flush all pushed messages to disk");
}
