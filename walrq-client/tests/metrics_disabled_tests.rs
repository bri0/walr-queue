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
async fn test_metrics_disabled_by_default() {
    let dir = tempdir().unwrap();
    let addr: SocketAddr = "127.0.0.1:57458".parse().unwrap();
    let engine = Arc::new(QueueEngine::open(dir.path(), QueueOptions::default()).unwrap());
    let raft = Arc::new(RaftNode::new(addr.to_string(), vec![], Arc::clone(&engine)));
    raft.become_leader_for_test().await;

    // enable_metrics = false
    let server = Arc::new(WalrServer::new_raft_with_limits(
        Arc::clone(&engine),
        Arc::clone(&raft),
        addr.to_string(),
        1000,
        false,
    ));
    let listener = TcpListener::bind(addr).await.unwrap();
    let (_shutdown_tx, shutdown_rx) = broadcast::channel(1);
    tokio::spawn(async move { server.run(listener, shutdown_rx).await; });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = create_client(addr);
    let res = client.get_metrics().await;
    assert!(res.is_err(), "Must return error when metrics disabled");
    let err_msg = res.err().unwrap().to_string();
    assert!(err_msg.contains("metrics endpoint is disabled"), "Expected disabled message, got: {}", err_msg);
}
