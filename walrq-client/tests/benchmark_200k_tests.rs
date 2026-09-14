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
async fn test_benchmark_200k_push_poll_throughput() {
    let dir = tempdir().unwrap();
    let addr: SocketAddr = "127.0.0.1:63051".parse().unwrap();

    let engine = Arc::new(QueueEngine::open(dir.path(), QueueOptions::default()).unwrap());
    let raft = Arc::new(RaftNode::new(addr.to_string(), vec![], Arc::clone(&engine)));
    raft.become_leader_for_test().await;

    let server = Arc::new(WalrServer::new_raft(engine, raft, addr.to_string()));
    let listener = TcpListener::bind(addr).await.unwrap();
    let (_tx, rx) = broadcast::channel(1);

    tokio::spawn(async move {
        server.run(listener, rx).await;
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let config = ClientConfig {
        buffer_window_ms: 20,
        max_batch_size: 500,
        max_redirects: 5,
    };
    let client = WalrClient::with_config(vec![addr.to_string()], config);

    let total_messages = 5_000;
    let mut batch = Vec::with_capacity(500);
    for i in 0..500 {
        batch.push(Bytes::from(format!("payload-bench-{}", i)));
    }

    let t0 = std::time::Instant::now();
    for _ in 0..(total_messages / 500) {
        let res = client.push_batch("bench-200k-q", batch.clone()).await.unwrap();
        assert_eq!(res.len(), 500);
    }
    let push_duration = t0.elapsed();
    println!("Pushed {} messages in {:?}", total_messages, push_duration);

    let mut polled_count = 0;
    while polled_count < total_messages {
        let msgs = client.poll("bench-200k-q", 30, 500).await.unwrap();
        polled_count += msgs.len();
        if msgs.is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    assert_eq!(polled_count, total_messages);
}
