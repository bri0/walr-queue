use bytes::Bytes;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::tempdir;
use tokio::net::TcpListener;
use tokio::sync::broadcast;

use walrq::cluster::raft::RaftNode;
use walrq::engine::queue::{QueueEngine, QueueOptions};
use walrq::server::tcp_service::WalrServer;
use walrq_client::{ClientConfig, WalrClient};

#[tokio::test]
async fn test_client_side_300ms_window_buffering_and_batch_dispatch() {
    let dir = tempdir().unwrap();
    let addr: SocketAddr = "127.0.0.1:61051".parse().unwrap();

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
        buffer_window_ms: 300,
        max_batch_size: 500,
        max_redirects: 5,
    };
    let client = WalrClient::with_config(vec![addr.to_string()], config);

    let t0 = Instant::now();

    let mut handles = Vec::new();
    for i in 0..10 {
        let cl = client.clone();
        handles.push(tokio::spawn(async move {
            cl.push(
                "buffered-queue",
                Bytes::from(format!("buffered-msg-{}", i)),
                0,
            )
            .await
        }));
    }

    let mut msg_ids = Vec::new();
    for h in handles {
        let res = h.await.unwrap();
        assert!(res.is_ok(), "Push should succeed via micro-batcher");
        msg_ids.push(res.unwrap());
    }

    let elapsed = t0.elapsed();
    assert!(
        elapsed >= Duration::from_millis(250),
        "Batch window did not buffer: completed in {:?}",
        elapsed
    );

    assert_eq!(msg_ids.len(), 10);
    for id in &msg_ids {
        assert!(!id.is_empty());
    }

    let polled = client.poll("buffered-queue", 30, 20).await.unwrap();
    assert_eq!(polled.len(), 10);
}

#[tokio::test]
async fn test_client_max_batch_size_immediate_flush() {
    let dir = tempdir().unwrap();
    let addr: SocketAddr = "127.0.0.1:61052".parse().unwrap();

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
        buffer_window_ms: 5000,
        max_batch_size: 50,
        max_redirects: 5,
    };
    let client = WalrClient::with_config(vec![addr.to_string()], config);

    let t0 = Instant::now();
    let mut handles = Vec::new();
    for i in 0..50 {
        let cl = client.clone();
        handles.push(tokio::spawn(async move {
            cl.push(
                "flush-fast-queue",
                Bytes::from(format!("msg-{}", i)),
                0,
            )
            .await
        }));
    }

    for h in handles {
        assert!(h.await.unwrap().is_ok());
    }

    let elapsed = t0.elapsed();
    assert!(
        elapsed < Duration::from_millis(1500),
        "Batch threshold of 50 did not trigger instant flush! Elapsed: {:?}",
        elapsed
    );
}
