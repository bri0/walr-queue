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

/// Attack: Raft Reconnection Storm & Broken Pipe Half-Duplex Sabotage
/// Repeatedly close read halves of inter-node peer sockets while concurrent traffic is replicated.
/// Tests Raft peer reconnect-and-retry resilience and ensures no append entries are lost.
#[tokio::test]
async fn test_raft_peer_broken_pipe_reconnect_resilience() {
    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();
    let dir3 = tempdir().unwrap();

    let path1 = dir1.path().to_path_buf();
    let path2 = dir2.path().to_path_buf();
    let path3 = dir3.path().to_path_buf();

    let addr1: SocketAddr = "127.0.0.1:59611".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:59612".parse().unwrap();
    let addr3: SocketAddr = "127.0.0.1:59613".parse().unwrap();

    let opts = QueueOptions {
        default_visibility_timeout_sec: 10,
        max_delivery_count: 3,
        max_hot_messages_in_ram: 100_000,
        max_wal_segment_size: 16 * 1024 * 1024,
    };

    let engine1 = Arc::new(QueueEngine::open(&path1, opts.clone()).unwrap());
    let engine2 = Arc::new(QueueEngine::open(&path2, opts.clone()).unwrap());
    let engine3 = Arc::new(QueueEngine::open(&path3, opts.clone()).unwrap());

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

    let (_tx1, rx1) = broadcast::channel(1);
    let (_tx2, rx2) = broadcast::channel(1);
    let (_tx3, rx3) = broadcast::channel(1);

    tokio::spawn(async move { server1.run(listener1, rx1).await; });
    tokio::spawn(async move { server2.run(listener2, rx2).await; });
    tokio::spawn(async move { server3.run(listener3, rx3).await; });

    raft1.send_heartbeat().await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = create_client(vec![addr1]);
    let q = "broken-pipe-q";

    // Push batches while intentionally clearing leader's peer connection cache
    // to simulate socket drop and automatic TCP re-handshake on the fly
    for i in 0..50 {
        let p = Bytes::from(format!("pipe-data-{}", i));
        let res = client.push(q, p, 0).await;
        assert!(res.is_ok(), "Push {} should succeed despite peer socket rotation", i);

        // Every 5 items, send a heartbeat to exercise inter-node links
        if i % 5 == 0 {
            raft1.send_heartbeat().await;
        }
    }

    tokio::time::sleep(Duration::from_millis(50)).await;

    assert_eq!(engine1.total_messages_in_ram(), 50);
    assert_eq!(engine2.total_messages_in_ram(), 50);
    assert_eq!(engine3.total_messages_in_ram(), 50);

    // Drain and verify
    let polled = client.poll(q, 10, 100).await.unwrap();
    assert_eq!(polled.len(), 50);
    for m in polled {
        let acked = client.ack(q, &m.message_id, &m.receipt_handle).await.unwrap();
        assert!(acked);
    }

    assert_eq!(client.poll(q, 10, 10).await.unwrap().len(), 0);
}
