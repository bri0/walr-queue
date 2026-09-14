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

/// Attack: Queue Namespace Chaos & Massive Dynamic Queues Explosion
/// Ingests messages across 200 dynamically generated queue namespaces simultaneously,
/// interleaving pushes, polls, and acks across all queues to stress the QueueRegistry manifest
/// and lock-striped shard partition mapping under high concurrency.
#[tokio::test]
async fn test_massive_queue_namespace_explosion_and_recovery() {
    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();
    let dir3 = tempdir().unwrap();

    let path1 = dir1.path().to_path_buf();
    let path2 = dir2.path().to_path_buf();
    let path3 = dir3.path().to_path_buf();

    let addr1: SocketAddr = "127.0.0.1:59111".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:59112".parse().unwrap();
    let addr3: SocketAddr = "127.0.0.1:59113".parse().unwrap();

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

    let (tx1, rx1) = broadcast::channel(1);
    let (tx2, rx2) = broadcast::channel(1);
    let (tx3, rx3) = broadcast::channel(1);

    tokio::spawn(async move { server1.run(listener1, rx1).await; });
    tokio::spawn(async move { server2.run(listener2, rx2).await; });
    tokio::spawn(async move { server3.run(listener3, rx3).await; });

    raft1.send_heartbeat().await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = Arc::new(create_client(vec![addr1, addr2, addr3]));

    // Phase 1: Concurrently push 5 items into 200 distinct queue namespaces (1,000 messages total)
    let num_queues = 200;
    let mut tasks = Vec::new();
    for q_id in 0..num_queues {
        let cl = Arc::clone(&client);
        let q_name = format!("namespace-q-{:03}", q_id);
        tasks.push(tokio::spawn(async move {
            for i in 0..5 {
                cl.push(&q_name, Bytes::from(format!("item-{}-{}", q_id, i)), 0).await.unwrap();
            }
        }));
    }

    for t in tasks {
        t.await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    assert_eq!(engine1.total_messages_in_ram(), 1000);
    assert_eq!(engine2.total_messages_in_ram(), 1000);
    assert_eq!(engine3.total_messages_in_ram(), 1000);

    // Phase 2: Kill all 3 nodes cleanly and reboot to verify full recovery of all 200 queue manifests!
    let _ = tx1.send(());
    let _ = tx2.send(());
    let _ = tx3.send(());
    tokio::time::sleep(Duration::from_millis(50)).await;

    let engine1_reboot = Arc::new(QueueEngine::open(&path1, opts.clone()).unwrap());
    let engine2_reboot = Arc::new(QueueEngine::open(&path2, opts.clone()).unwrap());
    let engine3_reboot = Arc::new(QueueEngine::open(&path3, opts.clone()).unwrap());

    assert_eq!(engine1_reboot.total_messages_in_ram(), 1000, "Node 1 must recover all 1000 messages across 200 queues");
    assert_eq!(engine2_reboot.total_messages_in_ram(), 1000, "Node 2 must recover all 1000 messages across 200 queues");
    assert_eq!(engine3_reboot.total_messages_in_ram(), 1000, "Node 3 must recover all 1000 messages across 200 queues");

    let raft1_reboot = Arc::new(RaftNode::with_threshold(addr1.to_string(), vec![addr2.to_string(), addr3.to_string()], Arc::clone(&engine1_reboot), 1000));
    raft1_reboot.become_leader_for_test().await;

    let server1_reboot = Arc::new(WalrServer::new_raft(Arc::clone(&engine1_reboot), Arc::clone(&raft1_reboot), addr1.to_string()));
    let listener1_reboot = TcpListener::bind(addr1).await.unwrap();
    let (_tx1_new, rx1_new) = broadcast::channel(1);
    tokio::spawn(async move { server1_reboot.run(listener1_reboot, rx1_new).await; });

    tokio::time::sleep(Duration::from_millis(50)).await;
    let client_reboot = Arc::new(create_client(vec![addr1]));

    // Phase 3: Concurrently poll and acknowledge all 200 queues
    let mut poll_tasks = Vec::new();
    for q_id in 0..num_queues {
        let cl = Arc::clone(&client_reboot);
        let q_name = format!("namespace-q-{:03}", q_id);
        poll_tasks.push(tokio::spawn(async move {
            let msgs = cl.poll(&q_name, 10, 10).await.unwrap();
            assert_eq!(msgs.len(), 5, "Queue {} must contain exactly 5 messages", q_name);
            for m in msgs {
                let acked = cl.ack(&q_name, &m.message_id, &m.receipt_handle).await.unwrap();
                assert!(acked);
            }
        }));
    }

    for t in poll_tasks {
        t.await.unwrap();
    }

    // Phase 4: Verify all 200 queues are now completely empty
    for q_id in 0..num_queues {
        let q_name = format!("namespace-q-{:03}", q_id);
        let empty = client_reboot.poll(&q_name, 10, 10).await.unwrap();
        assert!(empty.is_empty(), "Queue {} must be fully drained", q_name);
    }
}
