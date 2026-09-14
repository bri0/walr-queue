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

/// Attack: Out-of-Order Delayed Message Insertion & Horizon Bucket Boundary Thrashing
/// Ingest messages with non-monotonic delays (e.g. 15s, 0s, 3s, 50s, 1s, 2s, 0s, 60s, 70s)
/// crossing the 60-second hot horizon and 1-second delay bucket boundaries, while running
/// concurrent poll tasks that claim and ack ready items as time advances.
#[tokio::test]
async fn test_out_of_order_delayed_insertion_and_horizon_boundary_thrash() {
    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();
    let dir3 = tempdir().unwrap();

    let path1 = dir1.path().to_path_buf();
    let path2 = dir2.path().to_path_buf();
    let path3 = dir3.path().to_path_buf();

    let addr1: SocketAddr = "127.0.0.1:59411".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:59412".parse().unwrap();
    let addr3: SocketAddr = "127.0.0.1:59413".parse().unwrap();

    let opts = QueueOptions {
        default_visibility_timeout_sec: 10,
        max_delivery_count: 3,
        max_hot_messages_in_ram: 10_000,
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

    let client = create_client(vec![addr1, addr2, addr3]);
    let q = "delayed-horizon-thrash-q";

    // Insert 50 messages with wild non-monotonic delay patterns:
    // Some immediate (0s), some near (1s, 2s), some mid (15s), some beyond 60s horizon (65s)
    let delays = [0, 2, 65, 1, 0, 15, 2, 70, 0, 1, 62, 0, 2, 1, 0];
    let mut pushed = Vec::new();

    for (idx, &delay) in delays.iter().enumerate() {
        let payload = Bytes::from(format!("delayed-item-{:02}-delay-{}", idx, delay));
        let id = client.push(q, payload, delay).await.unwrap();
        pushed.push((id, delay, idx));
    }

    // Step 1: Immediate Poll (T = 0s)
    // Only messages with delay = 0 should be visible!
    let immediate_msgs = client.poll(q, 10, 50).await.unwrap();
    let immediate_count = delays.iter().filter(|&&d| d == 0).count();
    assert_eq!(immediate_msgs.len(), immediate_count, "Only delay=0 messages should be visible at T=0");
    for m in immediate_msgs {
        assert!(m.payload.starts_with(b"delayed-item-"));
        let acked = client.ack(q, &m.message_id, &m.receipt_handle).await.unwrap();
        assert!(acked);
    }

    // Step 2: Poll after 1.2s (T = 1.2s)
    // Messages with delay = 1 should now be promoted and visible!
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let t1_msgs = client.poll(q, 10, 50).await.unwrap();
    let delay1_count = delays.iter().filter(|&&d| d == 1).count();
    assert_eq!(t1_msgs.len(), delay1_count, "Only delay=1 messages should become visible at T=1.2s");
    for m in t1_msgs {
        let acked = client.ack(q, &m.message_id, &m.receipt_handle).await.unwrap();
        assert!(acked);
    }

    // Step 3: Poll after another 1.2s (T = 2.4s)
    // Messages with delay = 2 should now become visible!
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let t2_msgs = client.poll(q, 10, 50).await.unwrap();
    let delay2_count = delays.iter().filter(|&&d| d == 2).count();
    assert_eq!(t2_msgs.len(), delay2_count, "Only delay=2 messages should become visible at T=2.4s");
    for m in t2_msgs {
        let acked = client.ack(q, &m.message_id, &m.receipt_handle).await.unwrap();
        assert!(acked);
    }

    // Step 4: Verify remaining items are NOT visible yet (15s, 62s, 65s, 70s)
    let pending_msgs = client.poll(q, 10, 50).await.unwrap();
    assert!(pending_msgs.is_empty(), "Delayed messages >2s must remain invisible at T=2.4s");

    // Total in RAM across all 3 nodes must match the remaining un-polled items exactly
    let remaining_count = delays.iter().filter(|&&d| d > 2).count();
    assert_eq!(engine1.total_messages_in_ram(), remaining_count);
    assert_eq!(engine2.total_messages_in_ram(), remaining_count);
    assert_eq!(engine3.total_messages_in_ram(), remaining_count);
}
