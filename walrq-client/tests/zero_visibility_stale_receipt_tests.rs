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

/// Attack: Zero Visibility Lease and Stale Receipt Storm
/// 1. Poll with visibility_timeout_sec = 0 (triggers default visibility timeout).
/// 2. Attempt to ack with expired/stale receipts while visibility expires instantly.
/// 3. Concurrent pollers continuously compete to claim the same rapidly-expiring message.
/// 4. Verify no message is acknowledged twice, timer wheel does not corrupt, and delivery counts strictly advance.
#[tokio::test]
async fn test_zero_visibility_and_stale_receipt_storm() {
    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();
    let dir3 = tempdir().unwrap();

    let addr1: SocketAddr = "127.0.0.1:59711".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:59712".parse().unwrap();
    let addr3: SocketAddr = "127.0.0.1:59713".parse().unwrap();

    // Set default visibility timeout to 1 second, max delivery count to 4
    let opts = QueueOptions {
        default_visibility_timeout_sec: 1,
        max_delivery_count: 4,
        max_hot_messages_in_ram: 10_000,
        max_wal_segment_size: 16 * 1024 * 1024,
    };

    let engine1 = Arc::new(QueueEngine::open(dir1.path(), opts.clone()).unwrap());
    let engine2 = Arc::new(QueueEngine::open(dir2.path(), opts.clone()).unwrap());
    let engine3 = Arc::new(QueueEngine::open(dir3.path(), opts.clone()).unwrap());

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
    let (_tx2, rx2) = broadcast::channel(1);
    let (_tx3, rx3) = broadcast::channel(1);

    tokio::spawn(async move { server1.run(listener1, rx1).await; });
    tokio::spawn(async move { server2.run(listener2, rx2).await; });
    tokio::spawn(async move { server3.run(listener3, rx3).await; });

    raft1.send_heartbeat().await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = create_client(vec![addr1, addr2, addr3]);
    let q = "stale-receipt-storm-q";

    // Push 10 messages
    let mut pushed_ids = Vec::new();
    for i in 0..10 {
        let id = client.push(q, Bytes::from(format!("storm-item-{}", i)), 0).await.unwrap();
        pushed_ids.push(id);
    }

    // Step 1: Poll with visibility_timeout_sec = 0 (falls back to default 1s lease)
    let p1 = client.poll(q, 0, 10).await.unwrap();
    assert_eq!(p1.len(), 10);
    assert_eq!(p1[0].delivery_count, 1);

    let old_receipts: Vec<(String, String)> = p1.into_iter().map(|m| (m.message_id, m.receipt_handle)).collect();

    // Step 2: Sleep 1.2s to let the 1s visibility lease expire
    tokio::time::sleep(Duration::from_millis(1200)).await;

    // Step 3: Concurrently attack with old receipts while a new poller claims the messages
    let client_attacker = create_client(vec![addr1]);
    let old_receipts_clone = old_receipts.clone();
    let q_name = q.to_string();

    let attack_handle = tokio::spawn(async move {
        let mut rejected = 0;
        for (m_id, r_handle) in old_receipts_clone {
            // Attempting to ack with expired receipt
            let ack_res = client_attacker.ack(&q_name, &m_id, &r_handle).await.unwrap();
            if !ack_res {
                rejected += 1;
            }
        }
        rejected
    });

    // Simultaneously, valid poller claims delivery 2
    let p2 = client.poll(q, 1, 10).await.unwrap();
    assert_eq!(p2.len(), 10, "All 10 messages must re-appear for delivery 2");
    assert_eq!(p2[0].delivery_count, 2);

    let rejected_count = attack_handle.await.unwrap();
    assert_eq!(rejected_count, 10, "All 10 stale receipts must be strictly rejected");

    // Step 4: Valid poller acknowledges with the fresh delivery 2 receipts
    for m in p2 {
        let ok = client.ack(q, &m.message_id, &m.receipt_handle).await.unwrap();
        assert!(ok, "Fresh delivery 2 receipt must successfully acknowledge");
    }

    // Step 5: Verify queue is completely empty
    let empty_poll = client.poll(q, 10, 10).await.unwrap();
    assert!(empty_poll.is_empty(), "Queue must be empty after valid acks");

    // Step 6: Total messages in RAM across all 3 nodes must be 0
    assert_eq!(engine1.total_messages_in_ram(), 0);
    assert_eq!(engine2.total_messages_in_ram(), 0);
    assert_eq!(engine3.total_messages_in_ram(), 0);

    let _ = tx1.send(());
}
