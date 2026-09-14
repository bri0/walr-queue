use bytes::Bytes;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use walrq::cluster::raft::RaftNode;
use walrq::engine::queue::{QueueEngine, QueueOptions};
use walrq::server::tcp_service::WalrServer;
use walrq_client::{AckItem, ClientConfig, WalrClient};

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_rolling_leader_election_with_in_flight_chaos() {
    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();
    let dir3 = tempdir().unwrap();

    let l1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let l2 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let l3 = TcpListener::bind("127.0.0.1:0").await.unwrap();

    let a1 = l1.local_addr().unwrap().to_string();
    let a2 = l2.local_addr().unwrap().to_string();
    let a3 = l3.local_addr().unwrap().to_string();

    let peers = vec![a1.clone(), a2.clone(), a3.clone()];

    let opts = QueueOptions {
        default_visibility_timeout_sec: 2,
        max_delivery_count: 3,
        max_hot_messages_in_ram: 50,
        max_wal_segment_size: 16 * 1024 * 1024,
    };

    let eng1 = Arc::new(QueueEngine::open(dir1.path(), opts.clone()).unwrap());
    let eng2 = Arc::new(QueueEngine::open(dir2.path(), opts.clone()).unwrap());
    let eng3 = Arc::new(QueueEngine::open(dir3.path(), opts.clone()).unwrap());

    let raft1 = Arc::new(RaftNode::new(a1.clone(), peers.clone(), Arc::clone(&eng1)));
    let raft2 = Arc::new(RaftNode::new(a2.clone(), peers.clone(), Arc::clone(&eng2)));
    let raft3 = Arc::new(RaftNode::new(a3.clone(), peers.clone(), Arc::clone(&eng3)));

    let srv1 = Arc::new(WalrServer::new_raft(Arc::clone(&eng1), Arc::clone(&raft1), a1.clone()));
    let srv2 = Arc::new(WalrServer::new_raft(Arc::clone(&eng2), Arc::clone(&raft2), a2.clone()));
    let srv3 = Arc::new(WalrServer::new_raft(Arc::clone(&eng3), Arc::clone(&raft3), a3.clone()));

    let (tx1, rx1) = broadcast::channel(1);
    let (tx2, rx2) = broadcast::channel(1);
    let (tx3, rx3) = broadcast::channel(1);

    tokio::spawn(async move { srv1.run(l1, rx1).await; });
    tokio::spawn(async move { srv2.run(l2, rx2).await; });
    tokio::spawn(async move { srv3.run(l3, rx3).await; });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Node 1 becomes initial leader
    raft1.become_leader_for_test().await;

    let client = WalrClient::with_config(
        peers.clone(),
        ClientConfig {
            buffer_window_ms: 1,
            max_batch_size: 20,
            max_redirects: 5,
        },
    );

    let q = "rolling_leader_q";

    // 1. Push 200 items into Node 1
    let mut batch = Vec::new();
    for i in 0..200 {
        batch.push(Bytes::from(format!("raft-failover-item-{:04}", i)));
    }
    client.push_batch(q, batch).await.unwrap();

    // 2. Poll 50 items from Node 1 -> in-flight
    let polled = client.poll(q, 2, 50).await.unwrap();
    assert_eq!(polled.len(), 50);

    // 3. Simulate sudden leader crash: Kill Node 1
    let _ = tx1.send(());
    tokio::time::sleep(Duration::from_millis(50)).await;

    // 4. Node 2 is elected as new leader
    raft2.become_leader_for_test().await;

    // 5. Client interacts with cluster via redirects/failover
    // Try to ACK from previous leader on Node 2 -> handled safely
    for m in &polled {
        let _ = client.ack(q, &m.message_id, &m.receipt_handle).await;
    }

    // 6. Push additional 100 items to new leader
    let mut batch2 = Vec::new();
    for i in 200..300 {
        batch2.push(Bytes::from(format!("raft-failover-item-{:04}", i)));
    }
    client.push_batch(q, batch2).await.unwrap();

    // 7. Let leases expire on unacked messages from Node 1
    tokio::time::sleep(Duration::from_millis(2200)).await;

    // 8. Drain all available messages from new leader
    let mut drained = 0;
    for _ in 0..20 {
        let msgs = client.poll(q, 2, 50).await.unwrap_or_default();
        if msgs.is_empty() {
            tokio::time::sleep(Duration::from_millis(50)).await;
            continue;
        }
        drained += msgs.len();
        let acks: Vec<AckItem> = msgs.into_iter().map(|m| AckItem { message_id: m.message_id, receipt_handle: m.receipt_handle }).collect();
        let _ = client.ack_batch_items(q, acks).await;
    }

    println!("Drained from new leader = {}", drained);
    assert!(drained >= 100, "New leader must successfully serve client traffic post-failover");

    let _ = tx2.send(());
    let _ = tx3.send(());
}
