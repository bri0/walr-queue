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
use walrq_client::{AckItem, ClientConfig, WalrClient};

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

/// Verification Test for Visibility-Based Disk Spilling (5-Minute Horizon & 1-Minute Buckets):
/// 1. Configure queue with RAM cap = 500.
/// 2. Push 1,000 messages with delay = 600s (10 minutes, beyond 5-minute horizon) -> ALL SPILL TO DISK.
///    - RAM usage must be ZERO for these messages!
/// 3. Push 300 messages with delay = 120s (2 minutes, inside 5-minute horizon) -> GO INTO 1-MINUTE BUCKET in RAM.
/// 4. Push 200 immediate messages with delay = 0s -> GO INTO READY DEQUE in RAM.
/// 5. Poll immediately -> drains all 200 immediate messages without waiting for delayed items.
/// 6. Total messages in RAM strictly reflects active horizon items (never exceeds 500 cap).
#[tokio::test]
async fn test_visibility_based_spilling_and_5min_horizon() {
    let dir = tempdir().unwrap();
    let addr: SocketAddr = "127.0.0.1:57331".parse().unwrap();

    let ram_cap = 500;
    let opts = QueueOptions {
        default_visibility_timeout_sec: 10,
        max_delivery_count: 3,
        max_hot_messages_in_ram: ram_cap,
        max_wal_segment_size: 64 * 1024 * 1024,
    };

    let engine = Arc::new(QueueEngine::open(dir.path(), opts).unwrap());
    let raft = Arc::new(RaftNode::new(addr.to_string(), vec![], Arc::clone(&engine)));
    raft.become_leader_for_test().await;

    let server = Arc::new(WalrServer::new_raft(Arc::clone(&engine), Arc::clone(&raft), addr.to_string()));
    let listener = TcpListener::bind(addr).await.unwrap();
    let (_tx, rx) = broadcast::channel(1);
    tokio::spawn(async move { server.run(listener, rx).await; });

    tokio::time::sleep(Duration::from_millis(30)).await;

    let client = create_client(addr);
    let q = "vis-spill-q";

    // Step 1: Push 1,000 far-future messages (delay = 600s, beyond 300s / 5m horizon)
    for i in 0..1000 {
        client.push(q, Bytes::from(format!("far-future-{}", i)), 600).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(50)).await;

    // RAM usage must be 0 because all 1,000 messages exceed the 5-minute hot horizon!
    let ram_far = engine.total_messages_in_ram();
    eprintln!(">>> RAM count after 1,000 far-future (10m delay) pushes: {}", ram_far);
    assert_eq!(ram_far, 0, "Far-future messages (>5m) must strictly spill to disk with 0 RAM footprint");

    // Step 2: Push 300 messages with delay = 120s (2 minutes, inside 5m horizon)
    for i in 0..300 {
        client.push(q, Bytes::from(format!("near-future-{}", i)), 120).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(50)).await;

    // RAM usage must now be exactly 300 (stored in the 1-minute bucket)
    let ram_near = engine.total_messages_in_ram();
    eprintln!(">>> RAM count after 300 near-future (2m delay) pushes: {}", ram_near);
    assert_eq!(ram_near, 300, "Near-future messages (<=5m) must enter 1-minute hot delay buckets in RAM");

    // Step 3: Push 200 immediate messages (delay = 0s)
    for i in 0..200 {
        client.push(q, Bytes::from(format!("immediate-{}", i)), 0).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(50)).await;

    // RAM usage reaches the 500 cap (300 in 1m buckets + 200 in ready deque)
    let ram_full = engine.total_messages_in_ram();
    eprintln!(">>> RAM count after 200 immediate pushes: {}", ram_full);
    assert_eq!(ram_full, 500, "RAM cap must hold exactly 500 hot messages");

    // Step 4: Poll immediately -> drains all 200 immediate messages in O(1)
    let immediate_polled = client.poll(q, 10, 250).await.unwrap();
    assert_eq!(immediate_polled.len(), 200, "Must poll all 200 immediate messages without waiting for delayed items");

    for m in &immediate_polled {
        assert!(m.payload.starts_with(b"immediate-"));
        let acked = client.ack(q, &m.message_id, &m.receipt_handle).await.unwrap();
        assert!(acked);
    }

    // After acking 200 items, RAM drops back to 300
    let ram_after_drain = engine.total_messages_in_ram();
    eprintln!(">>> RAM count after acking 200 immediate messages: {}", ram_after_drain);
    assert_eq!(ram_after_drain, 300, "RAM must prune payload immediately on ACK in O(1)");

    // Polling again returns 0 messages (because remaining 300 items are delayed by 2 minutes, and 1,000 delayed by 10 minutes)
    let empty_poll = client.poll(q, 10, 10).await.unwrap();
    assert!(empty_poll.is_empty(), "Delayed messages must not be visible prematurely");
}
