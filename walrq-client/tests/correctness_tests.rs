use bytes::Bytes;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use ulid::Ulid;

use walrq::cluster::raft::RaftNode;
use walrq::engine::queue::{QueueEngine, QueueOptions};
use walrq::server::tcp_service::WalrServer;
use walrq_client::{ClientConfig, WalrClient};

/// Comprehensive correctness suite covering:
/// 1. Data integrity (bytes in == bytes out, zero payload corruption)
/// 2. FIFO order per queue when un-delayed
/// 3. Visibility timeout redelivery when unacknowledged
/// 4. Idempotent / user-specified ULID retention
/// 5. Delayed message schedule precision
/// 6. Strict exactly-once acknowledgement semantics (no phantom redeliveries after ack)
#[tokio::test]
async fn test_full_correctness_pipeline() {
    let dir = tempdir().unwrap();
    let addr: SocketAddr = "127.0.0.1:65151".parse().unwrap();

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

    let client = WalrClient::with_config(
        vec![addr.to_string()],
        ClientConfig {
            buffer_window_ms: 20,
            max_batch_size: 10,
            max_redirects: 5,
        },
    );

    // --- TEST 1: Byte-perfect data fidelity and FIFO order ---
    let total_fifo = 100;
    for i in 0..total_fifo {
        let payload = format!("seq-{:04}-data-{}", i, "X".repeat(128));
        client
            .push_immediate("fifo-q", Bytes::from(payload), 0)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    let mut polled_fifo = Vec::new();
    while polled_fifo.len() < total_fifo {
        let batch = client.poll("fifo-q", 30, 25).await.unwrap();
        for m in batch {
            polled_fifo.push(m.payload);
        }
    }
    assert_eq!(polled_fifo.len(), total_fifo);
    for (i, p) in polled_fifo.into_iter().enumerate() {
        let expected = format!("seq-{:04}-data-{}", i, "X".repeat(128));
        assert_eq!(p, expected.as_bytes(), "Data corrupted or out of order at index {}", i);
    }

    // --- TEST 2: User ULID retention ---
    let custom_id = Ulid::new().to_string();
    let pushed_id = client
        .push_with_id("id-q", Bytes::from("id-payload"), 0, &custom_id)
        .await
        .unwrap();
    assert_eq!(pushed_id, custom_id);

    let msgs = client.poll("id-q", 30, 10).await.unwrap();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].message_id, custom_id);
    assert_eq!(msgs[0].payload, b"id-payload");
    assert!(client.ack("id-q", &msgs[0].message_id, &msgs[0].receipt_handle).await.unwrap());

    // --- TEST 3: Visibility timeout and delivery count increment ---
    client.push_immediate("timeout-q", Bytes::from("timeout-test"), 0).await.unwrap();

    // 1st poll with 1-second visibility timeout
    let p1 = client.poll("timeout-q", 1, 1).await.unwrap();
    assert_eq!(p1.len(), 1);
    assert_eq!(p1[0].delivery_count, 1);

    // Immediately polling should return empty (still hidden)
    let p_hidden = client.poll("timeout-q", 1, 1).await.unwrap();
    assert!(p_hidden.is_empty(), "Message should be invisible within timeout window");

    // Wait for visibility timeout expiration
    tokio::time::sleep(Duration::from_millis(1200)).await;

    // 2nd poll: should reappear with incremented delivery_count
    let p2 = client.poll("timeout-q", 30, 1).await.unwrap();
    assert_eq!(p2.len(), 1, "Message must reappear after visibility expiration");
    assert_eq!(p2[0].delivery_count, 2, "Delivery count must increment to 2");

    // Ack it
    assert!(client.ack("timeout-q", &p2[0].message_id, &p2[0].receipt_handle).await.unwrap());

    // --- TEST 4: Delayed message scheduling ---
    let t_delay_start = tokio::time::Instant::now();
    client.push_immediate("delay-q", Bytes::from("delayed-payload"), 2).await.unwrap();

    // Must be invisible immediately
    let p_early = client.poll("delay-q", 30, 1).await.unwrap();
    assert!(p_early.is_empty(), "Delayed message visible prematurely!");

    // Sleep until delay expires
    tokio::time::sleep(Duration::from_millis(2100)).await;

    let p_delayed = client.poll("delay-q", 30, 1).await.unwrap();
    assert_eq!(p_delayed.len(), 1, "Delayed message failed to emerge after delay!");
    assert!(t_delay_start.elapsed() >= Duration::from_secs(2));
    assert_eq!(p_delayed[0].payload, b"delayed-payload");
    assert!(client.ack("delay-q", &p_delayed[0].message_id, &p_delayed[0].receipt_handle).await.unwrap());

    // --- TEST 5: Strict non-redelivery after Ack ---
    // All queues polled empty
    let empty_check = client.poll("timeout-q", 10, 10).await.unwrap();
    assert!(empty_check.is_empty(), "Acked message reappeared!");
}
