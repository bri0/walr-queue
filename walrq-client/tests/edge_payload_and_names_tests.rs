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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_extreme_payload_and_queue_name_edges() {
    let dir = tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let opts = QueueOptions {
        default_visibility_timeout_sec: 2,
        max_delivery_count: 3,
        max_hot_messages_in_ram: 100,
        max_wal_segment_size: 16 * 1024 * 1024,
    };

    let engine = Arc::new(QueueEngine::open(dir.path(), opts).unwrap());
    let raft = Arc::new(RaftNode::new(addr.to_string(), vec![], Arc::clone(&engine)));
    raft.become_leader_for_test().await;

    let server = Arc::new(WalrServer::new_raft(Arc::clone(&engine), Arc::clone(&raft), addr.to_string()));
    let (_tx, rx) = broadcast::channel(1);
    tokio::spawn(async move { server.run(listener, rx).await; });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = WalrClient::with_config(
        vec![addr.to_string()],
        ClientConfig {
            buffer_window_ms: 1,
            max_batch_size: 10,
            max_redirects: 5,
        },
    );

    // Edge Case 1: Zero-byte empty payload
    let q_empty = "edge_zero_byte";
    let empty_payload = Bytes::new();
    let mid_empty = client.push(q_empty, empty_payload.clone(), 0).await.unwrap();
    let polled_empty = client.poll(q_empty, 1, 1).await.unwrap();
    assert_eq!(polled_empty.len(), 1);
    assert_eq!(polled_empty[0].message_id, mid_empty);
    assert_eq!(polled_empty[0].payload, Bytes::new());
    assert!(client.ack(q_empty, &mid_empty, &polled_empty[0].receipt_handle).await.unwrap());

    // Edge Case 2: Multi-megabyte large payload (2MB payload)
    let q_large = "edge_large_payload";
    let large_payload = Bytes::from(vec![0x42u8; 2 * 1024 * 1024]);
    let mid_large = client.push(q_large, large_payload.clone(), 0).await.unwrap();
    let polled_large = client.poll(q_large, 2, 1).await.unwrap();
    assert_eq!(polled_large.len(), 1);
    assert_eq!(polled_large[0].message_id, mid_large);
    assert_eq!(polled_large[0].payload, large_payload);
    assert!(client.ack(q_large, &mid_large, &polled_large[0].receipt_handle).await.unwrap());

    // Edge Case 3: Unicode & special character queue names
    let special_queues = vec![
        "queue/with/slashes",
        "queue.with.dots",
        "queue-with-dashes_and_underscores",
        "queue:with:colons",
        "queue-🚀-emoji-🔥",
        "очередь-юникод-тест",
    ];

    for sq in &special_queues {
        let test_payload = Bytes::from(format!("payload-for-{}", sq));
        let mid = client.push(sq, test_payload.clone(), 0).await.unwrap();
        let polled = client.poll(sq, 1, 1).await.unwrap();
        assert_eq!(polled.len(), 1, "Failed for queue {}", sq);
        assert_eq!(polled[0].message_id, mid);
        assert_eq!(polled[0].payload, test_payload);
        assert!(client.ack(sq, &mid, &polled[0].receipt_handle).await.unwrap());
    }

    // Edge Case 4: Rapid duplicate acks for already acked message
    let q_dup = "edge_dup_ack";
    let mid_dup = client.push(q_dup, Bytes::from("dup-test"), 0).await.unwrap();
    let polled_dup = client.poll(q_dup, 1, 1).await.unwrap();
    let rh = polled_dup[0].receipt_handle.clone();
    assert!(client.ack(q_dup, &mid_dup, &rh).await.unwrap(), "1st ACK must succeed");
    assert!(!client.ack(q_dup, &mid_dup, &rh).await.unwrap(), "2nd duplicate ACK must return false");
    assert!(!client.ack(q_dup, &mid_dup, &rh).await.unwrap(), "3rd duplicate ACK must return false");
}
