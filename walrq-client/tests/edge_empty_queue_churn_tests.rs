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
async fn test_empty_queue_churn_and_zero_poll_spikes() {
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
            max_batch_size: 50,
            max_redirects: 5,
        },
    );

    // 1. Poll non-existent queues: must cleanly return empty without creating leaks
    for i in 0..50 {
        let q_name = format!("non_existent_q_{}", i);
        let res = client.poll(&q_name, 1, 10).await.unwrap();
        assert!(res.is_empty());
    }

    // 2. Poll with max_messages = 0: must return empty vec safely
    let zero_poll = client.poll("dummy_q", 1, 0).await.unwrap();
    assert!(zero_poll.is_empty());

    // 3. Ack against non-existent message ID and random receipt: must return false cleanly
    let fake_ack = client.ack("dummy_q", "01ARZ3NDEKTSV4RRFFQ69G5FAV", "fake-receipt-token").await.unwrap();
    assert!(!fake_ack);

    // 4. Batch ACK with empty list: must return 0 without error
    let empty_ack_count = client.ack_batch_items("dummy_q", vec![]).await.unwrap();
    assert_eq!(empty_ack_count, 0);

    // 5. Batch push with empty list: must return empty list without error
    let empty_push = client.push_batch("dummy_q", vec![]).await.unwrap();
    assert!(empty_push.is_empty());
}
