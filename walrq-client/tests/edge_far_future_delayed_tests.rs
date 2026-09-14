use bytes::Bytes;
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
async fn test_far_future_delayed_disk_paging() {
    let dir = tempdir().unwrap();
    let data_path = dir.path().to_path_buf();

    let opts = QueueOptions {
        default_visibility_timeout_sec: 2,
        max_delivery_count: 3,
        max_hot_messages_in_ram: 50,
        max_wal_segment_size: 16 * 1024 * 1024,
    };

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let engine = Arc::new(QueueEngine::open(&data_path, opts).unwrap());
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

    let q = "far_future_q";
    // Push 100 messages with 2-second delay
    let mut batch = Vec::new();
    for i in 0..100 {
        batch.push(Bytes::from(format!("future-{:04}", i)));
    }
    client.push_batch_with_delay(q, batch, 2).await.unwrap();

    // Immediately poll: MUST BE EMPTY
    let empty = client.poll(q, 2, 100).await.unwrap();
    assert!(empty.is_empty(), "Delayed messages must not be visible immediately");

    // Sleep 1 second (still not matured): MUST STILL BE EMPTY
    tokio::time::sleep(Duration::from_millis(1000)).await;
    let still_empty = client.poll(q, 2, 100).await.unwrap();
    assert!(still_empty.is_empty(), "Delayed messages must not be visible before 2s delay");

    // Sleep another 1.2 seconds (total 2.2s > 2s delay)
    tokio::time::sleep(Duration::from_millis(1200)).await;

    // Now poll: ALL 100 messages must be visible
    let mut drained = 0;
    for _ in 0..10 {
        let msgs = client.poll(q, 2, 50).await.unwrap();
        if msgs.is_empty() {
            tokio::time::sleep(Duration::from_millis(50)).await;
            continue;
        }
        drained += msgs.len();
        let acks: Vec<AckItem> = msgs.into_iter().map(|m| AckItem { message_id: m.message_id, receipt_handle: m.receipt_handle }).collect();
        client.ack_batch_items(q, acks).await.unwrap();
        if drained == 100 {
            break;
        }
    }

    assert_eq!(drained, 100, "All delayed messages must mature and be drained");
}
