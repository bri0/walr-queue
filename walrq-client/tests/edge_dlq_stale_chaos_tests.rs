use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio::time::sleep;
use walrq::cluster::raft::RaftNode;
use walrq::engine::queue::{QueueEngine, QueueOptions};
use walrq::server::tcp_service::WalrServer;
use walrq_client::{ClientConfig, WalrClient};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_extreme_stale_ack_and_dlq_lifecycle() {
    let dir = tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let opts = QueueOptions {
        default_visibility_timeout_sec: 1, // 1s timeout
        max_delivery_count: 2,             // 2 delivery attempts then DLQ
        max_hot_messages_in_ram: 100,
        max_wal_segment_size: 64 * 1024 * 1024,
    };

    let engine = Arc::new(QueueEngine::open(dir.path(), opts).unwrap());
    let raft = Arc::new(RaftNode::new(addr.to_string(), vec![], Arc::clone(&engine)));
    raft.become_leader_for_test().await;

    let server = Arc::new(WalrServer::new_raft(Arc::clone(&engine), Arc::clone(&raft), addr.to_string()));
    let (_tx, rx) = broadcast::channel(1);
    tokio::spawn(async move { server.run(listener, rx).await; });

    sleep(Duration::from_millis(50)).await;

    let client = WalrClient::with_config(
        vec![addr.to_string()],
        ClientConfig {
            buffer_window_ms: 1,
            max_batch_size: 50,
            max_redirects: 5,
        },
    );

    let q = "dlq_stale_test";
    client.push(q, Bytes::from("stale-candidate"), 0).await.unwrap();

    // 1st Poll: Delivery 1
    let msgs1 = client.poll(q, 1, 1).await.unwrap();
    assert_eq!(msgs1.len(), 1);
    let msg1 = &msgs1[0];
    let stale_receipt = msg1.receipt_handle.clone();
    let msg_id = msg1.message_id.clone();

    // Let lease expire (1.2s > 1s)
    sleep(Duration::from_millis(1200)).await;

    // 2nd Poll: Delivery 2 (now reached max_delivery_count=2)
    let msgs2 = client.poll(q, 1, 1).await.unwrap();
    assert_eq!(msgs2.len(), 1);
    assert_eq!(msgs2[0].message_id, msg_id);
    let valid_receipt2 = msgs2[0].receipt_handle.clone();
    assert_ne!(stale_receipt, valid_receipt2);

    // Try to ACK using the stale receipt from poll 1 -> MUST FAIL
    let stale_ack_res = client.ack(q, &msg_id, &stale_receipt).await.unwrap();
    assert!(!stale_ack_res, "Stale ACK must return false and not delete active lease");

    // Let lease 2 expire (1.2s > 1s) -> Exceeds max_delivery=2 -> Should route to DLQ
    sleep(Duration::from_millis(1200)).await;

    // Poll main queue -> should be empty
    let empty_main = client.poll(q, 1, 1).await.unwrap();
    assert!(empty_main.is_empty(), "Main queue must be empty after DLQ redirection");

    // Try ACK with receipt 2 after message moved to DLQ -> MUST FAIL
    let stale_ack_res2 = client.ack(q, &msg_id, &valid_receipt2).await.unwrap();
    assert!(!stale_ack_res2, "Stale ACK on dead message must return false");

    // Poll DLQ queue: "<queue>.dlq"
    let dlq_name = format!("{}.dlq", q);
    let dlq_msgs = client.poll(&dlq_name, 10, 1).await.unwrap();
    assert_eq!(dlq_msgs.len(), 1, "Message must exist in DLQ");
    assert_eq!(dlq_msgs[0].message_id, msg_id);
    assert_eq!(dlq_msgs[0].payload, Bytes::from("stale-candidate"));

    // Successfully ACK from DLQ
    let dlq_ack = client.ack(&dlq_name, &dlq_msgs[0].message_id, &dlq_msgs[0].receipt_handle).await.unwrap();
    assert!(dlq_ack, "ACK from DLQ must succeed");

    // DLQ should now be empty
    let dlq_empty = client.poll(&dlq_name, 10, 1).await.unwrap();
    assert!(dlq_empty.is_empty(), "DLQ should be empty after ACK");
}
