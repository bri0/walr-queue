use bytes::Bytes;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use walrq::cluster::raft::RaftNode;
use walrq::engine::queue::{QueueEngine, QueueOptions};
use walrq::server::tcp_service::WalrServer;
use walrq_client::{AckItem, ClientConfig, WalrClient};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_compact_under_spill_bug_hunt_extreme() {
    let dir = tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let opts = QueueOptions {
        default_visibility_timeout_sec: 5,
        max_delivery_count: 3,
        max_hot_messages_in_ram: 50, // Only 50 in RAM!
        max_wal_segment_size: 16 * 1024, // 16KB tiny compaction threshold!
    };

    let engine = Arc::new(QueueEngine::open(dir.path(), opts).unwrap());
    let raft = Arc::new(RaftNode::new(addr.to_string(), vec![], Arc::clone(&engine)));
    raft.become_leader_for_test().await;

    let server = Arc::new(WalrServer::new_raft(Arc::clone(&engine), Arc::clone(&raft), addr.to_string()));
    let (_tx, rx) = broadcast::channel(1);
    tokio::spawn(async move { server.run(listener, rx).await; });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let client = WalrClient::with_config(
        vec![addr.to_string()],
        ClientConfig {
            buffer_window_ms: 1,
            max_batch_size: 50,
            max_redirects: 5,
        },
    );

    let q = "compact_spill_extreme_q";
    let mut batch = Vec::with_capacity(50);
    for i in 0..50 {
        batch.push(Bytes::from(format!("payload-compaction-{:04}", i)));
    }
    // Push 5,000 messages (triggering multiple flip-flop compactions while hundreds of messages remain unread on disk)
    for _ in 0..100 {
        client.push_batch(q, batch.clone()).await.unwrap();
    }

    let mut total_polled = 0;
    for _ in 0..500 {
        let msgs = client.poll(q, 5, 100).await.unwrap();
        if msgs.is_empty() {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            continue;
        }
        total_polled += msgs.len();
        let acks: Vec<AckItem> = msgs.into_iter().map(|m| AckItem { message_id: m.message_id, receipt_handle: m.receipt_handle }).collect();
        client.ack_batch_items(q, acks).await.unwrap();
        if total_polled == 5000 {
            break;
        }
    }

    println!("Total polled & acked = {} / 5000", total_polled);
    assert_eq!(total_polled, 5000, "Compaction during disk spilling must not lose unread spilled records");
}
