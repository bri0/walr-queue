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
async fn test_high_intensity_dlq_poison_cascade() {
    let dir = tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let opts = QueueOptions {
        default_visibility_timeout_sec: 1, // 1s visibility
        max_delivery_count: 2,             // 2 delivery attempts then DLQ
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

    let q = "poison_q";
    let total_poison = 500;

    // Push 500 poison pill messages
    let mut batch = Vec::with_capacity(50);
    for chunk in 0..10 {
        batch.clear();
        for i in 0..50 {
            batch.push(Bytes::from(format!("poison-{}-{}", chunk, i)));
        }
        client.push_batch(q, batch.clone()).await.unwrap();
    }

    // Step 1: Poll 1 (attempt 1) -> don't ack, let all expire
    let mut poll1_count = 0;
    for _ in 0..20 {
        let msgs = client.poll(q, 1, 50).await.unwrap();
        poll1_count += msgs.len();
        if poll1_count == total_poison {
            break;
        }
    }
    assert_eq!(poll1_count, total_poison, "Must receive all poison pills in round 1");

    // Sleep 1.2s to expire leases
    tokio::time::sleep(Duration::from_millis(1200)).await;

    // Step 2: Poll 2 (attempt 2 = max_delivery) -> don't ack, let all expire into DLQ!
    let mut poll2_count = 0;
    for _ in 0..20 {
        let msgs = client.poll(q, 1, 50).await.unwrap();
        poll2_count += msgs.len();
        if poll2_count == total_poison {
            break;
        }
    }
    assert_eq!(poll2_count, total_poison, "Must receive all poison pills in round 2");

    // Sleep 1.2s to trigger automatic DLQ routing
    tokio::time::sleep(Duration::from_millis(1200)).await;

    // Step 3: Main queue must be completely empty!
    let empty_main = client.poll(q, 1, 50).await.unwrap();
    assert!(empty_main.is_empty(), "Main queue must be empty after DLQ routing");

    // Step 4: DLQ must contain exactly all 500 poison pills
    let dlq_name = format!("{}.dlq", q);
    let mut dlq_drained = 0;
    for _ in 0..50 {
        let msgs = client.poll(&dlq_name, 10, 50).await.unwrap();
        if msgs.is_empty() {
            tokio::time::sleep(Duration::from_millis(20)).await;
            continue;
        }
        dlq_drained += msgs.len();
        let acks: Vec<AckItem> = msgs.into_iter().map(|m| AckItem { message_id: m.message_id, receipt_handle: m.receipt_handle }).collect();
        client.ack_batch_items(&dlq_name, acks).await.unwrap();
        if dlq_drained == total_poison {
            break;
        }
    }

    println!("Total poisoned messages recovered and acked from DLQ = {}", dlq_drained);
    assert_eq!(dlq_drained, total_poison, "DLQ must safely preserve every poison pill");

    // DLQ is now clean
    assert!(client.poll(&dlq_name, 10, 50).await.unwrap().is_empty());
}
