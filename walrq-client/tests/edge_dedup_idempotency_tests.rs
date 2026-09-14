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
async fn test_extreme_duplicate_dedup_chaos() {
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

    let q = "dedup_chaos_q";

    // Create 100 unique items with explicit custom IDs
    let fixed_id_items: Vec<(Bytes, Option<String>)> = (0..100)
        .map(|i| {
            let ulid = ulid::Ulid::from_string(&format!("01ARZ3NDEKTSV4RRFFQ69G5{:03}", i)).unwrap();
            (Bytes::from(format!("dedup-payload-{}", i)), Some(ulid.to_string()))
        })
        .collect();

    // 1. Push batch with custom IDs
    let ids1 = client.push_batch_with_ids(q, fixed_id_items.clone()).await.unwrap();
    assert_eq!(ids1.len(), 100);

    // 2. Re-push the EXACT same 100 IDs (network retry simulation)
    let ids2 = client.push_batch_with_ids(q, fixed_id_items.clone()).await.unwrap();
    assert_eq!(ids2.len(), 100);

    // 3. Re-push single duplicates
    for item in &fixed_id_items[0..10] {
        let _ = client.push_batch_with_ids(q, vec![item.clone()]).await.unwrap();
    }

    // 4. Poll queue: must only return the 100 unique messages, ZERO DUPLICATES!
    let mut drained = 0;
    let mut seen_ids = std::collections::HashSet::new();
    for _ in 0..10 {
        let msgs = client.poll(q, 2, 50).await.unwrap();
        if msgs.is_empty() {
            break;
        }
        for m in &msgs {
            assert!(seen_ids.insert(m.message_id.clone()), "Duplicate message delivered: {}", m.message_id);
        }
        drained += msgs.len();
        let acks: Vec<AckItem> = msgs.into_iter().map(|m| AckItem { message_id: m.message_id, receipt_handle: m.receipt_handle }).collect();
        client.ack_batch_items(q, acks).await.unwrap();
    }

    println!("Total unique messages drained = {} / 100", drained);
    assert_eq!(drained, 100, "Idempotent push must deduplicate identical message IDs completely");
    assert!(client.poll(q, 2, 10).await.unwrap().is_empty());
}
