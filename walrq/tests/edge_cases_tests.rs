use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;

use walrq::engine::queue::{QueueEngine, QueueOptions};

#[tokio::test]
async fn test_edge_case_zero_byte_payload() {
    let dir = tempdir().unwrap();
    let engine = QueueEngine::open(dir.path(), QueueOptions::default()).unwrap();

    let id = engine.push("empty-q", Bytes::new(), 0).await.unwrap();
    let polled = engine.poll("empty-q", 60, 1).await.unwrap();
    assert_eq!(polled.len(), 1);
    assert_eq!(polled[0].message_id, id);
    assert_eq!(polled[0].payload.len(), 0);

    let acked = engine.ack("empty-q", &id, &polled[0].receipt_handle).await.unwrap();
    assert!(acked);
}

#[tokio::test]
async fn test_edge_case_large_payload_10mb() {
    let dir = tempdir().unwrap();
    let engine = QueueEngine::open(dir.path(), QueueOptions::default()).unwrap();

    let large_data = vec![0xAB; 10 * 1024 * 1024]; // 10 MB payload
    let id = engine.push("large-q", Bytes::from(large_data.clone()), 0).await.unwrap();

    let polled = engine.poll("large-q", 60, 1).await.unwrap();
    assert_eq!(polled.len(), 1);
    assert_eq!(polled[0].message_id, id);
    assert_eq!(polled[0].payload.len(), 10 * 1024 * 1024);
    assert_eq!(polled[0].payload[..4], [0xAB, 0xAB, 0xAB, 0xAB]);
}

#[tokio::test]
async fn test_edge_case_duplicate_ack_idempotency() {
    let dir = tempdir().unwrap();
    let engine = QueueEngine::open(dir.path(), QueueOptions::default()).unwrap();

    let id = engine.push("ack-q", Bytes::from_static(b"data"), 0).await.unwrap();
    let polled = engine.poll("ack-q", 60, 1).await.unwrap();
    let receipt = &polled[0].receipt_handle;

    // 1st Ack -> True
    assert!(engine.ack("ack-q", &id, receipt).await.unwrap());

    // 2nd Duplicate Ack -> False (no error, safe idempotent return)
    assert!(!engine.ack("ack-q", &id, receipt).await.unwrap());
}

#[tokio::test]
async fn test_edge_case_stale_receipt_handle_rejected_after_timeout() {
    tokio::time::pause();
    let dir = tempdir().unwrap();
    let engine = QueueEngine::open(dir.path(), QueueOptions::default()).unwrap();

    let id = engine.push("stale-q", Bytes::from_static(b"data"), 0).await.unwrap();

    // 1. Worker 1 polls with 10s timeout
    let p1 = engine.poll("stale-q", 10, 1).await.unwrap();
    let stale_receipt = p1[0].receipt_handle.clone();

    // 2. Worker 1 hangs past 10s -> message becomes visible again
    tokio::time::advance(Duration::from_secs(12)).await;

    // 3. Worker 2 polls same message -> gets NEW receipt handle
    let p2 = engine.poll("stale-q", 10, 1).await.unwrap();
    assert_eq!(p2[0].message_id, id);
    let fresh_receipt = p2[0].receipt_handle.clone();
    assert_ne!(stale_receipt, fresh_receipt);

    // 4. Worker 1 wakes up late and tries to ACK with stale receipt -> REJECTED (Exact-one guarantee!)
    assert!(!engine.ack("stale-q", &id, &stale_receipt).await.unwrap());

    // 5. Worker 2 ACKs with fresh receipt -> ACCEPTED
    assert!(engine.ack("stale-q", &id, &fresh_receipt).await.unwrap());
}

#[tokio::test]
async fn test_edge_case_high_load_churn_1000_msgs() {
    let dir = tempdir().unwrap();
    let engine = Arc::new(QueueEngine::open(dir.path(), QueueOptions::default()).unwrap());

    // Concurrent push 1000 messages
    let mut push_handles = Vec::new();
    for i in 0..1000 {
        let eng = Arc::clone(&engine);
        push_handles.push(tokio::spawn(async move {
            eng.push("churn-q", Bytes::from(format!("msg-{}", i)), 0).await.unwrap()
        }));
    }
    for h in push_handles {
        h.await.unwrap();
    }

    // Concurrent poll and ack with 10 workers
    let mut worker_handles = Vec::new();
    for _ in 0..10 {
        let eng = Arc::clone(&engine);
        worker_handles.push(tokio::spawn(async move {
            let mut total_processed = 0;
            for _ in 0..100 {
                let batch = eng.poll("churn-q", 30, 20).await.unwrap();
                for m in batch {
                    let acked = eng.ack("churn-q", &m.message_id, &m.receipt_handle).await.unwrap();
                    if acked {
                        total_processed += 1;
                    }
                }
            }
            total_processed
        }));
    }

    let mut total_acked = 0;
    for h in worker_handles {
        total_acked += h.await.unwrap();
    }

    assert_eq!(total_acked, 1000, "All 1000 messages processed and acked exactly once");
    assert_eq!(engine.poll("churn-q", 30, 10).await.unwrap().len(), 0);
}
