use bytes::Bytes;
use std::collections::HashSet;
use std::time::Duration;
use tempfile::tempdir;

use walrq::engine::queue::{QueueEngine, QueueOptions};

#[tokio::test]
async fn test_sliding_window_crash_replay_with_zero_loss() {
    let dir = tempdir().unwrap();
    let data_path = dir.path().to_path_buf();

    let mut opts = QueueOptions::default();
    opts.max_hot_messages_in_ram = 1_000;
    opts.max_wal_segment_size = 50 * 1024;

    let mut pushed_ids = HashSet::new();

    // Session 1: Push 300 messages (100 immediate + 200 delayed)
    {
        let engine = QueueEngine::open(&data_path, opts.clone()).unwrap();
        for i in 0..300 {
            let delay = if i < 100 { 0 } else { 1 };
            let id = engine.push("sliding-chaos-q", Bytes::from(format!("m-{}", i)), delay).await.unwrap();
            pushed_ids.insert(id);
        }

        // Poll and ack 50
        let polled = engine.poll("sliding-chaos-q", 30, 50).await.unwrap();
        assert_eq!(polled.len(), 50);
        for m in polled {
            engine.ack("sliding-chaos-q", &m.message_id, &m.receipt_handle).await.unwrap();
            pushed_ids.remove(&m.message_id); // 250 remaining
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Wait 1.5s for 1s delayed items to mature
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // Session 2: Reopen from disk -> verify all remaining 250 recovered seamlessly
    {
        let engine = QueueEngine::open(&data_path, opts).unwrap();
        let mut recovered_ids = HashSet::new();

        // Wait for 1-second delays to mature
        tokio::time::sleep(Duration::from_millis(1200)).await;

        loop {
            let batch = engine.poll("sliding-chaos-q", 30, 100).await.unwrap();
            if batch.is_empty() {
                break;
            }
            for m in batch {
                let ok = engine.ack("sliding-chaos-q", &m.message_id, &m.receipt_handle).await.unwrap();
                assert!(ok);
                recovered_ids.insert(m.message_id);
            }
        }

        assert_eq!(recovered_ids.len(), 250, "All remaining 250 messages recovered through sliding window with ZERO loss");
        for id in &pushed_ids {
            assert!(recovered_ids.contains(id));
        }
    }
}
