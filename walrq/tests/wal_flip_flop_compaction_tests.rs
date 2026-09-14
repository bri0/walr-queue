use bytes::Bytes;
use std::time::Duration;
use tempfile::tempdir;

use walrq::engine::queue::{QueueEngine, QueueOptions};

#[tokio::test]
async fn test_wal_flip_flop_compaction_and_disk_reclamation() {
    let dir = tempdir().unwrap();
    let mut opts = QueueOptions::default();
    opts.max_wal_segment_size = 5 * 1024; // 5KB segment limit

    let engine = QueueEngine::open(dir.path(), opts.clone()).unwrap();

    println!("\n=== Testing A/B Flip-Flop WAL Compaction ===");

    // 1. Push 500 messages
    let payload = Bytes::from(vec![b'X'; 256]);
    let mut pushed_ids = Vec::new();

    for i in 0..500 {
        let q_name = format!("compact-q-{}", i % 4);
        let id = engine.push(&q_name, payload.clone(), 0).await.unwrap();
        pushed_ids.push((q_name, id));
    }

    println!("Pushed 500 messages into WAL.");

    // 2. Poll and Ack 480 messages (Keep 20 in-flight/unacked)
    let mut acked_count = 0;
    for q_idx in 0..4 {
        let q_name = format!("compact-q-{}", q_idx);
        let msgs = engine.poll(&q_name, 30, 150).await.unwrap();
        for m in msgs {
            if acked_count < 480 {
                let ok = engine.ack(&q_name, &m.message_id, &m.receipt_handle).await.unwrap();
                assert!(ok);
                acked_count += 1;
            }
        }
    }

    println!("Acked {} messages (Surviving unacked backlog: 20).", acked_count);

    // Force compaction on the remaining 20 messages
    engine.force_compaction().await.unwrap();
    engine.flush_wal().await;

    // 3. Inspect Physical Disk Sizes
    let wal_a_path = dir.path().join("wal_a.log");
    let wal_b_path = dir.path().join("wal_b.log");

    let size_a = if wal_a_path.exists() { wal_a_path.metadata().unwrap().len() } else { 0 };
    let size_b = if wal_b_path.exists() { wal_b_path.metadata().unwrap().len() } else { 0 };

    println!("Disk State after Compaction: wal_a.log = {} bytes, wal_b.log = {} bytes", size_a, size_b);

    let total_wal_size = size_a + size_b;
    println!("Total WAL Size: {} bytes (vs >50KB uncompacted)", total_wal_size);
    assert!(total_wal_size < 15 * 1024, "Total WAL size must be strictly bounded under 15KB!");

    // 4. CRASH & RESTART RECOVERY TEST
    drop(engine);

    println!("\n=== Simulating Node Crash & WAL Replay ===");
    let recovered_engine = QueueEngine::open(dir.path(), opts).unwrap();

    let mut recovered_polled = 0;
    for q_idx in 0..4 {
        let q_name = format!("compact-q-{}", q_idx);
        let msgs = recovered_engine.poll(&q_name, 30, 100).await.unwrap();
        recovered_polled += msgs.len();
    }

    println!("Recovered surviving messages from replayed WAL: {}", recovered_polled);
    assert_eq!(recovered_polled, 20, "Exact 20 unacked messages must be recovered on boot!");
    println!("SUCCESS: A/B Flip-Flop Compaction + Crash Recovery Verified 100%!");
}
