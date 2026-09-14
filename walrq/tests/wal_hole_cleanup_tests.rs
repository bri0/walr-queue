use bytes::Bytes;
use std::collections::{HashMap, HashSet};
use tempfile::tempdir;

use walrq::engine::queue::{QueueEngine, QueueOptions};

#[tokio::test]
async fn test_random_out_of_order_ack_holes_cleaned_and_disk_reclaimed() {
    let dir = tempdir().unwrap();
    let data_path = dir.path().to_path_buf();

    let mut opts = QueueOptions::default();
    opts.max_wal_segment_size = 10 * 1024; // 10KB threshold to trigger flip-flop compactor

    let engine = QueueEngine::open(&data_path, opts.clone()).unwrap();

    println!("\n=== Testing Random Out-of-Order Ack Hole Cleanup ===");

    // 1. Push 1,000 messages (Each ~250 bytes -> ~250KB total uncompacted data)
    let payload = Bytes::from(vec![b'A'; 256]);
    let mut pushed_ids = Vec::new();

    for i in 0..1000 {
        let q_name = format!("hole-q-{}", i % 4);
        let id = engine.push(&q_name, payload.clone(), 0).await.unwrap();
        pushed_ids.push((q_name, id));
    }

    println!("Pushed 1,000 messages (Total raw data: ~250KB).");

    // 2. Poll and acknowledge in a Swiss-Cheese pattern:
    // Ack 950 messages, leaving 50 random isolated surviving messages ("holes" everywhere)
    let mut acked_ids = HashSet::new();
    let mut surviving_ids = HashSet::new();

    let mut ack_map = HashMap::new();
    for (idx, (q_name, msg_id)) in pushed_ids.iter().enumerate() {
        if idx % 20 != 0 {
            ack_map.insert(msg_id.clone(), q_name.clone());
        } else {
            surviving_ids.insert(msg_id.clone());
        }
    }

    for q_idx in 0..4 {
        let q_name = format!("hole-q-{}", q_idx);
        let msgs = engine.poll(&q_name, 30, 300).await.unwrap();
        for m in msgs {
            if ack_map.contains_key(&m.message_id) {
                let ok = engine.ack(&q_name, &m.message_id, &m.receipt_handle).await.unwrap();
                assert!(ok);
                acked_ids.insert(m.message_id);
            }
        }
    }

    println!("Created 950 random ack holes across the WAL files.");
    println!("Surviving unacked messages: {}", surviving_ids.len());

    // 3. Trigger Flip-Flop Compactor to compact holes
    engine.force_compaction().await.unwrap();
    engine.flush_wal().await;

    // 4. Physical Disk Check: Ensure dead holes were purged and disk is tiny
    let wal_a_path = data_path.join("wal_a.log");
    let wal_b_path = data_path.join("wal_b.log");

    let size_a = if wal_a_path.exists() { wal_a_path.metadata().unwrap().len() } else { 0 };
    let size_b = if wal_b_path.exists() { wal_b_path.metadata().unwrap().len() } else { 0 };

    let total_wal_size = size_a + size_b;
    println!("Total Physical WAL Disk Size after Hole Cleanup: {} bytes (vs ~250KB original)", total_wal_size);

    // 50 surviving messages compressed take < 15KB
    assert!(
        total_wal_size < 15 * 1024,
        "Total WAL size must be < 15KB after hole cleanup! Got {} bytes",
        total_wal_size
    );

    // 5. Crash Recovery Verification
    drop(engine);

    println!("\n=== Reopening Engine from Compacted WAL (Crash Recovery) ===");
    let recovered_engine = QueueEngine::open(&data_path, opts).unwrap();

    let mut recovered_polled_ids = HashSet::new();
    for q_idx in 0..4 {
        let q_name = format!("hole-q-{}", q_idx);
        let msgs = recovered_engine.poll(&q_name, 30, 100).await.unwrap();
        for m in msgs {
            recovered_polled_ids.insert(m.message_id);
        }
    }

    println!("Recovered IDs from Compacted WAL: {}", recovered_polled_ids.len());

    // Assert zero lost surviving messages and zero resurrected acked messages
    for id in &surviving_ids {
        assert!(recovered_polled_ids.contains(id), "Surviving message {} must exist after compaction", id);
    }

    for id in &acked_ids {
        assert!(!recovered_polled_ids.contains(id), "Acked hole message {} must NEVER be resurrected", id);
    }

    println!("SUCCESS: 100% of holes purged, surviving messages preserved, disk reclaimed!\n");
}
