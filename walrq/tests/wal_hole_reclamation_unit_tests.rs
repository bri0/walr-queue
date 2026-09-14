use bytes::Bytes;
use std::collections::{HashMap, HashSet};
use tempfile::tempdir;

use walrq::engine::disk_log::QueueRegistry;
use walrq::engine::queue::{QueueEngine, QueueOptions};

#[tokio::test]
async fn test_queue_registry_encoding_and_persistence() {
    let dir = tempdir().unwrap();
    let meta_dir = dir.path().join("meta");

    // 1. Register queues
    {
        let mut reg = QueueRegistry::open(&meta_dir).unwrap();
        let id0 = reg.get_or_register("orders-queue").unwrap();
        let id1 = reg.get_or_register("payments-queue").unwrap();
        let id0_again = reg.get_or_register("orders-queue").unwrap();

        assert_eq!(id0, 0);
        assert_eq!(id1, 1);
        assert_eq!(id0_again, 0);
        assert_eq!(reg.get_name(0).as_deref(), Some("orders-queue"));
        assert_eq!(reg.get_name(1).as_deref(), Some("payments-queue"));
    }

    // 2. Reopen and verify persistence from disk
    {
        let mut reg = QueueRegistry::open(&meta_dir).unwrap();
        assert_eq!(reg.get_name(0).as_deref(), Some("orders-queue"));
        assert_eq!(reg.get_name(1).as_deref(), Some("payments-queue"));

        let id2 = reg.get_or_register("notifications-queue").unwrap();
        assert_eq!(id2, 2);
    }
}

#[tokio::test]
async fn test_arbitrary_swiss_cheese_wal_hole_reclamation() {
    let dir = tempdir().unwrap();
    let data_path = dir.path().to_path_buf();

    let mut opts = QueueOptions::default();
    opts.max_wal_segment_size = 8 * 1024; // 8KB segment size

    let engine = QueueEngine::open(&data_path, opts.clone()).unwrap();

    // 1. Push 600 records across 3 queues (each payload 200B)
    let payload = Bytes::from(vec![b'Z'; 200]);
    let mut all_pushed = Vec::new();

    for i in 0..600 {
        let q_name = format!("swiss-q-{}", i % 3);
        let id = engine.push(&q_name, payload.clone(), 0).await.unwrap();
        all_pushed.push((q_name, id));
    }

    // 2. Punch holes: Ack 550 items, leaving 50 unacked survivors
    let mut acked_ids = HashSet::new();
    let mut surviving_ids = HashSet::new();

    let mut to_ack_map = HashMap::new();
    for (i, (q_name, id)) in all_pushed.iter().enumerate() {
        if i % 12 != 0 {
            to_ack_map.insert(id.clone(), q_name.clone());
        } else {
            surviving_ids.insert(id.clone());
        }
    }

    for q_idx in 0..3 {
        let q_name = format!("swiss-q-{}", q_idx);
        let polled = engine.poll(&q_name, 30, 250).await.unwrap();
        for m in polled {
            if to_ack_map.contains_key(&m.message_id) {
                let ok = engine.ack(&q_name, &m.message_id, &m.receipt_handle).await.unwrap();
                assert!(ok);
                acked_ids.insert(m.message_id);
            }
        }
    }

    // 3. Compact & clean holes
    engine.force_compaction().await.unwrap();
    engine.flush_wal().await;

    // 4. Validate physical disk reduction
    let size_a = data_path.join("wal_a.log").metadata().map(|m| m.len()).unwrap_or(0);
    let size_b = data_path.join("wal_b.log").metadata().map(|m| m.len()).unwrap_or(0);
    let total_size = size_a + size_b;

    println!("Compacted WAL disk size: {} bytes", total_size);
    assert!(total_size < 12 * 1024, "Compacted size must be < 12KB, got {}", total_size);

    // 5. Crash and restart engine
    drop(engine);
    let recovered_engine = QueueEngine::open(&data_path, opts).unwrap();

    let mut recovered_ids = HashSet::new();
    for q_idx in 0..3 {
        let q_name = format!("swiss-q-{}", q_idx);
        let polled = recovered_engine.poll(&q_name, 30, 100).await.unwrap();
        for m in polled {
            recovered_ids.insert(m.message_id);
        }
    }

    assert_eq!(recovered_ids.len(), surviving_ids.len());
    for s_id in &surviving_ids {
        assert!(recovered_ids.contains(s_id));
    }
    for a_id in &acked_ids {
        assert!(!recovered_ids.contains(a_id));
    }
}
