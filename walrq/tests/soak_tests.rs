use bytes::Bytes;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::tempdir;

use walrq::engine::queue::{QueueEngine, QueueOptions};

#[tokio::test]
async fn test_soak_and_stress() {
    let test_duration_secs = std::env::var("SOAK_DURATION_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(3); // Default 3s in normal test run, 1800s for 30-min soak

    let dir = tempdir().unwrap();
    let engine = Arc::new(QueueEngine::open(dir.path(), QueueOptions::default()).unwrap());

    let running = Arc::new(AtomicBool::new(true));
    let total_pushed = Arc::new(AtomicU64::new(0));
    let total_acked = Arc::new(AtomicU64::new(0));

    // 1. Spawn 4 Concurrent Producers
    let mut producer_handles = Vec::new();
    for p_id in 0..4 {
        let eng = Arc::clone(&engine);
        let run = Arc::clone(&running);
        let pushed_counter = Arc::clone(&total_pushed);
        let acked_counter = Arc::clone(&total_acked);

        producer_handles.push(tokio::spawn(async move {
            let mut seq = 0u64;
            while run.load(Ordering::Relaxed) {
                let payload = format!("prod-{}-msg-{}", p_id, seq);
                let q_name = format!("soak-q-{}", seq % 8); // 8 distinct queues

                let p = pushed_counter.load(Ordering::Relaxed);
                let a = acked_counter.load(Ordering::Relaxed);
                if p.saturating_sub(a) > 20_000 {
                    tokio::time::sleep(Duration::from_millis(2)).await;
                    continue;
                }

                if eng.push(&q_name, Bytes::from(payload), 0).await.is_ok() {
                    pushed_counter.fetch_add(1, Ordering::Relaxed);
                    seq += 1;
                }
                tokio::task::yield_now().await;
            }
        }));
    }

    // 2. Spawn 8 Concurrent Pollers + Consumers
    let mut consumer_handles = Vec::new();
    for _ in 0..8 {
        let eng = Arc::clone(&engine);
        let run = Arc::clone(&running);
        let acked_counter = Arc::clone(&total_acked);

        consumer_handles.push(tokio::spawn(async move {
            let mut q_idx = 0usize;
            while run.load(Ordering::Relaxed) {
                let q_name = format!("soak-q-{}", q_idx % 8);
                q_idx += 1;

                if let Ok(msgs) = eng.poll(&q_name, 60, 20).await {
                    for m in msgs {
                        if eng.ack(&q_name, &m.message_id, &m.receipt_handle).await.unwrap_or(false) {
                            acked_counter.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                tokio::task::yield_now().await;
            }
        }));
    }

    // 3. Monitor Loop with Periodic Heartbeat
    let start = Instant::now();
    let target = Duration::from_secs(test_duration_secs);

    while start.elapsed() < target {
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // 4. Graceful Shutdown & Drain Phase
    running.store(false, Ordering::Relaxed);

    for h in producer_handles {
        let _ = h.await;
    }
    for h in consumer_handles {
        let _ = h.await;
    }

    // Drain remaining in queues
    let drain_start = Instant::now();
    while drain_start.elapsed() < Duration::from_secs(15) {
        let mut drained_any = false;
        for q_idx in 0..8 {
            let q_name = format!("soak-q-{}", q_idx);
            while let Ok(msgs) = engine.poll(&q_name, 60, 500).await {
                if msgs.is_empty() {
                    break;
                }
                for m in msgs {
                    if engine.ack(&q_name, &m.message_id, &m.receipt_handle).await.unwrap_or(false) {
                        total_acked.fetch_add(1, Ordering::Relaxed);
                        drained_any = true;
                    }
                }
            }
        }
        if !drained_any {
            tokio::time::sleep(Duration::from_millis(10)).await;
            // Check once more
            let mut more = false;
            for q_idx in 0..8 {
                let q_name = format!("soak-q-{}", q_idx);
                if let Ok(msgs) = engine.poll(&q_name, 60, 10).await {
                    if !msgs.is_empty() {
                        more = true;
                        break;
                    }
                }
            }
            if !more {
                break;
            }
        }
    }

    let final_pushed = total_pushed.load(Ordering::Relaxed);
    let final_acked = total_acked.load(Ordering::Relaxed);
    println!(">>> soak finished: pushed={}, acked={}, in_ram={}", final_pushed, final_acked, engine.total_messages_in_ram());

    assert_eq!(final_pushed, final_acked, "Zero message loss under continuous churn");
}
