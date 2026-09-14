use bytes::Bytes;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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
async fn test_high_concurrency_soak_edge_churn() {
    let dir = tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let opts = QueueOptions {
        default_visibility_timeout_sec: 1, // fast 1s timeout
        max_delivery_count: 5,
        max_hot_messages_in_ram: 100,      // tight RAM ceiling to force continuous disk spilling & page-ins
        max_wal_segment_size: 1024 * 1024, // 1MB segment size
    };

    let engine = Arc::new(QueueEngine::open(dir.path(), opts).unwrap());
    let raft = Arc::new(RaftNode::new(addr.to_string(), vec![], Arc::clone(&engine)));
    raft.become_leader_for_test().await;

    let server = Arc::new(WalrServer::new_raft(Arc::clone(&engine), Arc::clone(&raft), addr.to_string()));
    let (_tx, rx) = broadcast::channel(1);
    tokio::spawn(async move { server.run(listener, rx).await; });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = Arc::new(WalrClient::with_config(
        vec![addr.to_string()],
        ClientConfig {
            buffer_window_ms: 1,
            max_batch_size: 100,
            max_redirects: 5,
        },
    ));

    let queues = vec!["soak_q_0", "soak_q_1", "soak_q_2", "soak_q_3"];
    let running = Arc::new(AtomicBool::new(true));
    let total_pushed = Arc::new(AtomicU64::new(0));
    let total_acked = Arc::new(AtomicU64::new(0));

    // 4 Producer tasks (1 per queue)
    let mut prod_handles = Vec::new();
    for (q_idx, &q_name) in queues.iter().enumerate() {
        let c = Arc::clone(&client);
        let run = Arc::clone(&running);
        let pushed_ctr = Arc::clone(&total_pushed);
        let q_str = q_name.to_string();

        prod_handles.push(tokio::spawn(async move {
            let mut seq = 0;
            while run.load(Ordering::Relaxed) {
                let mut batch = Vec::with_capacity(50);
                for i in 0..50 {
                    batch.push(Bytes::from(format!("soak-{}-{}-{}", q_idx, seq, i)));
                }
                seq += 1;
                if c.push_batch(&q_str, batch).await.is_ok() {
                    pushed_ctr.fetch_add(50, Ordering::Relaxed);
                }
                tokio::task::yield_now().await;
            }
        }));
    }

    // Soak for 3 seconds while producers push
    tokio::time::sleep(Duration::from_secs(3)).await;
    running.store(false, Ordering::Relaxed);

    for h in prod_handles {
        let _ = h.await;
    }

    // 4 Consumer tasks polling across the queues round-robin
    let mut cons_handles = Vec::new();
    let consuming = Arc::new(AtomicBool::new(true));
    for worker_id in 0..4 {
        let c = Arc::clone(&client);
        let run_cons = Arc::clone(&consuming);
        let acked_ctr = Arc::clone(&total_acked);
        let q_list = queues.clone();

        cons_handles.push(tokio::spawn(async move {
            let mut step = worker_id;
            while run_cons.load(Ordering::Relaxed) {
                let target_q = q_list[step % q_list.len()];
                step += 1;
                if let Ok(msgs) = c.poll(target_q, 1, 50).await {
                    if msgs.is_empty() {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                        continue;
                    }
                    let count = msgs.len();
                    let acks: Vec<AckItem> = msgs.into_iter().map(|m| AckItem { message_id: m.message_id, receipt_handle: m.receipt_handle }).collect();
                    if c.ack_batch_items(target_q, acks).await.is_ok() {
                        acked_ctr.fetch_add(count as u64, Ordering::Relaxed);
                    }
                }
            }
        }));
    }

    // Consume until caught up or timeout
    let drain_start = std::time::Instant::now();
    let pushed = total_pushed.load(Ordering::Relaxed);
    while drain_start.elapsed() < Duration::from_secs(10) {
        if total_acked.load(Ordering::Relaxed) >= pushed {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    consuming.store(false, Ordering::Relaxed);
    for h in cons_handles {
        let _ = h.await;
    }

    let pushed = total_pushed.load(Ordering::Relaxed);
    let mut acked = total_acked.load(Ordering::Relaxed);

    println!(">>> Soak Phase 1 Done: Pushed={}, Acked during flight={}", pushed, acked);

    // Drain all remaining messages across all queues
    for &q_name in &queues {
        let drain_start = std::time::Instant::now();
        let mut empty_retries = 0;
        while drain_start.elapsed() < Duration::from_secs(10) {
            if let Ok(msgs) = client.poll(q_name, 1, 100).await {
                if msgs.is_empty() {
                    empty_retries += 1;
                    if empty_retries > 10 {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    continue;
                }
                empty_retries = 0;
                let count = msgs.len();
                acked += count as u64;
                let acks: Vec<AckItem> = msgs.into_iter().map(|m| AckItem { message_id: m.message_id, receipt_handle: m.receipt_handle }).collect();
                let _ = client.ack_batch_items(q_name, acks).await;
            }
        }
    }

    println!(">>> Soak Final Drain Done: Pushed={}, Total Acked={}", pushed, acked);
    assert_eq!(pushed, acked, "Every message must be drained across multi-queue soak");
}
