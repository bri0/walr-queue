use bytes::Bytes;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio::time::sleep;
use walrq::cluster::raft::RaftNode;
use walrq::engine::queue::{QueueEngine, QueueOptions};
use walrq::server::tcp_service::WalrServer;
use walrq_client::{AckItem, ClientConfig, WalrClient};

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_heavy_delayed_churn_soak_edge() {
    let dir = tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let opts = QueueOptions {
        default_visibility_timeout_sec: 2,
        max_delivery_count: 5,
        max_hot_messages_in_ram: 100, // force aggressive disk spilling
        max_wal_segment_size: 4 * 1024 * 1024,
    };

    let engine = Arc::new(QueueEngine::open(dir.path(), opts).unwrap());
    let raft = Arc::new(RaftNode::new(addr.to_string(), vec![], Arc::clone(&engine)));
    raft.become_leader_for_test().await;

    let server = Arc::new(WalrServer::new_raft(Arc::clone(&engine), Arc::clone(&raft), addr.to_string()));
    let (_tx, rx) = broadcast::channel(1);
    tokio::spawn(async move { server.run(listener, rx).await; });

    sleep(Duration::from_millis(50)).await;

    let client = Arc::new(WalrClient::with_config(
        vec![addr.to_string()],
        ClientConfig {
            buffer_window_ms: 1,
            max_batch_size: 50,
            max_redirects: 5,
        },
    ));

    let q = "churn_delay_q";
    let running = Arc::new(AtomicBool::new(true));
    let pushed_immediate = Arc::new(AtomicU64::new(0));
    let pushed_delayed = Arc::new(AtomicU64::new(0));
    let acked_count = Arc::new(AtomicU64::new(0));

    // Producer 1: Immediate messages
    let c1 = Arc::clone(&client);
    let run1 = Arc::clone(&running);
    let p_imm = Arc::clone(&pushed_immediate);
    let prod_imm = tokio::spawn(async move {
        let mut idx = 0;
        while run1.load(Ordering::Relaxed) {
            let mut batch = Vec::with_capacity(20);
            for _ in 0..20 {
                batch.push(Bytes::from(format!("imm-{:08}", idx)));
                idx += 1;
            }
            if c1.push_batch(q, batch).await.is_ok() {
                p_imm.fetch_add(20, Ordering::Relaxed);
            }
            tokio::task::yield_now().await;
        }
    });

    // Producer 2: Delayed messages (1s delay, near horizon)
    let c2 = Arc::clone(&client);
    let run2 = Arc::clone(&running);
    let p_del = Arc::clone(&pushed_delayed);
    let prod_del = tokio::spawn(async move {
        let mut idx = 0;
        while run2.load(Ordering::Relaxed) {
            let mut batch = Vec::with_capacity(20);
            for _ in 0..20 {
                batch.push(Bytes::from(format!("del-{:08}", idx)));
                idx += 1;
            }
            // push with 1 second delay
            if c2.push_batch_with_delay(q, batch, 1).await.is_ok() {
                p_del.fetch_add(20, Ordering::Relaxed);
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    });

    // 4 Pollers concurrently draining and acking
    let mut consumers = Vec::new();
    for _ in 0..4 {
        let c = Arc::clone(&client);
        let a_cnt = Arc::clone(&acked_count);
        let run_c = Arc::clone(&running);
        consumers.push(tokio::spawn(async move {
            while run_c.load(Ordering::Relaxed) {
                if let Ok(msgs) = c.poll(q, 2, 50).await {
                    if msgs.is_empty() {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                        continue;
                    }
                    let count = msgs.len();
                    let acks: Vec<AckItem> = msgs.into_iter().map(|m| AckItem { message_id: m.message_id, receipt_handle: m.receipt_handle }).collect();
                    if c.ack_batch_items(q, acks).await.is_ok() {
                        a_cnt.fetch_add(count as u64, Ordering::Relaxed);
                    }
                }
            }
        }));
    }

    // Soak for 4 seconds under combined immediate + delayed crossfire
    sleep(Duration::from_millis(4000)).await;
    running.store(false, Ordering::Relaxed);

    let _ = prod_imm.await;
    let _ = prod_del.await;
    for c in consumers {
        let _ = c.await;
    }

    let imm_total = pushed_immediate.load(Ordering::Relaxed);
    let del_total = pushed_delayed.load(Ordering::Relaxed);
    let total_pushed = imm_total + del_total;
    let mut total_drained = acked_count.load(Ordering::Relaxed);

    // Sleep 1.5s to let all 1s delayed messages mature past their visible_at horizon
    // Let all delayed messages (1s delay) mature
    sleep(Duration::from_millis(2000)).await;

    // Drain everything remaining with retries
    let drain_start = std::time::Instant::now();
    let mut consecutive_empty = 0;
    while drain_start.elapsed() < Duration::from_secs(15) {
        if let Ok(msgs) = client.poll(q, 2, 500).await {
            if msgs.is_empty() {
                consecutive_empty += 1;
                if consecutive_empty > 20 && total_drained == total_pushed {
                    break;
                }
                sleep(Duration::from_millis(100)).await;
                continue;
            }
            consecutive_empty = 0;
            let count = msgs.len();
            total_drained += count as u64;
            let acks: Vec<AckItem> = msgs.into_iter().map(|m| AckItem { message_id: m.message_id, receipt_handle: m.receipt_handle }).collect();
            let _ = client.ack_batch_items(q, acks).await;
        }
    }

    println!(
        ">>> Churn Soak Result: Total Pushed={}, Drained={}, Missing={}",
        total_pushed,
        total_drained,
        total_pushed.saturating_sub(total_drained)
    );

    assert_eq!(
        total_pushed, total_drained,
        "Every message (both immediate and delayed) must be drained without loss or dups"
    );
}
