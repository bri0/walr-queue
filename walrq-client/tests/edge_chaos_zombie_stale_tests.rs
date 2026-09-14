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
async fn test_high_intensity_stale_ack_and_zombie_chaos() {
    let dir = tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let opts = QueueOptions {
        default_visibility_timeout_sec: 1, // 1s quick visibility turnover
        max_delivery_count: 5,
        max_hot_messages_in_ram: 200,
        max_wal_segment_size: 16 * 1024 * 1024,
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
            max_batch_size: 50,
            max_redirects: 5,
        },
    ));

    let q = "chaos_zombie_q";
    let num_messages = 3_000;

    // Push 3k messages in batches of 50
    for chunk in 0..(num_messages / 50) {
        let mut batch = Vec::with_capacity(50);
        for i in 0..50 {
            batch.push(Bytes::from(format!("chaos-payload-{}-{}", chunk, i)));
        }
        client.push_batch(q, batch).await.unwrap();
    }

    let running = Arc::new(AtomicBool::new(true));
    let valid_acks = Arc::new(AtomicU64::new(0));
    let stale_rejections = Arc::new(AtomicU64::new(0));

    // Stale ACK graveyard channel: pollers send zombie receipts here to be acked late
    let (zombie_tx, mut zombie_rx) = tokio::sync::mpsc::channel::<(String, String)>(10_000);

    // Zombie replayer task: furiously attempts to ACK old/stale receipts
    let c_zombie = Arc::clone(&client);
    let run_zombie = Arc::clone(&running);
    let rej_count = Arc::clone(&stale_rejections);
    let zombie_handle = tokio::spawn(async move {
        while run_zombie.load(Ordering::Relaxed) || !zombie_rx.is_empty() {
            match tokio::time::timeout(Duration::from_millis(50), zombie_rx.recv()).await {
                Ok(Some((mid, handle))) => {
                    tokio::time::sleep(Duration::from_millis(15)).await;
                    if let Ok(ack_ok) = c_zombie.ack(q, &mid, &handle).await {
                        if !ack_ok {
                            rej_count.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                _ => {}
            }
        }
    });

    // 4 concurrent pollers with simulated zombie drops
    let mut poller_handles = Vec::new();
    let counter = Arc::new(AtomicU64::new(0));
    for _ in 0..4 {
        let c = Arc::clone(&client);
        let z_tx = zombie_tx.clone();
        let val_ack = Arc::clone(&valid_acks);
        let run_p = Arc::clone(&running);
        let local_ctr = Arc::clone(&counter);

        poller_handles.push(tokio::spawn(async move {
            while run_p.load(Ordering::Relaxed) {
                let msgs = match c.poll(q, 1, 20).await {
                    Ok(m) => m,
                    Err(_) => continue,
                };
                if msgs.is_empty() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    continue;
                }

                for msg in msgs {
                    let n = local_ctr.fetch_add(1, Ordering::Relaxed);
                    if n % 5 == 0 {
                        // 20% of the time: simulate delayed zombie ack
                        let _ = z_tx.send((msg.message_id.clone(), msg.receipt_handle.clone())).await;
                    } else {
                        // 80% of the time: prompt valid ACK
                        if let Ok(true) = c.ack(q, &msg.message_id, &msg.receipt_handle).await {
                            val_ack.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            }
        }));
    }

    // Run chaos for 3.5 seconds
    tokio::time::sleep(Duration::from_millis(3500)).await;
    running.store(false, Ordering::Relaxed);

    for h in poller_handles {
        let _ = h.await;
    }
    drop(zombie_tx);
    let _ = zombie_handle.await;

    println!(
        ">>> Chaos Finished: Valid ACKs={}, Stale Rejections={}",
        valid_acks.load(Ordering::Relaxed),
        stale_rejections.load(Ordering::Relaxed)
    );

    // Drain remaining messages gracefully
    let mut remaining_drained = 0;
    for _ in 0..50 {
        let msgs = client.poll(q, 5, 50).await.unwrap_or_default();
        if msgs.is_empty() {
            tokio::time::sleep(Duration::from_millis(50)).await;
            continue;
        }
        let count = msgs.len();
        remaining_drained += count;
        let acks: Vec<AckItem> = msgs.into_iter().map(|m| AckItem { message_id: m.message_id, receipt_handle: m.receipt_handle }).collect();
        let _ = client.ack_batch_items(q, acks).await;
    }

    // Also check DLQ for messages that exceeded max_delivery_count during zombie chaos
    let dlq_name = format!("{}.dlq", q);
    let mut dlq_drained = 0;
    for _ in 0..50 {
        let msgs = client.poll(&dlq_name, 5, 50).await.unwrap_or_default();
        if msgs.is_empty() {
            break;
        }
        dlq_drained += msgs.len();
        let acks: Vec<AckItem> = msgs.into_iter().map(|m| AckItem { message_id: m.message_id, receipt_handle: m.receipt_handle }).collect();
        let _ = client.ack_batch_items(&dlq_name, acks).await;
    }

    println!(
        "Drained remaining: {} main, {} DLQ. Total accounted = {}",
        remaining_drained,
        dlq_drained,
        valid_acks.load(Ordering::Relaxed) + remaining_drained as u64 + dlq_drained as u64
    );

    assert!(stale_rejections.load(Ordering::Relaxed) > 0, "Zombie chaos must have triggered stale rejections");
}
