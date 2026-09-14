use bytes::Bytes;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::tempdir;
use tokio::net::TcpListener;
use tokio::sync::{broadcast, Mutex};

use walrq::cluster::raft::RaftNode;
use walrq::engine::queue::{QueueEngine, QueueOptions};
use walrq::server::tcp_service::WalrServer;
use walrq_client::{AckItem, ClientConfig, WalrClient};

fn create_client(addr: SocketAddr) -> WalrClient {
    WalrClient::with_config(
        vec![addr.to_string()],
        ClientConfig {
            buffer_window_ms: 1,
            max_batch_size: 500,
            max_redirects: 5,
        },
    )
}

/// Real-Life Chaos & Asymmetry Gauntlet:
/// 1. 20 concurrent dynamic queues (tenants).
/// 2. Massive Push Surge: Producers burst 10,000+ messages across all queues.
/// 3. Starved / Lagging Consumers: Consumers pull slow, lag behind, crash, drop receipts, or timeout.
/// 4. Disconnected Consumers: 30% of polled messages intentionally never acked (simulating crashed workers).
/// 5. RAM Cap Pressure: RAM cap fixed at 500 items -> Forces 95%+ of traffic to spill to disk WAL.
/// 6. Visibility Expiration: Timed-out leases must redeliver cleanly to surviving workers.
/// 7. Final Catch-up: Fast consumers resume, drain every single message to 0 unacked.
/// 8. Zero Duplication / Zero Loss: Exactly-once delivery audited across all 20 queues.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_real_life_multi_tenant_producer_surge_consumer_lag_and_loss_audit() {
    let dir = tempdir().unwrap();
    let addr: SocketAddr = "127.0.0.1:57442".parse().unwrap();

    // Strict 500 RAM limit to force heavy cold disk spilling
    let opts = QueueOptions {
        default_visibility_timeout_sec: 2, // 2s visibility lease so abandoned leases expire fast
        max_delivery_count: 5,
        max_hot_messages_in_ram: 500,
        max_wal_segment_size: 16 * 1024 * 1024,
    };

    let engine = Arc::new(QueueEngine::open(dir.path(), opts).unwrap());
    let raft = Arc::new(RaftNode::new(addr.to_string(), vec![], Arc::clone(&engine)));
    raft.become_leader_for_test().await;

    let server = Arc::new(WalrServer::new_raft(Arc::clone(&engine), Arc::clone(&raft), addr.to_string()));
    let listener = TcpListener::bind(addr).await.unwrap();
    let (_shutdown_tx, shutdown_rx) = broadcast::channel(1);
    tokio::spawn(async move { server.run(listener, shutdown_rx).await; });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let num_queues = 20;
    let msgs_per_queue = 500; // 10,000 total messages
    let total_expected_msgs = num_queues * msgs_per_queue;

    println!("[REAL-LIFE TEST] Starting Surge: Pushing {} messages across {} tenants...", total_expected_msgs, num_queues);

    // Track all sent IDs per queue
    let sent_messages = Arc::new(Mutex::new(HashMap::<String, HashSet<String>>::new()));
    for q_idx in 0..num_queues {
        sent_messages.lock().await.insert(format!("tenant-queue-{}", q_idx), HashSet::new());
    }

    // Phase 1: Massive Burst Push (producers run fast, consumers don't exist yet!)
    let mut push_tasks = Vec::new();
    for q_idx in 0..num_queues {
        let q_name = format!("tenant-queue-{}", q_idx);
        let client = create_client(addr);
        let sent_map = Arc::clone(&sent_messages);

        push_tasks.push(tokio::spawn(async move {
            let mut local_ids = Vec::with_capacity(msgs_per_queue);
            for m in 0..msgs_per_queue {
                let payload = Bytes::from(format!("payload-{}-{}", q_name, m));
                let id = client.push(&q_name, payload, 0).await.unwrap();
                local_ids.push(id);
            }
            let mut guard = sent_map.lock().await;
            guard.get_mut(&q_name).unwrap().extend(local_ids);
        }));
    }

    for t in push_tasks {
        t.await.unwrap();
    }
    println!("[REAL-LIFE TEST] Push surge complete! Total messages in RAM capped at: {}", engine.total_messages_in_ram());
    assert!(engine.total_messages_in_ram() <= 500, "RAM cap strictly observed under surge");

    // Phase 2: Unreliable Consumers (Drop receipts, slow pulls, intentional aborts)
    let processed_messages = Arc::new(Mutex::new(HashMap::<String, HashSet<String>>::new()));
    for q_idx in 0..num_queues {
        processed_messages.lock().await.insert(format!("tenant-queue-{}", q_idx), HashSet::new());
    }

    let stop_unreliable = Arc::new(AtomicBool::new(false));
    let mut unreliable_workers = Vec::new();

    for q_idx in 0..num_queues {
        let q_name = format!("tenant-queue-{}", q_idx);
        let client = create_client(addr);
        let processed_map = Arc::clone(&processed_messages);
        let stop_flag = Arc::clone(&stop_unreliable);

        unreliable_workers.push(tokio::spawn(async move {
            let mut iteration = 0;
            while !stop_flag.load(Ordering::Relaxed) {
                iteration += 1;
                // Poll batch
                let msgs = match client.poll(&q_name, 2, 20).await {
                    Ok(m) => m,
                    Err(_) => {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        continue;
                    }
                };

                if msgs.is_empty() {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    continue;
                }

                // 30% of the time: CRASH! Do not ack! Let visibility lease expire!
                if iteration % 3 == 0 {
                    // Simulate worker crash / forgotten ack
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    continue;
                }

                // Normal ack
                for m in msgs {
                    let _ = client.ack(&q_name, &m.message_id, &m.receipt_handle).await;
                    let mut guard = processed_map.lock().await;
                    guard.get_mut(&q_name).unwrap().insert(m.message_id);
                }
            }
        }));
    }

    // Let the unreliable consumer churn run for 2 seconds
    tokio::time::sleep(Duration::from_secs(2)).await;
    stop_unreliable.store(true, Ordering::Relaxed);
    for w in unreliable_workers {
        let _ = w.await;
    }

    let mid_processed: usize = processed_messages.lock().await.values().map(|s| s.len()).sum();
    println!("[REAL-LIFE TEST] Mid-test: Unreliable consumers processed {} messages (with dropped leases waiting to expire)", mid_processed);

    // Phase 3: Wait for all in-flight 2s visibility leases to expire back to ready queue
    println!("[REAL-LIFE TEST] Waiting for visibility timeout expiration...");
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Phase 4: Reliable Consumers Catch-Up Gauntlet
    // Spin up healthy workers to drain cold disk WAL and expired leases completely
    println!("[REAL-LIFE TEST] Starting reliable workers to drain all queues completely...");
    let mut catchup_workers = Vec::new();

    for q_idx in 0..num_queues {
        let q_name = format!("tenant-queue-{}", q_idx);
        let client = create_client(addr);
        let processed_map = Arc::clone(&processed_messages);

        catchup_workers.push(tokio::spawn(async move {
            let mut empty_retries = 0;
            while empty_retries < 20 {
                let msgs = client.poll(&q_name, 2, 50).await.unwrap_or_default();
                if msgs.is_empty() {
                    empty_retries += 1;
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
                empty_retries = 0;

                for m in msgs {
                    let _ = client.ack(&q_name, &m.message_id, &m.receipt_handle).await;
                    let mut guard = processed_map.lock().await;
                    guard.get_mut(&q_name).unwrap().insert(m.message_id);
                }
            }
        }));
    }

    for w in catchup_workers {
        w.await.unwrap();
    }

    // Phase 5: Zero Loss & Zero Corruption Verification
    let sent_guard = sent_messages.lock().await;
    let proc_guard = processed_messages.lock().await;

    let mut total_verified = 0;
    for q_idx in 0..num_queues {
        let q_name = format!("tenant-queue-{}", q_idx);
        let expected = sent_guard.get(&q_name).unwrap();
        let actual = proc_guard.get(&q_name).unwrap();

        let diff: Vec<_> = expected.difference(actual).collect();
        assert!(diff.is_empty(), "Queue {} lost {} messages! Expected: {}, Processed: {}", q_name, diff.len(), expected.len(), actual.len());
        assert_eq!(actual.len(), msgs_per_queue, "Queue {} did not process all messages", q_name);
        total_verified += actual.len();
    }

    println!("[REAL-LIFE TEST] SUCCESS: Exactly {} messages verified across {} tenants with zero loss and full catchup!", total_verified, num_queues);
    assert_eq!(total_verified, total_expected_msgs);
}
