use bytes::Bytes;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use ulid::Ulid;

use walrq::cluster::raft::RaftNode;
use walrq::engine::queue::{QueueEngine, QueueOptions};
use walrq::server::tcp_service::WalrServer;
use walrq_client::{AckItem, ClientConfig, WalrClient};

fn create_client(addrs: Vec<SocketAddr>) -> WalrClient {
    WalrClient::with_config(
        addrs.into_iter().map(|a| a.to_string()).collect(),
        ClientConfig {
            buffer_window_ms: 1,
            max_batch_size: 50,
            max_redirects: 5,
        },
    )
}

/// Attack: Non-Stop Network Jitter, Strobe Kills, and Ingestion Cyclone
/// Every 80ms, a random node is abruptly killed and resurrected, while 12 concurrent client
/// tasks constantly push batches, poll messages, and ack receipts across 6 queues.
#[tokio::test]
async fn test_cyclone_strobe_kill_resurrection_under_heavy_traffic() {
    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();
    let dir3 = tempdir().unwrap();

    let path1 = dir1.path().to_path_buf();
    let path2 = dir2.path().to_path_buf();
    let path3 = dir3.path().to_path_buf();

    let addr1: SocketAddr = "127.0.0.1:58911".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:58912".parse().unwrap();
    let addr3: SocketAddr = "127.0.0.1:58913".parse().unwrap();

    let opts = QueueOptions {
        default_visibility_timeout_sec: 15,
        max_delivery_count: 5,
        max_hot_messages_in_ram: 100_000,
        max_wal_segment_size: 16 * 1024 * 1024,
    };

    let engine1 = Arc::new(QueueEngine::open(&path1, opts.clone()).unwrap());
    let engine2 = Arc::new(QueueEngine::open(&path2, opts.clone()).unwrap());
    let engine3 = Arc::new(QueueEngine::open(&path3, opts.clone()).unwrap());

    let raft1 = Arc::new(RaftNode::with_threshold(addr1.to_string(), vec![addr2.to_string(), addr3.to_string()], Arc::clone(&engine1), 500));
    let raft2 = Arc::new(RaftNode::with_threshold(addr2.to_string(), vec![addr1.to_string(), addr3.to_string()], Arc::clone(&engine2), 500));
    let raft3 = Arc::new(RaftNode::with_threshold(addr3.to_string(), vec![addr1.to_string(), addr2.to_string()], Arc::clone(&engine3), 500));

    raft1.become_leader_for_test().await;

    let server1 = Arc::new(WalrServer::new_raft(Arc::clone(&engine1), Arc::clone(&raft1), addr1.to_string()));
    let server2 = Arc::new(WalrServer::new_raft(Arc::clone(&engine2), Arc::clone(&raft2), addr2.to_string()));
    let server3 = Arc::new(WalrServer::new_raft(Arc::clone(&engine3), Arc::clone(&raft3), addr3.to_string()));

    let listener1 = TcpListener::bind(addr1).await.unwrap();
    let listener2 = TcpListener::bind(addr2).await.unwrap();
    let listener3 = TcpListener::bind(addr3).await.unwrap();

    let (mut tx1, rx1) = broadcast::channel(1);
    let (mut tx2, rx2) = broadcast::channel(1);
    let (mut tx3, rx3) = broadcast::channel(1);

    tokio::spawn(async move { server1.run(listener1, rx1).await; });
    tokio::spawn(async move { server2.run(listener2, rx2).await; });
    tokio::spawn(async move { server3.run(listener3, rx3).await; });

    raft1.send_heartbeat().await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = Arc::new(create_client(vec![addr1, addr2, addr3]));

    let is_running = Arc::new(AtomicBool::new(true));
    let total_pushed = Arc::new(AtomicU64::new(0));
    let total_acked = Arc::new(AtomicU64::new(0));

    // Spawn 6 concurrent producers
    let mut prod_handles = Vec::new();
    for p_id in 0..6 {
        let cl = Arc::clone(&client);
        let run = Arc::clone(&is_running);
        let pushed_cnt = Arc::clone(&total_pushed);
        let q_name = format!("cyclone-q-{}", p_id);

        prod_handles.push(tokio::spawn(async move {
            let mut seq = 0;
            while run.load(Ordering::Relaxed) {
                let payload = Bytes::from(format!("cyclone-item-{}-{}", p_id, seq));
                if cl.push(&q_name, payload, 0).await.is_ok() {
                    pushed_cnt.fetch_add(1, Ordering::Relaxed);
                    seq += 1;
                } else {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        }));
    }

    // Spawn 6 concurrent consumers
    let mut cons_handles = Vec::new();
    for c_id in 0..6 {
        let cl = Arc::clone(&client);
        let run = Arc::clone(&is_running);
        let acked_cnt = Arc::clone(&total_acked);
        let q_name = format!("cyclone-q-{}", c_id);

        cons_handles.push(tokio::spawn(async move {
            while run.load(Ordering::Relaxed) {
                if let Ok(msgs) = cl.poll(&q_name, 15, 20).await {
                    if !msgs.is_empty() {
                        let ack_items: Vec<AckItem> = msgs.into_iter().map(|m| AckItem {
                            message_id: m.message_id,
                            receipt_handle: m.receipt_handle,
                        }).collect();
                        if let Ok(c) = cl.ack_batch_items(&q_name, ack_items).await {
                            acked_cnt.fetch_add(c as u64, Ordering::Relaxed);
                        }
                    } else {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                } else {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        }));
    }

    // Strobe-Chaos Loop: Induce node flips & re-elections every 80ms for 3 seconds
    let strobe_duration = Duration::from_millis(3000);
    let start_strobe = tokio::time::Instant::now();
    let mut flip = 0;

    while start_strobe.elapsed() < strobe_duration {
        tokio::time::sleep(Duration::from_millis(80)).await;
        match flip % 3 {
            0 => {
                // Kill Node 3 briefly, then revive
                let _ = tx3.send(());
                tokio::time::sleep(Duration::from_millis(20)).await;
                let eng3 = Arc::new(QueueEngine::open(&path3, opts.clone()).unwrap());
                let r3 = Arc::new(RaftNode::with_threshold(addr3.to_string(), vec![addr1.to_string(), addr2.to_string()], Arc::clone(&eng3), 500));
                let s3 = Arc::new(WalrServer::new_raft(Arc::clone(&eng3), Arc::clone(&r3), addr3.to_string()));
                if let Ok(l3) = TcpListener::bind(addr3).await {
                    let (t_new, r_new) = broadcast::channel(1);
                    tx3 = t_new;
                    tokio::spawn(async move { s3.run(l3, r_new).await; });
                }
            }
            1 => {
                // Failover Leader: Node 2 becomes leader
                raft2.recover_as_new_leader().await;
                raft2.send_heartbeat().await;
            }
            2 => {
                // Failover Leader: Node 1 becomes leader
                raft1.recover_as_new_leader().await;
                raft1.send_heartbeat().await;
            }
            _ => unreachable!(),
        }
        flip += 1;
    }

    is_running.store(false, Ordering::Relaxed);
    for h in prod_handles { let _ = h.await; }
    for h in cons_handles { let _ = h.await; }

    let final_pushed = total_pushed.load(Ordering::Relaxed);
    let final_acked = total_acked.load(Ordering::Relaxed);

    eprintln!("Cyclone Strobe Finished: {} pushed, {} acked under 3000ms continuous node death/elections", final_pushed, final_acked);
    assert!(final_pushed > 500, "Should have pushed over 500 messages despite node strobe chaos: got {}", final_pushed);
    assert!(final_acked > 100, "Should have acknowledged over 100 messages during strobe chaos: got {}", final_acked);
}
