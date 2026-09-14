use bytes::Bytes;
use std::fs;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::tempdir;
use tokio::net::TcpListener;
use tokio::sync::broadcast;

use walrq::cluster::raft::RaftNode;
use walrq::engine::queue::{QueueEngine, QueueOptions};
use walrq::server::tcp_service::WalrServer;
use walrq_client::{AckItem, ClientConfig, WalrClient};

fn get_dir_size<P: AsRef<Path>>(path: P) -> u64 {
    let mut total = 0;
    if let Ok(entries) = fs::read_dir(path) {
        for entry in entries.flatten() {
            let p = entry.path();
            if p.is_file() {
                if let Ok(meta) = p.metadata() {
                    total += meta.len();
                }
            } else if p.is_dir() {
                total += get_dir_size(&p);
            }
        }
    }
    total
}

fn get_current_rss_kb() -> usize {
    #[cfg(target_os = "macos")]
    {
        use std::process::Command;
        let pid = std::process::id().to_string();
        if let Ok(output) = Command::new("ps").args(&["-o", "rss=", "-p", &pid]).output() {
            if let Ok(s) = String::from_utf8(output.stdout) {
                if let Ok(kb) = s.trim().parse::<usize>() {
                    return kb;
                }
            }
        }
    }
    0
}

/// Continuous Soak Test:
/// - 3-Node Raft Cluster with in-memory limits and WAL compaction
/// - Continuous pipeline of pushes, polls, and acks across multiple queues
/// - Periodic live audits of RSS memory usage, in-memory queue count, and total on-disk bytes
#[tokio::test]
async fn run_continuous_cluster_soak_test() {
    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();
    let dir3 = tempdir().unwrap();

    let path1 = dir1.path().to_path_buf();
    let path2 = dir2.path().to_path_buf();
    let path3 = dir3.path().to_path_buf();

    let addr1: SocketAddr = "127.0.0.1:59151".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:59152".parse().unwrap();
    let addr3: SocketAddr = "127.0.0.1:59153".parse().unwrap();

    let opts = QueueOptions {
        default_visibility_timeout_sec: 30,
        max_delivery_count: 3,
        max_hot_messages_in_ram: 1_000_000,
        max_wal_segment_size: 16 * 1024 * 1024, // 16MB segments to trigger compaction during soak
    };

    let engine1 = Arc::new(QueueEngine::open(&path1, opts.clone()).unwrap());
    let engine2 = Arc::new(QueueEngine::open(&path2, opts.clone()).unwrap());
    let engine3 = Arc::new(QueueEngine::open(&path3, opts.clone()).unwrap());

    let raft1 = Arc::new(RaftNode::with_threshold(
        addr1.to_string(),
        vec![addr2.to_string(), addr3.to_string()],
        Arc::clone(&engine1),
        5_000, // compact every 5k raft entries
    ));
    let raft2 = Arc::new(RaftNode::with_threshold(
        addr2.to_string(),
        vec![addr1.to_string(), addr3.to_string()],
        Arc::clone(&engine2),
        5_000,
    ));
    let raft3 = Arc::new(RaftNode::with_threshold(
        addr3.to_string(),
        vec![addr1.to_string(), addr2.to_string()],
        Arc::clone(&engine3),
        5_000,
    ));

    raft1.become_leader_for_test().await;

    let server1 = Arc::new(WalrServer::new_raft(Arc::clone(&engine1), Arc::clone(&raft1), addr1.to_string()));
    let server2 = Arc::new(WalrServer::new_raft(Arc::clone(&engine2), Arc::clone(&raft2), addr2.to_string()));
    let server3 = Arc::new(WalrServer::new_raft(Arc::clone(&engine3), Arc::clone(&raft3), addr3.to_string()));

    let listener1 = TcpListener::bind(addr1).await.unwrap();
    let listener2 = TcpListener::bind(addr2).await.unwrap();
    let listener3 = TcpListener::bind(addr3).await.unwrap();

    let (_tx1, rx1) = broadcast::channel(1);
    let (_tx2, rx2) = broadcast::channel(1);
    let (_tx3, rx3) = broadcast::channel(1);

    tokio::spawn(async move { server1.run(listener1, rx1).await; });
    tokio::spawn(async move { server2.run(listener2, rx2).await; });
    tokio::spawn(async move { server3.run(listener3, rx3).await; });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = Arc::new(WalrClient::with_config(
        vec![addr1.to_string()],
        ClientConfig {
            buffer_window_ms: 10,
            max_batch_size: 500,
            max_redirects: 5,
        },
    ));

    eprintln!("\n==========================================================================================");
    eprintln!("                        CONTINUOUS CLUSTER SOAK & RESOURCE AUDIT                          ");
    eprintln!("==========================================================================================");

    let initial_rss = get_current_rss_kb();
    eprintln!(">>> Baseline Process RSS: {:.2} MB", initial_rss as f64 / 1024.0);

    let is_running = Arc::new(AtomicBool::new(true));
    let total_pushed = Arc::new(AtomicU64::new(0));
    let total_acked = Arc::new(AtomicU64::new(0));

    // Target duration: 10 seconds of high-velocity load
    let soak_duration = Duration::from_secs(10);
    let start_time = Instant::now();

    // 4 Producer worker tasks continuously publishing across 4 queues
    let mut producer_handles = Vec::new();
    for p_id in 0..4 {
        let cl = Arc::clone(&client);
        let run = Arc::clone(&is_running);
        let pushed_cnt = Arc::clone(&total_pushed);
        producer_handles.push(tokio::spawn(async move {
            let mut seq = 0u64;
            let q_name = format!("soak-q-{}", p_id);
            while run.load(Ordering::Relaxed) {
                let mut batch = Vec::with_capacity(250);
                for _ in 0..250 {
                    seq += 1;
                    batch.push(Bytes::from(format!("p{}-payload-data-{}", p_id, seq)));
                }
                if cl.push_batch(&q_name, batch).await.is_ok() {
                    pushed_cnt.fetch_add(250, Ordering::Relaxed);
                }
                tokio::task::yield_now().await;
            }
        }));
    }

    // 4 Consumer worker tasks continuously polling and acknowledging
    let mut consumer_handles = Vec::new();
    for c_id in 0..4 {
        let cl = Arc::clone(&client);
        let run = Arc::clone(&is_running);
        let acked_cnt = Arc::clone(&total_acked);
        consumer_handles.push(tokio::spawn(async move {
            let q_name = format!("soak-q-{}", c_id);
            while run.load(Ordering::Relaxed) {
                if let Ok(msgs) = cl.poll(&q_name, 30, 250).await {
                    if !msgs.is_empty() {
                        let items: Vec<AckItem> = msgs
                            .into_iter()
                            .map(|m| AckItem {
                                message_id: m.message_id,
                                receipt_handle: m.receipt_handle,
                            })
                            .collect();
                        if let Ok(c) = cl.ack_batch_items(&q_name, items).await {
                            acked_cnt.fetch_add(c as u64, Ordering::Relaxed);
                        }
                    } else {
                        tokio::task::yield_now().await;
                    }
                }
            }
        }));
    }

    // Monitor loop auditing memory and disk usage every 2 seconds
    let mut checkpoints = Vec::new();
    let mut interval = tokio::time::interval(Duration::from_secs(2));

    while start_time.elapsed() < soak_duration {
        interval.tick().await;

        let elapsed = start_time.elapsed().as_secs_f64();
        let current_rss = get_current_rss_kb() as f64 / 1024.0;
        let d1 = get_dir_size(&path1) as f64 / (1024.0 * 1024.0);
        let d2 = get_dir_size(&path2) as f64 / (1024.0 * 1024.0);
        let d3 = get_dir_size(&path3) as f64 / (1024.0 * 1024.0);
        let total_disk = d1 + d2 + d3;

        let in_ram_e1 = engine1.total_messages_in_ram();
        let raft1_log = raft1.log.read().await.len();
        let p_count = total_pushed.load(Ordering::Relaxed);
        let a_count = total_acked.load(Ordering::Relaxed);

        eprintln!(
            "[{:>4.1}s] Pushed: {:>7} | Acked: {:>7} | Hot RAM Msgs: {:>5} | Raft Log: {:>4} | RSS: {:>5.1} MB | Disk: {:>5.2} MB",
            elapsed, p_count, a_count, in_ram_e1, raft1_log, current_rss, total_disk
        );

        checkpoints.push((current_rss, total_disk, in_ram_e1, raft1_log));
    }

    // Stop workers
    is_running.store(false, Ordering::Relaxed);
    for h in producer_handles {
        let _ = h.await;
    }
    for h in consumer_handles {
        let _ = h.await;
    }

    let final_elapsed = start_time.elapsed().as_secs_f64();
    let final_pushed = total_pushed.load(Ordering::Relaxed);
    let final_acked = total_acked.load(Ordering::Relaxed);
    let final_rss = get_current_rss_kb() as f64 / 1024.0;
    let final_disk = (get_dir_size(&path1) + get_dir_size(&path2) + get_dir_size(&path3)) as f64 / (1024.0 * 1024.0);

    eprintln!("------------------------------------------------------------------------------------------");
    eprintln!("SOAK RESULTS SUMMARY:");
    eprintln!("  Total Duration:     {:.2}s", final_elapsed);
    eprintln!("  Total Pushed:       {} msgs ({:.1} msgs/sec)", final_pushed, final_pushed as f64 / final_elapsed);
    eprintln!("  Total Acked:        {} msgs ({:.1} msgs/sec)", final_acked, final_acked as f64 / final_elapsed);
    eprintln!("  Final Process RSS:  {:.2} MB", final_rss);
    eprintln!("  Final Total Disk:   {:.2} MB across all 3 nodes", final_disk);
    eprintln!("==========================================================================================\n");

    // Memory growth audit: RSS should not balloon uncontrollably
    assert!(final_rss < 500.0, "Memory leak detected: Process RSS exceeded 500MB ({:.2} MB)", final_rss);
    // Disk audit: Flip-flop compaction and snapshot retention should bound disk usage
    assert!(final_disk < 300.0, "Unbounded disk growth detected: Total disk exceeded 300MB ({:.2} MB)", final_disk);
}
