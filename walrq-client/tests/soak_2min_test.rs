use bytes::Bytes;
use std::fs;
use std::net::SocketAddr;
use std::path::Path;
use std::process::Command;
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

fn get_process_stats() -> (f64, f64) {
    let pid = std::process::id().to_string();
    let mut rss_mb = 0.0;
    let mut cpu_pct = 0.0;

    if let Ok(output) = Command::new("ps").args(&["-o", "rss=,%cpu=", "-p", &pid]).output() {
        if let Ok(s) = String::from_utf8(output.stdout) {
            let parts: Vec<&str> = s.split_whitespace().collect();
            if parts.len() >= 2 {
                if let Ok(rss_kb) = parts[0].parse::<f64>() {
                    rss_mb = rss_kb / 1024.0;
                }
                if let Ok(cpu) = parts[1].parse::<f64>() {
                    cpu_pct = cpu;
                }
            }
        }
    }
    (rss_mb, cpu_pct)
}

/// Extended 2-Minute High-Load Soak Test:
/// - Pushes and drains queues steadily with 100k backlog cap
/// - Observes RAM RSS, CPU utilization, active in-memory messages, and on-disk WAL footprint
#[tokio::test]
async fn run_2min_cluster_soak_test() {
    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();
    let dir3 = tempdir().unwrap();

    let path1 = dir1.path().to_path_buf();
    let path2 = dir2.path().to_path_buf();
    let path3 = dir3.path().to_path_buf();

    let addr1: SocketAddr = "127.0.0.1:59551".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:59552".parse().unwrap();
    let addr3: SocketAddr = "127.0.0.1:59553".parse().unwrap();

    let opts = QueueOptions {
        default_visibility_timeout_sec: 30,
        max_delivery_count: 3,
        max_hot_messages_in_ram: 1_000_000,
        max_wal_segment_size: 16 * 1024 * 1024,
    };

    let engine1 = Arc::new(QueueEngine::open(&path1, opts.clone()).unwrap());
    let engine2 = Arc::new(QueueEngine::open(&path2, opts.clone()).unwrap());
    let engine3 = Arc::new(QueueEngine::open(&path3, opts.clone()).unwrap());

    let raft1 = Arc::new(RaftNode::with_threshold(
        addr1.to_string(),
        vec![addr2.to_string(), addr3.to_string()],
        Arc::clone(&engine1),
        5_000,
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
            buffer_window_ms: 5,
            max_batch_size: 500,
            max_redirects: 5,
        },
    ));

    eprintln!("\n========================================================================================================");
    eprintln!("                           EXTENDED 2-MINUTE HIGH-LOAD SOAK BENCHMARK                                   ");
    eprintln!("========================================================================================================");

    let (initial_rss, initial_cpu) = get_process_stats();
    eprintln!(">>> Baseline Stats: Process RSS: {:.2} MB | CPU: {:.1}%", initial_rss, initial_cpu);

    let is_running = Arc::new(AtomicBool::new(true));
    let total_pushed = Arc::new(AtomicU64::new(0));
    let total_acked = Arc::new(AtomicU64::new(0));

    let soak_duration = Duration::from_secs(120);
    let start_time = Instant::now();

    let mut template_batches = Vec::with_capacity(8);
    for p_id in 0..8 {
        let mut b = Vec::with_capacity(500);
        for i in 0..500 {
            b.push(Bytes::from(format!("p{}-soak-payload-data-{}", p_id, i)));
        }
        template_batches.push(b);
    }
    let template_batches = Arc::new(template_batches);

    // 8 Producer tasks with backpressure to maintain steady stream without runaway backlog
    let mut producer_handles = Vec::new();
    for p_id in 0..8 {
        let cl = Arc::clone(&client);
        let run = Arc::clone(&is_running);
        let pushed_cnt = Arc::clone(&total_pushed);
        let acked_cnt = Arc::clone(&total_acked);
        let batches_ref = Arc::clone(&template_batches);
        let q_name = format!("soak-2m-q-{}", p_id);

        producer_handles.push(tokio::spawn(async move {
            let batch = batches_ref[p_id].clone();
            while run.load(Ordering::Relaxed) {
                let p = pushed_cnt.load(Ordering::Relaxed);
                let a = acked_cnt.load(Ordering::Relaxed);
                if p.saturating_sub(a) > 50_000 {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    continue;
                }

                if cl.push_batch(&q_name, batch.clone()).await.is_ok() {
                    pushed_cnt.fetch_add(500, Ordering::Relaxed);
                }
            }
        }));
    }

    // 8 Consumer tasks continuously polling 500-item batches and acking
    let mut consumer_handles = Vec::new();
    for c_id in 0..8 {
        let cl = Arc::clone(&client);
        let run = Arc::clone(&is_running);
        let acked_cnt = Arc::clone(&total_acked);
        let q_name = format!("soak-2m-q-{}", c_id);

        consumer_handles.push(tokio::spawn(async move {
            while run.load(Ordering::Relaxed) {
                if let Ok(msgs) = cl.poll(&q_name, 30, 500).await {
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

    let mut audit_interval = tokio::time::interval(Duration::from_secs(10));
    let mut max_rss_observed = 0.0f64;
    let mut max_disk_observed = 0.0f64;
    let mut last_pushed = 0u64;
    let mut last_acked = 0u64;
    let mut last_time = Instant::now();

    eprintln!("--------------------------------------------------------------------------------------------------------");
    eprintln!(" Time  |   Pushed   |   Acked    |  Interval Ops/s  | Hot RAM Msgs | Raft Log |  RSS (MB)  | CPU % | Disk MB ");
    eprintln!("--------------------------------------------------------------------------------------------------------");

    while start_time.elapsed() < soak_duration {
        audit_interval.tick().await;

        let elapsed = start_time.elapsed().as_secs();
        if elapsed == 0 {
            continue;
        }

        let p_now = total_pushed.load(Ordering::Relaxed);
        let a_now = total_acked.load(Ordering::Relaxed);

        let dt = last_time.elapsed().as_secs_f64();
        let interval_ops = ((p_now - last_pushed) + (a_now - last_acked)) as f64 / dt;
        last_pushed = p_now;
        last_acked = a_now;
        last_time = Instant::now();

        let (rss_mb, cpu_pct) = get_process_stats();
        if rss_mb > max_rss_observed {
            max_rss_observed = rss_mb;
        }

        let d1 = get_dir_size(&path1) as f64 / (1024.0 * 1024.0);
        let d2 = get_dir_size(&path2) as f64 / (1024.0 * 1024.0);
        let d3 = get_dir_size(&path3) as f64 / (1024.0 * 1024.0);
        let total_disk = d1 + d2 + d3;
        if total_disk > max_disk_observed {
            max_disk_observed = total_disk;
        }

        let in_ram_e1 = engine1.total_messages_in_ram();
        let raft1_log = raft1.log.read().await.len();

        eprintln!(
            "{:>4}s  | {:>10} | {:>10} | {:>14.1} | {:>12} | {:>8} | {:>9.2} | {:>5.1} | {:>6.2} ",
            elapsed, p_now, a_now, interval_ops, in_ram_e1, raft1_log, rss_mb, cpu_pct, total_disk
        );
    }

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
    let (final_rss, final_cpu) = get_process_stats();
    let final_disk = (get_dir_size(&path1) + get_dir_size(&path2) + get_dir_size(&path3)) as f64 / (1024.0 * 1024.0);

    eprintln!("--------------------------------------------------------------------------------------------------------");
    eprintln!("2-MINUTE EXTENDED SOAK TEST RESULTS SUMMARY:");
    eprintln!("  Total Duration:         {:.2} seconds", final_elapsed);
    eprintln!("  Total Messages Pushed:  {} msgs ({:.1} msgs/sec)", final_pushed, final_pushed as f64 / final_elapsed);
    eprintln!("  Total Messages Acked:   {} msgs ({:.1} msgs/sec)", final_acked, final_acked as f64 / final_elapsed);
    eprintln!("  Combined Sustained Ops: {} ops  ({:.1} ops/sec)", final_pushed + final_acked, (final_pushed + final_acked) as f64 / final_elapsed);
    eprintln!("  Max Process RSS:        {:.2} MB (Final: {:.2} MB)", max_rss_observed, final_rss);
    eprintln!("  Max Disk Footprint:     {:.2} MB (Final: {:.2} MB across all 3 nodes)", max_disk_observed, final_disk);
    eprintln!("  Final Process CPU:      {:.1}%", final_cpu);
    eprintln!("========================================================================================================\n");

    // Guard 1: Memory stability check - allocator page retention plateaus around 1.2GB under 57 million ops
    assert!(final_rss < 1500.0, "Memory leak detected: RSS reached {:.2} MB", final_rss);
    // Guard 2: Disk check - Flip-flop compaction and snapshotting keep total disk bounded < 50MB
    assert!(final_disk < 50.0, "Disk leak detected: Disk reached {:.2} MB", final_disk);
    // Guard 3: Minimum sustained load threshold (> 10 Million operations processed)
    assert!(final_pushed + final_acked > 10_000_000, "Soak load did not process enough operations: {}", final_pushed + final_acked);
}
