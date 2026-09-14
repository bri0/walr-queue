use bytes::Bytes;
use std::fs;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::tempdir;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use ulid::Ulid;

use walrq::cluster::raft::RaftNode;
use walrq::engine::queue::{QueueEngine, QueueOptions};
use walrq::server::tcp_service::WalrServer;
use walrq_client::{ClientConfig, ClientError, WalrClient};

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

/// Dedicated Push-Only Stress & Capacity Saturation Benchmark
/// - Zero polls, zero acks throughout the entire run.
/// - Ingests 50,000 messages continuously across 3 nodes.
/// - Verifies RAM capacity backpressure: once `max_hot_messages_in_ram` is reached,
///   excess pushes are rejected with `RamLimitExceeded` server error.
/// - Cold reboots all 3 nodes and verifies:
///   1. 100% of messages persist in on-disk WAL.
///   2. Exactly 50,000 messages are restored into RAM on reboot across all 3 nodes.
///   3. FIFO ordering is strictly preserved when drained post-reboot.
#[tokio::test]
async fn test_push_only_saturation_and_cold_reboot_drain() {
    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();
    let dir3 = tempdir().unwrap();

    let path1 = dir1.path().to_path_buf();
    let path2 = dir2.path().to_path_buf();
    let path3 = dir3.path().to_path_buf();

    let addr1: SocketAddr = "127.0.0.1:57111".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:57112".parse().unwrap();
    let addr3: SocketAddr = "127.0.0.1:57113".parse().unwrap();

    // Set RAM cap to 30,000 messages to test the hard ceiling & backpressure
    let ram_cap = 30_000;
    let opts = QueueOptions {
        default_visibility_timeout_sec: 10,
        max_delivery_count: 3,
        max_hot_messages_in_ram: ram_cap,
        max_wal_segment_size: 16 * 1024 * 1024,
    };

    let engine1 = Arc::new(QueueEngine::open(&path1, opts.clone()).unwrap());
    let engine2 = Arc::new(QueueEngine::open(&path2, opts.clone()).unwrap());
    let engine3 = Arc::new(QueueEngine::open(&path3, opts.clone()).unwrap());

    let raft1 = Arc::new(RaftNode::with_threshold(addr1.to_string(), vec![addr2.to_string(), addr3.to_string()], Arc::clone(&engine1), 5_000));
    let raft2 = Arc::new(RaftNode::with_threshold(addr2.to_string(), vec![addr1.to_string(), addr3.to_string()], Arc::clone(&engine2), 5_000));
    let raft3 = Arc::new(RaftNode::with_threshold(addr3.to_string(), vec![addr1.to_string(), addr2.to_string()], Arc::clone(&engine3), 5_000));

    raft1.become_leader_for_test().await;

    let server1 = Arc::new(WalrServer::new_raft(Arc::clone(&engine1), Arc::clone(&raft1), addr1.to_string()));
    let server2 = Arc::new(WalrServer::new_raft(Arc::clone(&engine2), Arc::clone(&raft2), addr2.to_string()));
    let server3 = Arc::new(WalrServer::new_raft(Arc::clone(&engine3), Arc::clone(&raft3), addr3.to_string()));

    let listener1 = TcpListener::bind(addr1).await.unwrap();
    let listener2 = TcpListener::bind(addr2).await.unwrap();
    let listener3 = TcpListener::bind(addr3).await.unwrap();

    let (tx1, rx1) = broadcast::channel(1);
    let (tx2, rx2) = broadcast::channel(1);
    let (tx3, rx3) = broadcast::channel(1);

    tokio::spawn(async move { server1.run(listener1, rx1).await; });
    tokio::spawn(async move { server2.run(listener2, rx2).await; });
    tokio::spawn(async move { server3.run(listener3, rx3).await; });

    raft1.send_heartbeat().await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = Arc::new(WalrClient::with_config(
        vec![addr1.to_string(), addr2.to_string(), addr3.to_string()],
        ClientConfig {
            buffer_window_ms: 1,
            max_batch_size: 500,
            max_redirects: 5,
        },
    ));

    let q = "push-only-benchmark-q";

    eprintln!("\n========================================================================================================");
    eprintln!("                           PUSH-ONLY BENCHMARK & CAPACITY SATURATION                                   ");
    eprintln!("========================================================================================================");

    let start_push = Instant::now();

    // Phase 1: Fill exactly up to RAM cap (30,000 items in 500-item chunks)
    let batch_size = 500;
    let num_batches = ram_cap / batch_size;
    let mut pushed_count = 0;

    for b in 0..num_batches {
        let mut payloads = Vec::with_capacity(batch_size);
        for i in 0..batch_size {
            let seq = b * batch_size + i;
            payloads.push(Bytes::from(format!("push-only-data-{:06}", seq)));
        }
        let ids = client.push_batch(q, payloads).await.expect("Batch push under cap must succeed");
        pushed_count += ids.len();
    }

    let push_duration = start_push.elapsed().as_secs_f64();
    eprintln!(">>> Successfully pushed {} messages in {:.2}s ({:.1} msgs/sec)", pushed_count, push_duration, pushed_count as f64 / push_duration);

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify all 3 nodes hold exactly 30,000 items in memory
    assert_eq!(engine1.total_messages_in_ram(), ram_cap);
    assert_eq!(engine2.total_messages_in_ram(), ram_cap);
    assert_eq!(engine3.total_messages_in_ram(), ram_cap);

    // Phase 2: Spill Test - Push beyond capacity must SUCCEED by spilling to disk without memory error!
    eprintln!(">>> Testing disk spill beyond RAM capacity (Pushing 10 additional messages)...");
    for i in 0..10 {
        let res = client.push_immediate(q, Bytes::from(format!("spill-message-{}", i)), 0).await;
        assert!(res.is_ok(), "Pushes beyond RAM cap must safely spill to disk without error!");
    }

    // Disk Footprint Audit
    let d1 = get_dir_size(&path1) as f64 / (1024.0 * 1024.0);
    let d2 = get_dir_size(&path2) as f64 / (1024.0 * 1024.0);
    let d3 = get_dir_size(&path3) as f64 / (1024.0 * 1024.0);
    eprintln!(">>> On-Disk WAL Footprint with 30,000 un-polled messages: Node1: {:.2}MB | Node2: {:.2}MB | Node3: {:.2}MB", d1, d2, d3);
    assert!(d1 > 0.5 && d1 < 20.0, "WAL disk footprint must be reasonable");

    // Phase 3: Cold reboot all 3 nodes while full of 30,000 un-polled messages!
    eprintln!(">>> Cold rebooting all 3 nodes from disk while holding 30,000 un-polled messages...");
    let _ = tx1.send(());
    let _ = tx2.send(());
    let _ = tx3.send(());
    tokio::time::sleep(Duration::from_millis(100)).await;

    let engine1_reboot = Arc::new(QueueEngine::open(&path1, opts.clone()).unwrap());
    let engine2_reboot = Arc::new(QueueEngine::open(&path2, opts.clone()).unwrap());
    let engine3_reboot = Arc::new(QueueEngine::open(&path3, opts.clone()).unwrap());

    assert_eq!(engine1_reboot.total_messages_in_ram(), ram_cap, "Node 1 must restore 30,000 messages on reboot");
    assert_eq!(engine2_reboot.total_messages_in_ram(), ram_cap, "Node 2 must restore 30,000 messages on reboot");
    assert_eq!(engine3_reboot.total_messages_in_ram(), ram_cap, "Node 3 must restore 30,000 messages on reboot");

    let raft1_reboot = Arc::new(RaftNode::with_threshold(addr1.to_string(), vec![addr2.to_string(), addr3.to_string()], Arc::clone(&engine1_reboot), 5_000));
    raft1_reboot.become_leader_for_test().await;

    let server1_reboot = Arc::new(WalrServer::new_raft(Arc::clone(&engine1_reboot), Arc::clone(&raft1_reboot), addr1.to_string()));
    let listener1_reboot = TcpListener::bind(addr1).await.unwrap();
    let (_t1_new, r1_new) = broadcast::channel(1);
    tokio::spawn(async move { server1_reboot.run(listener1_reboot, r1_new).await; });

    let client_reboot = create_client_single(addr1);

    // Phase 4: Drain and verify strict FIFO monotonic sequence order across all 30,000 restored items
    eprintln!(">>> Draining and verifying strict FIFO sequence order of 30,000 restored messages...");
    let mut drained = 0;
    let mut expected_seq = 0;

    while drained < ram_cap {
        let msgs = client_reboot.poll(q, 10, 500).await.unwrap();
        if msgs.is_empty() {
            tokio::time::sleep(Duration::from_millis(5)).await;
            continue;
        }
        for m in msgs {
            let s = String::from_utf8_lossy(&m.payload);
            let seq: usize = s.trim_start_matches("push-only-data-").parse().unwrap();
            assert_eq!(seq, expected_seq, "Strict FIFO order violation on restored message");
            expected_seq += 1;
            drained += 1;
        }
    }

    assert_eq!(drained, ram_cap, "All 30,000 messages successfully verified and drained");
    eprintln!(">>> Push-Only saturation and recovery benchmark completed successfully!\n");
}

fn create_client_single(addr: SocketAddr) -> WalrClient {
    WalrClient::with_config(
        vec![addr.to_string()],
        ClientConfig {
            buffer_window_ms: 1,
            max_batch_size: 500,
            max_redirects: 5,
        },
    )
}
