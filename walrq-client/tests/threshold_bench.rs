use bytes::Bytes;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::tempdir;
use tokio::net::TcpListener;
use tokio::sync::broadcast;

use walrq::cluster::raft::RaftNode;
use walrq::engine::queue::{QueueEngine, QueueOptions};
use walrq::server::tcp_service::WalrServer;
use walrq_client::{ClientConfig, WalrClient};

#[tokio::test]
async fn benchmark_compaction_thresholds() {
    let thresholds = vec![1_000, 5_000, 10_000, 25_000, 50_000, 100_000];
    let total_messages = 50_000;

    eprintln!("\n=========================================================================");
    eprintln!(">>> BENCHMARKING RAFT LOG IN-MEMORY COMPACTION THRESHOLDS <<<");
    eprintln!("Testing total {} messages per threshold...", total_messages);
    eprintln!("=========================================================================");

    for (idx, threshold) in thresholds.into_iter().enumerate() {
        let dir1 = tempdir().unwrap();
        let dir2 = tempdir().unwrap();
        let dir3 = tempdir().unwrap();

        let port_base = 53100 + (idx as u16 * 10);
        let addr1: SocketAddr = format!("127.0.0.1:{}", port_base).parse().unwrap();
        let addr2: SocketAddr = format!("127.0.0.1:{}", port_base + 1).parse().unwrap();
        let addr3: SocketAddr = format!("127.0.0.1:{}", port_base + 2).parse().unwrap();

        let engine1 = Arc::new(QueueEngine::open(dir1.path(), QueueOptions::default()).unwrap());
        let engine2 = Arc::new(QueueEngine::open(dir2.path(), QueueOptions::default()).unwrap());
        let engine3 = Arc::new(QueueEngine::open(dir3.path(), QueueOptions::default()).unwrap());

        let raft1 = Arc::new(RaftNode::with_threshold(addr1.to_string(), vec![addr2.to_string(), addr3.to_string()], Arc::clone(&engine1), threshold));
        let raft2 = Arc::new(RaftNode::with_threshold(addr2.to_string(), vec![addr1.to_string(), addr3.to_string()], Arc::clone(&engine2), threshold));
        let raft3 = Arc::new(RaftNode::with_threshold(addr3.to_string(), vec![addr1.to_string(), addr2.to_string()], Arc::clone(&engine3), threshold));

        raft1.become_leader_for_test().await;

        let server1 = Arc::new(WalrServer::new_raft(engine1, Arc::clone(&raft1), addr1.to_string()));
        let server2 = Arc::new(WalrServer::new_raft(engine2, Arc::clone(&raft2), addr2.to_string()));
        let server3 = Arc::new(WalrServer::new_raft(engine3, Arc::clone(&raft3), addr3.to_string()));

        let listener1 = TcpListener::bind(addr1).await.unwrap();
        let listener2 = TcpListener::bind(addr2).await.unwrap();
        let listener3 = TcpListener::bind(addr3).await.unwrap();

        let (_tx1, rx1) = broadcast::channel(1);
        let (_tx2, rx2) = broadcast::channel(1);
        let (_tx3, rx3) = broadcast::channel(1);

        tokio::spawn(async move { server1.run(listener1, rx1).await; });
        tokio::spawn(async move { server2.run(listener2, rx2).await; });
        tokio::spawn(async move { server3.run(listener3, rx3).await; });

        tokio::time::sleep(Duration::from_millis(30)).await;

        let client = WalrClient::with_config(
            vec![addr1.to_string()],
            ClientConfig {
                buffer_window_ms: 5,
                max_batch_size: 500,
                max_redirects: 5,
            },
        );

        let t0 = Instant::now();
        let batches = total_messages / 500;
        let mut sample_batch = Vec::with_capacity(500);
        for i in 0..500 {
            sample_batch.push(Bytes::from(format!("thresh-item-{}", i)));
        }

        for _ in 0..batches {
            client.push_batch("bench-thresh-q", sample_batch.clone()).await.unwrap();
        }

        let push_time = t0.elapsed();
        let push_rate = total_messages as f64 / push_time.as_secs_f64();
        let final_ram_entries = raft1.log.read().await.len();

        // Memory estimate: ~128 bytes per LogEntry in RAM
        let est_ram_kb = (final_ram_entries * 128) / 1024;

        eprintln!(
            "Threshold: {:>6} | Time: {:>6.2}ms | Throughput: {:>8.2} msg/s | In-Memory Entries: {:>6} (~{:>4} KB)",
            threshold,
            push_time.as_secs_f64() * 1000.0,
            push_rate,
            final_ram_entries,
            est_ram_kb
        );
    }
    eprintln!("=========================================================================\n");
}
