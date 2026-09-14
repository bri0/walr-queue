use bytes::Bytes;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tempfile::tempdir;
use tokio::net::TcpListener;
use tokio::sync::broadcast;

use walrq::cluster::raft::RaftNode;
use walrq::engine::queue::{QueueEngine, QueueOptions};
use walrq::server::tcp_service::WalrServer;
use walrq_client::{ClientConfig, WalrClient};

#[tokio::test]
async fn test_cpu_alloc_benchmark() {
    let dir1 = tempdir().unwrap();
    let addr1: SocketAddr = "127.0.0.1:55151".parse().unwrap();

    let engine1 = Arc::new(QueueEngine::open(dir1.path(), QueueOptions::default()).unwrap());
    let raft1 = Arc::new(RaftNode::new(addr1.to_string(), vec![], Arc::clone(&engine1)));
    raft1.become_leader_for_test().await;

    let server1 = Arc::new(WalrServer::new_raft(engine1, Arc::clone(&raft1), addr1.to_string()));
    let listener1 = TcpListener::bind(addr1).await.unwrap();
    let (_tx1, rx1) = broadcast::channel(1);
    tokio::spawn(async move { server1.run(listener1, rx1).await; });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let client = Arc::new(WalrClient::with_config(
        vec![addr1.to_string()],
        ClientConfig {
            buffer_window_ms: 5,
            max_batch_size: 500,
            max_redirects: 5,
        },
    ));

    let total = 60_000;
    let concurrency = 6;
    let per_worker = total / concurrency;

    let t0 = Instant::now();
    let mut handles = Vec::new();

    for w_id in 0..concurrency {
        let cl = Arc::clone(&client);
        handles.push(tokio::spawn(async move {
            let batches = per_worker / 500;
            for b in 0..batches {
                let mut batch = Vec::with_capacity(500);
                for i in 0..500 {
                    batch.push(Bytes::from(format!("payload-w{}-b{}-item{}", w_id, b, i)));
                }
                cl.push_batch("cpu-bench-q", batch).await.unwrap();
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let elapsed = t0.elapsed();
    let rate = total as f64 / elapsed.as_secs_f64();
    eprintln!("\n==========================================================================");
    eprintln!(">>> CPU / ALLOC BASELINE: {} msgs in {:?} ({:.2} msgs/sec) <<<", total, elapsed, rate);
    eprintln!("==========================================================================\n");
}
