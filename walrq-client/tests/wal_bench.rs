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

/// Benchmark testing unbatched single pushes hitting the WAL
#[tokio::test]
async fn benchmark_unbatched_wal_flush() {
    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();
    let dir3 = tempdir().unwrap();

    let addr1: SocketAddr = "127.0.0.1:57151".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:57152".parse().unwrap();
    let addr3: SocketAddr = "127.0.0.1:57153".parse().unwrap();

    let engine1 = Arc::new(QueueEngine::open(dir1.path(), QueueOptions::default()).unwrap());
    let engine2 = Arc::new(QueueEngine::open(dir2.path(), QueueOptions::default()).unwrap());
    let engine3 = Arc::new(QueueEngine::open(dir3.path(), QueueOptions::default()).unwrap());

    let raft1 = Arc::new(RaftNode::new(addr1.to_string(), vec![addr2.to_string(), addr3.to_string()], Arc::clone(&engine1)));
    let raft2 = Arc::new(RaftNode::new(addr2.to_string(), vec![addr1.to_string(), addr3.to_string()], Arc::clone(&engine2)));
    let raft3 = Arc::new(RaftNode::new(addr3.to_string(), vec![addr1.to_string(), addr2.to_string()], Arc::clone(&engine3)));

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

    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = Arc::new(WalrClient::new(vec![addr1.to_string()]));

    let total_unbatched = 10_000;
    let concurrency = 8;
    let per_worker = total_unbatched / concurrency;

    let t0 = Instant::now();
    let mut handles = Vec::new();

    for w_id in 0..concurrency {
        let cl = Arc::clone(&client);
        handles.push(tokio::spawn(async move {
            for i in 0..per_worker {
                cl.push_immediate("wal-bench-q", Bytes::from(format!("w{}-{}", w_id, i)), 0)
                    .await
                    .unwrap();
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let elapsed = t0.elapsed();
    let rate = total_unbatched as f64 / elapsed.as_secs_f64();
    eprintln!("\n==========================================================================");
    eprintln!(">>> UNBATCHED CONCURRENT WAL FLUSH: {} msgs in {:?} ({:.2} msgs/sec) <<<", total_unbatched, elapsed, rate);
    eprintln!("==========================================================================\n");
}
