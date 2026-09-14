use bytes::Bytes;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tempfile::tempdir;
use tokio::net::TcpListener;
use tokio::sync::broadcast;

use walrq::cluster::raft::RaftNode;
use walrq::engine::queue::{QueueEngine, QueueOptions};
use walrq::server::tcp_service::WalrServer;
use walrq_client::WalrClient;

#[tokio::test]
async fn benchmark_parallel_client_throughput() {
    let dir1 = tempdir().unwrap();
    let addr1: SocketAddr = "127.0.0.1:58151".parse().unwrap();
    let engine1 = Arc::new(QueueEngine::open(dir1.path(), QueueOptions::default()).unwrap());
    let raft1 = Arc::new(RaftNode::new(addr1.to_string(), vec![], Arc::clone(&engine1)));
    raft1.become_leader_for_test().await;

    let server1 = Arc::new(WalrServer::new_raft(engine1, Arc::clone(&raft1), addr1.to_string()));
    let listener1 = TcpListener::bind(addr1).await.unwrap();
    let (_tx1, rx1) = broadcast::channel(1);
    tokio::spawn(async move { server1.run(listener1, rx1).await; });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let client = Arc::new(WalrClient::new(vec![addr1.to_string()]));

    let num_tasks = 16;
    let msgs_per_task = 1000;
    let total_msgs = num_tasks * msgs_per_task;

    let t0 = Instant::now();
    let mut handles = Vec::new();

    for t in 0..num_tasks {
        let cl = Arc::clone(&client);
        handles.push(tokio::spawn(async move {
            let q_name = format!("bench-q-{}", t % 4);
            for i in 0..msgs_per_task {
                cl.push_immediate(&q_name, Bytes::from(format!("payload-{}-{}", t, i)), 0)
                    .await
                    .unwrap();
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let dur = t0.elapsed();
    let rate = total_msgs as f64 / dur.as_secs_f64();
    eprintln!("\n>>> 16-WORKER CONCURRENT PUSH: {} msgs in {:?} ({:.2} msgs/s) <<<\n", total_msgs, dur, rate);
}
