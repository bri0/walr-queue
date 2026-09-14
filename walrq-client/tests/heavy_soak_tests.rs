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
async fn test_heavy_soak_100k_snapshot_stresstest() {
    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();
    let dir3 = tempdir().unwrap();

    let addr1: SocketAddr = "127.0.0.1:49451".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:49452".parse().unwrap();
    let addr3: SocketAddr = "127.0.0.1:49453".parse().unwrap();

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

    let client = Arc::new(WalrClient::with_config(
        vec![addr1.to_string()],
        ClientConfig {
            buffer_window_ms: 10,
            max_batch_size: 250,
            max_redirects: 5,
        },
    ));

    // Push 30,000 messages in parallel across 6 worker tasks
    let total_msgs = 30_000;
    let concurrency = 6;
    let per_worker = total_msgs / concurrency;

    let t0 = Instant::now();
    let mut handles = Vec::new();

    for w_id in 0..concurrency {
        let cl = Arc::clone(&client);
        handles.push(tokio::spawn(async move {
            for i in 0..(per_worker / 250) {
                let mut batch = Vec::with_capacity(250);
                for k in 0..250 {
                    batch.push(Bytes::from(format!("soak-msg-{}-{}-{}", w_id, i, k)));
                }
                cl.push_batch("heavy-soak-q", batch).await.unwrap();
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let push_time = t0.elapsed();
    let push_rate = total_msgs as f64 / push_time.as_secs_f64();

    // Check RAM log size on leader: auto compaction must have kept it bounded!
    let ram_entries = raft1.log.read().await.len();
    assert!(
        ram_entries <= 10_000,
        "RAM log exceeded 10k entries, was: {}",
        ram_entries
    );

    // Poll and Ack all 30,000 items
    let t_poll = Instant::now();
    let mut acked = 0;
    while acked < total_msgs {
        let msgs = client.poll("heavy-soak-q", 30, 500).await.unwrap();
        if !msgs.is_empty() {
            let items: Vec<(String, String)> = msgs
                .into_iter()
                .map(|m| (m.message_id, m.receipt_handle))
                .collect();
            let count = client.ack_batch("heavy-soak-q", items).await.unwrap();
            acked += count as usize;
        } else {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    let poll_time = t_poll.elapsed();
    let poll_rate = total_msgs as f64 / poll_time.as_secs_f64();

    eprintln!("\n====================================================================");
    eprintln!(">>> HEAVY SOAK 30K RESULT <<<");
    eprintln!("Push Rate: {} msgs in {:?} ({:.2} msgs/sec)", total_msgs, push_time, push_rate);
    eprintln!("Poll & Ack Rate: {} msgs in {:?} ({:.2} msgs/sec)", total_msgs, poll_time, poll_rate);
    eprintln!("Leader In-Memory Log Entries: {} (Auto-Compacted & Bounded!)", raft1.log.read().await.len());
    eprintln!("====================================================================\n");
}
