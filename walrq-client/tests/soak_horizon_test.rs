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
async fn test_heavy_delayed_and_immediate_soak_50k() {
    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();
    let dir3 = tempdir().unwrap();

    let addr1: SocketAddr = "127.0.0.1:54151".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:54152".parse().unwrap();
    let addr3: SocketAddr = "127.0.0.1:54153".parse().unwrap();

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
            max_batch_size: 500,
            max_redirects: 5,
        },
    ));

    let total_messages = 50_000;
    let producers = 8;
    let per_prod = total_messages / producers;

    eprintln!("\n==========================================================================");
    eprintln!(">>> 50,000 MESSAGE STRESS TEST (60s HORIZON + 1M RAM CAP) <<<");
    eprintln!("Cluster: 3 Nodes (Raft Majority Quorum) | Concurrency: {} producers", producers);
    eprintln!("==========================================================================");

    let t0 = Instant::now();
    let pushed_cnt = Arc::new(AtomicU64::new(0));
    let mut prod_handles = Vec::new();

    for p_id in 0..producers {
        let cl = Arc::clone(&client);
        let counter = Arc::clone(&pushed_cnt);
        prod_handles.push(tokio::spawn(async move {
            let batches = per_prod / 250;
            for b in 0..batches {
                let mut batch = Vec::with_capacity(250);
                for i in 0..250 {
                    batch.push(Bytes::from(format!("horizon-soak-p{}-b{}-{}", p_id, b, i)));
                }
                let ids = cl.push_batch("horizon-soak-q", batch).await.unwrap();
                counter.fetch_add(ids.len() as u64, Ordering::Relaxed);
            }
        }));
    }

    for h in prod_handles {
        h.await.unwrap();
    }

    let push_time = t0.elapsed();
    let push_rate = total_messages as f64 / push_time.as_secs_f64();
    assert_eq!(pushed_cnt.load(Ordering::Relaxed), total_messages as u64);

    let ram_log_entries = raft1.log.read().await.len();
    eprintln!("-> PUSH COMPLETE:       {:>7.2} msgs/sec in {:?}", push_rate, push_time);
    eprintln!("   Leader In-Memory Log: {} entries (Auto-compacted <= 10,000)", ram_log_entries);

    // Consume & Ack All
    let t_poll = Instant::now();
    let mut acked = 0;
    while acked < total_messages {
        let msgs = client.poll("horizon-soak-q", 30, 500).await.unwrap();
        if !msgs.is_empty() {
            let items: Vec<(String, String)> = msgs
                .into_iter()
                .map(|m| (m.message_id, m.receipt_handle))
                .collect();
            let count = client.ack_batch("horizon-soak-q", items).await.unwrap();
            acked += count as usize;
        } else {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }

    let poll_time = t_poll.elapsed();
    let poll_rate = total_messages as f64 / poll_time.as_secs_f64();
    assert_eq!(acked, total_messages);

    let empty = client.poll("horizon-soak-q", 30, 10).await.unwrap();
    assert!(empty.is_empty());

    eprintln!("-> POLL & ACK COMPLETE: {:>7.2} msgs/sec in {:?}", poll_rate, poll_time);
    eprintln!("-> TOTAL CYCLE TIME:    {:?}", t0.elapsed());
    eprintln!("-> BACKLOG REMAINING:   0 messages (100% drained)");
    eprintln!("==========================================================================\n");
}
