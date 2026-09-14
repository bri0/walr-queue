use bytes::Bytes;
use std::net::SocketAddr;
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

#[tokio::main]
async fn main() {
    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();
    let dir3 = tempdir().unwrap();

    let addr1: SocketAddr = "127.0.0.1:59951".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:59952".parse().unwrap();
    let addr3: SocketAddr = "127.0.0.1:59953".parse().unwrap();

    let opts = QueueOptions {
        default_visibility_timeout_sec: 30,
        max_delivery_count: 3,
        max_hot_messages_in_ram: 1_000_000,
        max_wal_segment_size: 16 * 1024 * 1024,
    };

    let engine1 = Arc::new(QueueEngine::open(dir1.path(), opts.clone()).unwrap());
    let engine2 = Arc::new(QueueEngine::open(dir2.path(), opts.clone()).unwrap());
    let engine3 = Arc::new(QueueEngine::open(dir3.path(), opts.clone()).unwrap());

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

    eprintln!(">>> PROFILING PROFILE BENCHMARK STARTING: 10 SECONDS FULL LOAD <<<");

    let is_running = Arc::new(AtomicBool::new(true));
    let total_pushed = Arc::new(AtomicU64::new(0));
    let total_acked = Arc::new(AtomicU64::new(0));

    let run_duration = Duration::from_secs(10);
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

    // 8 Producer tasks
    let mut producer_handles = Vec::new();
    for p_id in 0..8 {
        let cl = Arc::clone(&client);
        let run = Arc::clone(&is_running);
        let pushed_cnt = Arc::clone(&total_pushed);
        let acked_cnt = Arc::clone(&total_acked);
        let batches_ref = Arc::clone(&template_batches);
        let q_name = format!("flame-q-{}", p_id);

        producer_handles.push(tokio::spawn(async move {
            let batch = batches_ref[p_id].clone();
            while run.load(Ordering::Relaxed) {
                let p = pushed_cnt.load(Ordering::Relaxed);
                let a = acked_cnt.load(Ordering::Relaxed);
                if p.saturating_sub(a) > 50_000 {
                    tokio::time::sleep(Duration::from_millis(2)).await;
                    continue;
                }

                if cl.push_batch(&q_name, batch.clone()).await.is_ok() {
                    pushed_cnt.fetch_add(500, Ordering::Relaxed);
                }
            }
        }));
    }

    // 8 Consumer tasks
    let mut consumer_handles = Vec::new();
    for c_id in 0..8 {
        let cl = Arc::clone(&client);
        let run = Arc::clone(&is_running);
        let acked_cnt = Arc::clone(&total_acked);
        let q_name = format!("flame-q-{}", c_id);

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

    tokio::time::sleep(run_duration).await;
    is_running.store(false, Ordering::Relaxed);

    for h in producer_handles {
        let _ = h.await;
    }
    for h in consumer_handles {
        let _ = h.await;
    }

    let elapsed = start_time.elapsed().as_secs_f64();
    let pushed = total_pushed.load(Ordering::Relaxed);
    let acked = total_acked.load(Ordering::Relaxed);

    eprintln!(">>> PROFILING RUN COMPLETED: {} msgs in {:.2}s ({:.1} ops/sec) <<<", pushed + acked, elapsed, (pushed + acked) as f64 / elapsed);
}
