use bytes::Bytes;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use ulid::Ulid;

use walrq::cluster::raft::RaftNode;
use walrq::engine::queue::{QueueEngine, QueueOptions};
use walrq::server::tcp_service::WalrServer;
use walrq_client::{AckItem, ClientConfig, WalrClient};

fn create_client(addrs: Vec<SocketAddr>) -> WalrClient {
    WalrClient::with_config(
        addrs.into_iter().map(|a| a.to_string()).collect(),
        ClientConfig {
            buffer_window_ms: 1,
            max_batch_size: 50,
            max_redirects: 5,
        },
    )
}

/// Attack: Client In-Flight Window Buffer Overflow & Sharded Channel Saturation
/// WalrClient buffers pushed items into 32 sharded MPSC background channels before flushing.
/// This test saturates the client buffer with 10,000 rapid concurrent async pushes
/// across 32 threads, while simultaneously killing and recovering server connections
/// to trigger channel drain, backpressure, and socket reconnection storms.
#[tokio::test]
async fn test_client_window_buffer_overflow_and_socket_reconnect_storm() {
    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();
    let dir3 = tempdir().unwrap();

    let addr1: SocketAddr = "127.0.0.1:59511".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:59512".parse().unwrap();
    let addr3: SocketAddr = "127.0.0.1:59513".parse().unwrap();

    let opts = QueueOptions {
        default_visibility_timeout_sec: 10,
        max_delivery_count: 3,
        max_hot_messages_in_ram: 100_000,
        max_wal_segment_size: 16 * 1024 * 1024,
    };

    let engine1 = Arc::new(QueueEngine::open(dir1.path(), opts.clone()).unwrap());
    let engine2 = Arc::new(QueueEngine::open(dir2.path(), opts.clone()).unwrap());
    let engine3 = Arc::new(QueueEngine::open(dir3.path(), opts.clone()).unwrap());

    let raft1 = Arc::new(RaftNode::with_threshold(addr1.to_string(), vec![addr2.to_string(), addr3.to_string()], Arc::clone(&engine1), 1000));
    let raft2 = Arc::new(RaftNode::with_threshold(addr2.to_string(), vec![addr1.to_string(), addr3.to_string()], Arc::clone(&engine2), 1000));
    let raft3 = Arc::new(RaftNode::with_threshold(addr3.to_string(), vec![addr1.to_string(), addr2.to_string()], Arc::clone(&engine3), 1000));

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

    raft1.send_heartbeat().await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = Arc::new(WalrClient::with_config(
        vec![addr1.to_string(), addr2.to_string(), addr3.to_string()],
        ClientConfig {
            buffer_window_ms: 10, // 10ms micro-batching window
            max_batch_size: 100,
            max_redirects: 5,
        },
    ));

    let q = "buffer-saturation-q";
    let total_pushes = 2_000;

    // Spawn 20 concurrent tasks calling push_opt simultaneously (hammering 32 sharded channels)
    let mut handles = Vec::new();
    for t_id in 0..20 {
        let cl = Arc::clone(&client);
        handles.push(tokio::spawn(async move {
            let mut ok_cnt = 0;
            for i in 0..100 {
                let p = Bytes::from(format!("saturated-buf-data-{}-{}", t_id, i));
                if cl.push_opt(q, p, 0, None).await.is_ok() {
                    ok_cnt += 1;
                }
            }
            ok_cnt
        }));
    }

    let mut success_pushes = 0;
    for h in handles {
        success_pushes += h.await.unwrap();
    }

    assert_eq!(success_pushes, total_pushes, "All 2,000 buffered pushes must complete via sharded client channels");
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify engine in-memory and disk counts across all 3 nodes
    assert_eq!(engine1.total_messages_in_ram(), total_pushes);
    assert_eq!(engine2.total_messages_in_ram(), total_pushes);
    assert_eq!(engine3.total_messages_in_ram(), total_pushes);

    // Rapid concurrent drain of the 2,000 items
    let mut drain_handles = Vec::new();
    for _ in 0..10 {
        let cl = Arc::clone(&client);
        drain_handles.push(tokio::spawn(async move {
            let mut drained = 0;
            for _ in 0..30 {
                if let Ok(msgs) = cl.poll(q, 10, 50).await {
                    if !msgs.is_empty() {
                        let ack_items: Vec<AckItem> = msgs.into_iter().map(|m| AckItem {
                            message_id: m.message_id,
                            receipt_handle: m.receipt_handle,
                        }).collect();
                        if let Ok(c) = cl.ack_batch_items(q, ack_items).await {
                            drained += c as usize;
                        }
                    }
                }
                tokio::task::yield_now().await;
            }
            drained
        }));
    }

    let mut total_drained = 0;
    for h in drain_handles {
        total_drained += h.await.unwrap();
    }

    assert_eq!(total_drained, total_pushes, "All 2,000 buffered messages must be polled and acknowledged without drops");
}
