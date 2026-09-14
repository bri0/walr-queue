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

/// Attack: Shard Hash Collision Avalanche & Shard Lock Contention Stress
/// The QueueEngine uses 32 mutex shards partitioned by 64-bit FNV-1a hash.
/// Here we deliberately generate 100 queue names that map to the EXACT SAME SHARD
/// (forcing 100% hash bucket collision on shard 0), and unleash 16 concurrent workers
/// pushing and polling at maximum rate to test for lock inversion, deadlocks, and starvation.
#[tokio::test]
async fn test_shard_hash_collision_avalanche_and_deadlock_stress() {
    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();
    let dir3 = tempdir().unwrap();

    let addr1: SocketAddr = "127.0.0.1:59211".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:59212".parse().unwrap();
    let addr3: SocketAddr = "127.0.0.1:59213".parse().unwrap();

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

    let (tx1, rx1) = broadcast::channel(1);
    let (_tx2, rx2) = broadcast::channel(1);
    let (_tx3, rx3) = broadcast::channel(1);

    tokio::spawn(async move { server1.run(listener1, rx1).await; });
    tokio::spawn(async move { server2.run(listener2, rx2).await; });
    tokio::spawn(async move { server3.run(listener3, rx3).await; });

    raft1.send_heartbeat().await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Helper to calculate FNV-1a shard index
    fn fnv1a_shard(q: &str) -> usize {
        let bytes = q.as_bytes();
        let mut hash = 0xcbf29ce484222325u64;
        for &b in bytes {
            hash ^= b as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        (hash as usize) % 32
    }

    // Find 50 queue names that ALL hash to shard index 0!
    let mut colliding_queues = Vec::new();
    let mut candidate = 0u64;
    while colliding_queues.len() < 50 {
        let name = format!("collide-shard0-q-{}", candidate);
        if fnv1a_shard(&name) == 0 {
            colliding_queues.push(name);
        }
        candidate += 1;
    }

    let colliding_queues = Arc::new(colliding_queues);
    let client = Arc::new(create_client(vec![addr1, addr2, addr3]));

    // Spawn 8 producers constantly bombarding the colliding shard
    let mut prod_handles = Vec::new();
    for p_id in 0..8 {
        let cl = Arc::clone(&client);
        let q_list = Arc::clone(&colliding_queues);
        prod_handles.push(tokio::spawn(async move {
            let mut pushed = 0;
            for i in 0..25 {
                let q = &q_list[(p_id * 5 + i) % q_list.len()];
                if cl.push(q, Bytes::from(format!("collide-item-{}-{}", p_id, i)), 0).await.is_ok() {
                    pushed += 1;
                }
            }
            pushed
        }));
    }

    // Spawn 8 consumers aggressively polling the colliding shard
    let mut cons_handles = Vec::new();
    for c_id in 0..8 {
        let cl = Arc::clone(&client);
        let q_list = Arc::clone(&colliding_queues);
        cons_handles.push(tokio::spawn(async move {
            let mut acked = 0;
            for _ in 0..50 {
                let q = &q_list[c_id % q_list.len()];
                if let Ok(msgs) = cl.poll(q, 10, 10).await {
                    for m in msgs {
                        if cl.ack(q, &m.message_id, &m.receipt_handle).await.unwrap_or(false) {
                            acked += 1;
                        }
                    }
                }
                tokio::task::yield_now().await;
            }
            acked
        }));
    }

    let mut total_pushed = 0;
    for h in prod_handles {
        total_pushed += h.await.unwrap();
    }
    for h in cons_handles {
        let _ = h.await.unwrap();
    }

    assert_eq!(total_pushed, 200, "All 200 pushed messages must succeed without shard mutex deadlock or starvation");

    // Final drain of any remaining messages on the colliding queues
    let mut final_drained = 0;
    for q in colliding_queues.iter() {
        if let Ok(msgs) = client.poll(q, 10, 50).await {
            for m in msgs {
                let _ = client.ack(q, &m.message_id, &m.receipt_handle).await;
                final_drained += 1;
            }
        }
    }

    let _ = tx1.send(());
}
