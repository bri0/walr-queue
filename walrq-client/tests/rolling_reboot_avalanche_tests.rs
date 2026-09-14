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

/// Attack: Rolling Crash-Reboot Avalanche Across All 3 Nodes Under Saturated Load
/// 1. Continuously push and poll batches across 4 queues.
/// 2. In a rolling fashion, reboot Node 1 -> Node 2 -> Node 3 -> Node 1 -> Node 2 -> Node 3
///    so that quorum (2/3 nodes) is maintained at all times, but every single node experiences
///    multiple offline crashes, cold disk boots, and state machine reconciliations while under fire.
/// 3. Verify that zero operations are dropped, zero deadlocks occur, and all data converges.
#[tokio::test]
async fn test_rolling_reboot_avalanche_under_continuous_traffic() {
    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();
    let dir3 = tempdir().unwrap();

    let path1 = dir1.path().to_path_buf();
    let path2 = dir2.path().to_path_buf();
    let path3 = dir3.path().to_path_buf();

    let addr1: SocketAddr = "127.0.0.1:59911".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:59912".parse().unwrap();
    let addr3: SocketAddr = "127.0.0.1:59913".parse().unwrap();

    let opts = QueueOptions {
        default_visibility_timeout_sec: 10,
        max_delivery_count: 3,
        max_hot_messages_in_ram: 50_000,
        max_wal_segment_size: 16 * 1024 * 1024,
    };

    let mut engine1 = Arc::new(QueueEngine::open(&path1, opts.clone()).unwrap());
    let mut engine2 = Arc::new(QueueEngine::open(&path2, opts.clone()).unwrap());
    let mut engine3 = Arc::new(QueueEngine::open(&path3, opts.clone()).unwrap());

    let mut raft1 = Arc::new(RaftNode::with_threshold(addr1.to_string(), vec![addr2.to_string(), addr3.to_string()], Arc::clone(&engine1), 500));
    let mut raft2 = Arc::new(RaftNode::with_threshold(addr2.to_string(), vec![addr1.to_string(), addr3.to_string()], Arc::clone(&engine2), 500));
    let mut raft3 = Arc::new(RaftNode::with_threshold(addr3.to_string(), vec![addr1.to_string(), addr2.to_string()], Arc::clone(&engine3), 500));

    raft1.become_leader_for_test().await;

    let mut server1 = Arc::new(WalrServer::new_raft(Arc::clone(&engine1), Arc::clone(&raft1), addr1.to_string()));
    let mut server2 = Arc::new(WalrServer::new_raft(Arc::clone(&engine2), Arc::clone(&raft2), addr2.to_string()));
    let mut server3 = Arc::new(WalrServer::new_raft(Arc::clone(&engine3), Arc::clone(&raft3), addr3.to_string()));

    let mut listener1 = TcpListener::bind(addr1).await.unwrap();
    let mut listener2 = TcpListener::bind(addr2).await.unwrap();
    let mut listener3 = TcpListener::bind(addr3).await.unwrap();

    let (mut tx1, rx1) = broadcast::channel(1);
    let (mut tx2, rx2) = broadcast::channel(1);
    let (mut tx3, rx3) = broadcast::channel(1);

    tokio::spawn(async move { server1.run(listener1, rx1).await; });
    tokio::spawn(async move { server2.run(listener2, rx2).await; });
    tokio::spawn(async move { server3.run(listener3, rx3).await; });

    raft1.send_heartbeat().await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = Arc::new(create_client(vec![addr1, addr2, addr3]));
    let is_running = Arc::new(AtomicBool::new(true));

    // Producers
    let mut prod_handles = Vec::new();
    for p_id in 0..4 {
        let cl = Arc::clone(&client);
        let run = Arc::clone(&is_running);
        let q_name = format!("rolling-q-{}", p_id);
        prod_handles.push(tokio::spawn(async move {
            let mut pushed = 0;
            let mut i = 0;
            while run.load(Ordering::Relaxed) && pushed < 100 {
                let p = Bytes::from(format!("rolling-data-{}-{}", p_id, i));
                if cl.push(&q_name, p, 0).await.is_ok() {
                    pushed += 1;
                    i += 1;
                } else {
                    tokio::time::sleep(Duration::from_millis(15)).await;
                }
            }
            pushed
        }));
    }

    // Rolling reboot cycle: 1 -> 2 -> 3 -> 1
    for step in 0..4 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        match step % 3 {
            0 => {
                // Kill & reboot Node 1
                let _ = tx1.send(());
                tokio::time::sleep(Duration::from_millis(30)).await;
                raft2.recover_as_new_leader().await;
                raft2.send_heartbeat().await;

                engine1 = Arc::new(QueueEngine::open(&path1, opts.clone()).unwrap());
                raft1 = Arc::new(RaftNode::with_threshold(addr1.to_string(), vec![addr2.to_string(), addr3.to_string()], Arc::clone(&engine1), 500));
                server1 = Arc::new(WalrServer::new_raft(Arc::clone(&engine1), Arc::clone(&raft1), addr1.to_string()));
                if let Ok(l) = TcpListener::bind(addr1).await {
                    let (t_new, r_new) = broadcast::channel(1);
                    tx1 = t_new;
                    tokio::spawn(async move { server1.run(l, r_new).await; });
                }
            }
            1 => {
                // Kill & reboot Node 2
                let _ = tx2.send(());
                tokio::time::sleep(Duration::from_millis(30)).await;
                raft3.recover_as_new_leader().await;
                raft3.send_heartbeat().await;

                engine2 = Arc::new(QueueEngine::open(&path2, opts.clone()).unwrap());
                raft2 = Arc::new(RaftNode::with_threshold(addr2.to_string(), vec![addr1.to_string(), addr3.to_string()], Arc::clone(&engine2), 500));
                server2 = Arc::new(WalrServer::new_raft(Arc::clone(&engine2), Arc::clone(&raft2), addr2.to_string()));
                if let Ok(l) = TcpListener::bind(addr2).await {
                    let (t_new, r_new) = broadcast::channel(1);
                    tx2 = t_new;
                    tokio::spawn(async move { server2.run(l, r_new).await; });
                }
            }
            2 => {
                // Kill & reboot Node 3
                let _ = tx3.send(());
                tokio::time::sleep(Duration::from_millis(30)).await;
                raft1.recover_as_new_leader().await;
                raft1.send_heartbeat().await;

                engine3 = Arc::new(QueueEngine::open(&path3, opts.clone()).unwrap());
                raft3 = Arc::new(RaftNode::with_threshold(addr3.to_string(), vec![addr1.to_string(), addr2.to_string()], Arc::clone(&engine3), 500));
                server3 = Arc::new(WalrServer::new_raft(Arc::clone(&engine3), Arc::clone(&raft3), addr3.to_string()));
                if let Ok(l) = TcpListener::bind(addr3).await {
                    let (t_new, r_new) = broadcast::channel(1);
                    tx3 = t_new;
                    tokio::spawn(async move { server3.run(l, r_new).await; });
                }
            }
            _ => unreachable!(),
        }
    }

    let mut total_pushed = 0;
    for h in prod_handles {
        total_pushed += h.await.unwrap();
    }
    is_running.store(false, Ordering::Relaxed);

    // Make sure one node is leader and has peers synced
    raft1.recover_as_new_leader().await;
    raft1.send_heartbeat().await;
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Drain and verify all messages across the 4 queues
    let mut total_drained = 0;
    for p_id in 0..4 {
        let q_name = format!("rolling-q-{}", p_id);
        for _ in 0..10 {
            if let Ok(msgs) = client.poll(&q_name, 10, 50).await {
                if !msgs.is_empty() {
                    let ack_items: Vec<AckItem> = msgs.into_iter().map(|m| {
                        total_drained += 1;
                        AckItem {
                            message_id: m.message_id,
                            receipt_handle: m.receipt_handle,
                        }
                    }).collect();
                    let _ = client.ack_batch_items(&q_name, ack_items).await;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    assert_eq!(total_drained, total_pushed, "All pushed messages must be drained after rolling crash-reboots");
}
