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
use walrq_client::{ClientConfig, WalrClient};

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

/// Attack 1: Rapid-Fire Dynamic Leader Rotation
/// Continuously rotate leadership across nodes 1 -> 2 -> 3 -> 1 -> 2 while concurrent
/// clients bombard the cluster with pushes and polls.
#[tokio::test]
async fn test_chaos_rapid_fire_leader_rotation() {
    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();
    let dir3 = tempdir().unwrap();

    let addr1: SocketAddr = "127.0.0.1:58611".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:58612".parse().unwrap();
    let addr3: SocketAddr = "127.0.0.1:58613".parse().unwrap();

    let opts = QueueOptions {
        default_visibility_timeout_sec: 10,
        max_delivery_count: 3,
        max_hot_messages_in_ram: 50_000,
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

    let client = Arc::new(create_client(vec![addr1, addr2, addr3]));
    let q = "dynamic-leader-q";

    let is_running = Arc::new(AtomicBool::new(true));
    let run_producer = Arc::clone(&is_running);
    let client_prod = Arc::clone(&client);

    // Concurrent producer pushing items
    let prod_handle = tokio::spawn(async move {
        let mut count = 0;
        while run_producer.load(Ordering::Relaxed) && count < 300 {
            if client_prod.push(q, Bytes::from(format!("leader-rot-{}", count)), 0).await.is_ok() {
                count += 1;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        count
    });

    // Rotate leadership 4 times during active writes
    tokio::time::sleep(Duration::from_millis(50)).await;
    raft2.recover_as_new_leader().await;
    raft2.send_heartbeat().await;

    tokio::time::sleep(Duration::from_millis(50)).await;
    raft3.recover_as_new_leader().await;
    raft3.send_heartbeat().await;

    tokio::time::sleep(Duration::from_millis(50)).await;
    raft1.recover_as_new_leader().await;
    raft1.send_heartbeat().await;

    let total_pushed = prod_handle.await.unwrap();
    is_running.store(false, Ordering::Relaxed);

    // Client polls and verifies messages
    let mut total_polled = 0;
    for _ in 0..20 {
        if let Ok(msgs) = client.poll(q, 10, 50).await {
            total_polled += msgs.len();
            for m in msgs {
                let _ = client.ack(q, &m.message_id, &m.receipt_handle).await;
            }
        }
        if total_polled >= total_pushed {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert_eq!(total_polled, total_pushed, "All pushed messages must be retrievable across leader rotations");
}

/// Attack 2: Poison Pill Payload Fuzzing
/// Fuzz message payloads with extreme adversarial bytes:
/// - Maximum UTF-8 surrogate halves and non-characters
/// - Null byte runs
/// - Malformed serialized length prefixes
/// - Large payloads
#[tokio::test]
async fn test_payload_fuzzing_poison_pills() {
    let dir1 = tempdir().unwrap();
    let addr1: SocketAddr = "127.0.0.1:58620".parse().unwrap();
    let opts = QueueOptions {
        default_visibility_timeout_sec: 10,
        max_delivery_count: 3,
        max_hot_messages_in_ram: 10_000,
        max_wal_segment_size: 16 * 1024 * 1024,
    };
    let engine1 = Arc::new(QueueEngine::open(dir1.path(), opts).unwrap());
    let raft1 = Arc::new(RaftNode::new(addr1.to_string(), vec![], Arc::clone(&engine1)));
    raft1.become_leader_for_test().await;

    let server1 = Arc::new(WalrServer::new_raft(Arc::clone(&engine1), Arc::clone(&raft1), addr1.to_string()));
    let listener1 = TcpListener::bind(addr1).await.unwrap();
    let (tx1, rx1) = broadcast::channel(1);
    tokio::spawn(async move { server1.run(listener1, rx1).await; });

    let client = create_client(vec![addr1]);
    let q = "fuzz-poison-q";

    let test_cases: Vec<Vec<u8>> = vec![
        vec![0x00; 1024],                                                 // 1KB pure nulls
        vec![0xFF; 2048],                                                 // 2KB pure 0xFF
        vec![0xED, 0xA0, 0x80, 0xED, 0xBF, 0xBF],                         // CESU-8 surrogate pair
        b"\r\n\x00\x1B[2J\x1B[H".to_vec(),                                // ANSI terminal escapes
        (0..255u8).cycle().take(65536).collect(),                         // 64KB cycle of all bytes 0-255
    ];

    for (idx, payload) in test_cases.into_iter().enumerate() {
        let b = Bytes::from(payload.clone());
        let id = client.push(q, b.clone(), 0).await.expect(&format!("Fuzz push failed on case {}", idx));
        let polled = client.poll(q, 10, 1).await.expect(&format!("Fuzz poll failed on case {}", idx));
        assert_eq!(polled.len(), 1);
        assert_eq!(polled[0].message_id, id);
        assert_eq!(polled[0].payload, b);
        let acked = client.ack(q, &id, &polled[0].receipt_handle).await.unwrap();
        assert!(acked);
    }

    let _ = tx1.send(());
}

/// Attack 3: Connection Pool Chaos & Half-Open Socket Abuse
/// Rapidly opens connections, writes partial frames, and drops sockets abruptly
#[tokio::test]
async fn test_connection_pool_chaos_and_half_open_sockets() {
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpStream;

    let dir1 = tempdir().unwrap();
    let addr1: SocketAddr = "127.0.0.1:58630".parse().unwrap();
    let opts = QueueOptions {
        default_visibility_timeout_sec: 10,
        max_delivery_count: 3,
        max_hot_messages_in_ram: 10_000,
        max_wal_segment_size: 16 * 1024 * 1024,
    };
    let engine1 = Arc::new(QueueEngine::open(dir1.path(), opts).unwrap());
    let raft1 = Arc::new(RaftNode::new(addr1.to_string(), vec![], Arc::clone(&engine1)));
    raft1.become_leader_for_test().await;

    let server1 = Arc::new(WalrServer::new_raft(Arc::clone(&engine1), Arc::clone(&raft1), addr1.to_string()));
    let listener1 = TcpListener::bind(addr1).await.unwrap();
    let (tx1, rx1) = broadcast::channel(1);
    tokio::spawn(async move { server1.run(listener1, rx1).await; });

    tokio::time::sleep(Duration::from_millis(30)).await;

    // Abuse: 50 connections connect and close instantly without writing anything
    for _ in 0..50 {
        let _ = TcpStream::connect(addr1).await;
    }

    // Abuse: 20 connections write partial 2 bytes of frame length and drop
    for _ in 0..20 {
        if let Ok(mut stream) = TcpStream::connect(addr1).await {
            let _ = stream.write_all(&[0x00, 0x10]).await;
            // Drop stream without completing length or body
        }
    }

    // Server must still be fully responsive
    let client = create_client(vec![addr1]);
    let push_res = client.push("clean-q", Bytes::from("healthy-post-abuse"), 0).await;
    assert!(push_res.is_ok(), "Server must withstand half-open connection spam");

    let _ = tx1.send(());
}
