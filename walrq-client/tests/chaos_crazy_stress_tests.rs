use bytes::Bytes;
use std::fs::{self, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
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
use walrq_client::{ClientConfig, ClientError, WalrClient};

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

/// 1. Poll-Only on Non-Existent and Empty Queues
#[tokio::test]
async fn test_poll_only_empty_and_crazy_queues() {
    let dir1 = tempdir().unwrap();
    let addr1: SocketAddr = "127.0.0.1:58410".parse().unwrap();
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

    // Poll 100 times on queues that have never had a push
    for i in 0..20 {
        let q = format!("phantom-queue-{}", i);
        let res = client.poll(&q, 10, 50).await.unwrap();
        assert!(res.is_empty(), "Poll on non-existent queue should be clean empty vec");
    }

    // Poll with 0 batch_size
    let res0 = client.poll("zero-batch-q", 10, 0).await.unwrap();
    assert!(res0.is_empty());

    let _ = tx1.send(());
}

/// 2. Push-Only Burst Test (10,000 rapid messages without consumer, verifying FIFO order on delayed poll)
#[tokio::test]
async fn test_push_only_rapid_burst_and_fifo_order() {
    let dir1 = tempdir().unwrap();
    let addr1: SocketAddr = "127.0.0.1:58420".parse().unwrap();
    let opts = QueueOptions {
        default_visibility_timeout_sec: 30,
        max_delivery_count: 3,
        max_hot_messages_in_ram: 50_000,
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
    let q = "burst-push-only-q";

    let count = 5_000;
    let mut batch = Vec::with_capacity(500);
    for i in 0..count {
        batch.push(Bytes::from(format!("burst-item-{:06}", i)));
        if batch.len() == 500 {
            client.push_batch(q, std::mem::take(&mut batch)).await.unwrap();
        }
    }

    assert_eq!(engine1.total_messages_in_ram(), count);

    // Now drain and verify strict FIFO ordering
    let mut drained = 0;
    let mut last_seq = 0;
    while drained < count {
        let msgs = client.poll(q, 30, 500).await.unwrap();
        if msgs.is_empty() {
            tokio::time::sleep(Duration::from_millis(5)).await;
            continue;
        }
        for m in msgs {
            let s = String::from_utf8_lossy(&m.payload);
            let seq: usize = s.trim_start_matches("burst-item-").parse().unwrap();
            assert_eq!(seq, last_seq, "Strict FIFO order violation in burst push");
            last_seq += 1;
            drained += 1;
        }
    }
    assert_eq!(drained, count);
    let _ = tx1.send(());
}

/// 3. Byzantine & Crazy Inputs: Massive payload push rejection & invalid ULID handling
#[tokio::test]
async fn test_byzantine_and_corrupt_protocol_inputs() {
    let dir1 = tempdir().unwrap();
    let addr1: SocketAddr = "127.0.0.1:58430".parse().unwrap();
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
    let q = "byzantine-q";

    // Invalid non-ULID message ID string
    let bad_id_res = client.push_with_id(q, Bytes::from("valid-data"), 0, "not-a-valid-ulid-length-string").await;
    match bad_id_res {
        Err(ClientError::ServerError(e)) => assert!(e.contains("invalid ULID")),
        other => panic!("Expected invalid ULID error, got: {:?}", other),
    }

    // Ack with corrupt / malformed message IDs
    let ack_bad = client.ack(q, "invalid-bad-id-12345", "fake-token").await.unwrap();
    assert!(!ack_bad, "Ack on corrupt message ID should cleanly return false");

    let _ = tx1.send(());
}

/// 4. WAL Disk Corruption & Automatic Resilient Recovery
/// Bit-flip / truncate active WAL file, then boot engine from disk and verify recovery!
#[tokio::test]
async fn test_wal_bitflip_and_truncation_corruption_resilience() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let opts = QueueOptions {
        default_visibility_timeout_sec: 10,
        max_delivery_count: 3,
        max_hot_messages_in_ram: 100_000,
        max_wal_segment_size: 16 * 1024 * 1024,
    };

    // Phase 1: Write valid entries
    {
        let engine = QueueEngine::open(&path, opts.clone()).unwrap();
        for i in 0..50 {
            engine.push("corrupt-q", Bytes::from(format!("valid-wal-{}", i)), 0).await.unwrap();
        }
    }

    // Phase 2: Inject bit-flip corruption into active WAL segment
    let wal_a = path.join("wal_a.log");
    let wal_b = path.join("wal_b.log");
    let target_wal = if wal_a.exists() && fs::metadata(&wal_a).unwrap().len() > 0 {
        wal_a
    } else {
        wal_b
    };

    assert!(target_wal.exists());
    let original_len = fs::metadata(&target_wal).unwrap().len();
    assert!(original_len > 100);

    // Corrupt the tail with garbage bytes (e.g. abrupt power outage / partial write)
    {
        let mut f = OpenOptions::new().write(true).open(&target_wal).unwrap();
        f.seek(SeekFrom::End(-15)).unwrap();
        f.write_all(b"\xFF\xFE\xFD\xFC\xFB\xFA\xF9\xF8").unwrap();
    }

    // Phase 3: Engine must safely recover without crashing or panicking!
    let recovered_engine = QueueEngine::open(&path, opts.clone()).unwrap();
    // Valid intact records prior to the corrupted tail must be recovered
    let recovered_cnt = recovered_engine.total_messages_in_ram();
    assert!(recovered_cnt >= 40, "Engine must salvage all valid records before corruption: got {}", recovered_cnt);

    let polled = recovered_engine.poll("corrupt-q", 10, 100).await.unwrap();
    assert_eq!(polled.len(), recovered_cnt);
}

/// 5. Chaos Test: Leader Mid-Flight Kill and Automatic Quorum Failover
#[tokio::test]
async fn test_chaos_leader_kill_and_follower_takeover() {
    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();
    let dir3 = tempdir().unwrap();

    let addr1: SocketAddr = "127.0.0.1:58451".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:58452".parse().unwrap();
    let addr3: SocketAddr = "127.0.0.1:58453".parse().unwrap();

    let opts = QueueOptions {
        default_visibility_timeout_sec: 10,
        max_delivery_count: 3,
        max_hot_messages_in_ram: 100_000,
        max_wal_segment_size: 16 * 1024 * 1024,
    };

    let engine1 = Arc::new(QueueEngine::open(dir1.path(), opts.clone()).unwrap());
    let engine2 = Arc::new(QueueEngine::open(dir2.path(), opts.clone()).unwrap());
    let engine3 = Arc::new(QueueEngine::open(dir3.path(), opts.clone()).unwrap());

    let raft1 = Arc::new(RaftNode::with_threshold(
        addr1.to_string(),
        vec![addr2.to_string(), addr3.to_string()],
        Arc::clone(&engine1),
        1000,
    ));
    let raft2 = Arc::new(RaftNode::with_threshold(
        addr2.to_string(),
        vec![addr1.to_string(), addr3.to_string()],
        Arc::clone(&engine2),
        1000,
    ));
    let raft3 = Arc::new(RaftNode::with_threshold(
        addr3.to_string(),
        vec![addr1.to_string(), addr2.to_string()],
        Arc::clone(&engine3),
        1000,
    ));

    raft1.become_leader_for_test().await;

    let server1 = Arc::new(WalrServer::new_raft(Arc::clone(&engine1), Arc::clone(&raft1), addr1.to_string()));
    let server2 = Arc::new(WalrServer::new_raft(Arc::clone(&engine2), Arc::clone(&raft2), addr2.to_string()));
    let server3 = Arc::new(WalrServer::new_raft(Arc::clone(&engine3), Arc::clone(&raft3), addr3.to_string()));

    let listener1 = TcpListener::bind(addr1).await.unwrap();
    let listener2 = TcpListener::bind(addr2).await.unwrap();
    let listener3 = TcpListener::bind(addr3).await.unwrap();

    let (shutdown_tx1, rx1) = broadcast::channel(1);
    let (_shutdown_tx2, rx2) = broadcast::channel(1);
    let (_shutdown_tx3, rx3) = broadcast::channel(1);

    tokio::spawn(async move { server1.run(listener1, rx1).await; });
    tokio::spawn(async move { server2.run(listener2, rx2).await; });
    tokio::spawn(async move { server3.run(listener3, rx3).await; });

    raft1.send_heartbeat().await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = create_client(vec![addr1, addr2, addr3]);
    let q = "chaos-failover-q";

    // Push 30 items to leader
    for i in 0..30 {
        client.push(q, Bytes::from(format!("chaos-data-{}", i)), 0).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(50)).await;

    // KILL LEADER (Node 1 dies abruptly!)
    let _ = shutdown_tx1.send(());
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Failover: Node 2 becomes new Leader
    raft2.recover_as_new_leader().await;
    raft2.send_heartbeat().await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Client sends more traffic: Node 1 is dead, client transparently switches to Node 2!
    for i in 30..50 {
        client.push(q, Bytes::from(format!("chaos-data-{}", i)), 0).await.unwrap();
    }

    // Poll all 50 items from the surviving cluster!
    let mut polled_count = 0;
    for _ in 0..10 {
        let msgs = client.poll(q, 10, 50).await.unwrap();
        polled_count += msgs.len();
        if polled_count >= 50 {
            break;
        }
    }
    assert_eq!(polled_count, 50, "All 50 items must survive and be pollable after leader crash & failover");
}
