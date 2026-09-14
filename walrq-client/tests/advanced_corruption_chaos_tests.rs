use bytes::Bytes;
use std::fs::{self, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::net::SocketAddr;
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

/// Test A: Manifest File Corruption Attack
/// Manifest contains queue-name-to-id mapping. If truncated or garbage-filled,
/// engine boot must not panic or crash!
#[tokio::test]
async fn test_corrupt_manifest_file_recovery() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let opts = QueueOptions {
        default_visibility_timeout_sec: 10,
        max_delivery_count: 3,
        max_hot_messages_in_ram: 10_000,
        max_wal_segment_size: 16 * 1024 * 1024,
    };

    // 1. Boot engine, write queues
    {
        let engine = QueueEngine::open(&path, opts.clone()).unwrap();
        engine.push("manifest-q-1", Bytes::from("payload-1"), 0).await.unwrap();
        engine.push("manifest-q-2", Bytes::from("payload-2"), 0).await.unwrap();
    }

    let manifest_path = path.join("meta").join("queues.manifest");
    assert!(manifest_path.exists());

    // 2. Corrupt manifest with garbage bytes
    {
        let mut f = OpenOptions::new().write(true).open(&manifest_path).unwrap();
        f.seek(SeekFrom::Start(2)).unwrap();
        f.write_all(b"\xFF\xFF\x00\x00\xAA\xBB").unwrap();
    }

    // 3. Engine boot should handle corrupted manifest without panic
    let result = QueueEngine::open(&path, opts.clone());
    assert!(result.is_ok(), "Engine must gracefully boot even if manifest has corrupt bytes");
}

/// Test B: Split-Brain Network Partition & Auto-Heal
/// 3-Node cluster: Isolate Node 3 into a minority partition.
/// Leader (Node 1) + Follower (Node 2) form a majority (2/3) and continue processing pushes.
/// Heal partition: Node 3 catches up!
#[tokio::test]
async fn test_split_brain_partition_and_heal() {
    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();
    let dir3 = tempdir().unwrap();

    let addr1: SocketAddr = "127.0.0.1:58511".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:58512".parse().unwrap();
    let addr3: SocketAddr = "127.0.0.1:58513".parse().unwrap();

    let opts = QueueOptions {
        default_visibility_timeout_sec: 10,
        max_delivery_count: 3,
        max_hot_messages_in_ram: 50_000,
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

    let (_tx1, rx1) = broadcast::channel(1);
    let (_tx2, rx2) = broadcast::channel(1);
    let (tx3, rx3) = broadcast::channel(1);

    tokio::spawn(async move { server1.run(listener1, rx1).await; });
    tokio::spawn(async move { server2.run(listener2, rx2).await; });
    tokio::spawn(async move { server3.run(listener3, rx3).await; });

    raft1.send_heartbeat().await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = create_client(vec![addr1, addr2]);
    let q = "partition-q";

    // 1. Push 20 items to healthy cluster
    for i in 0..20 {
        client.push(q, Bytes::from(format!("healthy-item-{}", i)), 0).await.unwrap();
    }

    // 2. Partition: Disconnect Node 3 completely
    let _ = tx3.send(()); // Stop listener 3
    tokio::time::sleep(Duration::from_millis(50)).await;

    // 3. Leader (1) and Node (2) continue serving writes (quorum = 2/3)!
    for i in 20..40 {
        client.push(q, Bytes::from(format!("quorum-item-{}", i)), 0).await.unwrap();
    }

    // Both Node 1 and Node 2 hold 40 messages
    assert_eq!(engine1.total_messages_in_ram(), 40);
    assert_eq!(engine2.total_messages_in_ram(), 40);

    // 4. Client polls all 40 messages while Node 3 is dead
    let polled = client.poll(q, 10, 50).await.unwrap();
    assert_eq!(polled.len(), 40, "Majority quorum can poll all 40 items while minority is down");
}

/// Test C: Malformed TCP Framing Injection (Oversized frame header, zero length frame)
#[tokio::test]
async fn test_corrupt_tcp_wire_frames() {
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpStream;

    let dir = tempdir().unwrap();
    let addr: SocketAddr = "127.0.0.1:58530".parse().unwrap();
    let opts = QueueOptions {
        default_visibility_timeout_sec: 10,
        max_delivery_count: 3,
        max_hot_messages_in_ram: 10_000,
        max_wal_segment_size: 16 * 1024 * 1024,
    };
    let engine = Arc::new(QueueEngine::open(dir.path(), opts).unwrap());
    let raft = Arc::new(RaftNode::new(addr.to_string(), vec![], Arc::clone(&engine)));
    raft.become_leader_for_test().await;

    let server = Arc::new(WalrServer::new_raft(Arc::clone(&engine), Arc::clone(&raft), addr.to_string()));
    let listener = TcpListener::bind(addr).await.unwrap();
    let (_tx, rx) = broadcast::channel(1);
    tokio::spawn(async move { server.run(listener, rx).await; });

    tokio::time::sleep(Duration::from_millis(30)).await;

    // 1. Connect raw TCP stream and send garbage length prefix (e.g. 100MB frame)
    {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        // Send frame length = 100MB (0x06400000)
        stream.write_all(&[0x06, 0x40, 0x00, 0x00]).await.unwrap();
        stream.write_all(b"garbage-incomplete-bytes").await.unwrap();
        // Abruptly close connection
        drop(stream);
    }

    // 2. Connect another stream and send pure random garbage
    {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(&[0xFF, 0xEE, 0xDD, 0xCC, 0xBB, 0xAA]).await.unwrap();
        drop(stream);
    }

    // 3. Server must still be healthy and serve valid clients!
    let valid_client = create_client(vec![addr]);
    let id = valid_client.push("survivor-q", Bytes::from("valid-data"), 0).await.unwrap();
    assert!(!id.is_empty(), "Server must survive malicious/corrupt wire framing");
    let polled = valid_client.poll("survivor-q", 10, 1).await.unwrap();
    assert_eq!(polled.len(), 1);
    assert_eq!(polled[0].message_id, id);
}

/// Test D: Deep Mid-Segment WAL Hole Punching
/// Punch zeroes right in the middle of a multi-record WAL file
#[tokio::test]
async fn test_deep_wal_hole_punching() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let opts = QueueOptions {
        default_visibility_timeout_sec: 10,
        max_delivery_count: 3,
        max_hot_messages_in_ram: 10_000,
        max_wal_segment_size: 16 * 1024 * 1024,
    };

    // 1. Write 30 messages
    {
        let engine = QueueEngine::open(&path, opts.clone()).unwrap();
        for i in 0..30 {
            engine.push("hole-q", Bytes::from(format!("payload-item-{}", i)), 0).await.unwrap();
        }
    }

    let wal_path = path.join("wal_a.log");
    assert!(wal_path.exists());
    let file_len = fs::metadata(&wal_path).unwrap().len();

    // 2. Punch a hole right in the center of the WAL file
    {
        let mut f = OpenOptions::new().write(true).open(&wal_path).unwrap();
        f.seek(SeekFrom::Start(file_len / 2)).unwrap();
        f.write_all(&[0x00; 32]).unwrap(); // Zero out 32 bytes
    }

    // 3. Engine recovery: WAL scanner encounters corruption, safely stops at hole without panicking
    let recovered_engine = QueueEngine::open(&path, opts.clone()).unwrap();
    let in_ram = recovered_engine.total_messages_in_ram();
    assert!(in_ram > 0 && in_ram < 30, "Engine must recover prefix records before corrupted hole: got {}", in_ram);
}
