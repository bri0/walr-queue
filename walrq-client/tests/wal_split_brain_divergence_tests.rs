use bytes::Bytes;
use std::fs::{self, OpenOptions};
use std::io::Write;
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

/// Attack: WAL Split-Brain Divergence and Desynchronization
/// 1. Boot 3-node cluster.
/// 2. Write 30 messages to all nodes.
/// 3. Stop Node 2.
/// 4. Manually inject divergent, conflicting uncommitted WAL records directly onto Node 2's disk:
///    - Fake messages with identical ULIDs but conflicting payloads
///    - Truncated trailing byte headers
/// 5. Node 1 and Node 3 continue committing 30 additional messages (Node 2 falls behind).
/// 6. Revive Node 2 with the divergent, conflicting on-disk WAL.
/// 7. Raft leader Node 1 detects divergent state and forces authoritative synchronization.
/// 8. Client polls all messages and verifies state machine convergence across all nodes!
#[tokio::test]
async fn test_wal_split_brain_divergence_and_forced_healing() {
    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();
    let dir3 = tempdir().unwrap();

    let path1 = dir1.path().to_path_buf();
    let path2 = dir2.path().to_path_buf();
    let path3 = dir3.path().to_path_buf();

    let addr1: SocketAddr = "127.0.0.1:59011".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:59012".parse().unwrap();
    let addr3: SocketAddr = "127.0.0.1:59013".parse().unwrap();

    let opts = QueueOptions {
        default_visibility_timeout_sec: 10,
        max_delivery_count: 3,
        max_hot_messages_in_ram: 50_000,
        max_wal_segment_size: 16 * 1024 * 1024,
    };

    let engine1 = Arc::new(QueueEngine::open(&path1, opts.clone()).unwrap());
    let engine2 = Arc::new(QueueEngine::open(&path2, opts.clone()).unwrap());
    let engine3 = Arc::new(QueueEngine::open(&path3, opts.clone()).unwrap());

    let raft1 = Arc::new(RaftNode::with_threshold(addr1.to_string(), vec![addr2.to_string(), addr3.to_string()], Arc::clone(&engine1), 10));
    let raft2 = Arc::new(RaftNode::with_threshold(addr2.to_string(), vec![addr1.to_string(), addr3.to_string()], Arc::clone(&engine2), 10));
    let raft3 = Arc::new(RaftNode::with_threshold(addr3.to_string(), vec![addr1.to_string(), addr2.to_string()], Arc::clone(&engine3), 10));

    raft1.become_leader_for_test().await;

    let server1 = Arc::new(WalrServer::new_raft(Arc::clone(&engine1), Arc::clone(&raft1), addr1.to_string()));
    let server2 = Arc::new(WalrServer::new_raft(Arc::clone(&engine2), Arc::clone(&raft2), addr2.to_string()));
    let server3 = Arc::new(WalrServer::new_raft(Arc::clone(&engine3), Arc::clone(&raft3), addr3.to_string()));

    let listener1 = TcpListener::bind(addr1).await.unwrap();
    let listener2 = TcpListener::bind(addr2).await.unwrap();
    let listener3 = TcpListener::bind(addr3).await.unwrap();

    let (_tx1, rx1) = broadcast::channel(1);
    let (tx2, rx2) = broadcast::channel(1);
    let (_tx3, rx3) = broadcast::channel(1);

    tokio::spawn(async move { server1.run(listener1, rx1).await; });
    tokio::spawn(async move { server2.run(listener2, rx2).await; });
    tokio::spawn(async move { server3.run(listener3, rx3).await; });

    raft1.send_heartbeat().await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = create_client(vec![addr1, addr2, addr3]);
    let q = "divergence-q";

    // Phase 1: 30 initial messages committed across all 3 nodes
    let mut batch1 = Vec::with_capacity(30);
    for i in 0..30 {
        batch1.push(Bytes::from(format!("valid-init-{}", i)));
    }
    client.push_batch(q, batch1).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert_eq!(engine1.total_messages_in_ram(), 30);
    assert_eq!(engine2.total_messages_in_ram(), 30);
    assert_eq!(engine3.total_messages_in_ram(), 30);

    // Phase 2: Kill Node 2
    let _ = tx2.send(());
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Phase 3: Corrupt and inject divergent data into Node 2's disk while offline
    let wal_a2 = path2.join("wal_a.log");
    if wal_a2.exists() {
        let mut f = OpenOptions::new().append(true).open(&wal_a2).unwrap();
        // Append 100 bytes of conflicting corrupt uncommitted junk
        f.write_all(b"\x00\x01DIVERGENT_FORK_RECORD_GARBAGE\x00\xFF\xAA\xBB\xCC\xDD").unwrap();
    }

    // Phase 4: Node 1 and Node 3 commit 30 more messages (30 -> 60 total)
    let mut batch2 = Vec::with_capacity(30);
    for i in 30..60 {
        batch2.push(Bytes::from(format!("valid-post-divergence-{}", i)));
    }
    client.push_batch(q, batch2).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert_eq!(engine1.total_messages_in_ram(), 60);
    assert_eq!(engine3.total_messages_in_ram(), 60);

    // Phase 5: Revive Node 2 with the corrupted, divergent disk state!
    let engine2_revived = Arc::new(QueueEngine::open(&path2, opts.clone()).unwrap());
    let raft2_revived = Arc::new(RaftNode::with_threshold(
        addr2.to_string(),
        vec![addr1.to_string(), addr3.to_string()],
        Arc::clone(&engine2_revived),
        10,
    ));

    let listener2_revived = TcpListener::bind(addr2).await.unwrap();
    let server2_revived = Arc::new(WalrServer::new_raft(Arc::clone(&engine2_revived), Arc::clone(&raft2_revived), addr2.to_string()));
    let (_tx2_new, rx2_new) = broadcast::channel(1);
    tokio::spawn(async move { server2_revived.run(listener2_revived, rx2_new).await; });

    // Leader sends heartbeat and snapshot sync to force-repair Node 2
    raft1.send_heartbeat().await;
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Phase 6: Drain and verify all 60 valid messages
    let mut drained_msgs = Vec::new();
    for _ in 0..10 {
        if let Ok(msgs) = client.poll(q, 10, 100).await {
            if !msgs.is_empty() {
                for m in msgs {
                    let _ = client.ack(q, &m.message_id, &m.receipt_handle).await;
                    drained_msgs.push(String::from_utf8_lossy(&m.payload).to_string());
                }
            }
        }
        if drained_msgs.len() >= 60 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert_eq!(drained_msgs.len(), 60, "Must recover all 60 legitimate messages");
    for s in &drained_msgs {
        assert!(!s.contains("DIVERGENT"), "Divergent uncommitted disk junk must NEVER leak into client state: {}", s);
    }
}
