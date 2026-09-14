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

fn create_client(addr: SocketAddr) -> WalrClient {
    WalrClient::with_config(
        vec![addr.to_string()],
        ClientConfig {
            buffer_window_ms: 1,
            max_batch_size: 500,
            max_redirects: 5,
        },
    )
}

/// Disk Spill Test:
/// - Configure `max_hot_messages_in_ram = 500`.
/// - Push 5,000 messages (10x the RAM capacity!).
/// - ZERO RAM errors or rejections: 500 stay in hot RAM ring, 4,500 spill to disk WAL.
/// - Poll and drain all 5,000 messages: Engine pages in spilled chunks sequentially.
/// - Strictly verifies monotonic FIFO delivery of all 5,000 messages without loss!
#[tokio::test]
async fn test_unbounded_push_disk_spill_and_sequential_drain() {
    let dir1 = tempdir().unwrap();
    let addr1: SocketAddr = "127.0.0.1:57221".parse().unwrap();

    let ram_cap = 500;
    let total_messages = 5_000;

    let opts = QueueOptions {
        default_visibility_timeout_sec: 10,
        max_delivery_count: 3,
        max_hot_messages_in_ram: ram_cap,
        max_wal_segment_size: 64 * 1024 * 1024,
    };

    let engine = Arc::new(QueueEngine::open(dir1.path(), opts).unwrap());
    let raft = Arc::new(RaftNode::new(addr1.to_string(), vec![], Arc::clone(&engine)));
    raft.become_leader_for_test().await;

    let server = Arc::new(WalrServer::new_raft(Arc::clone(&engine), Arc::clone(&raft), addr1.to_string()));
    let listener = TcpListener::bind(addr1).await.unwrap();
    let (_tx, rx) = broadcast::channel(1);
    tokio::spawn(async move { server.run(listener, rx).await; });

    tokio::time::sleep(Duration::from_millis(30)).await;

    let client = create_client(addr1);
    let q = "spill-test-q";

    // 1. Push 5,000 messages in 10 batches of 500
    for b in 0..10 {
        let mut batch = Vec::with_capacity(500);
        for i in 0..500 {
            let seq = b * 500 + i;
            batch.push(Bytes::from(format!("spill-data-{:06}", seq)));
        }
        let ids = client.push_batch(q, batch).await.expect("Push must succeed even when exceeding RAM cap");
        assert_eq!(ids.len(), 500);
    }

    // Flush disk
    tokio::time::sleep(Duration::from_millis(50)).await;

    // 2. RAM capacity must remain strictly bounded at or below 500!
    let current_in_ram = engine.total_messages_in_ram();
    eprintln!(">>> Total messages in RAM after 5,000 pushes (RAM cap = 500): {}", current_in_ram);
    assert!(current_in_ram <= ram_cap, "RAM must be bounded: got {}", current_in_ram);

    // 3. Consumers poll and acknowledge all 5,000 messages!
    // As messages are drained, engine automatically pages in spilled cold records from disk.
    let mut drained_count = 0;
    let mut expected_seq = 0;

    for _ in 0..100 {
        let msgs = client.poll(q, 10, 100).await.unwrap();
        if msgs.is_empty() {
            if drained_count >= total_messages {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
            continue;
        }

        println!(">>> polled {} msgs, first seq: {}", msgs.len(), msgs[0].payload.escape_ascii());
        let mut ack_items = Vec::new();
        for m in msgs {
            let s = String::from_utf8_lossy(&m.payload);
            let seq: usize = s.trim_start_matches("spill-data-").parse().unwrap();
            assert_eq!(seq, expected_seq, "Strict FIFO sequence violation during disk-spill paging");
            expected_seq += 1;
            drained_count += 1;
            ack_items.push(AckItem {
                message_id: m.message_id,
                receipt_handle: m.receipt_handle,
            });
        }
        let acked = client.ack_batch_items(q, ack_items).await.unwrap();
        assert!(acked > 0);
    }

    assert_eq!(drained_count, total_messages, "Must drain all 5,000 spilled messages in exact sequence");
}
