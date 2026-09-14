use bytes::Bytes;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use walrq::cluster::raft::RaftNode;
use walrq::engine::queue::{QueueEngine, QueueOptions};
use walrq::server::tcp_service::WalrServer;
use walrq_client::{ClientConfig, WalrClient};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_extreme_malformed_wire_and_disconnect_chaos() {
    let dir = tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let opts = QueueOptions {
        default_visibility_timeout_sec: 2,
        max_delivery_count: 3,
        max_hot_messages_in_ram: 100,
        max_wal_segment_size: 16 * 1024 * 1024,
    };

    let engine = Arc::new(QueueEngine::open(dir.path(), opts).unwrap());
    let raft = Arc::new(RaftNode::new(addr.to_string(), vec![], Arc::clone(&engine)));
    raft.become_leader_for_test().await;

    let server = Arc::new(WalrServer::new_raft(Arc::clone(&engine), Arc::clone(&raft), addr.to_string()));
    let (_tx, rx) = broadcast::channel(1);
    tokio::spawn(async move { server.run(listener, rx).await; });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Attacker 1: Connect and immediately disconnect (zero bytes sent)
    for _ in 0..10 {
        let _stream = TcpStream::connect(addr).await.unwrap();
        // drop immediately
    }

    // Attacker 2: Send incomplete length header (1, 2, or 3 bytes instead of 4) and close
    for len in 1..=3 {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(&vec![0x00u8; len]).await.unwrap();
        stream.shutdown().await.unwrap();
    }

    // Attacker 3: Send length header declaring 1GB frame (OOM attack), then drop
    {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let huge_len = (1024 * 1024 * 1024u32).to_be_bytes();
        stream.write_all(&huge_len).await.unwrap();
        stream.flush().await.unwrap();
        // Server frame decoder should reject without allocating 1GB
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Attacker 4: Send length header declaring 100 bytes, but send only 10 bytes of garbage and disconnect
    {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let len = 100u32.to_be_bytes();
        stream.write_all(&len).await.unwrap();
        stream.write_all(&[0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06]).await.unwrap();
        stream.shutdown().await.unwrap();
    }

    // Attacker 5: Send valid length header with corrupt non-Postcard random bytes
    {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let garbage = vec![0xFFu8; 64];
        let len = (garbage.len() as u32).to_be_bytes();
        stream.write_all(&len).await.unwrap();
        stream.write_all(&garbage).await.unwrap();
        stream.flush().await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Attacker 6: Send zero length frame (len = 0)
    {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let len = 0u32.to_be_bytes();
        stream.write_all(&len).await.unwrap();
        stream.flush().await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Now verify the server is still 100% operational and healthy for legitimate clients!
    let client = WalrClient::with_config(
        vec![addr.to_string()],
        ClientConfig {
            buffer_window_ms: 1,
            max_batch_size: 10,
            max_redirects: 5,
        },
    );

    let test_q = "wire_chaos_survivor_q";
    let test_payload = Bytes::from("legitimate-client-payload-post-fuzz");
    let mid = client.push(test_q, test_payload.clone(), 0).await.expect("Server must remain healthy");
    let polled = client.poll(test_q, 2, 1).await.expect("Poll must succeed");
    assert_eq!(polled.len(), 1);
    assert_eq!(polled[0].message_id, mid);
    assert_eq!(polled[0].payload, test_payload);

    let ack_ok = client.ack(test_q, &mid, &polled[0].receipt_handle).await.expect("Ack must succeed");
    assert!(ack_ok);
}
