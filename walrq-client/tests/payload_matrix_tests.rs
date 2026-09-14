use bytes::Bytes;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;
use tokio::net::TcpListener;
use tokio::sync::broadcast;

use walrq::cluster::raft::RaftNode;
use walrq::engine::queue::{QueueEngine, QueueOptions};
use walrq::server::tcp_service::WalrServer;
use walrq_client::{ClientConfig, WalrClient};

struct TestCluster {
    _dir1: tempfile::TempDir,
    _dir2: tempfile::TempDir,
    _dir3: tempfile::TempDir,
    pub addr1: SocketAddr,
    _shutdown_tx1: broadcast::Sender<()>,
    _shutdown_tx2: broadcast::Sender<()>,
    _shutdown_tx3: broadcast::Sender<()>,
}

async fn start_test_cluster(base_port: u16) -> TestCluster {
    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();
    let dir3 = tempdir().unwrap();

    let addr1: SocketAddr = format!("127.0.0.1:{}", base_port).parse().unwrap();
    let addr2: SocketAddr = format!("127.0.0.1:{}", base_port + 1).parse().unwrap();
    let addr3: SocketAddr = format!("127.0.0.1:{}", base_port + 2).parse().unwrap();

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
    let (shutdown_tx2, rx2) = broadcast::channel(1);
    let (shutdown_tx3, rx3) = broadcast::channel(1);

    tokio::spawn(async move { server1.run(listener1, rx1).await; });
    tokio::spawn(async move { server2.run(listener2, rx2).await; });
    tokio::spawn(async move { server3.run(listener3, rx3).await; });

    tokio::time::sleep(Duration::from_millis(50)).await;

    TestCluster {
        _dir1: dir1,
        _dir2: dir2,
        _dir3: dir3,
        addr1,
        _shutdown_tx1: shutdown_tx1,
        _shutdown_tx2: shutdown_tx2,
        _shutdown_tx3: shutdown_tx3,
    }
}

fn create_client(addr: SocketAddr) -> WalrClient {
    WalrClient::with_config(
        vec![addr.to_string()],
        ClientConfig {
            buffer_window_ms: 1,
            max_batch_size: 100,
            max_redirects: 5,
        },
    )
}

/// Helper function to test push, poll, content verification, and ack
async fn roundtrip_test(client: &WalrClient, queue: &str, payload: Bytes, test_name: &str) {
    let id = client.push(queue, payload.clone(), 0).await.expect(&format!("{}: push failed", test_name));
    let polled = client.poll(queue, 10, 1).await.expect(&format!("{}: poll failed", test_name));
    assert_eq!(polled.len(), 1, "{}: expected 1 message", test_name);
    assert_eq!(polled[0].message_id, id, "{}: message ID mismatch", test_name);
    assert_eq!(polled[0].payload, payload, "{}: payload content mismatch", test_name);
    let acked = client.ack(queue, &id, &polled[0].receipt_handle).await.expect(&format!("{}: ack failed", test_name));
    assert!(acked, "{}: ack should succeed", test_name);
}

#[tokio::test]
async fn test_payload_matrix() {
    let cluster = start_test_cluster(58210).await;
    let client = create_client(cluster.addr1);
    let q = "payload-matrix-q";

    // 1. Empty Payload (0 bytes)
    roundtrip_test(&client, q, Bytes::from_static(b""), "empty-0-bytes").await;

    // 2. Single Byte Payloads (0x00, 0xFF, 0x7F)
    roundtrip_test(&client, q, Bytes::from(vec![0x00]), "single-null-byte").await;
    roundtrip_test(&client, q, Bytes::from(vec![0xFF]), "single-0xff-byte").await;
    roundtrip_test(&client, q, Bytes::from(vec![0x7F]), "single-ascii-del-byte").await;

    // 3. Raw Binary Null Bytes (Embedded \0 in varying positions)
    let binary_nulls = vec![0x00, 0x01, 0x00, 0x02, 0x00, 0x00, 0xFF, 0xFE, 0x00];
    roundtrip_test(&client, q, Bytes::from(binary_nulls), "raw-binary-nulls").await;

    // 4. Boundary around Zstd Compression Threshold (127 bytes uncompressed, 128 bytes, 129 bytes compressed)
    let p_127 = Bytes::from(vec![b'A'; 127]);
    let p_128 = Bytes::from(vec![b'B'; 128]);
    let p_129 = Bytes::from(vec![b'C'; 129]);
    roundtrip_test(&client, q, p_127, "boundary-127-bytes").await;
    roundtrip_test(&client, q, p_128, "boundary-128-bytes").await;
    roundtrip_test(&client, q, p_129, "boundary-129-bytes-compressed").await;

    // 5. UTF-8 Edge Cases: Multilingual, Emojis, RTL, Zero-width joiners
    let utf8_sample = "Hello 世界 🌍 🦀 🚀 \u{200B}\u{200D} عَرَبِيّ \u{0000} \u{FFFF}";
    roundtrip_test(&client, q, Bytes::from(utf8_sample), "utf8-emoji-multilingual").await;

    // 6. JSON Object & Nested Documents
    let json_data = r#"{"event":"purchase","user_id":"u_9921","price":199.99,"items":[{"sku":"A1","qty":2}],"active":true,"null_field":null}"#;
    roundtrip_test(&client, q, Bytes::from(json_data), "json-payload").await;

    // 7. Base64 & Encoded Data Strings
    let base64_str = "TG9yZW0gaXBzdW0gZG9sb3Igc2l0IGFtZXQsIGNvbnNlY3RldHVyIGFkaXBpc2NpbmcgZWxpdC4=";
    roundtrip_test(&client, q, Bytes::from(base64_str), "base64-encoded").await;

    // 8. High Entropy Random Binary (Incompressible data)
    let mut high_entropy = Vec::with_capacity(4096);
    let mut seed = 0x12345678u32;
    for _ in 0..4096 {
        seed = seed.wrapping_mul(1664525).wrapping_add(1013904223);
        high_entropy.push((seed >> 16) as u8);
    }
    roundtrip_test(&client, q, Bytes::from(high_entropy), "high-entropy-random-4kb").await;

    // 9. Highly Repetitive Run-Length Data (Maximal Zstd compression ratio)
    let repetitive = vec![0x42u8; 64 * 1024]; // 64 KB of 'B'
    roundtrip_test(&client, q, Bytes::from(repetitive), "repetitive-64kb-compressed").await;

    // 10. Large Binary Payload (512 KB)
    let mut large_buf = Vec::with_capacity(512 * 1024);
    for i in 0..(512 * 1024) {
        large_buf.push((i % 256) as u8);
    }
    roundtrip_test(&client, q, Bytes::from(large_buf), "large-512kb-binary").await;

    // 11. 1 MB Binary Payload
    let mut one_mb_buf = Vec::with_capacity(1024 * 1024);
    for i in 0..(1024 * 1024) {
        one_mb_buf.push((i % 251) as u8);
    }
    roundtrip_test(&client, q, Bytes::from(one_mb_buf), "large-1mb-binary").await;
}

#[tokio::test]
async fn test_batch_payload_heterogeneous_mix() {
    let cluster = start_test_cluster(58220).await;
    let client = create_client(cluster.addr1);
    let q = "heterogeneous-batch-q";

    let payloads = vec![
        Bytes::from_static(b""),                                    // 0-byte empty
        Bytes::from(vec![0x00]),                                    // 1 null byte
        Bytes::from("Unicode 🚀 🌟 ⚡️"),                           // Emoji UTF-8
        Bytes::from(vec![0xFF; 256]),                               // 256B compressed
        Bytes::from(vec![0xAA; 10 * 1024]),                         // 10KB repetitive
        Bytes::from(r#"{"nested":{"array":[1,2,3],"ok":true}}"#),   // JSON
    ];

    let ids = client.push_batch(q, payloads.clone()).await.unwrap();
    assert_eq!(ids.len(), payloads.len());

    let polled = client.poll(q, 10, 10).await.unwrap();
    assert_eq!(polled.len(), payloads.len());

    for (p, expected) in polled.iter().zip(payloads.iter()) {
        assert_eq!(&p.payload, expected);
        let acked = client.ack(q, &p.message_id, &p.receipt_handle).await.unwrap();
        assert!(acked);
    }

    let empty = client.poll(q, 10, 10).await.unwrap();
    assert!(empty.is_empty());
}
