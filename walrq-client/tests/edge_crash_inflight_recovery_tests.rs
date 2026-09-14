use bytes::Bytes;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use walrq::cluster::raft::RaftNode;
use walrq::engine::queue::{QueueEngine, QueueOptions};
use walrq::server::tcp_service::WalrServer;
use walrq_client::{AckItem, ClientConfig, WalrClient};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_extreme_restart_with_in_flight_leases_and_dlq() {
    let dir = tempdir().unwrap();
    let data_path = dir.path().to_path_buf();

    let opts = QueueOptions {
        default_visibility_timeout_sec: 10,
        max_delivery_count: 2,
        max_hot_messages_in_ram: 50,
        max_wal_segment_size: 16 * 1024 * 1024,
    };

    let q = "restart_flight_q";
    let addr_str: String;
    let mid_flight: String;
    let receipt_flight: String;
    let mid_unpolled: String;

    // === EPOCH 1: Start node, push messages, poll one, then abruptly shut down while lease is active ===
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        addr_str = addr.to_string();

        let engine = Arc::new(QueueEngine::open(&data_path, opts.clone()).unwrap());
        let raft = Arc::new(RaftNode::new(addr_str.clone(), vec![], Arc::clone(&engine)));
        raft.become_leader_for_test().await;

        let server = Arc::new(WalrServer::new_raft(Arc::clone(&engine), Arc::clone(&raft), addr_str.clone()));
        let (tx, rx) = broadcast::channel(1);
        let s_handle = tokio::spawn(async move { server.run(listener, rx).await; });

        tokio::time::sleep(Duration::from_millis(50)).await;

        let client = WalrClient::with_config(
            vec![addr_str.clone()],
            ClientConfig {
                buffer_window_ms: 1,
                max_batch_size: 10,
                max_redirects: 5,
            },
        );

        mid_flight = client.push(q, Bytes::from("in-flight-at-crash"), 0).await.unwrap();
        mid_unpolled = client.push(q, Bytes::from("unpolled-at-crash"), 0).await.unwrap();

        // Poll message 1 -> creates active lease
        let polled = client.poll(q, 10, 1).await.unwrap();
        assert_eq!(polled.len(), 1);
        assert_eq!(polled[0].message_id, mid_flight);
        receipt_flight = polled[0].receipt_handle.clone();

        // Abrupt shutdown while msg 1 is in-flight!
        let _ = tx.send(());
        let _ = s_handle.await;
    }

    tokio::time::sleep(Duration::from_millis(100)).await;

    // === EPOCH 2: Restart node from same WAL storage ===
    {
        let listener = TcpListener::bind(&addr_str).await.unwrap();

        let engine = Arc::new(QueueEngine::open(&data_path, opts.clone()).unwrap());
        let raft = Arc::new(RaftNode::new(addr_str.clone(), vec![], Arc::clone(&engine)));
        raft.become_leader_for_test().await;

        let server = Arc::new(WalrServer::new_raft(Arc::clone(&engine), Arc::clone(&raft), addr_str.clone()));
        let (_tx, rx) = broadcast::channel(1);
        tokio::spawn(async move { server.run(listener, rx).await; });

        tokio::time::sleep(Duration::from_millis(50)).await;

        let client = WalrClient::with_config(
            vec![addr_str.clone()],
            ClientConfig {
                buffer_window_ms: 1,
                max_batch_size: 10,
                max_redirects: 5,
            },
        );

        // Client from before restart attempts to ACK using old receipt -> MUST FAIL (crash reset in-memory wheel)
        let stale_ack_res = client.ack(q, &mid_flight, &receipt_flight).await.unwrap();
        assert!(!stale_ack_res, "Old in-flight receipt from pre-crash session must not be valid");

        // Now poll the queue: both unpolled message AND the crashed in-flight message must be safely available!
        let polled_all = client.poll(q, 5, 10).await.unwrap();
        assert_eq!(polled_all.len(), 2, "Both messages must survive crash and be repollable");

        let ids: Vec<String> = polled_all.iter().map(|m| m.message_id.clone()).collect();
        assert!(ids.contains(&mid_flight));
        assert!(ids.contains(&mid_unpolled));

        // ACK both cleanly
        for m in polled_all {
            assert!(client.ack(q, &m.message_id, &m.receipt_handle).await.unwrap());
        }

        // Queue is now empty
        assert!(client.poll(q, 5, 10).await.unwrap().is_empty());
    }
}
