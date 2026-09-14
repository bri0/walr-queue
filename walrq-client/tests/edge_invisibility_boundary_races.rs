use bytes::Bytes;
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
async fn test_extreme_invisibility_boundary_races() {
    let dir = tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let opts = QueueOptions {
        default_visibility_timeout_sec: 1, // 1s timeout
        max_delivery_count: 5,
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

    let client = WalrClient::with_config(
        vec![addr.to_string()],
        ClientConfig {
            buffer_window_ms: 1,
            max_batch_size: 10,
            max_redirects: 5,
        },
    );

    let q = "race_invis_q";
    let mid = client.push(q, Bytes::from("boundary-msg"), 0).await.unwrap();

    // Poll 1
    let p1 = client.poll(q, 1, 1).await.unwrap();
    assert_eq!(p1.len(), 1);
    let r1 = p1[0].receipt_handle.clone();

    // Immediately poll again while still in-flight -> MUST BE EMPTY
    let p_empty = client.poll(q, 1, 1).await.unwrap();
    assert!(p_empty.is_empty(), "Queue must not return leased message");

    // Sleep 1050ms so lease expires
    tokio::time::sleep(Duration::from_millis(1050)).await;

    // Concurrent race: Poller tries to poll(q) while old worker tries to ack(r1) simultaneously
    let client_ack = client.clone();
    let client_poll = client.clone();
    let q_ack = q.to_string();
    let mid_ack = mid.clone();
    let r1_ack = r1.clone();

    let ack_task = tokio::spawn(async move {
        client_ack.ack(&q_ack, &mid_ack, &r1_ack).await
    });

    let poll_task = tokio::spawn(async move {
        client_poll.poll(q, 1, 1).await
    });

    let (ack_res, poll_res) = tokio::join!(ack_task, poll_task);
    let ack_ok = ack_res.unwrap().unwrap_or(false);
    let polled = poll_res.unwrap().unwrap();

    // Exactly one of two outcomes is mathematically valid in this race:
    // Outcome A: ACK landed right before TimerWheel expired -> ack_ok = true, poll returned empty.
    // Outcome B: TimerWheel expired & poll re-acquired lease -> ack_ok = false, poll returned 1 msg with new receipt.
    if ack_ok {
        assert!(polled.is_empty(), "If stale ack won race, message must not be repolled");
    } else {
        assert_eq!(polled.len(), 1, "If poll won race, message must be repolled");
        assert_ne!(polled[0].receipt_handle, r1, "New lease must have distinct receipt");
        assert!(client.ack(q, &mid, &polled[0].receipt_handle).await.unwrap());
    }

    // After resolution, queue must be empty
    assert!(client.poll(q, 1, 1).await.unwrap().is_empty());
}
