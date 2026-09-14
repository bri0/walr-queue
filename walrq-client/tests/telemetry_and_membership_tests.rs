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

#[tokio::test]
async fn test_prometheus_metrics_and_dynamic_raft_membership() {
    let dir1 = tempdir().unwrap();
    let addr1: SocketAddr = "127.0.0.1:57451".parse().unwrap();
    let engine1 = Arc::new(QueueEngine::open(dir1.path(), QueueOptions::default()).unwrap());
    let raft1 = Arc::new(RaftNode::new(addr1.to_string(), vec![], Arc::clone(&engine1)));
    raft1.become_leader_for_test().await;

    let server1 = Arc::new(WalrServer::new_raft(Arc::clone(&engine1), Arc::clone(&raft1), addr1.to_string()));
    let listener1 = TcpListener::bind(addr1).await.unwrap();
    let (_shutdown_tx1, shutdown_rx1) = broadcast::channel(1);
    tokio::spawn(async move { server1.run(listener1, shutdown_rx1).await; });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = create_client(addr1);
    let q = "telemetry-dynamic-q";

    // 1. Push 10 messages and ack 5
    for i in 0..10 {
        client.push(q, Bytes::from(format!("m-{}", i)), 0).await.unwrap();
    }
    let polled = client.poll(q, 10, 5).await.unwrap();
    assert_eq!(polled.len(), 5);
    for m in &polled {
        client.ack(q, &m.message_id, &m.receipt_handle).await.unwrap();
    }

    // 2. Fetch Prometheus text format metrics from cluster
    let metrics_text = client.get_metrics().await.unwrap();
    println!(">>> Prometheus Metrics Output:\n{}", metrics_text);

    assert!(metrics_text.contains("walrq_messages_pushed_total 10"));
    assert!(metrics_text.contains("walrq_messages_acked_total 5"));
    assert!(metrics_text.contains("walrq_messages_polled_total 5"));
    assert!(metrics_text.contains("walrq_is_leader 1"));

    // 3. Dynamic Raft Membership: Join node 2 on the fly!
    let new_node_addr = "127.0.0.1:57452";
    let members_after_join = client.join_cluster(new_node_addr).await.unwrap();
    println!(">>> Members after join: {:?}", members_after_join);
    assert!(members_after_join.contains(&addr1.to_string()));
    assert!(members_after_join.contains(&new_node_addr.to_string()));
    assert_eq!(members_after_join.len(), 2);

    // 4. Dynamic Raft Membership: Leave node 2 on the fly!
    let members_after_leave = client.leave_cluster(new_node_addr).await.unwrap();
    println!(">>> Members after leave: {:?}", members_after_leave);
    assert!(members_after_leave.contains(&addr1.to_string()));
    assert!(!members_after_leave.contains(&new_node_addr.to_string()));
    assert_eq!(members_after_leave.len(), 1);
}
