use bytes::Bytes;
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

#[tokio::test]
async fn test_user_defined_and_auto_generated_ulid() {
    let dir = tempdir().unwrap();
    let addr: SocketAddr = "127.0.0.1:61152".parse().unwrap();

    let engine = Arc::new(QueueEngine::open(dir.path(), QueueOptions::default()).unwrap());
    let raft = Arc::new(RaftNode::new(addr.to_string(), vec![], Arc::clone(&engine)));
    raft.become_leader_for_test().await;

    let server = Arc::new(WalrServer::new_raft(engine, raft, addr.to_string()));
    let listener = TcpListener::bind(addr).await.unwrap();
    let (_tx, rx) = broadcast::channel(1);

    tokio::spawn(async move {
        server.run(listener, rx).await;
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let config = ClientConfig {
        buffer_window_ms: 50,
        max_batch_size: 10,
        max_redirects: 5,
    };
    let client = WalrClient::with_config(vec![addr.to_string()], config);

    // 1. Auto-generated ULID via push (buffered)
    let auto_id = client.push("test-q", Bytes::from("payload-auto"), 0).await.unwrap();
    assert!(Ulid::from_string(&auto_id).is_ok());

    // 2. Custom ULID via push_with_id (buffered)
    let custom_ulid = Ulid::new().to_string();
    let pushed_custom_id = client
        .push_with_id("test-q", Bytes::from("payload-custom"), 0, &custom_ulid)
        .await
        .unwrap();
    assert_eq!(pushed_custom_id, custom_ulid);

    // 3. Immediate push with custom ULID
    let custom_immediate_ulid = Ulid::new().to_string();
    let res_imm = client
        .push_immediate_with_id("test-q", Bytes::from("payload-imm"), 0, &custom_immediate_ulid)
        .await
        .unwrap();
    assert_eq!(res_imm, custom_immediate_ulid);

    // 4. Batch push with mixed explicit / auto ULIDs
    let batch_custom_id = Ulid::new().to_string();
    let batch_ids = client
        .push_batch_with_ids(
            "test-q",
            vec![
                (Bytes::from("b1"), Some(batch_custom_id.clone())),
                (Bytes::from("b2"), None),
            ],
        )
        .await
        .unwrap();

    assert_eq!(batch_ids.len(), 2);
    assert_eq!(batch_ids[0], batch_custom_id);
    assert!(Ulid::from_string(&batch_ids[1]).is_ok());

    // 5. Poll all and verify receipt of IDs
    let msgs = client.poll("test-q", 30, 10).await.unwrap();
    let polled_ids: Vec<String> = msgs.into_iter().map(|m| m.message_id).collect();

    assert!(polled_ids.contains(&auto_id));
    assert!(polled_ids.contains(&custom_ulid));
    assert!(polled_ids.contains(&custom_immediate_ulid));
    assert!(polled_ids.contains(&batch_custom_id));
}
