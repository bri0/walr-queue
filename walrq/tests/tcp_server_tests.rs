use bytes::Bytes;
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;
use tokio::net::TcpListener;
use tokio::sync::broadcast;

use walrq::cluster::raft::RaftNode;
use walrq::engine::queue::{QueueEngine, QueueOptions};
use walrq::protocol::{codec, Request, Response};
use walrq::server::tcp_service::WalrServer;
use tokio::net::TcpStream;

#[tokio::test]
async fn test_tcp_server_push_poll_ack() {
    let dir = tempdir().unwrap();
    let addr: SocketAddr = "127.0.0.1:51051".parse().unwrap();

    let engine = Arc::new(QueueEngine::open(dir.path(), QueueOptions::default()).unwrap());
    let raft = Arc::new(RaftNode::new(addr.to_string(), vec![], Arc::clone(&engine)));
    raft.become_leader_for_test().await;

    let server = Arc::new(WalrServer::new_raft(engine, raft, addr.to_string()));
    let listener = TcpListener::bind(addr).await.unwrap();
    let (tx, rx) = broadcast::channel(1);

    tokio::spawn(async move {
        server.run(listener, rx).await;
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let stream = TcpStream::connect(addr).await.unwrap();
    let (mut reader, mut writer) = stream.into_split();

    // 1. Push
    let req = Request::Push {
        queue_name: "test-q".to_string(),
        payload: b"hello postcard".to_vec(),
        delay_seconds: 0,
        message_id: String::new(),
    };
    codec::write_message(&mut writer, &req).await.unwrap();
    let resp: Response = codec::read_message(&mut reader).await.unwrap();

    let push_id = match resp {
        Response::Push { message_id } => message_id,
        other => panic!("expected push response, got {:?}", other),
    };
    assert!(!push_id.is_empty());

    // 2. Poll
    let poll_req = Request::Poll {
        queue_name: "test-q".to_string(),
        visibility_timeout_sec: 30,
        batch_size: 10,
    };
    codec::write_message(&mut writer, &poll_req).await.unwrap();
    let poll_resp: Response = codec::read_message(&mut reader).await.unwrap();

    let msgs = match poll_resp {
        Response::Poll { messages } => messages,
        other => panic!("expected poll response, got {:?}", other),
    };
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].message_id, push_id);
    assert_eq!(msgs[0].payload, b"hello postcard");

    // 3. Ack
    let ack_req = Request::Ack {
        queue_name: "test-q".to_string(),
        message_id: msgs[0].message_id.clone(),
        receipt_handle: msgs[0].receipt_handle.clone(),
    };
    codec::write_message(&mut writer, &ack_req).await.unwrap();
    let ack_resp: Response = codec::read_message(&mut reader).await.unwrap();

    match ack_resp {
        Response::Ack { success } => assert!(success),
        other => panic!("expected ack response, got {:?}", other),
    }

    let _ = tx.send(());
}
