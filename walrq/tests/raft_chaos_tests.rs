use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, Mutex};

use walrq::cluster::raft::RaftNode;
use walrq::engine::queue::{QueueEngine, QueueOptions};
use walrq::protocol::{codec, Request, Response};
use walrq::server::tcp_service::WalrServer;

#[tokio::test]
async fn test_raft_chaos_cluster() {
    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();
    let dir3 = tempdir().unwrap();

    let addr1: SocketAddr = "127.0.0.1:44051".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:44052".parse().unwrap();
    let addr3: SocketAddr = "127.0.0.1:44053".parse().unwrap();

    let engine1 = Arc::new(QueueEngine::open(dir1.path(), QueueOptions::default()).unwrap());
    let engine2 = Arc::new(QueueEngine::open(dir2.path(), QueueOptions::default()).unwrap());
    let engine3 = Arc::new(QueueEngine::open(dir3.path(), QueueOptions::default()).unwrap());

    let raft1 = Arc::new(RaftNode::new(addr1.to_string(), vec![addr2.to_string(), addr3.to_string()], Arc::clone(&engine1)));
    let raft2 = Arc::new(RaftNode::new(addr2.to_string(), vec![addr1.to_string(), addr3.to_string()], Arc::clone(&engine2)));
    let raft3 = Arc::new(RaftNode::new(addr3.to_string(), vec![addr1.to_string(), addr2.to_string()], Arc::clone(&engine3)));

    raft1.become_leader_for_test().await;

    let server1 = Arc::new(WalrServer::new_raft(engine1, Arc::clone(&raft1), addr1.to_string()));
    let server2 = Arc::new(WalrServer::new_raft(engine2, Arc::clone(&raft2), addr2.to_string()));
    let server3 = Arc::new(WalrServer::new_raft(engine3, Arc::clone(&raft3), addr3.to_string()));

    let listener1 = TcpListener::bind(addr1).await.unwrap();
    let listener2 = TcpListener::bind(addr2).await.unwrap();
    let listener3 = TcpListener::bind(addr3).await.unwrap();

    let (_tx1, rx1) = broadcast::channel(1);
    let (_tx2, rx2) = broadcast::channel(1);
    let (_tx3, rx3) = broadcast::channel(1);

    tokio::spawn(async move { server1.run(listener1, rx1).await; });
    tokio::spawn(async move { server2.run(listener2, rx2).await; });
    tokio::spawn(async move { server3.run(listener3, rx3).await; });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Leader push
    let stream = TcpStream::connect(addr1).await.unwrap();
    let (mut r, mut w) = stream.into_split();

    let req = Request::Push {
        queue_name: "raft-chaos-q".to_string(),
        payload: b"chaos-msg".to_vec(),
        delay_seconds: 0,
        message_id: String::new(),
    };
    codec::write_message(&mut w, &req).await.unwrap();
    let resp: Response = codec::read_message(&mut r).await.unwrap();
    let id = match resp {
        Response::Push { message_id } => message_id,
        other => panic!("expected push, got {:?}", other),
    };

    // Poll on leader
    let poll_req = Request::Poll {
        queue_name: "raft-chaos-q".to_string(),
        visibility_timeout_sec: 30,
        batch_size: 10,
    };
    codec::write_message(&mut w, &poll_req).await.unwrap();
    let poll_resp: Response = codec::read_message(&mut r).await.unwrap();
    let msgs = match poll_resp {
        Response::Poll { messages } => messages,
        other => panic!("expected poll, got {:?}", other),
    };
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].message_id, id);
}
