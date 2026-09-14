use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;

use walrq::cluster::raft::RaftNode;
use walrq::engine::queue::{QueueEngine, QueueOptions};
use walrq::protocol::{codec, Request, Response};
use walrq::server::tcp_service::WalrServer;

#[tokio::test]
async fn test_raft_strict_quorum_replication_and_follower_recovery() {
    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();
    let dir3 = tempdir().unwrap();

    let addr1: SocketAddr = "127.0.0.1:42051".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:42052".parse().unwrap();
    let addr3: SocketAddr = "127.0.0.1:42053".parse().unwrap();

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

    let stream1 = TcpStream::connect(addr1).await.unwrap();
    let (mut r1, mut w1) = stream1.into_split();

    let mut pushed_ids = HashSet::new();

    for i in 0..100 {
        let req = Request::Push {
            queue_name: "raft-strict-q".to_string(),
            payload: format!("payload-{}", i).into_bytes(),
            delay_seconds: 0,
            message_id: String::new(),
        };
        codec::write_message(&mut w1, &req).await.unwrap();
        let resp: Response = codec::read_message(&mut r1).await.unwrap();
        match resp {
            Response::Push { message_id } => {
                pushed_ids.insert(message_id);
            }
            other => panic!("expected push, got {:?}", other),
        }
    }
    assert_eq!(pushed_ids.len(), 100);

    // Poll and ack 30 messages on Leader
    let poll_req = Request::Poll {
        queue_name: "raft-strict-q".to_string(),
        visibility_timeout_sec: 30,
        batch_size: 30,
    };
    codec::write_message(&mut w1, &poll_req).await.unwrap();
    let poll_resp: Response = codec::read_message(&mut r1).await.unwrap();

    let msgs = match poll_resp {
        Response::Poll { messages } => messages,
        other => panic!("expected poll, got {:?}", other),
    };
    assert_eq!(msgs.len(), 30);

    for m in &msgs {
        let ack_req = Request::Ack {
            queue_name: "raft-strict-q".to_string(),
            message_id: m.message_id.clone(),
            receipt_handle: m.receipt_handle.clone(),
        };
        codec::write_message(&mut w1, &ack_req).await.unwrap();
        let resp: Response = codec::read_message(&mut r1).await.unwrap();
        match resp {
            Response::Ack { success } => assert!(success),
            other => panic!("expected ack, got {:?}", other),
        }
    }

    // Recover follower Node 2
    raft2.recover_as_new_leader().await;

    let stream2 = TcpStream::connect(addr2).await.unwrap();
    let (mut r2, mut w2) = stream2.into_split();

    let poll2_req = Request::Poll {
        queue_name: "raft-strict-q".to_string(),
        visibility_timeout_sec: 30,
        batch_size: 100,
    };
    codec::write_message(&mut w2, &poll2_req).await.unwrap();
    let poll2_resp: Response = codec::read_message(&mut r2).await.unwrap();

    let msgs2 = match poll2_resp {
        Response::Poll { messages } => messages,
        other => panic!("expected poll, got {:?}", other),
    };

    assert_eq!(msgs2.len(), 70);
}
