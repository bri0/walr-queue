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

const TOTAL_MESSAGES: usize = 5_000;

#[tokio::test]
async fn test_node_kill_and_disk_recovery() {
    let dir = tempdir().unwrap();
    let addr: SocketAddr = "127.0.0.1:49051".parse().unwrap();

    let options = QueueOptions {
        max_delivery_count: 5,
        default_visibility_timeout_sec: 30,
        max_hot_messages_in_ram: 10_000,
        max_wal_segment_size: 64 * 1024 * 1024,
    };

    // 1. First Boot
    {
        let engine = Arc::new(QueueEngine::open(dir.path(), options.clone()).unwrap());
        let raft = Arc::new(RaftNode::new(addr.to_string(), vec![], Arc::clone(&engine)));
        raft.become_leader_for_test().await;

        let server = Arc::new(WalrServer::new_raft(engine, raft, addr.to_string()));
        let listener = TcpListener::bind(addr).await.unwrap();
        let (tx, rx) = broadcast::channel(1);

        tokio::spawn(async move { server.run(listener, rx).await; });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut push_handles = Vec::new();
        for batch_idx in 0..10 {
            push_handles.push(tokio::spawn(async move {
                let stream = TcpStream::connect(addr).await.unwrap();
                let (mut r, mut w) = stream.into_split();
                let mut batch_ids = Vec::new();
                for i in 0..500 {
                    let seq = batch_idx * 500 + i;
                    let req = Request::Push {
                        queue_name: "heavy-durability-q".to_string(),
                        payload: format!("heavy-payload-{}", seq).into_bytes(),
                        delay_seconds: 0,
                        message_id: String::new(),
                    };
                    codec::write_message(&mut w, &req).await.unwrap();
                    let resp: Response = codec::read_message(&mut r).await.unwrap();
                    if let Response::Push { message_id } = resp {
                        batch_ids.push(message_id);
                    }
                }
                batch_ids
            }));
        }

        let mut all_pushed_ids = Vec::new();
        for h in push_handles {
            all_pushed_ids.extend(h.await.unwrap());
        }
        assert_eq!(all_pushed_ids.len(), TOTAL_MESSAGES);

        // Abrupt server kill
        let _ = tx.send(());
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // 2. Node Revival
    {
        let engine = Arc::new(QueueEngine::open(dir.path(), options).unwrap());
        let raft = Arc::new(RaftNode::new(addr.to_string(), vec![], Arc::clone(&engine)));
        raft.become_leader_for_test().await;

        let server = Arc::new(WalrServer::new_raft(engine, raft, addr.to_string()));
        let listener = TcpListener::bind(addr).await.unwrap();
        let (tx, rx) = broadcast::channel(1);

        tokio::spawn(async move { server.run(listener, rx).await; });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let stream = TcpStream::connect(addr).await.unwrap();
        let (mut r, mut w) = stream.into_split();

        let mut polled_count = 0;
        while polled_count < TOTAL_MESSAGES {
            let req = Request::Poll {
                queue_name: "heavy-durability-q".to_string(),
                visibility_timeout_sec: 30,
                batch_size: 500,
            };
            codec::write_message(&mut w, &req).await.unwrap();
            let resp: Response = codec::read_message(&mut r).await.unwrap();
            if let Response::Poll { messages } = resp {
                polled_count += messages.len();
                if messages.is_empty() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        }
        assert_eq!(polled_count, TOTAL_MESSAGES);
        let _ = tx.send(());
    }
}
