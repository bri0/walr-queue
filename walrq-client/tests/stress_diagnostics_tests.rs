use bytes::Bytes;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;
use tokio::net::TcpListener;
use tokio::sync::broadcast;

use walrq::cluster::raft::RaftNode;
use walrq::engine::queue::{QueueEngine, QueueOptions};
use walrq::server::tcp_service::WalrServer;
use walrq_client::{ClientConfig, WalrClient};

#[tokio::test]
async fn test_stress_diagnostics_multi_producer_consumer() {
    let dir = tempdir().unwrap();
    let addr: SocketAddr = "127.0.0.1:64051".parse().unwrap();

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
        buffer_window_ms: 10,
        max_batch_size: 100,
        max_redirects: 5,
    };
    let client = Arc::new(WalrClient::with_config(vec![addr.to_string()], config));

    let pushed = Arc::new(AtomicU64::new(0));
    let acked = Arc::new(AtomicU64::new(0));

    let mut prod_handles = Vec::new();
    for p_id in 0..4 {
        let cl = Arc::clone(&client);
        let p_cnt = Arc::clone(&pushed);
        prod_handles.push(tokio::spawn(async move {
            for i in 0..250 {
                let p = Bytes::from(format!("stress-p{}-{}", p_id, i));
                if cl.push("stress-diag-q", p, 0).await.is_ok() {
                    p_cnt.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }

    for h in prod_handles {
        h.await.unwrap();
    }
    assert_eq!(pushed.load(Ordering::Relaxed), 1000);

    let mut polled = 0;
    while polled < 1000 {
        let msgs = client.poll("stress-diag-q", 30, 100).await.unwrap();
        for m in &msgs {
            let ok = client.ack("stress-diag-q", &m.message_id, &m.receipt_handle).await.unwrap();
            if ok {
                acked.fetch_add(1, Ordering::Relaxed);
            }
        }
        polled += msgs.len();
        if msgs.is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    assert_eq!(acked.load(Ordering::Relaxed), 1000);
}
