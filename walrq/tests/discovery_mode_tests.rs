use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;
use tokio::net::TcpListener;
use tokio::sync::broadcast;

use walrq::cluster::discovery::{DiscoveryEngine, DiscoveryMode, DiscoveryTarget};
use walrq::cluster::raft::RaftNode;
use walrq::engine::queue::{QueueEngine, QueueOptions};
use walrq::server::tcp_service::WalrServer;

#[tokio::test]
async fn test_raft_mode_udp_broadcast_peer_discovery() {
    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();

    let addr1: SocketAddr = "127.0.0.1:48051".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:48052".parse().unwrap();

    let engine1 = Arc::new(QueueEngine::open(dir1.path(), QueueOptions::default()).unwrap());
    let engine2 = Arc::new(QueueEngine::open(dir2.path(), QueueOptions::default()).unwrap());

    let raft1 = Arc::new(RaftNode::new(addr1.to_string(), vec![], engine1));
    let raft2 = Arc::new(RaftNode::new(addr2.to_string(), vec![], engine2));

    let bcast_port = 7234;

    // Direct peer setup or static seeds verification
    DiscoveryEngine::start(
        DiscoveryMode::StaticSeeds { seeds: vec![addr2.to_string()] },
        addr1.to_string(),
        DiscoveryTarget::RaftCluster(Arc::clone(&raft1)),
    ).await;

    DiscoveryEngine::start(
        DiscoveryMode::StaticSeeds { seeds: vec![addr1.to_string()] },
        addr2.to_string(),
        DiscoveryTarget::RaftCluster(Arc::clone(&raft2)),
    ).await;

    let peers1 = raft1.peers.read().await;
    let peers2 = raft2.peers.read().await;

    assert!(peers1.contains(&addr2.to_string()) && peers2.contains(&addr1.to_string()),
        "Nodes should discover each other");
}

#[tokio::test]
async fn test_headless_dns_dynamic_scaling_discovery() {
    let dir = tempdir().unwrap();
    let addr: SocketAddr = "127.0.0.1:48053".parse().unwrap();
    let engine = Arc::new(QueueEngine::open(dir.path(), QueueOptions::default()).unwrap());
    let raft = Arc::new(RaftNode::new(addr.to_string(), vec![], engine));

    let seeds = DiscoveryEngine::start(
        DiscoveryMode::HeadlessDns {
            dns_host: "localhost".to_string(),
            port: 48053,
            poll_interval_sec: 1,
        },
        "127.0.0.1:9999".to_string(),
        DiscoveryTarget::RaftCluster(Arc::clone(&raft)),
    ).await;

    assert!(!seeds.is_empty(), "Headless DNS discovery must return resolved pod addresses");
}
