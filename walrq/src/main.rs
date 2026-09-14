use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::broadcast;

use walrq::cluster::discovery::{DiscoveryEngine, DiscoveryMode, DiscoveryTarget};
use walrq::cluster::raft::RaftNode;
use walrq::engine::queue::{QueueEngine, QueueOptions};
use walrq::server::tcp_service::WalrServer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let node_id = std::env::var("NODE_ID").unwrap_or_else(|_| "127.0.0.1:50051".to_string());
    let server_addr = std::env::var("SERVER_ADDR").unwrap_or_else(|_| node_id.clone());
    let data_dir = PathBuf::from(std::env::var("DATA_DIR").unwrap_or_else(|_| "./data".to_string()));
    let discovery_mode = DiscoveryMode::from_env();

    println!(
        "Starting Walrq node_id={}, server_addr={}, data_dir={:?}",
        node_id, server_addr, data_dir
    );

    let engine = Arc::new(QueueEngine::open(&data_dir, QueueOptions::default())?);

    let initial_peers = Vec::new();
    let raft = Arc::new(RaftNode::new(
        node_id.clone(),
        initial_peers,
        Arc::clone(&engine),
    ));

    DiscoveryEngine::start(
        discovery_mode,
        node_id.clone(),
        DiscoveryTarget::RaftCluster(Arc::clone(&raft)),
    )
    .await;

    raft.start_heartbeat_and_election_loop();

    let enable_metrics = std::env::var("ENABLE_METRICS").map(|v| v == "1" || v.to_lowercase() == "true").unwrap_or(false);

    let server = Arc::new(WalrServer::new_raft_with_limits(
        engine,
        raft,
        node_id,
        walrq::server::tcp_service::DEFAULT_MAX_CONNECTIONS,
        enable_metrics,
    ));
    let listener = TcpListener::bind(&server_addr).await?;
    println!("TCP Server listening on {}", server_addr);

    let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
    let shutdown_tx_clone = shutdown_tx.clone();

    tokio::spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let mut sigterm = signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    println!("Received SIGINT/Ctrl+C, shutting down gracefully...");
                }
                _ = sigterm.recv() => {
                    println!("Received SIGTERM, shutting down gracefully...");
                }
            }
        }
        #[cfg(not(unix))]
        {
            tokio::signal::ctrl_c().await.ok();
            println!("Received SIGINT/Ctrl+C, shutting down gracefully...");
        }

        let _ = shutdown_tx_clone.send(());
    });

    server.run(listener, shutdown_rx).await;
    println!("Walrq shut down cleanly.");
    Ok(())
}
