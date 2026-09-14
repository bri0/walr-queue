use std::sync::Arc;
use std::time::Duration;
use tokio::net::{lookup_host, UdpSocket};

use super::raft::RaftNode;

#[derive(Debug, Clone)]
pub enum DiscoveryMode {
    /// Mode 1: UDP Broadcast beacon + listener for Bare-Metal / Local LAN / Host networking
    Broadcast { broadcast_port: u16 },
    /// Mode 2: Kubernetes Headless DNS polling
    HeadlessDns { dns_host: String, port: u16, poll_interval_sec: u64 },
    /// Mode 3: Static seed list fallback
    StaticSeeds { seeds: Vec<String> },
}

impl DiscoveryMode {
    pub fn from_env() -> Self {
        if let Ok(dns) = std::env::var("DISCOVERY_HEADLESS_DNS") {
            let port = std::env::var("SERVER_PORT")
                .or_else(|_| std::env::var("TCP_PORT"))
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(50051);
            let poll_interval = std::env::var("DISCOVERY_POLL_INTERVAL_SEC")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(3);
            return Self::HeadlessDns {
                dns_host: dns,
                port,
                poll_interval_sec: poll_interval,
            };
        }

        if let Ok(port_str) = std::env::var("DISCOVERY_BROADCAST_PORT") {
            if let Ok(port) = port_str.parse() {
                return Self::Broadcast { broadcast_port: port };
            }
        }

        if let Ok(seeds_str) = std::env::var("SEEDS") {
            let seeds: Vec<String> = seeds_str
                .split(',')
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
                .collect();
            if !seeds.is_empty() {
                return Self::StaticSeeds { seeds };
            }
        }

        Self::Broadcast {
            broadcast_port: 7001,
        }
    }
}

pub enum DiscoveryTarget {
    RaftCluster(Arc<RaftNode>),
}

pub struct DiscoveryEngine;

impl DiscoveryEngine {
    pub async fn start(
        mode: DiscoveryMode,
        self_node_id: String,
        target: DiscoveryTarget,
    ) -> Vec<String> {
        match mode {
            DiscoveryMode::StaticSeeds { seeds } => {
                let filtered: Vec<String> = seeds.into_iter().filter(|s| s != &self_node_id).collect();
                eprintln!("[DEBUG-DISCOVERY] Static seeds configured: {:?}", filtered);
                for s in &filtered {
                    match &target {
                        DiscoveryTarget::RaftCluster(raft) => {
                            raft.add_peer(s).await;
                        }
                    }
                }
                filtered
            }
            DiscoveryMode::Broadcast { broadcast_port } => {
                eprintln!("[DEBUG-DISCOVERY] Starting UDP Broadcast discovery on port {}", broadcast_port);
                Self::spawn_broadcast_discovery(broadcast_port, self_node_id, target).await
            }
            DiscoveryMode::HeadlessDns {
                dns_host,
                port,
                poll_interval_sec,
            } => {
                eprintln!(
                    "[DEBUG-DISCOVERY] Starting K8s Headless DNS discovery on host='{}', port={}, interval={}s",
                    dns_host, port, poll_interval_sec
                );
                Self::spawn_headless_dns_discovery(dns_host, port, poll_interval_sec, self_node_id, target).await
            }
        }
    }

    async fn spawn_broadcast_discovery(
        broadcast_port: u16,
        self_node_id: String,
        target: DiscoveryTarget,
    ) -> Vec<String> {
        let listen_addr = format!("0.0.0.0:{}", broadcast_port);
        let socket = match UdpSocket::bind(&listen_addr).await {
            Ok(s) => {
                let _ = s.set_broadcast(true);
                Arc::new(s)
            }
            Err(e) => {
                eprintln!("[DEBUG-DISCOVERY-ERR] Failed to bind UDP broadcast socket: {:?}", e);
                return Vec::new();
            }
        };

        let beacon_sock = Arc::clone(&socket);
        let beacon_msg = format!("WALRQ_BEACON:{}", self_node_id);
        let broadcast_target = format!("255.255.255.255:{}", broadcast_port);

        tokio::spawn(async move {
            loop {
                let _ = beacon_sock
                    .send_to(beacon_msg.as_bytes(), &broadcast_target)
                    .await;
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        });

        let mut initial_discovered = Vec::new();
        let target_arc = Arc::new(target);
        let target_clone = Arc::clone(&target_arc);
        let self_id = self_node_id.clone();

        tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            loop {
                if let Ok((len, _addr)) = socket.recv_from(&mut buf).await {
                    if let Ok(msg) = std::str::from_utf8(&buf[..len]) {
                        if let Some(peer_addr) = msg.strip_prefix("WALRQ_BEACON:") {
                            if peer_addr != self_id {
                                match &*target_clone {
                                    DiscoveryTarget::RaftCluster(raft) => {
                                        raft.add_peer(peer_addr).await;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });

        let wait_budget = Duration::from_millis(150);
        let start = tokio::time::Instant::now();
        while start.elapsed() < wait_budget {
            tokio::time::sleep(Duration::from_millis(20)).await;
            match &*target_arc {
                DiscoveryTarget::RaftCluster(raft) => {
                    let peers = raft.peers.read().await;
                    if !peers.is_empty() {
                        initial_discovered = peers.clone();
                        break;
                    }
                }
            }
        }

        initial_discovered
    }

    async fn spawn_headless_dns_discovery(
        dns_host: String,
        port: u16,
        poll_interval_sec: u64,
        self_node_id: String,
        target: DiscoveryTarget,
    ) -> Vec<String> {
        let mut initial_discovered = Vec::new();
        let query = format!("{}:{}", dns_host, port);

        if let Ok(addrs) = lookup_host(&query).await {
            for addr in addrs {
                let node_endpoint = addr.to_string();
                if node_endpoint != self_node_id {
                    match &target {
                        DiscoveryTarget::RaftCluster(raft) => {
                            raft.add_peer(&node_endpoint).await;
                        }
                    }
                    initial_discovered.push(node_endpoint);
                }
            }
        }

        let target_arc = Arc::new(target);
        let self_id = self_node_id.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(poll_interval_sec));
            loop {
                interval.tick().await;
                if let Ok(addrs) = lookup_host(&query).await {
                    for addr in addrs {
                        let node_endpoint = addr.to_string();
                        if node_endpoint != self_id {
                            match &*target_arc {
                                DiscoveryTarget::RaftCluster(raft) => {
                                    raft.add_peer(&node_endpoint).await;
                                }
                            }
                        }
                    }
                }
            }
        });

        initial_discovered
    }
}
