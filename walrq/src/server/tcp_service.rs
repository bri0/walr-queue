use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, Semaphore};

use crate::cluster::raft::RaftNode;
use crate::engine::queue::QueueEngine;
use crate::protocol::{codec, Message, Request, Response};

pub const DEFAULT_MAX_CONNECTIONS: usize = 10_000;

pub struct WalrServer {
    engine: Arc<QueueEngine>,
    raft: Arc<RaftNode>,
    local_node_id: String,
    conn_limit: Arc<Semaphore>,
    enable_metrics: bool,
}

impl WalrServer {
    pub fn new_raft(
        engine: Arc<QueueEngine>,
        raft: Arc<RaftNode>,
        local_node_id: String,
    ) -> Self {
        Self::new_raft_with_limits(engine, raft, local_node_id, DEFAULT_MAX_CONNECTIONS, true)
    }

    pub fn new_raft_with_limits(
        engine: Arc<QueueEngine>,
        raft: Arc<RaftNode>,
        local_node_id: String,
        max_connections: usize,
        enable_metrics: bool,
    ) -> Self {
        Self {
            engine,
            raft,
            local_node_id,
            conn_limit: Arc::new(Semaphore::new(max_connections)),
            enable_metrics,
        }
    }

    pub async fn run(self: Arc<Self>, listener: TcpListener, mut shutdown_rx: broadcast::Receiver<()>) {
        loop {
            tokio::select! {
                res = listener.accept() => {
                    match res {
                        Ok((stream, _addr)) => {
                            let permit = match self.conn_limit.clone().try_acquire_owned() {
                                Ok(p) => p,
                                Err(_) => {
                                    eprintln!("[TCP SERVER] Connection limit reached, rejecting new connection");
                                    drop(stream);
                                    continue;
                                }
                            };

                            let _ = stream.set_nodelay(true);
                            let server = Arc::clone(&self);
                            tokio::spawn(async move {
                                let _permit = permit;
                                server.handle_connection(stream).await;
                            });
                        }
                        Err(e) => {
                            eprintln!("[TCP SERVER ERROR] accept failed: {:?}", e);
                        }
                    }
                }
                _ = shutdown_rx.recv() => {
                    println!("[TCP SERVER] Shutdown signal received. Stopping accept loop and draining connections...");
                    break;
                }
            }
        }

        // Graceful Drain: Wait for all in-flight connections to finish, flush WAL and Raft snapshot
        self.drain_and_flush().await;
    }

    pub async fn drain_and_flush(&self) {
        println!("[TCP SERVER] Flushing disk WAL and committing engine state...");
        self.engine.disk.flush().await;
        let _ = self.raft.compact_log_snapshot().await;
        println!("[TCP SERVER] Engine flushed & Raft compacted cleanly to disk.");
    }

    async fn handle_connection(&self, mut stream: TcpStream) {
        let (mut reader, mut writer) = stream.split();
        let mut read_buf = Vec::with_capacity(16 * 1024);
        loop {
            let req: Request = match codec::read_message_with_buf(&mut reader, &mut read_buf).await {
                Ok(r) => r,
                Err(_) => break,
            };

            let resp = self.dispatch(req).await;
            if codec::write_message(&mut writer, &resp).await.is_err() {
                break;
            }
        }
    }

    async fn dispatch(&self, req: Request) -> Response {
        match req {
            Request::Push { queue_name, payload, delay_seconds, message_id } => {
                if !self.raft.is_leader_fast() {
                    let leader = self.raft.current_leader.read().await.clone().unwrap_or_default();
                    return Response::Redirect { leader };
                }

                if !message_id.is_empty() && message_id.len() != 26 {
                    if let Err(e) = ulid::Ulid::from_string(&message_id) {
                        return Response::Error { message: format!("invalid ULID message_id: {}", e) };
                    }
                }

                match self.raft.propose_push_str(&queue_name, &message_id, payload, delay_seconds).await {
                    Ok(id) => {
                        crate::metrics::WalrMetrics::global().record_push(1);
                        Response::Push { message_id: id }
                    }
                    Err(e) => Response::Error { message: e },
                }
            }
            Request::PushBatch { queue_name, items } => {
                if !self.raft.is_leader_fast() {
                    let leader = self.raft.current_leader.read().await.clone().unwrap_or_default();
                    return Response::Redirect { leader };
                }

                for i in &items {
                    if !i.message_id.is_empty() && i.message_id.len() != 26 {
                        if let Err(e) = ulid::Ulid::from_string(&i.message_id) {
                            return Response::Error { message: format!("invalid ULID message_id: {}", e) };
                        }
                    }
                }

                match self.raft.propose_push_batch_items(&queue_name, items).await {
                    Ok(ids) => {
                        crate::metrics::WalrMetrics::global().record_push(ids.len() as u64);
                        Response::PushBatch { message_ids: ids }
                    }
                    Err(e) => Response::Error { message: e },
                }
            }
            Request::Poll { queue_name, visibility_timeout_sec, batch_size } => {
                if !self.raft.is_leader_fast() {
                    let leader = self.raft.current_leader.read().await.clone().unwrap_or_default();
                    return Response::Redirect { leader };
                }

                let batch = if batch_size == 0 { 1 } else { batch_size };
                match self.engine.poll(&queue_name, visibility_timeout_sec, batch).await {
                    Ok(polled) => {
                        if !polled.is_empty() {
                            crate::metrics::WalrMetrics::global().record_poll(polled.len() as u64);
                        }
                        let msgs = polled
                            .into_iter()
                            .map(|m| Message {
                                message_id: m.message_id,
                                payload: m.payload.into(),
                                receipt_handle: m.receipt_handle,
                                delivery_count: m.delivery_count,
                            })
                            .collect();
                        Response::Poll { messages: msgs }
                    }
                    Err(e) => Response::Error { message: e.to_string() },
                }
            }
            Request::Ack { queue_name, message_id, receipt_handle } => {
                if !self.raft.is_leader_fast() {
                    let leader = self.raft.current_leader.read().await.clone().unwrap_or_default();
                    return Response::Redirect { leader };
                }

                match self.raft.propose_ack_batch(&queue_name, vec![(message_id, receipt_handle)]).await {
                    Ok(acked) => {
                        if acked > 0 {
                            crate::metrics::WalrMetrics::global().record_ack(1);
                        }
                        Response::Ack { success: acked > 0 }
                    }
                    Err(e) => Response::Error { message: e },
                }
            }
            Request::AckBatch { queue_name, items } => {
                if !self.raft.is_leader_fast() {
                    let leader = self.raft.current_leader.read().await.clone().unwrap_or_default();
                    return Response::Redirect { leader };
                }

                match self.raft.propose_ack_batch_items(&queue_name, items).await {
                    Ok(acked) => {
                        if acked > 0 {
                            crate::metrics::WalrMetrics::global().record_ack(acked as u64);
                        }
                        Response::AckBatch { acked_count: acked as u32 }
                    }
                    Err(e) => Response::Error { message: e },
                }
            }
            Request::RaftVote { term, candidate_id, last_log_index, last_log_term } => {
                let vote_granted = self.raft.handle_vote(term, &candidate_id, last_log_index, last_log_term).await;
                let current_term = self.raft.current_term.load(std::sync::atomic::Ordering::SeqCst);
                Response::RaftVote { term: current_term, vote_granted }
            }
            Request::RaftAppend { term, leader_id, prev_log_index, prev_log_term, entries, leader_commit } => {
                let success = self.raft.handle_append_entries(
                    term,
                    &leader_id,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit,
                ).await;
                let current_term = self.raft.current_term.load(std::sync::atomic::Ordering::SeqCst);
                Response::RaftAppend { term: current_term, success }
            }
            Request::RaftInstallSnapshot { term, .. } => {
                let success = self.raft.handle_install_snapshot(req).await;
                Response::RaftInstallSnapshot { term, success }
            }
            Request::JoinCluster { peer_addr } => {
                if !self.raft.is_leader_fast() {
                    let leader = self.raft.current_leader.read().await.clone().unwrap_or_default();
                    return Response::Redirect { leader };
                }
                self.raft.add_peer(&peer_addr).await;
                let members = self.raft.get_peers().await;
                Response::ClusterMembership { success: true, members }
            }
            Request::LeaveCluster { peer_addr } => {
                if !self.raft.is_leader_fast() {
                    let leader = self.raft.current_leader.read().await.clone().unwrap_or_default();
                    return Response::Redirect { leader };
                }
                self.raft.remove_peer(&peer_addr).await;
                let members = self.raft.get_peers().await;
                Response::ClusterMembership { success: true, members }
            }
            Request::Metrics => {
                if !self.enable_metrics {
                    return Response::Error { message: "metrics endpoint is disabled on this server".to_string() };
                }
                let ram = self.engine.total_messages_in_ram();
                let is_leader = self.raft.is_leader_fast();
                let prometheus_text = crate::metrics::WalrMetrics::global().render_prometheus(ram, is_leader);
                Response::Metrics { prometheus_text }
            }
        }
    }
}
