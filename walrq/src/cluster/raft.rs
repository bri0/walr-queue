use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, RwLock};
use ulid::Ulid;
use uuid::Uuid;

use crate::engine::queue::{PushItem, QueueEngine};
use crate::protocol::{codec, RaftLogEntryWire, Request, Response, SnapshotItem};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RaftRole {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftCommand {
    Push {
        queue: String,
        msg_id: String,
        payload: Vec<u8>,
        visible_at: u64,
    },
    Ack {
        queue: String,
        msg_id: String,
        receipt: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub term: u64,
    pub index: u64,
    pub command: RaftCommand,
}

struct PeerConnection {
    writer: OwnedWriteHalf,
    reader: OwnedReadHalf,
    read_buf: Vec<u8>,
}

pub struct RaftNode {
    pub node_id: String,
    pub peers: Arc<RwLock<Vec<String>>>,
    pub role: Arc<RwLock<RaftRole>>,
    pub current_term: Arc<AtomicU64>,
    pub current_leader: Arc<RwLock<Option<String>>>,
    pub log: Arc<RwLock<Vec<LogEntry>>>,
    pub commit_index: Arc<AtomicU64>,
    pub snapshot_index: Arc<AtomicU64>,
    pub snapshot_term: Arc<AtomicU64>,
    pub engine: Arc<QueueEngine>,
    pub is_running: Arc<AtomicBool>,
    pub compaction_threshold: u64,
    is_leader_cached: Arc<AtomicBool>,
    peer_conns: Arc<RwLock<HashMap<String, Arc<Mutex<PeerConnection>>>>>,
}

impl RaftNode {
    pub fn new(node_id: String, peers: Vec<String>, engine: Arc<QueueEngine>) -> Self {
        Self::with_threshold(node_id, peers, engine, 10_000)
    }

    pub fn with_threshold(node_id: String, peers: Vec<String>, engine: Arc<QueueEngine>, compaction_threshold: u64) -> Self {
        Self {
            node_id,
            peers: Arc::new(RwLock::new(peers)),
            role: Arc::new(RwLock::new(RaftRole::Follower)),
            current_term: Arc::new(AtomicU64::new(0)),
            current_leader: Arc::new(RwLock::new(None)),
            log: Arc::new(RwLock::new(Vec::new())),
            commit_index: Arc::new(AtomicU64::new(0)),
            snapshot_index: Arc::new(AtomicU64::new(0)),
            snapshot_term: Arc::new(AtomicU64::new(0)),
            engine,
            is_running: Arc::new(AtomicBool::new(true)),
            compaction_threshold,
            is_leader_cached: Arc::new(AtomicBool::new(false)),
            peer_conns: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    async fn get_peer_conn_handle(&self, peer: &str) -> Option<Arc<Mutex<PeerConnection>>> {
        {
            let conns = self.peer_conns.read().await;
            if let Some(h) = conns.get(peer) {
                return Some(Arc::clone(h));
            }
        }

        let mut conns = self.peer_conns.write().await;
        if let Some(h) = conns.get(peer) {
            return Some(Arc::clone(h));
        }

        let stream = match TcpStream::connect(peer).await {
            Ok(s) => {
                let _ = s.set_nodelay(true);
                s
            }
            Err(_) => return None,
        };

        let (reader, writer) = stream.into_split();
        let handle = Arc::new(Mutex::new(PeerConnection {
            writer,
            reader,
            read_buf: Vec::with_capacity(16 * 1024),
        }));
        conns.insert(peer.to_string(), Arc::clone(&handle));
        Some(handle)
    }

    async fn send_peer_request(&self, peer: &str, req: Request) -> Result<Response, String> {
        let handle = match self.get_peer_conn_handle(peer).await {
            Some(h) => h,
            None => return Err(format!("Peer {} unreachable", peer)),
        };
        let mut conn = handle.lock().await;
        let PeerConnection { ref mut writer, ref mut reader, ref mut read_buf } = *conn;

        if codec::write_message(writer, &req).await.is_ok() {
            if let Ok(resp) = codec::read_message_with_buf(reader, read_buf).await {
                return Ok(resp);
            }
        }

        // Drop broken connection and reconnect once
        drop(conn);
        {
            let mut conns = self.peer_conns.write().await;
            conns.remove(peer);
        }

        let new_handle = match self.get_peer_conn_handle(peer).await {
            Some(h) => h,
            None => return Err(format!("Peer {} reconnect failed", peer)),
        };
        let mut conn = new_handle.lock().await;
        let PeerConnection { ref mut writer, ref mut reader, ref mut read_buf } = *conn;
        codec::write_message(writer, &req).await.map_err(|e| e.to_string())?;
        codec::read_message_with_buf(reader, read_buf).await.map_err(|e| e.to_string())
    }

    pub async fn drop_peer_connection(&self, peer: &str) {
        let mut conns = self.peer_conns.write().await;
        conns.remove(peer);
    }

    pub fn start_heartbeat_and_election_loop(self: &Arc<Self>) {
        let node = Arc::clone(self);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_millis(1500));
            loop {
                ticker.tick().await;
                if !node.is_running.load(Ordering::Relaxed) {
                    break;
                }

                let is_leader = *node.role.read().await == RaftRole::Leader;
                if is_leader {
                    let peers = node.peers.read().await.clone();
                    let term = node.current_term.load(Ordering::SeqCst);
                    for peer in peers {
                        let n_ref = Arc::clone(&node);
                        let p_clone = peer.clone();
                        tokio::spawn(async move {
                            let req = Request::RaftAppend {
                                term,
                                leader_id: n_ref.node_id.clone(),
                                prev_log_index: 0,
                                prev_log_term: 0,
                                entries: Vec::new(),
                                leader_commit: n_ref.commit_index.load(Ordering::SeqCst),
                            };
                            let _ = n_ref.send_peer_request(&p_clone, req).await;
                        });
                    }
                } else {
                    let has_leader = node.current_leader.read().await.is_some();
                    if !has_leader {
                        let peers = node.peers.read().await.clone();
                        let mut all = peers.clone();
                        all.push(node.node_id.clone());
                        all.sort();
                        if all.first() == Some(&node.node_id) {
                            eprintln!("[RAFT-ELECTION] Node {} electing itself as initial Leader", node.node_id);
                            node.recover_as_new_leader().await;
                        }
                    }
                }
            }
        });
    }

    pub async fn compact_log_snapshot(&self) {
        let commit = self.commit_index.load(Ordering::SeqCst);
        let mut log = self.log.write().await;

        if let Some(first) = log.first() {
            if commit > first.index {
                if let Some(pos) = log.iter().rposition(|e| e.index <= commit) {
                    let snap_term = log[pos].term;
                    let snap_idx = log[pos].index;
                    log.drain(0..=pos);
                    self.snapshot_index.store(snap_idx, Ordering::SeqCst);
                    self.snapshot_term.store(snap_term, Ordering::SeqCst);
                }
            }
        }
    }

    pub async fn sync_all_entries_to_peer(&self, peer_addr: &str) {
        let surviving = self.engine.get_surviving_snapshot_messages().await;
        let snapshot_items: Vec<SnapshotItem> = surviving
            .into_iter()
            .map(|(id, queue, visible_at, payload)| SnapshotItem {
                id: id.to_string(),
                queue,
                visible_at,
                payload: payload.to_vec(),
            })
            .collect();

        let snap_term = self.snapshot_term.load(Ordering::SeqCst);
        let snap_idx = self.snapshot_index.load(Ordering::SeqCst);

        let snap_req = Request::RaftInstallSnapshot {
            term: self.current_term.load(Ordering::SeqCst),
            leader_id: self.node_id.clone(),
            last_included_index: snap_idx,
            last_included_term: snap_term,
            items: snapshot_items,
        };
        let _ = self.send_peer_request(peer_addr, snap_req).await;

        let log = self.log.read().await.clone();
        if !log.is_empty() {
            let term = self.current_term.load(Ordering::SeqCst);
            let mut entries = Vec::with_capacity(log.len());

            for entry in log {
                if let Ok(cmd_bytes) = postcard::to_allocvec(&entry.command) {
                    entries.push(RaftLogEntryWire {
                        term: entry.term,
                        index: entry.index,
                        command: cmd_bytes,
                    });
                }
            }

            let req = Request::RaftAppend {
                term,
                leader_id: self.node_id.clone(),
                prev_log_index: snap_idx,
                prev_log_term: snap_term,
                entries,
                leader_commit: self.commit_index.load(Ordering::SeqCst),
            };

            let _ = self.send_peer_request(peer_addr, req).await;
        }
    }

    pub async fn send_heartbeat(&self) {
        let peers = self.peers.read().await.clone();
        let term = self.current_term.load(Ordering::SeqCst);
        let commit = self.commit_index.load(Ordering::SeqCst);
        for peer in peers {
            let req = Request::RaftAppend {
                term,
                leader_id: self.node_id.clone(),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: Vec::new(),
                leader_commit: commit,
            };
            let _ = self.send_peer_request(&peer, req).await;
        }
    }

    pub async fn add_peer(&self, peer_addr: &str) {
        let mut p = self.peers.write().await;
        if !p.contains(&peer_addr.to_string()) && peer_addr != self.node_id {
            p.push(peer_addr.to_string());
        }
    }

    pub async fn remove_peer(&self, peer_addr: &str) {
        let mut p = self.peers.write().await;
        p.retain(|addr| addr != peer_addr);
        drop(p);
        self.drop_peer_connection(peer_addr).await;
    }

    pub async fn get_peers(&self) -> Vec<String> {
        let p = self.peers.read().await;
        let mut all = vec![self.node_id.clone()];
        all.extend(p.iter().cloned());
        all
    }

    pub fn is_leader_fast(&self) -> bool {
        self.is_leader_cached.load(Ordering::Relaxed)
    }

    pub async fn is_leader(&self) -> bool {
        *self.role.read().await == RaftRole::Leader
    }

    pub async fn become_leader_for_test(&self) {
        let mut r = self.role.write().await;
        *r = RaftRole::Leader;
        self.is_leader_cached.store(true, Ordering::Release);
        let mut l = self.current_leader.write().await;
        *l = Some(self.node_id.clone());
    }

    pub async fn propose_push(&self, queue: &str, msg_id: Uuid, payload: Vec<u8>) -> Result<String, String> {
        self.propose_push_str(queue, &msg_id.to_string(), payload, 0).await
    }

    pub async fn propose_push_str(&self, queue: &str, msg_id: &str, payload: Vec<u8>, delay_sec: u64) -> Result<String, String> {
        let results = self.propose_push_batch(queue, vec![(msg_id.to_string(), payload, delay_sec)]).await?;
        Ok(results.into_iter().next().unwrap_or_default())
    }

    pub async fn propose_push_batch_items(&self, queue: &str, items: Vec<crate::protocol::BatchPushItem>) -> Result<Vec<String>, String> {
        let wire_items: Vec<(String, Vec<u8>, u64)> = items
            .into_iter()
            .map(|i| (i.message_id, i.payload, i.delay_seconds))
            .collect();
        self.propose_push_batch(queue, wire_items).await
    }

    pub async fn propose_ack_batch_items(&self, queue: &str, items: Vec<crate::protocol::AckItem>) -> Result<usize, String> {
        let wire_items: Vec<(String, String)> = items
            .into_iter()
            .map(|i| (i.message_id, i.receipt_handle))
            .collect();
        self.propose_ack_batch(queue, wire_items).await
    }

    pub async fn propose_push_batch(&self, queue: &str, items: Vec<(String, Vec<u8>, u64)>) -> Result<Vec<String>, String> {
        let role = self.role.read().await.clone();
        if role != RaftRole::Leader {
            let leader = self.current_leader.read().await.clone().unwrap_or_default();
            return Err(format!("NOT_LEADER:{}", leader));
        }

        if items.is_empty() {
            return Ok(Vec::new());
        }

        let term = self.current_term.load(Ordering::SeqCst);
        let mut final_ids = Vec::with_capacity(items.len());
        let mut commands = Vec::with_capacity(items.len());
        let mut push_ulids = Vec::with_capacity(items.len());

        for (m_id, payload, delay_seconds) in items {
            let (actual_id, actual_ulid) = if m_id.is_empty() {
                let u = ulid::Ulid::new();
                (u.to_string(), u)
            } else if m_id.len() == 26 {
                if let Ok(u) = ulid::Ulid::from_string(&m_id) {
                    (m_id, u)
                } else {
                    let u = ulid::Ulid::new();
                    (u.to_string(), u)
                }
            } else if let Ok(u_uuid) = Uuid::parse_str(&m_id) {
                let u = Ulid::from(u_uuid.as_u128());
                (m_id, u)
            } else {
                let u = ulid::Ulid::new();
                (u.to_string(), u)
            };
            final_ids.push(actual_id.clone());
            push_ulids.push(actual_ulid);
            commands.push(RaftCommand::Push {
                queue: queue.to_string(),
                msg_id: actual_id,
                payload,
                visible_at: delay_seconds,
            });
        }

        let (first_index, last_index, wire_entries) = {
            let mut log = self.log.write().await;
            let snap_offset = self.snapshot_index.load(Ordering::SeqCst);
            let start_idx = if let Some(last) = log.last() {
                last.index + 1
            } else {
                snap_offset + 1
            };
            let mut wire = Vec::with_capacity(commands.len());

            for (offset, cmd) in commands.iter().enumerate() {
                let idx = start_idx + offset as u64;
                log.push(LogEntry {
                    term,
                    index: idx,
                    command: cmd.clone(),
                });
                let cmd_bytes = postcard::to_allocvec(cmd).map_err(|e| e.to_string())?;
                wire.push(RaftLogEntryWire {
                    term,
                    index: idx,
                    command: cmd_bytes,
                });
            }
            (start_idx, start_idx + commands.len() as u64 - 1, wire)
        };

        let peers = self.peers.read().await.clone();
        let total_nodes = peers.len() + 1;
        let quorum = (total_nodes / 2) + 1;

        if total_nodes > 1 {
            let (ack_tx, mut ack_rx) = tokio::sync::mpsc::channel(total_nodes);
            for peer in peers {
                let req = Request::RaftAppend {
                    term,
                    leader_id: self.node_id.clone(),
                    prev_log_index: first_index.saturating_sub(1),
                    prev_log_term: term,
                    entries: wire_entries.clone(),
                    leader_commit: self.commit_index.load(Ordering::SeqCst),
                };
                let handle = self.get_peer_conn_handle(&peer).await;
                let tx = ack_tx.clone();
                tokio::spawn(async move {
                    if let Some(h) = handle {
                        let mut conn = h.lock().await;
                        let PeerConnection { ref mut writer, ref mut reader, ref mut read_buf } = *conn;
                        if codec::write_message(writer, &req).await.is_ok() {
                            if let Ok(Response::RaftAppend { success, .. }) = codec::read_message_with_buf(reader, read_buf).await {
                                let _ = tx.send(success).await;
                                return;
                            }
                        }
                    }
                    let _ = tx.send(false).await;
                });
            }
            drop(ack_tx);

            let mut acks = 1;
            while let Some(success) = ack_rx.recv().await {
                if success {
                    acks += 1;
                    if acks >= quorum {
                        break;
                    }
                }
            }

            if acks < quorum {
                return Err("QUORUM_FAILED: Failed to replicate batch to majority quorum".to_string());
            }
        }

        self.commit_index.store(last_index, Ordering::SeqCst);

        let _now = self.engine.current_time_sec();
        let mut push_items = Vec::with_capacity(final_ids.len());
        for (u_id, cmd) in push_ulids.into_iter().zip(commands.into_iter()) {
            if let RaftCommand::Push { payload, visible_at, .. } = cmd {
                push_items.push(PushItem {
                    id: Some(u_id),
                    payload: Bytes::from(payload),
                    delay_seconds: visible_at,
                });
            }
        }
        let _ = self.engine.push_batch(queue, push_items).await;

        if self.compaction_threshold > 0 && last_index % self.compaction_threshold == 0 {
            self.compact_log_snapshot().await;
        }

        Ok(final_ids)
    }

    pub async fn propose_ack_batch(&self, queue: &str, items: Vec<(String, String)>) -> Result<usize, String> {
        let role = self.role.read().await.clone();
        if role != RaftRole::Leader {
            let leader = self.current_leader.read().await.clone().unwrap_or_default();
            return Err(format!("NOT_LEADER:{}", leader));
        }

        if items.is_empty() {
            return Ok(0);
        }

        let acked_count = self.engine.ack_batch(queue, items.clone()).await.unwrap_or(0);
        if acked_count == 0 {
            return Ok(0);
        }

        let term = self.current_term.load(Ordering::SeqCst);
        let commands: Vec<RaftCommand> = items
            .into_iter()
            .map(|(msg_id, receipt)| RaftCommand::Ack {
                queue: queue.to_string(),
                msg_id,
                receipt,
            })
            .collect();

        let (first_index, last_index, wire_entries) = {
            let mut log = self.log.write().await;
            let snap_offset = self.snapshot_index.load(Ordering::SeqCst);
            let start_idx = if let Some(last) = log.last() {
                last.index + 1
            } else {
                snap_offset + 1
            };
            let mut wire = Vec::with_capacity(commands.len());

            for (offset, cmd) in commands.iter().enumerate() {
                let idx = start_idx + offset as u64;
                log.push(LogEntry {
                    term,
                    index: idx,
                    command: cmd.clone(),
                });
                let cmd_bytes = postcard::to_allocvec(cmd).map_err(|e| e.to_string())?;
                wire.push(RaftLogEntryWire {
                    term,
                    index: idx,
                    command: cmd_bytes,
                });
            }
            (start_idx, start_idx + commands.len() as u64 - 1, wire)
        };

        let peers = self.peers.read().await.clone();
        if !peers.is_empty() {
            let total_nodes = peers.len() + 1;
            let quorum = (total_nodes / 2) + 1;
            let (ack_tx, mut ack_rx) = tokio::sync::mpsc::channel(total_nodes);

            for peer in peers {
                let req = Request::RaftAppend {
                    term,
                    leader_id: self.node_id.clone(),
                    prev_log_index: first_index.saturating_sub(1),
                    prev_log_term: term,
                    entries: wire_entries.clone(),
                    leader_commit: self.commit_index.load(Ordering::SeqCst),
                };
                let handle = self.get_peer_conn_handle(&peer).await;
                let tx = ack_tx.clone();
                tokio::spawn(async move {
                    if let Some(h) = handle {
                        let mut conn = h.lock().await;
                        let PeerConnection { ref mut writer, ref mut reader, ref mut read_buf } = *conn;
                        if codec::write_message(writer, &req).await.is_ok() {
                            if let Ok(Response::RaftAppend { success, .. }) = codec::read_message_with_buf(reader, read_buf).await {
                                let _ = tx.send(success).await;
                                return;
                            }
                        }
                    }
                    let _ = tx.send(false).await;
                });
            }
            drop(ack_tx);

            let mut acks = 1;
            while let Some(success) = ack_rx.recv().await {
                if success {
                    acks += 1;
                    if acks >= quorum {
                        break;
                    }
                }
            }
        }

        self.commit_index.store(last_index, Ordering::SeqCst);

        if self.compaction_threshold > 0 && last_index % self.compaction_threshold == 0 {
            self.compact_log_snapshot().await;
        }

        Ok(acked_count)
    }

    pub async fn propose_ack(&self, queue: &str, message_id_str: &str, receipt: &str) -> Result<bool, String> {
        let count = self.propose_ack_batch(queue, vec![(message_id_str.to_string(), receipt.to_string())]).await?;
        Ok(count > 0)
    }

    pub async fn handle_vote(&self, term: u64, candidate_id: &str, _last_log_index: u64, _last_log_term: u64) -> bool {
        let my_term = self.current_term.load(Ordering::SeqCst);
        if term > my_term {
            self.current_term.store(term, Ordering::SeqCst);
            let mut r = self.role.write().await;
            *r = RaftRole::Follower;
            self.is_leader_cached.store(false, Ordering::Release);
            let mut l = self.current_leader.write().await;
            *l = Some(candidate_id.to_string());
            true
        } else {
            false
        }
    }

    pub async fn handle_install_snapshot(&self, req: Request) -> bool {
        if let Request::RaftInstallSnapshot { term, leader_id, last_included_index, last_included_term, items } = req {
            let my_term = self.current_term.load(Ordering::SeqCst);
            if term < my_term {
                return false;
            }

            let mut l = self.current_leader.write().await;
            *l = Some(leader_id);

            self.snapshot_index.store(last_included_index, Ordering::SeqCst);
            self.snapshot_term.store(last_included_term, Ordering::SeqCst);
            self.commit_index.store(last_included_index, Ordering::SeqCst);

            self.engine.clear_for_raft_recovery().await;
            for item in items {
                let delay = item.visible_at.saturating_sub(self.engine.current_time_sec());
                if let Ok(u_uuid) = Uuid::parse_str(&item.id) {
                    let _ = self.engine.push_with_id(&item.queue, u_uuid, Bytes::from(item.payload), delay).await;
                } else if let Ok(ulid_val) = ulid::Ulid::from_string(&item.id) {
                    let _ = self.engine.push_with_ulid(&item.queue, ulid_val, Bytes::from(item.payload), delay).await;
                }
            }

            let mut log = self.log.write().await;
            log.retain(|e| e.index > last_included_index);
            true
        } else {
            false
        }
    }

    pub async fn handle_append_entries(
        &self,
        term: u64,
        leader_id: &str,
        _prev_log_index: u64,
        _prev_log_term: u64,
        entries: Vec<RaftLogEntryWire>,
        _leader_commit: u64,
    ) -> bool {
        let my_term = self.current_term.load(Ordering::SeqCst);
        if term < my_term {
            return false;
        }

        let mut l = self.current_leader.write().await;
        *l = Some(leader_id.to_string());

        let mut log = self.log.write().await;
        for entry in entries {
            if let Ok(cmd) = postcard::from_bytes::<RaftCommand>(&entry.command) {
                match cmd {
                    RaftCommand::Push { ref queue, ref msg_id, ref payload, visible_at } => {
                        let delay = visible_at;
                        if let Ok(ulid_val) = ulid::Ulid::from_string(msg_id) {
                            let _ = self.engine.push_with_ulid(queue, ulid_val, Bytes::from(payload.clone()), delay).await;
                        } else if let Ok(u_uuid) = Uuid::parse_str(msg_id) {
                            let _ = self.engine.push_with_id(queue, u_uuid, Bytes::from(payload.clone()), delay).await;
                        }
                    }
                    RaftCommand::Ack { ref queue, ref msg_id, ref receipt } => {
                        let _ = self.engine.ack_follower(queue, msg_id).await;
                    }
                }
                log.push(LogEntry {
                    term: entry.term,
                    index: entry.index,
                    command: cmd,
                });
            }
        }

        if self.compaction_threshold > 0 && log.len() > (self.compaction_threshold * 2) as usize {
            let keep_from = log.len().saturating_sub(self.compaction_threshold as usize);
            if keep_from > 0 {
                let snap_idx = log[keep_from - 1].index;
                let snap_term = log[keep_from - 1].term;
                log.drain(0..keep_from);
                self.snapshot_index.store(snap_idx, Ordering::SeqCst);
                self.snapshot_term.store(snap_term, Ordering::SeqCst);
            }
        }
        true
    }

    pub async fn recover_as_new_leader(&self) {
        let mut r = self.role.write().await;
        *r = RaftRole::Leader;
        self.is_leader_cached.store(true, Ordering::Release);
        let mut l = self.current_leader.write().await;
        *l = Some(self.node_id.clone());

        let snap_idx = self.snapshot_index.load(Ordering::SeqCst);
        let log = self.log.read().await.clone();
        if snap_idx > 0 {
            let mut acked_ids = HashSet::new();
            for entry in &log {
                if let RaftCommand::Ack { ref msg_id, .. } = entry.command {
                    acked_ids.insert(msg_id.clone());
                }
            }
            for entry in log {
                if entry.index > snap_idx {
                    if let RaftCommand::Push { queue, msg_id, payload, visible_at } = entry.command {
                        if !acked_ids.contains(&msg_id) {
                            if let Ok(u_uuid) = Uuid::parse_str(&msg_id) {
                                let _ = self.engine.push_with_id(&queue, u_uuid, Bytes::from(payload), visible_at).await;
                            } else if let Ok(ulid_val) = ulid::Ulid::from_string(&msg_id) {
                                let _ = self.engine.push_with_ulid(&queue, ulid_val, Bytes::from(payload), visible_at).await;
                            }
                        }
                    }
                }
            }
            return;
        }

        let mut acked_ids = HashSet::new();
        for entry in &log {
            if let RaftCommand::Ack { ref msg_id, .. } = entry.command {
                acked_ids.insert(msg_id.clone());
            }
        }

        self.engine.clear_for_raft_recovery().await;

        for entry in log {
            if let RaftCommand::Push { queue, msg_id, payload, visible_at } = entry.command {
                if !acked_ids.contains(&msg_id) {
                    if let Ok(u_uuid) = Uuid::parse_str(&msg_id) {
                        let _ = self.engine.push_with_id(&queue, u_uuid, Bytes::from(payload), visible_at).await;
                    } else if let Ok(ulid_val) = ulid::Ulid::from_string(&msg_id) {
                        let _ = self.engine.push_with_ulid(&queue, ulid_val, Bytes::from(payload), visible_at).await;
                    }
                }
            }
        }
    }
}
