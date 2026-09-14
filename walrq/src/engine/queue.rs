use bytes::Bytes;
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio::sync::Mutex;
use tokio::time::Instant;
use ulid::Ulid;
use uuid::Uuid;

use super::disk_log::{DiskLog, WalRecord};
use super::timer_wheel::{InFlightItem, TimerWheel};

const NUM_SHARDS: usize = 32;
/// Horizon for hot in-memory delay buckets: 5 minutes (300 seconds)
pub const HOT_DELAY_HORIZON_SEC: u64 = 300;

#[derive(Debug, Error)]
pub enum QueueError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Queue not found: {0}")]
    NotFound(String),
    #[error("RAM limit exceeded: hot queue memory full ({0} messages)")]
    RamLimitExceeded(usize),
}

#[derive(Debug, Clone)]
pub struct QueueOptions {
    pub default_visibility_timeout_sec: u32,
    pub max_delivery_count: u32,
    pub max_hot_messages_in_ram: usize, // Default: 1,000,000
    pub max_wal_segment_size: u64,
}

impl Default for QueueOptions {
    fn default() -> Self {
        Self {
            default_visibility_timeout_sec: 300,
            max_delivery_count: 3,
            max_hot_messages_in_ram: 1_000_000,
            max_wal_segment_size: 64 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PolledMessage {
    pub message_id: String,
    pub payload: Bytes,
    pub receipt_handle: String,
    pub delivery_count: u32,
}

#[derive(Debug, Clone)]
pub struct PushItem {
    pub id: Option<Ulid>,
    pub payload: Bytes,
    pub delay_seconds: u64,
}

#[derive(Debug, Clone)]
struct StoredMsg {
    id: Ulid,
    queue: String,
    payload: Bytes,
    visible_at: u64,
    delivery_count: u32,
    max_delivery: u32,
}

/// Cold far-future delayed item metadata (payload is on disk, loaded when entering 60s horizon)
#[derive(Debug, Clone)]
struct FarFutureItem {
    id: Ulid,
    visible_at: u64,
}

/// High-Performance O(1) Queue with 1-Second Resolution Delay Buckets
struct SingleQueue {
    /// O(1) FIFO queue for immediate ready messages
    ready: VecDeque<Ulid>,
    /// Hot 1-Second delayed buckets within the 60s horizon (keyed by absolute second `visible_at`)
    hot_delay_buckets: HashMap<u64, Vec<Ulid>>,
    /// Far-future items (> 60s in the future) waiting to enter the hot horizon
    far_future: Vec<FarFutureItem>,
    /// Tombstones for acknowledged/deleted items awaiting drain
    deleted: HashSet<Ulid>,
    last_drained_sec: u64,
}

impl SingleQueue {
    fn new(now_sec: u64) -> Self {
        Self {
            ready: VecDeque::new(),
            hot_delay_buckets: HashMap::new(),
            far_future: Vec::new(),
            deleted: HashSet::new(),
            last_drained_sec: now_sec,
        }
    }

    /// Advance delay buckets to now_sec, moving mature delayed messages into ready FIFO
    fn advance_and_drain(&mut self, now_sec: u64) {
        if now_sec < self.last_drained_sec {
            return;
        }

        // 1. Promote any far-future items entering the 5-minute horizon into 1-minute buckets
        let horizon_limit = now_sec + HOT_DELAY_HORIZON_SEC;
        if !self.far_future.is_empty() {
            let mut remaining = Vec::new();
            for item in self.far_future.drain(..) {
                if self.deleted.contains(&item.id) {
                    continue;
                }
                if item.visible_at <= horizon_limit {
                    let bucket_min = (item.visible_at / 60) * 60;
                    self.hot_delay_buckets.entry(bucket_min).or_default().push(item.id);
                } else {
                    remaining.push(item);
                }
            }
            self.far_future = remaining;
        }

        // 2. Drain hot delay buckets that have matured (visible_at <= now_sec)
        let ready_secs: Vec<u64> = self
            .hot_delay_buckets
            .keys()
            .copied()
            .filter(|&sec| sec <= now_sec)
            .collect();

        for sec in ready_secs {
            if let Some(bucket) = self.hot_delay_buckets.remove(&sec) {
                for id in bucket {
                    if !self.deleted.remove(&id) {
                        self.ready.push_back(id);
                    }
                }
            }
        }

        self.last_drained_sec = now_sec;
    }

    /// Insert message into ready queue, 1-minute resolution hot delay bucket (<= 300s), or far-future schedule
    fn insert(&mut self, id: Ulid, visible_at: u64, now_sec: u64) {
        self.deleted.remove(&id);
        if visible_at <= now_sec {
            self.ready.push_back(id);
        } else if visible_at <= now_sec + HOT_DELAY_HORIZON_SEC {
            self.hot_delay_buckets.entry(visible_at).or_default().push(id);
        } else {
            self.far_future.push(FarFutureItem { id, visible_at });
        }
    }

    /// O(1) Tombstone deletion without array traversal
    fn mark_deleted(&mut self, id: &Ulid) {
        if self.ready.front() == Some(id) {
            self.ready.pop_front();
        } else {
            self.deleted.insert(*id);
        }
    }

    fn ready_len(&self) -> usize {
        self.ready.len().saturating_sub(self.deleted.len())
    }

    /// Pop next non-deleted ready message in O(1)
    fn pop_ready(&mut self) -> Option<Ulid> {
        while let Some(id) = self.ready.pop_front() {
            if self.deleted.remove(&id) {
                continue;
            }
            return Some(id);
        }
        if self.ready.is_empty() && !self.deleted.is_empty() {
            self.deleted.clear();
        }
        None
    }
}

struct ShardState {
    queues: HashMap<String, SingleQueue>,
    messages: HashMap<Ulid, StoredMsg>,
    seen_ids: HashSet<Ulid>,
    acked_ids: HashSet<Ulid>,
    wheel: TimerWheel,
}

impl ShardState {
    fn new() -> Self {
        Self {
            queues: HashMap::new(),
            messages: HashMap::new(),
            seen_ids: HashSet::new(),
            acked_ids: HashSet::new(),
            wheel: TimerWheel::new(3600),
        }
    }
}

pub struct QueueEngine {
    shards: Vec<Mutex<ShardState>>,
    pub disk: Arc<DiskLog>,
    pub options: QueueOptions,
    base_epoch_sec: u64,
    start_instant: Instant,
    total_messages_in_ram: Arc<AtomicUsize>,
    spill_read_offset: Arc<AtomicU64>,
}

impl QueueEngine {
    #[inline(always)]
    fn shard_idx(queue: &str) -> usize {
        let bytes = queue.as_bytes();
        let mut hash = 0xcbf29ce484222325u64;
        for &b in bytes {
            hash ^= b as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        (hash as usize) % NUM_SHARDS
    }

    pub fn current_time_sec(&self) -> u64 {
        self.base_epoch_sec + (Instant::now() - self.start_instant).as_secs()
    }

    pub fn total_messages_in_ram(&self) -> usize {
        self.total_messages_in_ram.load(Ordering::Relaxed)
    }

    pub fn set_initial_spill_offset(&self, offset: u64) {
        self.spill_read_offset.store(offset, Ordering::SeqCst);
    }

    pub fn open<P: AsRef<Path>>(path: P, opts: QueueOptions) -> Result<Self, QueueError> {
        let (disk, recovered) = DiskLog::open(path, opts.max_wal_segment_size)?;
        let disk = Arc::new(disk);

        let mut raw_shards = Vec::with_capacity(NUM_SHARDS);
        for _ in 0..NUM_SHARDS {
            raw_shards.push(ShardState::new());
        }

        let base_epoch_sec = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let mut total_in_ram: usize = 0;
        let mut initial_spill_offset = 0u64;

        for (idx, entry) in recovered.iter().enumerate() {
            match entry {
                WalRecord::Push { msg_id, queue_name, visible_at, payload } => {
                    let s_idx = Self::shard_idx(&queue_name);
                    let state = &mut raw_shards[s_idx];
                    state.seen_ids.insert(*msg_id);

                    if total_in_ram < opts.max_hot_messages_in_ram {
                        let msg = StoredMsg {
                            id: *msg_id,
                            queue: queue_name.clone(),
                            payload: payload.clone(),
                            visible_at: *visible_at,
                            delivery_count: 0,
                            max_delivery: opts.max_delivery_count,
                        };
                        state
                            .queues
                            .entry(queue_name.clone())
                            .or_insert_with(|| SingleQueue::new(base_epoch_sec))
                            .insert(*msg_id, *visible_at, base_epoch_sec);
                        state.messages.insert(*msg_id, msg);
                        total_in_ram += 1;
                    }
                }
                WalRecord::Ack { msg_id, queue_name } => {
                    let s_idx = Self::shard_idx(&queue_name);
                    let state = &mut raw_shards[s_idx];
                    state.acked_ids.insert(*msg_id);
                    if let Some(_) = state.messages.remove(msg_id) {
                        if let Some(q) = state.queues.get_mut(queue_name) {
                            q.mark_deleted(msg_id);
                        }
                        total_in_ram = total_in_ram.saturating_sub(1);
                    } else if let Some(q) = state.queues.get_mut(queue_name) {
                        q.mark_deleted(msg_id);
                    }
                }
            }
        }

        let shards = raw_shards.into_iter().map(Mutex::new).collect();

        Ok(Self {
            shards,
            disk,
            options: opts,
            base_epoch_sec,
            start_instant: Instant::now(),
            total_messages_in_ram: Arc::new(AtomicUsize::new(total_in_ram)),
            spill_read_offset: Arc::new(AtomicU64::new(0)),
        })
    }

    pub async fn push(&self, queue: &str, payload: Bytes, delay_sec: u64) -> Result<String, QueueError> {
        let id = Ulid::new();
        self.push_with_ulid(queue, id, payload, delay_sec).await
    }

    pub async fn push_with_id(&self, queue: &str, id: Uuid, payload: Bytes, delay_sec: u64) -> Result<String, QueueError> {
        let ulid_val = Ulid::from(id.as_u128());
        self.push_with_ulid(queue, ulid_val, payload, delay_sec).await
    }

    pub async fn push_with_ulid(&self, queue: &str, id: Ulid, payload: Bytes, delay_sec: u64) -> Result<String, QueueError> {
        let now = self.current_time_sec();
        let visible_at = now + delay_sec;

        self.disk.append_push_durable(id, queue, visible_at, payload.clone()).await;

        let s_idx = Self::shard_idx(queue);
        let mut state = self.shards[s_idx].lock().await;

        if state.seen_ids.contains(&id) {
            return Ok(id.to_string());
        }
        state.seen_ids.insert(id);

        let current_ram = self.total_messages_in_ram.load(Ordering::Relaxed);
        let within_horizon = visible_at <= now + HOT_DELAY_HORIZON_SEC;

        if within_horizon && current_ram < self.options.max_hot_messages_in_ram {
            if !state.messages.contains_key(&id) {
                let msg = StoredMsg {
                    id,
                    queue: queue.to_string(),
                    payload,
                    visible_at,
                    delivery_count: 0,
                    max_delivery: self.options.max_delivery_count,
                };

                state
                    .queues
                    .entry(queue.to_string())
                    .or_insert_with(|| SingleQueue::new(now))
                    .insert(id, visible_at, now);

                state.messages.insert(id, msg);
                self.total_messages_in_ram.fetch_add(1, Ordering::Relaxed);
            }
        }
        drop(state);
        // If visible_at > now + 300s OR current_ram >= max_hot_messages_in_ram,
        // the message is safely durable on disk and will be paged in as its time horizon matures!

        self.check_and_trigger_compaction().await?;

        Ok(id.to_string())
    }

    pub async fn push_batch(&self, queue: &str, items: Vec<PushItem>) -> Result<Vec<String>, QueueError> {
        let now = self.current_time_sec();
        let mut ids = Vec::with_capacity(items.len());

        let s_idx = Self::shard_idx(queue);
        let mut state = self.shards[s_idx].lock().await;

        let mut added_count = 0;
        let cap = self.options.max_hot_messages_in_ram;
        let mut cur_ram = self.total_messages_in_ram.load(Ordering::Relaxed);

        for item in items {
            let id = item.id.unwrap_or_else(Ulid::new);
            ids.push(id.to_string());

            if state.seen_ids.contains(&id) {
                continue;
            }
            state.seen_ids.insert(id);

            let visible_at = now + item.delay_seconds;
            self.disk.append_push_async(id, queue, visible_at, item.payload.clone());

            let within_horizon = visible_at <= now + HOT_DELAY_HORIZON_SEC;

            if within_horizon && cur_ram < cap && !state.messages.contains_key(&id) {
                let msg = StoredMsg {
                    id,
                    queue: queue.to_string(),
                    payload: item.payload,
                    visible_at,
                    delivery_count: 0,
                    max_delivery: self.options.max_delivery_count,
                };

                state
                    .queues
                    .entry(queue.to_string())
                    .or_insert_with(|| SingleQueue::new(now))
                    .insert(id, visible_at, now);

                state.messages.insert(id, msg);
                added_count += 1;
                cur_ram += 1;
            }
        }
        drop(state);

        if added_count > 0 {
            self.total_messages_in_ram.fetch_add(added_count, Ordering::Relaxed);
        }
        self.disk.flush().await;
        self.check_and_trigger_compaction().await?;

        Ok(ids)
    }

    pub async fn poll(&self, queue: &str, visibility_sec: u32, max_messages: u32) -> Result<Vec<PolledMessage>, QueueError> {
        let now = self.current_time_sec();
        let s_idx = Self::shard_idx(queue);

        // 1. Collect expired in-flight messages from TimerWheel for this target shard
        let expired = {
            let mut state = self.shards[s_idx].lock().await;
            let exp = state.wheel.collect_expired(now);
            let mut expired_items = Vec::with_capacity(exp.len());
            for item in exp {
                if let Some(msg) = state.messages.remove(&Ulid::from(item.id.as_u128())) {
                    expired_items.push((item, msg));
                }
            }
            expired_items
        };

        // 2. Route expired messages back to ready or DLQ
        for (_item, mut msg) in expired {
            if msg.delivery_count >= msg.max_delivery {
                let dlq_name = format!("{}.dlq", msg.queue);
                let orig_shard = Self::shard_idx(&msg.queue);
                {
                    let mut orig_state = self.shards[orig_shard].lock().await;
                    orig_state.acked_ids.insert(msg.id);
                    if let Some(q) = orig_state.queues.get_mut(&msg.queue) {
                        q.mark_deleted(&msg.id);
                    }
                }

                msg.queue = dlq_name.clone();
                msg.visible_at = now;
                let target_shard = Self::shard_idx(&dlq_name);
                let mut target_state = self.shards[target_shard].lock().await;
                target_state.seen_ids.insert(msg.id);
                target_state
                    .queues
                    .entry(dlq_name)
                    .or_insert_with(|| SingleQueue::new(now))
                    .insert(msg.id, now, now);
                target_state.messages.insert(msg.id, msg);
            } else {
                msg.visible_at = now;
                let q_name = msg.queue.clone();
                let target_shard = Self::shard_idx(&q_name);
                let mut target_state = self.shards[target_shard].lock().await;
                target_state
                    .queues
                    .entry(q_name)
                    .or_insert_with(|| SingleQueue::new(now))
                    .insert(msg.id, now, now);
                target_state.messages.insert(msg.id, msg);
            }
        }

        let mut state = self.shards[s_idx].lock().await;

        let timeout = if visibility_sec == 0 {
            self.options.default_visibility_timeout_sec
        } else {
            visibility_sec
        };

        // 3. Page in from spilled cold disk WAL:
        // Prefetch ahead if ready deque is low (< max_messages * 2) so consumers never stall
        let prefetch_threshold = (max_messages as usize).saturating_mul(2).max(200);
        let needs_page_in = match state.queues.get(queue) {
            Some(q) => q.ready_len() < prefetch_threshold,
            None => true,
        };
        if needs_page_in {
            drop(state);
            self.page_in_cold_spill(queue, now, prefetch_threshold).await;
            state = self.shards[s_idx].lock().await;
        }

        // 4. O(1) Pop from ready FIFO (with 60s horizon promotion)
        let mut claimed_ids = Vec::new();
        if let Some(q) = state.queues.get_mut(queue) {
            q.advance_and_drain(now);
            while claimed_ids.len() < max_messages as usize {
                if let Some(id) = q.pop_ready() {
                    claimed_ids.push(id);
                } else {
                    break;
                }
            }
        }
        let mut polled = Vec::with_capacity(claimed_ids.len());
        let ShardState { ref mut messages, ref mut wheel, .. } = *state;
        for id in claimed_ids {
            if let Some(msg) = messages.get_mut(&id) {
                msg.delivery_count += 1;
                let receipt = Ulid::new().to_string();
                let expire_at = now + timeout as u64;

                wheel.insert(InFlightItem {
                    id: Uuid::from_u128(id.0),
                    queue: queue.to_string(),
                    receipt: receipt.clone(),
                    expire_at,
                });

                polled.push(PolledMessage {
                    message_id: id.to_string(),
                    payload: msg.payload.clone(),
                    receipt_handle: receipt,
                    delivery_count: msg.delivery_count,
                });
            }
        }

        Ok(polled)
    }

    pub async fn page_in_cold_spill(&self, target_queue: &str, now: u64, needed: usize) {
        let fetch_limit = needed.max(1000);
        let target_shard = Self::shard_idx(target_queue);
        let mut flushed = false;

        loop {
            let cur_offset = self.spill_read_offset.load(Ordering::Relaxed);
            match self.disk.read_entries_from_offset(cur_offset, fetch_limit) {
                Ok((records, new_offset)) if !records.is_empty() => {
                    self.spill_read_offset.store(new_offset, Ordering::Relaxed);

                    // If all read records were ACKs (dead history), advance offset without shard locks if we already have hot messages
                    let has_pushes = records.iter().any(|r| matches!(r, WalRecord::Push { .. }));
                    if !has_pushes {
                        continue;
                    }

                    // Group recovered records by shard index to acquire shard locks once
                    const EMPTY_VEC: Vec<WalRecord> = Vec::new();
                    let mut per_shard: [Vec<WalRecord>; 32] = [EMPTY_VEC; 32];
                    for record in records {
                        let q_name = match &record {
                            WalRecord::Push { queue_name, .. } => queue_name,
                            WalRecord::Ack { queue_name, .. } => queue_name,
                        };
                        let s_idx = Self::shard_idx(q_name);
                        per_shard[s_idx].push(record);
                    }

                    let mut added = 0;
                    let mut target_added = 0;
                    for (s_idx, s_records) in per_shard.into_iter().enumerate() {
                        if s_records.is_empty() {
                            continue;
                        }
                        let is_target = s_idx == target_shard;
                        let mut state = self.shards[s_idx].lock().await;
                        for record in s_records {
                            match record {
                                WalRecord::Push { msg_id, queue_name, visible_at, payload } => {
                                    let already_known = state.messages.contains_key(&msg_id);
                                    let is_acked = state.acked_ids.contains(&msg_id);
                                    let within_horizon = visible_at <= now + HOT_DELAY_HORIZON_SEC;

                                    if within_horizon && !already_known && !is_acked {
                                        let msg = StoredMsg {
                                            id: msg_id,
                                            queue: queue_name.clone(),
                                            payload,
                                            visible_at,
                                            delivery_count: 0,
                                            max_delivery: self.options.max_delivery_count,
                                        };

                                        state.seen_ids.insert(msg_id);
                                        state
                                            .queues
                                            .entry(queue_name.clone())
                                            .or_insert_with(|| SingleQueue::new(now))
                                            .insert(msg_id, visible_at, now);

                                        state.messages.insert(msg_id, msg);
                                        added += 1;
                                        if is_target && queue_name == target_queue {
                                            target_added += 1;
                                        }
                                    }
                                }
                                WalRecord::Ack { msg_id, queue_name } => {
                                    state.acked_ids.insert(msg_id);
                                    if let Some(_) = state.messages.remove(&msg_id) {
                                        if let Some(q) = state.queues.get_mut(&queue_name) {
                                            q.mark_deleted(&msg_id);
                                        }
                                        self.total_messages_in_ram.fetch_sub(1, Ordering::Relaxed);
                                    } else if let Some(q) = state.queues.get_mut(&queue_name) {
                                        q.mark_deleted(&msg_id);
                                    }
                                }
                            }
                        }
                    }

                    if added > 0 {
                        self.total_messages_in_ram.fetch_add(added, Ordering::Relaxed);
                    }

                    if target_added > 0 || self.total_messages_in_ram.load(Ordering::Relaxed) >= self.options.max_hot_messages_in_ram {
                        break;
                    }
                }
                Ok((records, new_offset)) if records.is_empty() => {
                    if !flushed {
                        self.disk.flush().await;
                        flushed = true;
                        continue;
                    }
                    if new_offset > 0 {
                        // Reached end of file; wrap around scan offset so future matured spills can be paged in
                        self.spill_read_offset.store(0, Ordering::Relaxed);
                    }
                    break;
                }
                Ok((_, new_offset)) if new_offset != cur_offset => {
                    self.spill_read_offset.store(new_offset, Ordering::Relaxed);
                }
                _ => break,
            }
        }
    }

    #[inline(always)]
    fn parse_id_fast(id_str: &str) -> Option<Ulid> {
        if id_str.len() == 26 {
            if let Ok(u) = Ulid::from_string(id_str) {
                return Some(u);
            }
        }
        if let Ok(u_uuid) = Uuid::parse_str(id_str) {
            return Some(Ulid::from(u_uuid.as_u128()));
        }
        Ulid::from_string(id_str).ok()
    }

    pub async fn ack(&self, queue: &str, message_id_str: &str, receipt: &str) -> Result<bool, QueueError> {
        let ulid_val = match Self::parse_id_fast(message_id_str) {
            Some(u) => u,
            None => return Ok(false),
        };

        let s_idx = Self::shard_idx(queue);
        let mut state = self.shards[s_idx].lock().await;
        let uuid_val = Uuid::from_u128(ulid_val.0);

        if !state.wheel.remove_if_valid(&uuid_val, receipt) {
            return Ok(false);
        }

        if let Some(_) = state.messages.remove(&ulid_val) {
            state.acked_ids.insert(ulid_val);
            if let Some(q) = state.queues.get_mut(queue) {
                q.mark_deleted(&ulid_val);
            }
            self.total_messages_in_ram.fetch_sub(1, Ordering::Relaxed);
        } else {
            state.acked_ids.insert(ulid_val);
        }
        drop(state);

        self.disk.append_ack_durable(ulid_val, queue).await;
        self.check_and_trigger_compaction().await?;

        Ok(true)
    }

    pub async fn ack_batch(&self, queue: &str, items: Vec<(String, String)>) -> Result<usize, QueueError> {
        let mut ulids = Vec::with_capacity(items.len());
        for (m_id, receipt) in &items {
            if let Some(ulid_val) = Self::parse_id_fast(m_id) {
                ulids.push((ulid_val, receipt.as_str()));
            }
        }

        let s_idx = Self::shard_idx(queue);
        let mut state = self.shards[s_idx].lock().await;
        let mut count = 0;

        for (ulid_val, receipt) in ulids {
            let uuid_val = Uuid::from_u128(ulid_val.0);
            if state.wheel.remove_if_valid(&uuid_val, receipt) {
                self.disk.append_ack_async(ulid_val, queue);
                if let Some(_) = state.messages.remove(&ulid_val) {
                    state.acked_ids.insert(ulid_val);
                    if let Some(q) = state.queues.get_mut(queue) {
                        q.mark_deleted(&ulid_val);
                    }
                    count += 1;
                } else {
                    state.acked_ids.insert(ulid_val);
                }
            }
        }
        drop(state);

        if count > 0 {
            self.total_messages_in_ram.fetch_sub(count, Ordering::Relaxed);
            self.disk.flush().await;
        }
        self.check_and_trigger_compaction().await?;

        Ok(count)
    }

    pub async fn ack_follower(&self, queue: &str, message_id_str: &str) -> Result<bool, QueueError> {
        let ulid_val = match Self::parse_id_fast(message_id_str) {
            Some(u) => u,
            None => return Ok(false),
        };

        let s_idx = Self::shard_idx(queue);
        let mut state = self.shards[s_idx].lock().await;
        state.acked_ids.insert(ulid_val);

        if let Some(_) = state.messages.remove(&ulid_val) {
            if let Some(q) = state.queues.get_mut(queue) {
                q.mark_deleted(&ulid_val);
            }
            self.total_messages_in_ram.fetch_sub(1, Ordering::Relaxed);
            drop(state);
            self.disk.append_ack_durable(ulid_val, queue).await;
            self.check_and_trigger_compaction().await?;
            Ok(true)
        } else {
            if let Some(q) = state.queues.get_mut(queue) {
                q.mark_deleted(&ulid_val);
            }
            drop(state);
            self.disk.append_ack_durable(ulid_val, queue).await;
            Ok(true)
        }
    }

#[inline(always)]
    pub async fn check_and_trigger_compaction(&self) -> Result<(), QueueError> {
        // Only compact when all spilled disk records have been caught up / drained into memory,
        // otherwise unread spilled messages on disk would be truncated.
        let at_eof = self.is_spill_at_eof();
        if at_eof && self.disk.should_compact() {
            let mut surviving = Vec::new();
            for shard_mutex in &self.shards {
                let state = shard_mutex.lock().await;
                for (&id, msg) in &state.messages {
                    surviving.push((id, msg.queue.clone(), msg.visible_at, msg.payload.clone()));
                }
            }
            self.disk.perform_flip_flop_compaction(surviving).await?;
            self.spill_read_offset.store(0, Ordering::Release);
        }
        Ok(())
    }

    fn is_spill_at_eof(&self) -> bool {
        let cur_offset = self.spill_read_offset.load(Ordering::Relaxed);
        let active_bytes = self.disk.active_bytes.load(Ordering::Relaxed);
        cur_offset >= active_bytes
    }

    pub async fn force_compaction(&self) -> Result<(), QueueError> {
        let mut surviving = Vec::new();
        for shard_mutex in &self.shards {
            let state = shard_mutex.lock().await;
            for (&id, msg) in &state.messages {
                surviving.push((id, msg.queue.clone(), msg.visible_at, msg.payload.clone()));
            }
        }
        self.disk.perform_flip_flop_compaction(surviving).await?;
        Ok(())
    }

    pub async fn get_surviving_snapshot_messages(&self) -> Vec<(Ulid, String, u64, Bytes)> {
        let mut surviving = Vec::new();
        for shard_mutex in &self.shards {
            let state = shard_mutex.lock().await;
            for (&id, msg) in &state.messages {
                surviving.push((id, msg.queue.clone(), msg.visible_at, msg.payload.clone()));
            }
        }
        surviving
    }

    pub async fn flush_wal(&self) {
        self.disk.flush().await;
    }

    pub async fn clear_for_raft_recovery(&self) {
        for shard_mutex in &self.shards {
            let mut state = shard_mutex.lock().await;
            state.queues.clear();
            state.messages.clear();
            state.seen_ids.clear();
            state.acked_ids.clear();
            state.wheel = TimerWheel::new(3600);
        }
        self.total_messages_in_ram.store(0, Ordering::Relaxed);
        self.spill_read_offset.store(0, Ordering::Relaxed);
    }
}
