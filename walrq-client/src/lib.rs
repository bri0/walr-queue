use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use bytes::Bytes;
use thiserror::Error;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, Mutex, RwLock};
use ulid::Ulid;

pub use walrq::protocol::{
    AckItem, BatchPushItem, Message, Request, Response,
};
use walrq::protocol::codec;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Postcard error: {0}")]
    Postcard(#[from] postcard::Error),
    #[error("Server error: {0}")]
    ServerError(String),
    #[error("All seed nodes unreachable or max redirects exceeded")]
    Unavailable,
    #[error("Invalid redirect target: {0}")]
    InvalidTarget(String),
    #[error("Buffer channel closed")]
    ChannelClosed,
}

#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub buffer_window_ms: u64, // Default: 300ms
    pub max_batch_size: usize,  // Default: 500
    pub max_redirects: usize,   // Default: 10
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            buffer_window_ms: 300,
            max_batch_size: 500,
            max_redirects: 10,
        }
    }
}

struct BufferedPushItem {
    queue: String,
    message_id: Option<String>,
    payload: Bytes,
    delay_seconds: u64,
    tx: oneshot::Sender<Result<String, ClientError>>,
}

struct PooledConnection {
    writer: OwnedWriteHalf,
    reader: OwnedReadHalf,
    read_buf: Vec<u8>,
}

#[derive(Clone)]
pub struct WalrClient {
    inner: Arc<ClientInner>,
    sharded_senders: Vec<mpsc::UnboundedSender<BufferedPushItem>>,
}

struct ClientInner {
    seed_nodes: Arc<RwLock<Vec<String>>>,
    routes: Arc<RwLock<HashMap<String, String>>>,
    connections: Arc<RwLock<HashMap<String, Vec<Arc<Mutex<PooledConnection>>>>>>,
    config: ClientConfig,
    conn_counter: std::sync::atomic::AtomicUsize,
}

impl WalrClient {
    pub fn new(seed_nodes: Vec<String>) -> Self {
        Self::with_config(seed_nodes, ClientConfig::default())
    }

    pub fn with_config(seed_nodes: Vec<String>, config: ClientConfig) -> Self {
        let inner = Arc::new(ClientInner {
            seed_nodes: Arc::new(RwLock::new(seed_nodes)),
            routes: Arc::new(RwLock::new(HashMap::new())),
            connections: Arc::new(RwLock::new(HashMap::new())),
            config: config.clone(),
            conn_counter: std::sync::atomic::AtomicUsize::new(0),
        });

        const NUM_SHARDS: usize = 32;
        let mut sharded_senders = Vec::with_capacity(NUM_SHARDS);

        for _ in 0..NUM_SHARDS {
            let (tx, mut rx) = mpsc::unbounded_channel::<BufferedPushItem>();
            sharded_senders.push(tx);

            let inner_clone = Arc::clone(&inner);
            let window_ms = config.buffer_window_ms;
            let max_batch = config.max_batch_size;

            tokio::spawn(async move {
                let mut queues_map: HashMap<String, Vec<(Option<String>, Bytes, u64, oneshot::Sender<Result<String, ClientError>>)>> = HashMap::new();
                let mut interval = tokio::time::interval_at(
                    tokio::time::Instant::now() + Duration::from_millis(window_ms),
                    Duration::from_millis(window_ms),
                );

                loop {
                    tokio::select! {
                        Some(item) = rx.recv() => {
                            let list = queues_map.entry(item.queue.clone()).or_insert_with(Vec::new);
                            list.push((item.message_id, item.payload, item.delay_seconds, item.tx));

                            if list.len() >= max_batch {
                                let items = queues_map.remove(&item.queue).unwrap();
                                let inner_ref = Arc::clone(&inner_clone);
                                let q_name = item.queue.clone();
                                tokio::spawn(async move {
                                    Self::flush_queue_batch(&inner_ref, &q_name, items).await;
                                });
                            }
                        }
                        _ = interval.tick() => {
                            if !queues_map.is_empty() {
                                let drained: HashMap<_, _> = queues_map.drain().collect();
                                for (q_name, items) in drained {
                                    if !items.is_empty() {
                                        let inner_ref = Arc::clone(&inner_clone);
                                        tokio::spawn(async move {
                                            Self::flush_queue_batch(&inner_ref, &q_name, items).await;
                                        });
                                    }
                                }
                            }
                        }
                    }
                }
            });
        }

        Self {
            inner,
            sharded_senders,
        }
    }

    pub async fn update_seeds(&self, seeds: Vec<String>) {
        let mut s = self.inner.seed_nodes.write().await;
        *s = seeds;
    }

    pub async fn push(&self, queue: &str, payload: Bytes, delay_seconds: u64) -> Result<String, ClientError> {
        self.push_opt(queue, payload, delay_seconds, None).await
    }

    pub async fn push_with_id(&self, queue: &str, payload: Bytes, delay_seconds: u64, message_id: &str) -> Result<String, ClientError> {
        self.push_opt(queue, payload, delay_seconds, Some(message_id.to_string())).await
    }

    pub async fn push_opt(&self, queue: &str, payload: Bytes, delay_seconds: u64, message_id: Option<String>) -> Result<String, ClientError> {
        let (tx, rx) = oneshot::channel();
        let bytes = queue.as_bytes();
        let mut hash = 0xcbf29ce484222325u64;
        for &b in bytes {
            hash ^= b as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        let shard = (hash as usize) % self.sharded_senders.len();

        let mid = Some(message_id.unwrap_or_else(|| Ulid::new().to_string()));

        self.sharded_senders[shard]
            .send(BufferedPushItem {
                queue: queue.to_string(),
                message_id: mid,
                payload,
                delay_seconds,
                tx,
            })
            .map_err(|_| ClientError::ChannelClosed)?;

        rx.await.map_err(|_| ClientError::ChannelClosed)?
    }

    pub async fn push_immediate(&self, queue: &str, payload: Bytes, delay_seconds: u64) -> Result<String, ClientError> {
        self.push_immediate_opt(queue, payload, delay_seconds, None).await
    }

    pub async fn push_immediate_with_id(&self, queue: &str, payload: Bytes, delay_seconds: u64, message_id: &str) -> Result<String, ClientError> {
        self.push_immediate_opt(queue, payload, delay_seconds, Some(message_id.to_string())).await
    }

    pub async fn push_immediate_opt(&self, queue: &str, payload: Bytes, delay_seconds: u64, message_id: Option<String>) -> Result<String, ClientError> {
        let message_id = message_id.unwrap_or_else(|| Ulid::new().to_string());
        let req = Request::Push {
            queue_name: queue.to_string(),
            payload: payload.into(),
            delay_seconds,
            message_id,
        };

        match self.inner.send_with_redirect(queue, req).await? {
            Response::Push { message_id } => Ok(message_id),
            Response::Error { message } => Err(ClientError::ServerError(message)),
            other => Err(ClientError::ServerError(format!("Unexpected response: {:?}", other))),
        }
    }

    pub async fn push_batch(&self, queue: &str, payloads: Vec<Bytes>) -> Result<Vec<String>, ClientError> {
        let items: Vec<BatchPushItem> = payloads
            .into_iter()
            .map(|p| BatchPushItem {
                payload: p.into(),
                delay_seconds: 0,
                message_id: Ulid::new().to_string(),
            })
            .collect();
        self.push_batch_internal(queue, items).await
    }

    pub async fn push_batch_with_ids(&self, queue: &str, items: Vec<(Bytes, Option<String>)>) -> Result<Vec<String>, ClientError> {
        let proto_items: Vec<BatchPushItem> = items
            .into_iter()
            .map(|(p, id)| BatchPushItem {
                payload: p.into(),
                delay_seconds: 0,
                message_id: id.unwrap_or_else(|| Ulid::new().to_string()),
            })
            .collect();
        self.push_batch_internal(queue, proto_items).await
    }

    async fn push_batch_internal(&self, queue: &str, items: Vec<BatchPushItem>) -> Result<Vec<String>, ClientError> {
        let req = Request::PushBatch {
            queue_name: queue.to_string(),
            items,
        };

        match self.inner.send_with_redirect(queue, req).await? {
            Response::PushBatch { message_ids } => Ok(message_ids),
            Response::Error { message } => Err(ClientError::ServerError(message)),
            other => Err(ClientError::ServerError(format!("Unexpected response: {:?}", other))),
        }
    }

    async fn flush_queue_batch(
        inner: &Arc<ClientInner>,
        queue: &str,
        items: Vec<(Option<String>, Bytes, u64, oneshot::Sender<Result<String, ClientError>>)>,
    ) {
        let mut senders = Vec::with_capacity(items.len());
        let mut proto_items = Vec::with_capacity(items.len());

        for (mid, payload, delay_seconds, tx) in items {
            senders.push(tx);
            proto_items.push(BatchPushItem {
                payload: payload.into(),
                delay_seconds,
                message_id: mid.unwrap_or_else(|| Ulid::new().to_string()),
            });
        }

        let req = Request::PushBatch {
            queue_name: queue.to_string(),
            items: proto_items,
        };

        match inner.send_with_redirect(queue, req).await {
            Ok(Response::PushBatch { message_ids }) => {
                for (tx, id) in senders.into_iter().zip(message_ids.into_iter()) {
                    let _ = tx.send(Ok(id));
                }
            }
            Ok(Response::Error { message }) => {
                for tx in senders {
                    let _ = tx.send(Err(ClientError::ServerError(message.clone())));
                }
            }
            _ => {
                for tx in senders {
                    let _ = tx.send(Err(ClientError::Unavailable));
                }
            }
        }
    }

    pub async fn poll(&self, queue: &str, visibility_timeout_sec: u32, batch_size: u32) -> Result<Vec<Message>, ClientError> {
        let req = Request::Poll {
            queue_name: queue.to_string(),
            visibility_timeout_sec,
            batch_size: if batch_size == 0 { 1 } else { batch_size },
        };

        match self.inner.send_with_redirect(queue, req).await? {
            Response::Poll { messages } => Ok(messages),
            Response::Error { message } => Err(ClientError::ServerError(message)),
            other => Err(ClientError::ServerError(format!("Unexpected response: {:?}", other))),
        }
    }

    pub async fn ack(&self, queue: &str, message_id: &str, receipt_handle: &str) -> Result<bool, ClientError> {
        let req = Request::Ack {
            queue_name: queue.to_string(),
            message_id: message_id.to_string(),
            receipt_handle: receipt_handle.to_string(),
        };

        match self.inner.send_with_redirect(queue, req).await? {
            Response::Ack { success } => Ok(success),
            Response::Error { message } => Err(ClientError::ServerError(message)),
            other => Err(ClientError::ServerError(format!("Unexpected response: {:?}", other))),
        }
    }

    pub async fn ack_batch(&self, queue: &str, items: Vec<(String, String)>) -> Result<u32, ClientError> {
        let ack_items: Vec<AckItem> = items
            .into_iter()
            .map(|(message_id, receipt_handle)| AckItem {
                message_id,
                receipt_handle,
            })
            .collect();

        self.ack_batch_items(queue, ack_items).await
    }

    pub async fn ack_batch_items(&self, queue: &str, items: Vec<AckItem>) -> Result<u32, ClientError> {
        let req = Request::AckBatch {
            queue_name: queue.to_string(),
            items,
        };

        match self.inner.send_with_redirect(queue, req).await? {
            Response::AckBatch { acked_count } => Ok(acked_count),
            Response::Error { message } => Err(ClientError::ServerError(message)),
            other => Err(ClientError::ServerError(format!("Unexpected response: {:?}", other))),
        }
    }

    /// Fetch Prometheus text format metrics directly from cluster
    pub async fn get_metrics(&self) -> Result<String, ClientError> {
        let req = Request::Metrics;
        match self.inner.send_with_redirect("", req).await? {
            Response::Metrics { prometheus_text } => Ok(prometheus_text),
            Response::Error { message } => Err(ClientError::ServerError(message)),
            other => Err(ClientError::ServerError(format!("Unexpected response: {:?}", other))),
        }
    }

    /// Dynamically add a new node to the Raft cluster
    pub async fn join_cluster(&self, new_peer_addr: &str) -> Result<Vec<String>, ClientError> {
        let req = Request::JoinCluster {
            peer_addr: new_peer_addr.to_string(),
        };
        match self.inner.send_with_redirect("", req).await? {
            Response::ClusterMembership { success: true, members } => Ok(members),
            Response::Error { message } => Err(ClientError::ServerError(message)),
            other => Err(ClientError::ServerError(format!("Unexpected response: {:?}", other))),
        }
    }

    /// Dynamically remove an existing node from the Raft cluster
    pub async fn leave_cluster(&self, peer_addr: &str) -> Result<Vec<String>, ClientError> {
        let req = Request::LeaveCluster {
            peer_addr: peer_addr.to_string(),
        };
        match self.inner.send_with_redirect("", req).await? {
            Response::ClusterMembership { success: true, members } => Ok(members),
            Response::Error { message } => Err(ClientError::ServerError(message)),
            other => Err(ClientError::ServerError(format!("Unexpected response: {:?}", other))),
        }
    }
}

impl ClientInner {
    async fn resolve_initial_target(&self, queue: &str) -> Result<String, ClientError> {
        {
            let routes = self.routes.read().await;
            if let Some(target) = routes.get(queue) {
                return Ok(target.clone());
            }
        }

        let seeds = self.seed_nodes.read().await;
        if seeds.is_empty() {
            return Err(ClientError::Unavailable);
        }

        let bytes = queue.as_bytes();
        let mut hash = 0xcbf29ce484222325u64;
        for &b in bytes {
            hash ^= b as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        let idx = (hash as usize) % seeds.len();
        Ok(seeds[idx].clone())
    }

    async fn update_route(&self, queue: &str, target: &str) {
        let mut routes = self.routes.write().await;
        routes.insert(queue.to_string(), target.to_string());
    }

    async fn invalidate_route(&self, queue: &str, target: &str) {
        let mut routes = self.routes.write().await;
        if let Some(current) = routes.get(queue) {
            if current == target {
                routes.remove(queue);
            }
        }
    }

    async fn get_connection(&self, target: &str) -> Result<Arc<Mutex<PooledConnection>>, ClientError> {
        const POOL_SIZE_PER_HOST: usize = 4;

        {
            let conns = self.connections.read().await;
            if let Some(pool) = conns.get(target) {
                if !pool.is_empty() {
                    let idx = self.conn_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % pool.len();
                    return Ok(Arc::clone(&pool[idx]));
                }
            }
        }

        let mut conns = self.connections.write().await;
        let pool = conns.entry(target.to_string()).or_insert_with(Vec::new);

        if pool.len() < POOL_SIZE_PER_HOST {
            let stream = TcpStream::connect(target).await?;
            let _ = stream.set_nodelay(true);
            let (reader, writer) = stream.into_split();
            let handle = Arc::new(Mutex::new(PooledConnection {
                writer,
                reader,
                read_buf: Vec::with_capacity(16 * 1024),
            }));
            pool.push(Arc::clone(&handle));
            Ok(handle)
        } else {
            let idx = self.conn_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % pool.len();
            Ok(Arc::clone(&pool[idx]))
        }
    }

    async fn remove_connection(&self, target: &str) {
        let mut conns = self.connections.write().await;
        conns.remove(target);
    }

    async fn send_with_redirect(&self, queue: &str, req: Request) -> Result<Response, ClientError> {
        let mut target = self.resolve_initial_target(queue).await?;
        let mut attempts = 0;

        while attempts <= self.config.max_redirects {
            let conn_handle = match self.get_connection(&target).await {
                Ok(c) => c,
                Err(_) => {
                    self.invalidate_route(queue, &target).await;
                    self.remove_connection(&target).await;
                    let seeds = self.seed_nodes.read().await;
                    if let Some(next_seed) = seeds.iter().find(|&s| s != &target) {
                        target = next_seed.clone();
                        attempts += 1;
                        continue;
                    }
                    return Err(ClientError::Unavailable);
                }
            };

            let mut conn = conn_handle.lock().await;
            let PooledConnection { ref mut reader, ref mut writer, ref mut read_buf } = *conn;
            if codec::write_message(writer, &req).await.is_err() {
                drop(conn);
                self.invalidate_route(queue, &target).await;
                self.remove_connection(&target).await;
                attempts += 1;
                continue;
            }

            match codec::read_message_with_buf::<_, Response>(reader, read_buf).await {
                Ok(Response::Redirect { leader }) => {
                    drop(conn);
                    if !leader.is_empty() {
                        target = leader;
                    } else {
                        let seeds = self.seed_nodes.read().await;
                        if let Some(next_seed) = seeds.iter().find(|&s| s != &target) {
                            target = next_seed.clone();
                        }
                    }
                    attempts += 1;
                    continue;
                }
                Ok(resp) => {
                    self.update_route(queue, &target).await;
                    return Ok(resp);
                }
                Err(_) => {
                    drop(conn);
                    self.invalidate_route(queue, &target).await;
                    self.remove_connection(&target).await;
                    attempts += 1;
                }
            }
        }

        Err(ClientError::Unavailable)
    }
}
