use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio::sync::oneshot;
use ulid::Ulid;

pub const EPOCH_2020_SEC: u64 = 1577836800;

#[derive(Debug, Clone)]
pub enum WalRecord {
    Push {
        msg_id: Ulid,
        queue_name: String,
        visible_at: u64,
        payload: Bytes,
    },
    Ack {
        msg_id: Ulid,
        queue_name: String,
    },
}

pub struct QueueRegistry {
    path: PathBuf,
    name_to_id: RwLock<HashMap<String, u16>>,
    id_to_name: RwLock<Vec<String>>,
    file_lock: Mutex<()>,
}

impl QueueRegistry {
    pub fn open(meta_dir: &Path) -> Result<Self, std::io::Error> {
        std::fs::create_dir_all(meta_dir)?;
        let path = meta_dir.join("queues.manifest");

        let mut name_to_id = HashMap::new();
        let mut id_to_name = Vec::new();

        if path.exists() {
            let mut file = File::open(&path)?;
            let mut buf = Vec::new();
            file.read_to_end(&mut buf)?;
            let mut cur = &buf[..];

            while cur.remaining() >= 4 {
                let id = cur.get_u16();
                let len = cur.get_u16() as usize;
                if cur.remaining() < len {
                    break;
                }
                let name = String::from_utf8_lossy(&cur[..len]).to_string();
                cur.advance(len);

                if id as usize >= id_to_name.len() {
                    id_to_name.resize(id as usize + 1, String::new());
                }
                id_to_name[id as usize] = name.clone();
                name_to_id.insert(name, id);
            }
        }

        Ok(Self {
            path,
            name_to_id: RwLock::new(name_to_id),
            id_to_name: RwLock::new(id_to_name),
            file_lock: Mutex::new(()),
        })
    }

    #[inline(always)]
    pub fn get_or_register(&self, queue_name: &str) -> Result<u16, std::io::Error> {
        {
            let r = self.name_to_id.read().unwrap();
            if let Some(&id) = r.get(queue_name) {
                return Ok(id);
            }
        }

        let _file_guard = self.file_lock.lock().unwrap();

        {
            let r = self.name_to_id.read().unwrap();
            if let Some(&id) = r.get(queue_name) {
                return Ok(id);
            }
        }

        let mut id_names = self.id_to_name.write().unwrap();
        let mut names = self.name_to_id.write().unwrap();

        let next_id = id_names.len() as u16;
        let mut file = OpenOptions::new().create(true).append(true).open(&self.path)?;

        let mut header = [0u8; 4];
        header[0..2].copy_from_slice(&next_id.to_be_bytes());
        header[2..4].copy_from_slice(&(queue_name.len() as u16).to_be_bytes());
        file.write_all(&header)?;
        file.write_all(queue_name.as_bytes())?;
        file.flush()?;

        names.insert(queue_name.to_string(), next_id);
        id_names.push(queue_name.to_string());

        Ok(next_id)
    }

    #[inline(always)]
    pub fn get_name(&self, id: u16) -> Option<String> {
        let r = self.id_to_name.read().unwrap();
        r.get(id as usize).cloned()
    }
}

pub struct DiskLog {
    data_dir: PathBuf,
    registry: Arc<QueueRegistry>,
    active_is_b: Arc<AtomicBool>,
    writer_tx: UnboundedSender<WalCommand>,
    active_bytes: Arc<AtomicU64>,
    pub max_segment_size: u64,
}

enum WalCommand {
    Push {
        msg_id: Ulid,
        queue_name: String,
        visible_at: u64,
        payload: Bytes,
        notify: Option<oneshot::Sender<()>>,
    },
    Ack {
        msg_id: Ulid,
        queue_name: String,
        notify: Option<oneshot::Sender<()>>,
    },
    Flush(oneshot::Sender<()>),
}

impl DiskLog {
    pub fn open(data_dir: impl AsRef<Path>, max_segment_size: u64) -> Result<(Self, Vec<WalRecord>), std::io::Error> {
        let data_dir = data_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&data_dir)?;

        let meta_dir = data_dir.join("meta");
        let registry = Arc::new(QueueRegistry::open(&meta_dir)?);

        let wal_a_path = data_dir.join("wal_a.log");
        let wal_b_path = data_dir.join("wal_b.log");

        let mut recovered_records = Vec::new();
        let mut active_is_b = false;

        let len_a = if wal_a_path.exists() { wal_a_path.metadata()?.len() } else { 0 };
        let len_b = if wal_b_path.exists() { wal_b_path.metadata()?.len() } else { 0 };

        if wal_a_path.exists() {
            Self::read_wal_file(&wal_a_path, &registry, &mut recovered_records)?;
        }
        if wal_b_path.exists() {
            Self::read_wal_file(&wal_b_path, &registry, &mut recovered_records)?;
        }

        if len_b > 0 && len_a == 0 {
            active_is_b = true;
        }

        let active_bytes_val = if active_is_b { len_b } else { len_a };
        let active_bytes = Arc::new(AtomicU64::new(active_bytes_val));
        let active_flag = Arc::new(AtomicBool::new(active_is_b));

        let (tx, mut rx) = mpsc::unbounded_channel::<WalCommand>();

        let bg_dir = data_dir.clone();
        let bg_reg = Arc::clone(&registry);
        let bg_flag = Arc::clone(&active_flag);
        let bg_bytes = Arc::clone(&active_bytes);

        // Group commit background thread
        std::thread::spawn(move || {
            let mut file_a = OpenOptions::new().create(true).append(true).open(bg_dir.join("wal_a.log")).unwrap();
            let mut file_b = OpenOptions::new().create(true).append(true).open(bg_dir.join("wal_b.log")).unwrap();

            let mut batch_buf = BytesMut::with_capacity(256 * 1024);
            let mut waiters = Vec::new();

            while let Some(cmd) = rx.blocking_recv() {
                match cmd {
                    WalCommand::Push { msg_id, queue_name, visible_at, payload, notify } => {
                        let q_id = bg_reg.get_or_register(&queue_name).unwrap();
                        Self::encode_push(&mut batch_buf, msg_id, q_id, visible_at, &payload);
                        if let Some(n) = notify {
                            waiters.push(n);
                        }
                    }
                    WalCommand::Ack { msg_id, queue_name, notify } => {
                        let q_id = bg_reg.get_or_register(&queue_name).unwrap();
                        Self::encode_ack(&mut batch_buf, msg_id, q_id);
                        if let Some(n) = notify {
                            waiters.push(n);
                        }
                    }
                    WalCommand::Flush(done) => {
                        waiters.push(done);
                    }
                }

                // Batch coalesce up to 1000 items in single group commit
                while let Ok(cmd) = rx.try_recv() {
                    match cmd {
                        WalCommand::Push { msg_id, queue_name, visible_at, payload, notify } => {
                            let q_id = bg_reg.get_or_register(&queue_name).unwrap();
                            Self::encode_push(&mut batch_buf, msg_id, q_id, visible_at, &payload);
                            if let Some(n) = notify {
                                waiters.push(n);
                            }
                        }
                        WalCommand::Ack { msg_id, queue_name, notify } => {
                            let q_id = bg_reg.get_or_register(&queue_name).unwrap();
                            Self::encode_ack(&mut batch_buf, msg_id, q_id);
                            if let Some(n) = notify {
                                waiters.push(n);
                            }
                        }
                        WalCommand::Flush(done) => {
                            waiters.push(done);
                        }
                    }
                    if batch_buf.len() > 128 * 1024 {
                        break;
                    }
                }

                if !batch_buf.is_empty() {
                    let is_b = bg_flag.load(Ordering::Relaxed);
                    let target_file = if is_b { &mut file_b } else { &mut file_a };
                    target_file.write_all(&batch_buf).unwrap();
                    if !waiters.is_empty() {
                        target_file.flush().unwrap();
                    }
                    bg_bytes.fetch_add(batch_buf.len() as u64, Ordering::Relaxed);
                    batch_buf.clear();
                }

                // Notify all callers that disk flush is completed before returning success!
                for w in waiters.drain(..) {
                    let _ = w.send(());
                }
            }
        });

        Ok((
            Self {
                data_dir,
                registry,
                active_is_b: active_flag,
                writer_tx: tx,
                active_bytes,
                max_segment_size,
            },
            recovered_records,
        ))
    }

    #[inline(always)]
    fn encode_push(buf: &mut BytesMut, msg_id: Ulid, q_id: u16, visible_at: u64, payload: &[u8]) {
        let flags = 0u8;
        let rel_ts = (visible_at.saturating_sub(EPOCH_2020_SEC)) as u32;

        buf.put_u8(flags);
        buf.put_u128(msg_id.0);
        buf.put_u32(rel_ts);
        buf.put_u16(q_id);
        buf.put_u32(payload.len() as u32);
        buf.put_slice(payload);
    }

    #[inline(always)]
    fn encode_ack(buf: &mut BytesMut, msg_id: Ulid, q_id: u16) {
        let flags = 0x01u8;
        buf.put_u8(flags);
        buf.put_u128(msg_id.0);
        buf.put_u16(q_id);
    }

    fn read_wal_file(path: &Path, registry: &Arc<QueueRegistry>, out: &mut Vec<WalRecord>) -> Result<(), std::io::Error> {
        let mut file = File::open(path)?;
        let mut raw = Vec::new();
        file.read_to_end(&mut raw)?;
        let mut cur = &raw[..];

        let reg = registry;

        while cur.remaining() > 0 {
            if cur.remaining() < 1 {
                break;
            }
            let flags = cur.get_u8();
            let is_ack = (flags & 0x01) != 0;
            let is_compressed = (flags & 0x02) != 0;

            if is_ack {
                if cur.remaining() < 18 {
                    break;
                }
                let ulid_raw = cur.get_u128();
                let q_id = cur.get_u16();
                let q_name = reg.get_name(q_id).unwrap_or_else(|| "default".to_string());
                out.push(WalRecord::Ack {
                    msg_id: Ulid(ulid_raw),
                    queue_name: q_name,
                });
            } else {
                if cur.remaining() < 26 {
                    break;
                }
                let ulid_raw = cur.get_u128();
                let rel_ts = cur.get_u32();
                let q_id = cur.get_u16();
                let payload_len = cur.get_u32() as usize;

                if cur.remaining() < payload_len {
                    break;
                }

                let raw_payload = &cur[..payload_len];
                cur.advance(payload_len);

                let payload_bytes = if is_compressed {
                    if let Ok(decomp) = zstd::decode_all(raw_payload) {
                        Bytes::from(decomp)
                    } else {
                        Bytes::copy_from_slice(raw_payload)
                    }
                } else {
                    Bytes::copy_from_slice(raw_payload)
                };

                let visible_at = rel_ts as u64 + EPOCH_2020_SEC;
                let q_name = reg.get_name(q_id).unwrap_or_else(|| "default".to_string());

                out.push(WalRecord::Push {
                    msg_id: Ulid(ulid_raw),
                    queue_name: q_name,
                    visible_at,
                    payload: payload_bytes,
                });
            }
        }

        Ok(())
    }

    pub async fn append_push_durable(&self, msg_id: Ulid, queue_name: &str, visible_at: u64, payload: Bytes) {
        let (tx, rx) = oneshot::channel();
        let _ = self.writer_tx.send(WalCommand::Push {
            msg_id,
            queue_name: queue_name.to_string(),
            visible_at,
            payload,
            notify: Some(tx),
        });
        let _ = rx.await;
    }

    pub fn append_push_async(&self, msg_id: Ulid, queue_name: &str, visible_at: u64, payload: Bytes) {
        let _ = self.writer_tx.send(WalCommand::Push {
            msg_id,
            queue_name: queue_name.to_string(),
            visible_at,
            payload,
            notify: None,
        });
    }

    pub async fn append_ack_durable(&self, msg_id: Ulid, queue_name: &str) {
        let (tx, rx) = oneshot::channel();
        let _ = self.writer_tx.send(WalCommand::Ack {
            msg_id,
            queue_name: queue_name.to_string(),
            notify: Some(tx),
        });
        let _ = rx.await;
    }

    pub fn append_ack_async(&self, msg_id: Ulid, queue_name: &str) {
        let _ = self.writer_tx.send(WalCommand::Ack {
            msg_id,
            queue_name: queue_name.to_string(),
            notify: None,
        });
    }

    pub async fn flush(&self) {
        let (tx, rx) = oneshot::channel();
        let _ = self.writer_tx.send(WalCommand::Flush(tx));
        let _ = rx.await;
    }

    pub fn should_compact(&self) -> bool {
        self.active_bytes.load(Ordering::Relaxed) >= self.max_segment_size
    }

    pub async fn perform_flip_flop_compaction(&self, surviving_unacked_messages: Vec<(Ulid, String, u64, Bytes)>) -> Result<(), std::io::Error> {
        self.flush().await;

        let was_b = self.active_is_b.load(Ordering::SeqCst);
        let new_is_b = !was_b;

        self.active_is_b.store(new_is_b, Ordering::SeqCst);
        self.active_bytes.store(0, Ordering::SeqCst);

        for (id, q, vis_at, data) in surviving_unacked_messages {
            self.append_push_async(id, &q, vis_at, data);
        }
        self.flush().await;

        let frozen_filename = if was_b { "wal_b.log" } else { "wal_a.log" };
        let frozen_path = self.data_dir.join(frozen_filename);
        let _ = File::create(&frozen_path)?;

        Ok(())
    }

    pub fn read_entries_from_offset(&self, offset: u64, max_entries: usize) -> Result<(Vec<WalRecord>, u64), std::io::Error> {
        let is_b = self.active_is_b.load(Ordering::SeqCst);
        let filename = if is_b { "wal_b.log" } else { "wal_a.log" };
        let path = self.data_dir.join(filename);

        if !path.exists() {
            return Ok((Vec::new(), offset));
        }

        let mut file = File::open(&path)?;
        let file_len = file.metadata()?.len();
        if offset >= file_len {
            return Ok((Vec::new(), file_len));
        }

        file.seek(SeekFrom::Start(offset))?;
        let to_read = (file_len - offset).min(1024 * 1024) as usize; // Read up to 1MB chunks
        let mut raw = BytesMut::zeroed(to_read);
        file.read_exact(&mut raw)?;
        let raw_frozen = raw.freeze();

        let mut cur = &raw_frozen[..];
        let mut records = Vec::new();
        let mut bytes_consumed = 0usize;

        while cur.remaining() > 0 && records.len() < max_entries {
            let record_start_rem = cur.remaining();
            let flags = cur.get_u8();
            let is_ack = (flags & 0x01) != 0;
            let is_compressed = (flags & 0x02) != 0;

            if is_ack {
                if cur.remaining() < 18 {
                    break;
                }
                let ulid_raw = cur.get_u128();
                let q_id = cur.get_u16();
                let q_name = self.registry.get_name(q_id).unwrap_or_else(|| "default".to_string());
                records.push(WalRecord::Ack {
                    msg_id: Ulid(ulid_raw),
                    queue_name: q_name,
                });
            } else {
                if cur.remaining() < 26 {
                    break;
                }
                let ulid_raw = cur.get_u128();
                let rel_ts = cur.get_u32();
                let q_id = cur.get_u16();
                let payload_len = cur.get_u32() as usize;

                if cur.remaining() < payload_len {
                    break;
                }

                let current_offset = to_read - cur.remaining();
                cur.advance(payload_len);

                let payload_bytes = if is_compressed {
                    if let Ok(decomp) = zstd::decode_all(&raw_frozen[current_offset..current_offset + payload_len]) {
                        Bytes::from(decomp)
                    } else {
                        raw_frozen.slice(current_offset..current_offset + payload_len)
                    }
                } else {
                    raw_frozen.slice(current_offset..current_offset + payload_len)
                };

                let visible_at = rel_ts as u64 + EPOCH_2020_SEC;
                let q_name = self.registry.get_name(q_id).unwrap_or_else(|| "default".to_string());

                records.push(WalRecord::Push {
                    msg_id: Ulid(ulid_raw),
                    queue_name: q_name,
                    visible_at,
                    payload: payload_bytes,
                });
            }
            bytes_consumed += record_start_rem - cur.remaining();
        }

        Ok((records, offset + bytes_consumed as u64))
    }
}
