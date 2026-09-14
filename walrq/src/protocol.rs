use bytes::Bytes;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BatchPushItem {
    pub payload: Vec<u8>,
    pub delay_seconds: u64,
    pub message_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AckItem {
    pub message_id: String,
    pub receipt_handle: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Message {
    pub message_id: String,
    pub payload: Vec<u8>,
    pub receipt_handle: String,
    pub delivery_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftLogEntryWire {
    pub term: u64,
    pub index: u64,
    pub command: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotItem {
    pub id: String,
    pub queue: String,
    pub visible_at: u64,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Request {
    // Client RPCs
    Push {
        queue_name: String,
        payload: Vec<u8>,
        delay_seconds: u64,
        message_id: String,
    },
    PushBatch {
        queue_name: String,
        items: Vec<BatchPushItem>,
    },
    Poll {
        queue_name: String,
        visibility_timeout_sec: u32,
        batch_size: u32,
    },
    Ack {
        queue_name: String,
        message_id: String,
        receipt_handle: String,
    },
    AckBatch {
        queue_name: String,
        items: Vec<AckItem>,
    },

    // Peer Raft RPCs
    RaftVote {
        term: u64,
        candidate_id: String,
        last_log_index: u64,
        last_log_term: u64,
    },
    RaftAppend {
        term: u64,
        leader_id: String,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<RaftLogEntryWire>,
        leader_commit: u64,
    },
    RaftInstallSnapshot {
        term: u64,
        leader_id: String,
        last_included_index: u64,
        last_included_term: u64,
        items: Vec<SnapshotItem>,
    },
    // Cluster Membership & Telemetry RPCs
    JoinCluster {
        peer_addr: String,
    },
    LeaveCluster {
        peer_addr: String,
    },
    Metrics,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Response {
    Push { message_id: String },
    PushBatch { message_ids: Vec<String> },
    Poll { messages: Vec<Message> },
    Ack { success: bool },
    AckBatch { acked_count: u32 },
    RaftVote { term: u64, vote_granted: bool },
    RaftAppend { term: u64, success: bool },
    RaftInstallSnapshot { term: u64, success: bool },
    ClusterMembership { success: bool, members: Vec<String> },
    Metrics { prometheus_text: String },
    Redirect { leader: String },
    Error { message: String },
}

pub mod codec {
    use super::*;
    use std::io;
    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

    #[inline(always)]
    pub async fn write_message<W: AsyncWrite + Unpin, T: Serialize>(writer: &mut W, msg: &T) -> io::Result<()> {
        let bytes = postcard::to_allocvec(msg)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        let len = bytes.len() as u32;
        let mut frame = Vec::with_capacity(4 + bytes.len());
        frame.extend_from_slice(&len.to_be_bytes());
        frame.extend_from_slice(&bytes);
        writer.write_all(&frame).await?;
        writer.flush().await?;
        Ok(())
    }

    #[inline(always)]
    pub async fn read_message<R: AsyncRead + Unpin, T: for<'de> Deserialize<'de>>(reader: &mut R) -> io::Result<T> {
        let mut len_buf = [0u8; 4];
        reader.read_exact(&mut len_buf).await?;
        let len = u32::from_be_bytes(len_buf) as usize;
        let mut buf = vec![0u8; len];
        reader.read_exact(&mut buf).await?;
        postcard::from_bytes(&buf)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
    }

    #[inline(always)]
    pub async fn read_message_with_buf<R: AsyncRead + Unpin, T: for<'de> Deserialize<'de>>(reader: &mut R, buf: &mut Vec<u8>) -> io::Result<T> {
        let mut len_buf = [0u8; 4];
        reader.read_exact(&mut len_buf).await?;
        let len = u32::from_be_bytes(len_buf) as usize;
        buf.resize(len, 0);
        reader.read_exact(buf).await?;
        postcard::from_bytes(buf)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
    }
}
