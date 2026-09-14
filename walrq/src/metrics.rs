use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::OnceLock;

static METRICS: OnceLock<WalrMetrics> = OnceLock::new();

#[derive(Default)]
pub struct WalrMetrics {
    pub messages_pushed_total: AtomicU64,
    pub messages_acked_total: AtomicU64,
    pub messages_polled_total: AtomicU64,
    pub in_flight_messages: AtomicI64,
    pub wal_compaction_count: AtomicU64,
}

impl WalrMetrics {
    #[inline(always)]
    pub fn global() -> &'static Self {
        METRICS.get_or_init(Self::default)
    }

    #[inline(always)]
    pub fn record_push(&self, count: u64) {
        self.messages_pushed_total.fetch_add(count, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn record_ack(&self, count: u64) {
        self.messages_acked_total.fetch_add(count, Ordering::Relaxed);
        self.in_flight_messages.fetch_sub(count as i64, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn record_poll(&self, count: u64) {
        self.messages_polled_total.fetch_add(count, Ordering::Relaxed);
        self.in_flight_messages.fetch_add(count as i64, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn record_wal_compact(&self) {
        self.wal_compaction_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Render standard Prometheus text format metrics exposition (RFC 0029 compliant)
    pub fn render_prometheus(&self, ram_messages: usize, is_leader: bool) -> String {
        let pushed = self.messages_pushed_total.load(Ordering::Relaxed);
        let acked = self.messages_acked_total.load(Ordering::Relaxed);
        let polled = self.messages_polled_total.load(Ordering::Relaxed);
        let in_flight = self.in_flight_messages.load(Ordering::Relaxed);
        let wal_compactions = self.wal_compaction_count.load(Ordering::Relaxed);
        let leader_val = if is_leader { 1 } else { 0 };

        format!(
            "# HELP walrq_messages_pushed_total Total messages pushed to queue\n\
             # TYPE walrq_messages_pushed_total counter\n\
             walrq_messages_pushed_total {}\n\
             # HELP walrq_messages_acked_total Total messages acknowledged\n\
             # TYPE walrq_messages_acked_total counter\n\
             walrq_messages_acked_total {}\n\
             # HELP walrq_messages_polled_total Total messages polled by consumers\n\
             # TYPE walrq_messages_polled_total counter\n\
             walrq_messages_polled_total {}\n\
             # HELP walrq_in_flight_messages Current active in-flight visibility leases\n\
             # TYPE walrq_in_flight_messages gauge\n\
             walrq_in_flight_messages {}\n\
             # HELP walrq_wal_compaction_total Total WAL flip-flop compactions executed\n\
             # TYPE walrq_wal_compaction_total counter\n\
             walrq_wal_compaction_total {}\n\
             # HELP walrq_hot_messages_in_ram Current messages residing in memory\n\
             # TYPE walrq_hot_messages_in_ram gauge\n\
             walrq_hot_messages_in_ram {}\n\
             # HELP walrq_is_leader Whether this node is the current Raft leader\n\
             # TYPE walrq_is_leader gauge\n\
             walrq_is_leader {}\n",
            pushed, acked, polled, in_flight, wal_compactions, ram_messages, leader_val
        )
    }
}
