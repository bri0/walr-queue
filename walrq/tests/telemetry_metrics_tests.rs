#[cfg(test)]
mod tests {
    use walrq::metrics::WalrMetrics;
    use std::sync::atomic::Ordering;

    #[test]
    fn test_telemetry_metrics_accumulation() {
        let metrics = WalrMetrics::global();
        let initial_push = metrics.messages_pushed_total.load(Ordering::Relaxed);
        let initial_ack = metrics.messages_acked_total.load(Ordering::Relaxed);
        let initial_poll = metrics.messages_polled_total.load(Ordering::Relaxed);

        metrics.record_push(10);
        metrics.record_poll(10);
        metrics.record_ack(10);
        metrics.record_wal_compact();

        assert_eq!(metrics.messages_pushed_total.load(Ordering::Relaxed), initial_push + 10);
        assert_eq!(metrics.messages_polled_total.load(Ordering::Relaxed), initial_poll + 10);
        assert_eq!(metrics.messages_acked_total.load(Ordering::Relaxed), initial_ack + 10);
        assert!(metrics.wal_compaction_count.load(Ordering::Relaxed) >= 1);
    }
}
