#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use std::time::Duration;
    use tempfile::tempdir;

    use walrq::engine::queue::{QueueEngine, QueueOptions};

    #[tokio::test]
    async fn test_push_poll_ack_happy_path() {
        let dir = tempdir().unwrap();
        let engine = QueueEngine::open(dir.path(), QueueOptions::default()).unwrap();

        // 1. Push immediate message to non-existent queue (auto-created)
        let msg_id = engine
            .push("orders", Bytes::from_static(b"order-101"), 0)
            .await
            .unwrap();

        // 2. Poll message
        let polled = engine.poll("orders", 300, 1).await.unwrap();
        assert_eq!(polled.len(), 1);
        let msg = &polled[0];
        assert_eq!(msg.message_id, msg_id);
        assert_eq!(msg.payload, Bytes::from_static(b"order-101"));
        assert_eq!(msg.delivery_count, 1);

        // 3. Immediately poll again -> queue should be empty (invisible for 300s)
        let empty_poll = engine.poll("orders", 300, 1).await.unwrap();
        assert_eq!(empty_poll.len(), 0);

        // 4. Ack message -> removed permanently
        let acked = engine
            .ack("orders", &msg.message_id, &msg.receipt_handle)
            .await
            .unwrap();
        assert!(acked);

        // 5. Poll still empty after ack
        let after_ack = engine.poll("orders", 300, 1).await.unwrap();
        assert_eq!(after_ack.len(), 0);
    }

    #[tokio::test]
    async fn test_future_delayed_visibility_up_to_month() {
        tokio::time::pause();
        let dir = tempdir().unwrap();
        let engine = QueueEngine::open(dir.path(), QueueOptions::default()).unwrap();

        // Delay 30 days = 30 * 86400 = 2,592,000 seconds
        let delay_30_days = 30 * 24 * 3600;
        let msg_id = engine
            .push("billing", Bytes::from_static(b"invoice-delayed"), delay_30_days)
            .await
            .unwrap();

        // Poll at t=0 -> invisible
        assert_eq!(engine.poll("billing", 300, 1).await.unwrap().len(), 0);

        // Advance time 29 days -> still invisible
        tokio::time::advance(Duration::from_secs(29 * 24 * 3600)).await;
        assert_eq!(engine.poll("billing", 300, 1).await.unwrap().len(), 0);

        // Advance 1 day + 1 sec -> visible now!
        tokio::time::advance(Duration::from_secs(24 * 3600 + 1)).await;
        let polled = engine.poll("billing", 300, 1).await.unwrap();
        assert_eq!(polled.len(), 1);
        assert_eq!(polled[0].message_id, msg_id);
    }

    #[tokio::test]
    async fn test_visibility_timeout_requeue_on_no_ack() {
        tokio::time::pause();
        let dir = tempdir().unwrap();
        let engine = QueueEngine::open(dir.path(), QueueOptions::default()).unwrap();

        let _ = engine
            .push("tasks", Bytes::from_static(b"task-payload"), 0)
            .await
            .unwrap();

        // Poller claims with 60s visibility timeout
        let polled = engine.poll("tasks", 60, 1).await.unwrap();
        assert_eq!(polled.len(), 1);
        assert_eq!(polled[0].delivery_count, 1);

        // Advance 30s -> still invisible
        tokio::time::advance(Duration::from_secs(30)).await;
        assert_eq!(engine.poll("tasks", 60, 1).await.unwrap().len(), 0);

        // Advance past 60s -> worker didn't ack, message becomes visible again
        tokio::time::advance(Duration::from_secs(31)).await;
        let retried = engine.poll("tasks", 60, 1).await.unwrap();
        assert_eq!(retried.len(), 1);
        assert_eq!(retried[0].delivery_count, 2);
    }

    #[tokio::test]
    async fn test_dlq_movement_after_max_delivery() {
        tokio::time::pause();
        let dir = tempdir().unwrap();
        let mut opts = QueueOptions::default();
        opts.max_delivery_count = 2; // Move to DLQ on 3rd attempt
        let engine = QueueEngine::open(dir.path(), opts).unwrap();

        let _ = engine
            .push("critical", Bytes::from_static(b"doomed-task"), 0)
            .await
            .unwrap();

        // Attempt 1
        let p1 = engine.poll("critical", 10, 1).await.unwrap();
        assert_eq!(p1.len(), 1);
        assert_eq!(p1[0].delivery_count, 1);
        tokio::time::advance(Duration::from_secs(11)).await;

        // Attempt 2
        let p2 = engine.poll("critical", 10, 1).await.unwrap();
        assert_eq!(p2.len(), 1);
        assert_eq!(p2[0].delivery_count, 2);
        tokio::time::advance(Duration::from_secs(11)).await;

        // Attempt 3 -> exceeded max_delivery_count (2), moved to critical.dlq
        let p3 = engine.poll("critical", 10, 1).await.unwrap();
        assert_eq!(p3.len(), 0, "Normal queue must be empty");

        // DLQ queue has message
        let dlq_poll = engine.poll("critical.dlq", 10, 1).await.unwrap();
        assert_eq!(dlq_poll.len(), 1, "Message must appear in DLQ");
        assert_eq!(dlq_poll[0].payload, Bytes::from_static(b"doomed-task"));
    }

    #[tokio::test]
    async fn test_concurrent_pollers_exact_one_delivery() {
        use std::sync::Arc;
        let dir = tempdir().unwrap();
        let engine = Arc::new(QueueEngine::open(dir.path(), QueueOptions::default()).unwrap());

        // Push 100 messages
        for i in 0..100 {
            engine
                .push("jobs", Bytes::from(format!("job-{}", i)), 0)
                .await
                .unwrap();
        }

        // Spawn 20 concurrent pollers
        let mut handles = Vec::new();
        for _ in 0..20 {
            let eng = Arc::clone(&engine);
            handles.push(tokio::spawn(async move {
                let mut claimed = Vec::new();
                for _ in 0..20 {
                    let msgs = eng.poll("jobs", 60, 5).await.unwrap();
                    for m in msgs {
                        claimed.push(m.message_id);
                    }
                }
                claimed
            }));
        }

        let mut all_claimed = Vec::new();
        for h in handles {
            let msgs = h.await.unwrap();
            all_claimed.extend(msgs);
        }

        // Exactly 100 claimed, zero duplicates across 20 concurrent workers
        assert_eq!(all_claimed.len(), 100);
        let mut dedupped = all_claimed.clone();
        dedupped.sort();
        dedupped.dedup();
        assert_eq!(dedupped.len(), 100, "Every message claimed exactly once");
    }

    #[tokio::test]
    async fn test_crash_recovery_from_disk() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();

        // 1. Session 1: Push 3 messages, ack 1
        {
            let engine = QueueEngine::open(&path, QueueOptions::default()).unwrap();
            let id1 = engine.push("persist", Bytes::from_static(b"msg-1"), 0).await.unwrap();
            let _id2 = engine.push("persist", Bytes::from_static(b"msg-2"), 0).await.unwrap();
            let _id3 = engine.push("persist", Bytes::from_static(b"msg-3"), 0).await.unwrap();

            // Poll id1 and ack it
            let p = engine.poll("persist", 300, 10).await.unwrap();
            let msg1 = p.iter().find(|m| m.message_id == id1).unwrap();
            engine.ack("persist", &msg1.message_id, &msg1.receipt_handle).await.unwrap();
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // 2. Session 2: Reopen from disk
        {
            let engine = QueueEngine::open(&path, QueueOptions::default()).unwrap();
            let p = engine.poll("persist", 300, 10).await.unwrap();
            assert_eq!(p.len(), 2, "Acked message should stay deleted, 2 unacked must recover");
            let payloads: Vec<Bytes> = p.into_iter().map(|m| m.payload).collect();
            assert!(payloads.contains(&Bytes::from_static(b"msg-2")));
            assert!(payloads.contains(&Bytes::from_static(b"msg-3")));
        }
    }
}
