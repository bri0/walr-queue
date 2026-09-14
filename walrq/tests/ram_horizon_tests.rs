use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;

use walrq::engine::queue::{QueueEngine, QueueError, QueueOptions};

#[tokio::test]
async fn test_ram_ceiling_and_hot_horizon_window() {
    tokio::time::pause();
    let dir = tempdir().unwrap();

    // 1. Strict RAM limit of 500 messages
    let mut opts = QueueOptions::default();
    opts.max_hot_messages_in_ram = 500;

    let engine = Arc::new(QueueEngine::open(dir.path(), opts).unwrap());

    // Push 500 messages -> OK
    for i in 0..500 {
        let res = engine.push("limit-q", Bytes::from(format!("m-{}", i)), 0).await;
        assert!(res.is_ok());
    }

    // 501st message -> Spills cleanly to disk without error! Total in RAM stays <= 500
    let overflow_res = engine.push("limit-q", Bytes::from("overflow"), 0).await;
    assert!(overflow_res.is_ok(), "Message beyond RAM cap must safely spill to disk");
    assert!(engine.total_messages_in_ram() <= 500);

    // Poll & Ack 100 messages -> drops RAM count to 400
    let polled = engine.poll("limit-q", 30, 100).await.unwrap();
    assert_eq!(polled.len(), 100);
    for m in polled {
        assert!(engine.ack("limit-q", &m.message_id, &m.receipt_handle).await.unwrap());
    }

    // Now pushing succeeds again!
    let new_push = engine.push("limit-q", Bytes::from("recovered"), 0).await;
    assert!(new_push.is_ok());
}

#[tokio::test]
async fn test_60_second_horizon_promotion() {
    tokio::time::pause();
    let dir = tempdir().unwrap();
    let engine = Arc::new(QueueEngine::open(dir.path(), QueueOptions::default()).unwrap());

    // Schedule 1 message at +120s (Far-future: outside 60s horizon)
    let far_id = engine.push("horizon-q", Bytes::from("far-future"), 120).await.unwrap();

    // Schedule 1 message at +30s (Within 60s hot horizon)
    let hot_id = engine.push("horizon-q", Bytes::from("hot-window"), 30).await.unwrap();

    // Poll at t=0 -> 0 messages
    assert_eq!(engine.poll("horizon-q", 30, 10).await.unwrap().len(), 0);

    // Advance 31s -> hot message matures into ready FIFO!
    tokio::time::advance(Duration::from_secs(31)).await;
    let p_hot = engine.poll("horizon-q", 30, 10).await.unwrap();
    assert_eq!(p_hot.len(), 1);
    assert_eq!(p_hot[0].message_id, hot_id);
    assert!(engine.ack("horizon-q", &p_hot[0].message_id, &p_hot[0].receipt_handle).await.unwrap());

    // Advance 60s more (t=91s) -> far_future is promoted into hot delay bucket
    tokio::time::advance(Duration::from_secs(20)).await;
    // Still 69s to go before mature (visible_at 120s)
    assert_eq!(engine.poll("horizon-q", 30, 10).await.unwrap().len(), 0);

    // Advance to t=121s -> far message matures into ready FIFO!
    tokio::time::advance(Duration::from_secs(70)).await;
    let p_far = engine.poll("horizon-q", 30, 10).await.unwrap();
    assert_eq!(p_far.len(), 1);
    assert_eq!(p_far[0].message_id, far_id);
}
