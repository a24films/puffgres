//! Chaos tests: verify TurbopufferClient handles mock server failures correctly.
//! Run with: cargo test -p puff --test chaos_sink -- --ignored --nocapture

use puff::TurbopufferClient;
use puff::mock_server::{ChaosConfig, ChaosMode, MockTurbopufferServer};
use puffgres_core::{Action, BackfillSink, DocumentId};
use serde_json::json;

fn sample_actions(start_id: u64, count: usize) -> Vec<Action> {
    (0..count)
        .map(|i| Action::Upsert {
            id: DocumentId::Uint(start_id + i as u64),
            document: json!({"title": format!("doc_{}", start_id + i as u64)}),
            vector: None,
            distance_metric: None,
            schema: None,
        })
        .collect()
}

#[tokio::test]
#[ignore]
async fn healthy_write_through_real_client() {
    let server = MockTurbopufferServer::start().await;
    let client = TurbopufferClient::with_base_url("test-key".into(), server.url());

    client
        .send_batch("test_ns", &sample_actions(1, 10))
        .await
        .unwrap();

    assert_eq!(server.total_upserts("test_ns").await, 10);
    assert_eq!(server.stats().await.total_writes, 1);
    server.stop();
}

#[tokio::test]
#[ignore]
async fn backfill_sink_trait_through_mock() {
    let server = MockTurbopufferServer::start().await;
    let client = TurbopufferClient::with_base_url("test-key".into(), server.url());

    client.write("ns_a", &sample_actions(1, 5)).await.unwrap();
    client.write("ns_b", &sample_actions(100, 3)).await.unwrap();

    assert_eq!(server.total_upserts("ns_a").await, 5);
    assert_eq!(server.total_upserts("ns_b").await, 3);
    server.stop();
}

#[tokio::test]
#[ignore]
async fn server_500_returns_transient_error() {
    let server = MockTurbopufferServer::start().await;
    let client = TurbopufferClient::with_base_url("test-key".into(), server.url());

    server
        .set_chaos(ChaosConfig {
            mode: ChaosMode::Error500,
            remaining: None,
        })
        .await;

    let err = client
        .send_batch("ns", &sample_actions(1, 1))
        .await
        .unwrap_err();
    assert!(err.is_transient(), "500 should be transient, got: {err}");
    assert_eq!(server.total_upserts("ns").await, 0);

    server.stop();
}

#[tokio::test]
#[ignore]
async fn server_429_returns_transient_error() {
    let server = MockTurbopufferServer::start().await;
    let client = TurbopufferClient::with_base_url("test-key".into(), server.url());

    server
        .set_chaos(ChaosConfig {
            mode: ChaosMode::RateLimit429,
            remaining: None,
        })
        .await;

    let err = client
        .send_batch("ns", &sample_actions(1, 1))
        .await
        .unwrap_err();
    assert!(err.is_transient(), "429 should be transient, got: {err}");

    server.stop();
}

#[tokio::test]
#[ignore]
async fn outage_then_recovery() {
    let server = MockTurbopufferServer::start().await;
    let client = TurbopufferClient::with_base_url("test-key".into(), server.url());

    // First batch succeeds
    client
        .send_batch("ns", &sample_actions(1, 5))
        .await
        .unwrap();

    // Enable 500s for 3 requests
    server
        .set_chaos(ChaosConfig {
            mode: ChaosMode::Error500,
            remaining: Some(3),
        })
        .await;

    // These fail
    for i in 0..3 {
        let err = client
            .send_batch("ns", &sample_actions(100 + i * 5, 5))
            .await;
        assert!(err.is_err(), "request {i} should fail during outage");
    }

    // Server auto-recovers (remaining exhausted)
    client
        .send_batch("ns", &sample_actions(200, 5))
        .await
        .unwrap();

    let stats = server.stats().await;
    println!(
        "total_writes: {}, total_errors: {}",
        stats.total_writes, stats.total_errors
    );
    assert_eq!(stats.total_writes, 2); // 2 successful writes
    assert_eq!(stats.total_errors, 3); // 3 failed
    assert_eq!(server.total_upserts("ns").await, 10); // 5 + 5

    server.stop();
}

#[tokio::test]
#[ignore]
async fn slow_response_still_succeeds() {
    let server = MockTurbopufferServer::start().await;
    let client = TurbopufferClient::with_base_url("test-key".into(), server.url());

    server
        .set_chaos(ChaosConfig {
            mode: ChaosMode::SlowResponse(std::time::Duration::from_millis(100)),
            remaining: Some(2),
        })
        .await;

    let start = std::time::Instant::now();
    client
        .send_batch("ns", &sample_actions(1, 3))
        .await
        .unwrap();
    let elapsed = start.elapsed();

    println!("slow write took {elapsed:.2?}");
    assert!(elapsed >= std::time::Duration::from_millis(90));
    assert_eq!(server.total_upserts("ns").await, 3);

    server.stop();
}

#[tokio::test]
#[ignore]
async fn many_writes_under_load() {
    let server = MockTurbopufferServer::start().await;
    let client = TurbopufferClient::with_base_url("test-key".into(), server.url());

    let num_batches = 100u64;
    let batch_size = 50usize;

    let start = std::time::Instant::now();
    for i in 0..num_batches {
        client
            .send_batch(
                "load_ns",
                &sample_actions(i * batch_size as u64, batch_size),
            )
            .await
            .unwrap();
    }
    let elapsed = start.elapsed();

    let total = num_batches as usize * batch_size;
    let writes_per_sec = num_batches as f64 / elapsed.as_secs_f64();
    println!(
        "{num_batches} batches ({total} actions) in {elapsed:.2?} = {writes_per_sec:.0} writes/sec"
    );

    assert_eq!(server.total_upserts("load_ns").await, total as usize);
    assert_eq!(server.stats().await.total_writes, num_batches as u64);

    server.stop();
}

#[tokio::test]
#[ignore]
async fn deletes_and_skips() {
    let server = MockTurbopufferServer::start().await;
    let client = TurbopufferClient::with_base_url("test-key".into(), server.url());

    let actions = vec![
        Action::Upsert {
            id: DocumentId::Uint(1),
            document: json!({"x": 1}),
            vector: None,
            distance_metric: None,
            schema: None,
        },
        Action::Delete {
            id: DocumentId::Uint(2),
        },
        Action::Skip,
        Action::Delete {
            id: DocumentId::Uint(3),
        },
    ];

    client.send_batch("ns", &actions).await.unwrap();

    let records = server.records("ns").await;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].upsert_count, 1);
    assert_eq!(records[0].delete_count, 2);

    server.stop();
}
