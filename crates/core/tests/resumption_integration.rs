mod common;

use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;

use pg::publication::ensure_publication;
use pg::slot::{ensure_slot, get_current_wal_lsn, terminate_active_slot_backend};
use puffgres_core::{BackfillOutcome, DocumentId, run_backfill};
use replication::{ReplicationStream, ReplicationStreamConfig, RowEvent};
use state::{BackfillProgress, BackfillStatus, ConfigRecord, StateDb, StreamingCheckpoint};
use tokio_util::sync::CancellationToken;

use common::*;

/// Create a StateDb backed by a real file inside a temp directory.
/// Returns both the dir handle (must be kept alive) and the initialized db.
fn create_state_db() -> (tempfile::TempDir, StateDb) {
    let dir = tempfile::tempdir().expect("failed to create tempdir");
    let path = dir.path().join("state.db");
    let db = StateDb::open(&path).expect("failed to open state db");
    db.insert_config(&ConfigRecord {
        name: "test".to_string(),
        namespace: "test_ns".to_string(),
        content_hash: "abc123".to_string(),
        transform_hash: None,
        applied_at: Utc::now(),
        tombstone_applied_at: None,
        namespace_prefix: None,
    })
    .expect("failed to insert config record");
    (dir, db)
}

/// Re-open the state db from the same directory (simulates restart).
fn reopen_state_db(dir: &tempfile::TempDir) -> StateDb {
    let path = dir.path().join("state.db");

    // No need to reinitialize -- tables already exist from first open.
    StateDb::open(&path).expect("failed to reopen state db")
}

/// Backfill resumption after simulated crash.
///
/// 1. Insert 100 rows, run backfill with batch_size=10
/// 2. A FailingAfterSink causes run_backfill to abort after 5 batches (50 rows)
/// 3. Verify run_backfill's own checkpointing saved the correct cursor
/// 4. Restart with fresh instances pointing at the same state DB
/// 5. Verify backfill resumes from batch 6 (row 51), not batch 1
/// 6. Verify watermark_lsn is preserved across the crash
#[tokio::test]
async fn backfill_resumption_after_crash() {
    let (_ctx, client) = setup_replication_test("resumption_items").await;
    insert_rows(&client, "resumption_items", 1..=100).await;

    let (state_dir, mut state_db) = create_state_db();

    // Record a watermark LSN before backfill, simulating what production does.
    let watermark_lsn = get_current_wal_lsn(&client)
        .await
        .expect("failed to get WAL LSN");

    // Persist the watermark in the backfill progress record (production flow).
    state_db
        .save_backfill_progress(&BackfillProgress {
            config_name: "test".to_string(),
            last_id: None,
            total_rows: None,
            processed_rows: 0,
            status: BackfillStatus::Pending,
            started_at: None,
            completed_at: None,
            error_message: None,
            watermark_lsn: Some(watermark_lsn),
        })
        .expect("failed to save initial backfill progress");

    // --- Phase 1: partial backfill using a sink that fails after 5 batches ---
    let sink1 = FailingAfterSink::new(5);
    let config = make_config("resumption_items", 10);

    let result1 = run_backfill(
        &config,
        &client,
        &sink1,
        &mut state_db,
        &PassthroughTransformer,
        CancellationToken::new(),
    )
    .await;

    // run_backfill retries the failing batch, exhausts retries, then returns Failed.
    assert!(
        matches!(result1.status, BackfillOutcome::Failed { .. }),
        "backfill should fail after sink error exhausts retries"
    );
    assert_eq!(
        result1.processed_rows, 50,
        "should have processed exactly 5 batches of 10 before failure"
    );

    // Verify the first 50 rows were written to the sink before the failure.
    assert_eq!(sink1.inner.total_actions(), 50);

    // --- Verify run_backfill's own checkpointing (Issue 2) ---
    // The checkpoints were saved by run_backfill itself, not manually set.
    let progress_after_crash = state_db
        .get_backfill_progress("test")
        .expect("failed to load progress after crash")
        .expect("progress should exist after crash");
    assert_eq!(
        progress_after_crash.watermark_lsn,
        Some(watermark_lsn),
        "watermark_lsn must be preserved by run_backfill's checkpointing"
    );
    assert_eq!(
        progress_after_crash.processed_rows, 50,
        "run_backfill should have checkpointed 50 processed rows"
    );
    assert_eq!(
        progress_after_crash.last_id,
        Some("0050".to_string()),
        "run_backfill should have checkpointed cursor at row 0050"
    );
    assert_eq!(
        progress_after_crash.status,
        BackfillStatus::InProgress,
        "status should be InProgress (not Completed or Pending)"
    );

    // Drop the state db handle (simulates process crash).
    drop(state_db);

    // --- Phase 2: restart from same state file ---
    let mut state_db2 = reopen_state_db(&state_dir);

    // Resume backfill using the reopened state db
    let sink2 = CollectingSink::new();
    let result2 = run_backfill(
        &config,
        &client,
        &sink2,
        &mut state_db2,
        &PassthroughTransformer,
        CancellationToken::new(),
    )
    .await;

    assert!(
        matches!(result2.status, BackfillOutcome::Completed),
        "resumed backfill should complete"
    );

    // The resumed run reports cumulative processed_rows (50 from checkpoint + 50 new)
    assert_eq!(
        result2.processed_rows, 100,
        "total processed_rows should be 100 (50 from checkpoint + 50 new)"
    );

    // The sink for the second run should only contain rows 51-100
    assert_eq!(
        sink2.total_actions(),
        50,
        "resumed backfill should only fetch rows 51-100, not re-fetch rows 1-50"
    );

    // Verify the phase 2 sink contains only IDs 0051-0100
    let phase2_all: Vec<_> = sink2
        .writes
        .lock()
        .expect("lock poisoned")
        .iter()
        .flatten()
        .cloned()
        .collect();
    let phase2_ids = extract_upsert_ids(&phase2_all);
    let expected_phase2: Vec<DocumentId> = (51..=100)
        .map(|i| DocumentId::String(format!("{:04}", i)))
        .collect();
    assert_eq!(
        phase2_ids, expected_phase2,
        "resumed backfill must produce exactly IDs 0051-0100"
    );
}

/// CDC resumption after simulated crash.
///
/// 1. Insert rows to generate CDC events, process them, save checkpoint
/// 2. Simulate crash (drop stream)
/// 3. Restart CDC from the checkpointed LSN
/// 4. Verify CDC resumes from the last acked LSN (no duplicates)
#[tokio::test]
async fn cdc_resumption_after_crash() {
    let (ctx, client) = setup_replication_test("resumption_items").await;

    let slot = "cdc_resume_slot";
    let pub_name = "cdc_resume_pub";
    ensure_slot(&client, slot)
        .await
        .expect("failed to create slot");
    ensure_publication(&client, pub_name, &["public.resumption_items".to_string()])
        .await
        .expect("failed to create publication");

    let (state_dir, state_db) = create_state_db();

    // Insert 10 rows to generate CDC events
    insert_rows(&client, "resumption_items", 1..=10).await;

    // --- Phase 1: consume all 10 CDC events ---
    let mut stream1 = ReplicationStream::connect(ReplicationStreamConfig {
        connection_string: ctx.connection_url.clone(),
        slot_name: slot.to_string(),
        publication_name: pub_name.to_string(),
        start_lsn: None,
        status_interval: Duration::from_secs(1),
        max_transaction_events: None,
        sub_batch_size: None,
        watched_columns: HashMap::new(),
    })
    .await
    .expect("failed to connect replication stream");

    let phase1_events = collect_cdc_events(&mut stream1, 10, 15).await;
    assert_eq!(
        phase1_events.len(),
        10,
        "phase 1 should collect all 10 CDC events"
    );

    // Save a streaming checkpoint at the last acked LSN
    let last_ack_lsn = phase1_events.last().expect("should have events").1;
    state_db
        .save_streaming_checkpoint(&StreamingCheckpoint {
            config_name: "test".to_string(),
            lsn: last_ack_lsn,
            events_processed: 10,
            updated_at: Utc::now(),
        })
        .expect("failed to save streaming checkpoint");

    // Give Postgres a moment to process the status update, then drop
    tokio::time::sleep(Duration::from_secs(2)).await;
    drop(stream1);

    // Wait for the slot to become inactive
    tokio::time::sleep(Duration::from_secs(1)).await;

    // --- Simulate crash: drop and reopen state db ---
    drop(state_db);
    let state_db2 = reopen_state_db(&state_dir);

    // Verify checkpoint survived
    let checkpoint = state_db2
        .get_streaming_checkpoint("test")
        .expect("failed to load checkpoint after crash")
        .expect("checkpoint should exist after crash");
    assert_eq!(
        checkpoint.lsn, last_ack_lsn,
        "checkpoint LSN must be preserved across crash"
    );
    assert_eq!(
        checkpoint.events_processed, 10,
        "events_processed must be preserved across crash"
    );

    // --- Phase 2: insert more rows, resume from checkpoint ---
    insert_rows(&client, "resumption_items", 11..=15).await;

    // Terminate any stale backend holding the slot before reconnecting
    terminate_active_slot_backend(&client, slot)
        .await
        .expect("failed to terminate stale backend");
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Resume CDC from the saved checkpoint LSN
    let mut stream2 = ReplicationStream::connect(ReplicationStreamConfig {
        connection_string: ctx.connection_url.clone(),
        slot_name: slot.to_string(),
        publication_name: pub_name.to_string(),
        start_lsn: Some(checkpoint.lsn),
        status_interval: Duration::from_secs(1),
        max_transaction_events: None,
        sub_batch_size: None,
        watched_columns: HashMap::new(),
    })
    .await
    .expect("failed to reconnect replication stream");

    let phase2_events = collect_cdc_events(&mut stream2, 5, 15).await;

    // Route and transform to extract IDs
    let raw_events: Vec<RowEvent> = phase2_events.iter().map(|(ev, _)| ev.clone()).collect();
    let actions =
        route_and_transform(&raw_events, stream2.relation_cache(), "resumption_items").await;
    let ids = extract_upsert_ids(&actions);

    // Verify exactly IDs 0011-0015 — no duplicates, no gaps, correct checkpoint continuation
    let expected_ids: Vec<DocumentId> = (11..=15)
        .map(|i| DocumentId::String(format!("{:04}", i)))
        .collect();
    assert_eq!(
        ids, expected_ids,
        "resumed CDC must produce exactly IDs 0011-0015, not replay old events or skip any"
    );
}

/// Backfill -> CDC handoff after crash.
///
/// 1. Start and complete backfill, mark it completed in state DB
/// 2. Process 5 CDC events, checkpoint, then crash
/// 3. Restart: verify backfill is NOT re-run (status=completed)
/// 4. Verify CDC resumes from the streaming checkpoint, not the watermark
#[tokio::test]
async fn backfill_to_cdc_handoff_after_crash() {
    let (ctx, client) = setup_replication_test("resumption_items").await;

    let slot = "handoff_slot";
    let pub_name = "handoff_pub";
    ensure_slot(&client, slot)
        .await
        .expect("failed to create slot");
    ensure_publication(&client, pub_name, &["public.resumption_items".to_string()])
        .await
        .expect("failed to create publication");

    let (state_dir, mut state_db) = create_state_db();

    // Insert seed data for backfill
    insert_rows(&client, "resumption_items", 1..=20).await;

    // Capture watermark before backfill
    let watermark_lsn = get_current_wal_lsn(&client)
        .await
        .expect("failed to get WAL LSN");

    // --- Phase 1: complete backfill ---
    let backfill_sink = CollectingSink::new();
    let config = make_config("resumption_items", 10);

    let result = run_backfill(
        &config,
        &client,
        &backfill_sink,
        &mut state_db,
        &PassthroughTransformer,
        CancellationToken::new(),
    )
    .await;

    assert!(
        matches!(result.status, BackfillOutcome::Completed),
        "backfill should complete"
    );
    assert_eq!(result.processed_rows, 20);

    // Mark backfill as completed with watermark in state db
    state_db
        .save_backfill_progress(&BackfillProgress {
            config_name: "test".to_string(),
            last_id: Some("0020".to_string()),
            total_rows: Some(20),
            processed_rows: 20,
            status: BackfillStatus::Completed,
            started_at: Some(Utc::now()),
            completed_at: Some(Utc::now()),
            error_message: None,
            watermark_lsn: Some(watermark_lsn),
        })
        .expect("failed to save completed backfill progress");

    // --- Phase 2: CDC from watermark, process 5 events ---
    // Insert 10 rows after backfill to generate CDC events
    insert_rows(&client, "resumption_items", 21..=30).await;

    let mut stream1 = ReplicationStream::connect(ReplicationStreamConfig {
        connection_string: ctx.connection_url.clone(),
        slot_name: slot.to_string(),
        publication_name: pub_name.to_string(),
        start_lsn: Some(watermark_lsn),
        status_interval: Duration::from_secs(1),
        max_transaction_events: None,
        sub_batch_size: None,
        watched_columns: HashMap::new(),
    })
    .await
    .expect("failed to connect replication stream");

    // Collect 5 events and checkpoint (simulating partial CDC processing)
    let phase2_events = collect_cdc_events(&mut stream1, 5, 15).await;
    assert!(
        phase2_events.len() >= 5,
        "should collect at least 5 CDC events, got {}",
        phase2_events.len()
    );

    let cdc_checkpoint_lsn = phase2_events.last().expect("should have events").1;

    // Save a streaming checkpoint after 5 events
    state_db
        .save_streaming_checkpoint(&StreamingCheckpoint {
            config_name: "test".to_string(),
            lsn: cdc_checkpoint_lsn,
            events_processed: 5,
            updated_at: Utc::now(),
        })
        .expect("failed to save streaming checkpoint");

    // Give Postgres time to process ack, then "crash"
    tokio::time::sleep(Duration::from_secs(2)).await;
    drop(stream1);
    tokio::time::sleep(Duration::from_secs(1)).await;
    drop(state_db);

    // --- Phase 3: restart after crash ---
    let state_db2 = reopen_state_db(&state_dir);

    // Verify backfill is marked Completed -- it should NOT be re-run
    let backfill_progress = state_db2
        .get_backfill_progress("test")
        .expect("failed to load backfill progress after crash")
        .expect("backfill progress should exist");
    assert_eq!(
        backfill_progress.status,
        BackfillStatus::Completed,
        "backfill must remain Completed after crash -- it should NOT be re-run"
    );
    assert_eq!(
        backfill_progress.watermark_lsn,
        Some(watermark_lsn),
        "watermark_lsn must be preserved"
    );

    // Verify streaming checkpoint exists and differs from watermark
    let streaming_ckpt = state_db2
        .get_streaming_checkpoint("test")
        .expect("failed to load streaming checkpoint after crash")
        .expect("streaming checkpoint should exist");
    assert_eq!(
        streaming_ckpt.lsn, cdc_checkpoint_lsn,
        "streaming checkpoint LSN must be the CDC checkpoint, not the watermark"
    );
    assert_ne!(
        streaming_ckpt.lsn, watermark_lsn,
        "streaming checkpoint LSN must differ from backfill watermark — CDC has advanced past it"
    );

    // Resume CDC from the streaming checkpoint (NOT the watermark)
    terminate_active_slot_backend(&client, slot)
        .await
        .expect("failed to terminate stale backend");
    tokio::time::sleep(Duration::from_secs(1)).await;

    let mut stream2 = ReplicationStream::connect(ReplicationStreamConfig {
        connection_string: ctx.connection_url.clone(),
        slot_name: slot.to_string(),
        publication_name: pub_name.to_string(),
        start_lsn: Some(streaming_ckpt.lsn),
        status_interval: Duration::from_secs(1),
        max_transaction_events: None,
        sub_batch_size: None,
        watched_columns: HashMap::new(),
    })
    .await
    .expect("failed to reconnect replication stream");

    // Should pick up the remaining 5 events (IDs 26-30), not replay 21-25
    let phase3_events = collect_cdc_events(&mut stream2, 5, 15).await;

    let raw_events: Vec<RowEvent> = phase3_events.iter().map(|(ev, _)| ev.clone()).collect();
    let actions =
        route_and_transform(&raw_events, stream2.relation_cache(), "resumption_items").await;
    let ids = extract_upsert_ids(&actions);

    // Verify exactly IDs 0026-0030 — resumed from CDC checkpoint, not watermark or start
    let expected_ids: Vec<DocumentId> = (26..=30)
        .map(|i| DocumentId::String(format!("{:04}", i)))
        .collect();
    assert_eq!(
        ids, expected_ids,
        "resumed CDC must produce exactly IDs 0026-0030, not replay backfill or prior CDC events"
    );

    // Final sanity: backfill progress is still Completed (was NOT re-run)
    let final_progress = state_db2
        .get_backfill_progress("test")
        .expect("failed to re-check backfill progress")
        .expect("backfill progress should still exist");
    assert_eq!(
        final_progress.status,
        BackfillStatus::Completed,
        "backfill must still be Completed after CDC resumption"
    );
}
