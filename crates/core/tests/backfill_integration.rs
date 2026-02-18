use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::json;

use config::IdType;
use pg::batch::BatchQueryConfig;
use pg::connect::connect;
use pg::publication::ensure_publication;
use pg::slot::{ensure_slot, get_current_wal_lsn};
use pg::test_utils::{TestContext, setup_postgres, setup_postgres_logical};
use puffgres_core::{
    Action, BackfillConfig, BackfillOutcome, BackfillSink, CoreError, DocumentId, Mapping, Router,
    Transformer, run_backfill,
};
use replication::{Operation, ReplicationStream, ReplicationStreamConfig, RowEvent};
use state::{BackfillCheckpointer, StateError};

// --- Test impls ---

// Accumulates written action batches, lets us assert what was written.
struct CollectingSink {
    writes: Arc<Mutex<Vec<Vec<Action>>>>,
}

impl CollectingSink {
    fn new() -> Self {
        Self {
            writes: Arc::new(Mutex::new(vec![])),
        }
    }
}

#[async_trait]
impl BackfillSink for CollectingSink {
    async fn write(&self, _namespace: &str, actions: &[Action]) -> Result<(), CoreError> {
        self.writes.lock().unwrap().push(actions.to_vec());
        Ok(())
    }
}

struct MemCheckpointer {
    progress: Mutex<Option<(String, u64)>>,
}

impl MemCheckpointer {
    fn new(initial: Option<(String, u64)>) -> Self {
        Self {
            progress: Mutex::new(initial),
        }
    }
}

impl BackfillCheckpointer for MemCheckpointer {
    fn load_progress(&self, _config_name: &str) -> Result<Option<(String, u64)>, StateError> {
        Ok(self.progress.lock().unwrap().clone())
    }

    fn save_progress(
        &self,
        _config_name: &str,
        last_id: &str,
        processed_rows: u64,
    ) -> Result<(), StateError> {
        *self.progress.lock().unwrap() = Some((last_id.to_string(), processed_rows));
        Ok(())
    }
}

struct PassthroughTransformer;

#[async_trait]
impl Transformer for PassthroughTransformer {
    async fn transform_batch(
        &self,
        events: &[(&RowEvent, DocumentId)],
    ) -> Result<Vec<Action>, CoreError> {
        Ok(events
            .iter()
            .map(|(event, id)| match event.operation {
                Operation::Delete => Action::Delete { id: id.clone() },
                _ => Action::Upsert {
                    id: id.clone(),
                    document: json!({}),
                    vector: None,
                    distance_metric: None,
                },
            })
            .collect())
    }
}

// Wraps CollectingSink and sends a one-shot notification after the first write,
// allowing tests to insert rows while backfill is still in progress.
struct NotifyingSink {
    inner: CollectingSink,
    first_write_tx: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
}

impl NotifyingSink {
    fn new(tx: tokio::sync::oneshot::Sender<()>) -> Self {
        Self {
            inner: CollectingSink::new(),
            first_write_tx: Mutex::new(Some(tx)),
        }
    }
}

#[async_trait]
impl BackfillSink for NotifyingSink {
    async fn write(&self, namespace: &str, actions: &[Action]) -> Result<(), CoreError> {
        self.inner.write(namespace, actions).await?;
        if let Some(tx) = self.first_write_tx.lock().unwrap().take() {
            let _ = tx.send(());
        }
        Ok(())
    }
}

// Always returns Err, used to verify the engine handles write failures correctly.
struct FailingSink;

#[async_trait]
impl BackfillSink for FailingSink {
    async fn write(&self, _namespace: &str, _actions: &[Action]) -> Result<(), CoreError> {
        Err(CoreError::Pipeline("sink failure".to_string()))
    }
}

// --- Helpers ---

async fn create_test_table(client: &tokio_postgres::Client) {
    client
        .execute(
            "CREATE TABLE backfill_items (id TEXT PRIMARY KEY, name TEXT, value TEXT)",
            &[],
        )
        .await
        .expect("Failed to create table");
}

async fn insert_rows(client: &tokio_postgres::Client, count: usize) {
    for i in 1..=count {
        let id = format!("{:04}", i);
        let name = format!("name_{}", i);
        let value = format!("value_{}", i);
        client
            .execute(
                "INSERT INTO backfill_items (id, name, value) VALUES ($1, $2, $3)",
                &[&id, &name, &value],
            )
            .await
            .expect("Failed to insert row");
    }
}

fn make_config(batch_size: u32) -> BackfillConfig {
    BackfillConfig {
        batch_size,
        max_retries: 3,
        config_name: "test".to_string(),
        namespace: "test_ns".to_string(),
        query_config: BatchQueryConfig {
            schema: "public".to_string(),
            table: "backfill_items".to_string(),
            id_column: "id".to_string(),
            columns: None,
            batch_size,
        },
        id_type: IdType::String,
    }
}

async fn setup_test_table() -> (TestContext, tokio_postgres::Client) {
    let ctx = setup_postgres().await;
    let client = connect(&ctx.connection_string)
        .await
        .expect("Failed to connect");
    create_test_table(&client).await;
    (ctx, client)
}

async fn setup_replication_test() -> (TestContext, tokio_postgres::Client) {
    let ctx = setup_postgres_logical().await;
    let client = connect(&ctx.connection_string)
        .await
        .expect("Failed to connect");
    create_test_table(&client).await;
    (ctx, client)
}

fn make_mapping() -> Mapping {
    Mapping {
        name: "test".to_string(),
        namespace: "test_ns".to_string(),
        source_schema: "public".to_string(),
        source_table: "backfill_items".to_string(),
        id_column: "id".to_string(),
        id_type: IdType::String,
        columns: None,
    }
}

/// Collect CDC events from a replication stream with a timeout.
/// Keeps receiving batches until `expected` events are collected or the timeout elapses.
async fn collect_cdc_events(
    stream: &mut ReplicationStream,
    expected: usize,
    timeout_secs: u64,
) -> Vec<replication::RowEvent> {
    let mut events = Vec::new();
    let start = tokio::time::Instant::now();
    let deadline = Duration::from_secs(timeout_secs);
    while events.len() < expected && start.elapsed() < deadline {
        let remaining = deadline.saturating_sub(start.elapsed());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, stream.recv_batch()).await {
            Ok(Ok(Some(batch))) => {
                events.extend(batch.events);
                stream.ack();
            }
            Ok(Ok(None)) => break,
            Ok(Err(e)) => panic!("Replication stream error: {e}"),
            Err(_) => break,
        }
    }
    events
}

// --- Tests ---

#[tokio::test]
async fn complete_backfill_processes_all_batches() {
    let (_ctx, client) = setup_test_table().await;
    insert_rows(&client, 7).await;

    let sink = CollectingSink::new();
    let checkpointer = MemCheckpointer::new(None);
    let config = make_config(3);

    let result = run_backfill(
        &config,
        &client,
        &sink,
        &checkpointer,
        &PassthroughTransformer,
    )
    .await;

    assert!(matches!(result.status, BackfillOutcome::Completed));
    assert_eq!(result.processed_rows, 7);
    assert_eq!(sink.writes.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn resumes_from_checkpoint() {
    let (_ctx, client) = setup_test_table().await;
    insert_rows(&client, 5).await;

    let sink = CollectingSink::new();
    let checkpointer = MemCheckpointer::new(Some(("0003".to_string(), 3)));
    let config = make_config(10);

    let result = run_backfill(
        &config,
        &client,
        &sink,
        &checkpointer,
        &PassthroughTransformer,
    )
    .await;

    assert!(matches!(result.status, BackfillOutcome::Completed));
    assert_eq!(result.processed_rows, 5);
    // Only rows 4-5 should have been fetched
    let writes = sink.writes.lock().unwrap();
    assert_eq!(writes.len(), 1);
    assert_eq!(writes[0].len(), 2);
}

#[tokio::test]
async fn saves_progress_after_each_batch() {
    let (_ctx, client) = setup_test_table().await;
    insert_rows(&client, 5).await;

    let sink = CollectingSink::new();
    let checkpointer = MemCheckpointer::new(None);
    let config = make_config(2);

    let result = run_backfill(
        &config,
        &client,
        &sink,
        &checkpointer,
        &PassthroughTransformer,
    )
    .await;

    assert!(matches!(result.status, BackfillOutcome::Completed));
    assert_eq!(result.processed_rows, 5);
    let progress = checkpointer.progress.lock().unwrap().clone();
    assert_eq!(progress, Some(("0005".to_string(), 5)));
}

#[tokio::test]
async fn empty_table_completes_immediately() {
    let (_ctx, client) = setup_test_table().await;

    let sink = CollectingSink::new();
    let checkpointer = MemCheckpointer::new(None);
    let config = make_config(10);

    let result = run_backfill(
        &config,
        &client,
        &sink,
        &checkpointer,
        &PassthroughTransformer,
    )
    .await;

    assert!(matches!(result.status, BackfillOutcome::Completed));
    assert_eq!(result.processed_rows, 0);
    assert!(sink.writes.lock().unwrap().is_empty());
}

#[tokio::test]
async fn sink_failure_exhausts_retries() {
    let (_ctx, client) = setup_test_table().await;
    insert_rows(&client, 3).await;

    let checkpointer = MemCheckpointer::new(None);
    let mut config = make_config(10);
    config.max_retries = 2;

    let result = run_backfill(
        &config,
        &client,
        &FailingSink,
        &checkpointer,
        &PassthroughTransformer,
    )
    .await;

    assert!(matches!(result.status, BackfillOutcome::Failed { .. }));
    assert_eq!(result.processed_rows, 0);
}

// --- Backfill → CDC integration tests ---

/// End-to-end: backfill existing data without CDC, then stream future changes.
/// Verifies the full pipeline: backfill captures all pre-existing rows, CDC
/// captures inserts/updates/deletes after the watermark, and the Router +
/// Transformer produce correct actions with correct IDs.
#[tokio::test]
async fn backfill_then_cdc_captures_all_changes() {
    let (ctx, client) = setup_replication_test().await;
    insert_rows(&client, 5).await;

    // Setup replication slot + publication
    let slot = "backfill_cdc_slot";
    let pub_name = "backfill_cdc_pub";
    ensure_slot(&client, slot)
        .await
        .expect("Failed to create slot");
    ensure_publication(&client, pub_name, &["public.backfill_items".to_string()])
        .await
        .expect("Failed to create publication");

    // Capture watermark before backfill (matches production flow in run.rs)
    let watermark_lsn = get_current_wal_lsn(&client)
        .await
        .expect("Failed to get WAL LSN");

    // --- Phase 1: Backfill (no CDC) ---
    let backfill_sink = CollectingSink::new();
    let checkpointer = MemCheckpointer::new(None);
    let config = make_config(10);

    let result = run_backfill(
        &config,
        &client,
        &backfill_sink,
        &checkpointer,
        &PassthroughTransformer,
    )
    .await;

    assert!(matches!(result.status, BackfillOutcome::Completed));
    assert_eq!(result.processed_rows, 5);

    let backfill_actions: Vec<Action> = backfill_sink
        .writes
        .lock()
        .unwrap()
        .iter()
        .flatten()
        .cloned()
        .collect();
    assert_eq!(backfill_actions.len(), 5);
    assert!(
        backfill_actions
            .iter()
            .all(|a| matches!(a, Action::Upsert { .. }))
    );

    // Verify the exact backfill ID set — not just count
    let backfill_ids: Vec<DocumentId> = backfill_actions
        .iter()
        .filter_map(|a| match a {
            Action::Upsert { id, .. } => Some(id.clone()),
            _ => None,
        })
        .collect();
    let expected_backfill: Vec<DocumentId> = (1..=5)
        .map(|i| DocumentId::String(format!("{:04}", i)))
        .collect();
    assert_eq!(
        backfill_ids, expected_backfill,
        "Backfill must capture exactly IDs 0001-0005"
    );

    // --- Phase 2: DML after watermark ---
    // 3 inserts
    for i in 6..=8 {
        let id = format!("{:04}", i);
        let name = format!("name_{}", i);
        let value = format!("value_{}", i);
        client
            .execute(
                "INSERT INTO backfill_items (id, name, value) VALUES ($1, $2, $3)",
                &[&id, &name, &value],
            )
            .await
            .expect("Failed to insert");
    }
    // 1 update
    client
        .execute(
            "UPDATE backfill_items SET value = 'updated' WHERE id = '0001'",
            &[],
        )
        .await
        .expect("Failed to update");
    // 1 delete
    client
        .execute("DELETE FROM backfill_items WHERE id = '0002'", &[])
        .await
        .expect("Failed to delete");

    // --- Phase 3: CDC from watermark ---
    let mut stream = ReplicationStream::connect(ReplicationStreamConfig {
        connection_string: ctx.connection_url.clone(),
        slot_name: slot.to_string(),
        publication_name: pub_name.to_string(),
        start_lsn: Some(watermark_lsn),
        status_interval: Duration::from_secs(10),
    })
    .await
    .expect("Failed to connect replication stream");

    let cdc_events = collect_cdc_events(&mut stream, 5, 10).await;

    assert_eq!(
        cdc_events.len(),
        5,
        "Expected 5 CDC events (3 inserts + 1 update + 1 delete), got {}",
        cdc_events.len()
    );

    // Route through Router
    let router = Router::new(vec![make_mapping()]);
    let routed = router.route_batch(&cdc_events, stream.relation_cache());
    let events_for_config = routed
        .get("test")
        .expect("Router should match events to 'test' config");
    assert_eq!(events_for_config.len(), 5);

    // Transform
    let actions = PassthroughTransformer
        .transform_batch(events_for_config.as_slice())
        .await
        .expect("Transform failed");

    // Verify action types
    let upsert_ids: Vec<DocumentId> = actions
        .iter()
        .filter_map(|a| match a {
            Action::Upsert { id, .. } => Some(id.clone()),
            _ => None,
        })
        .collect();
    let delete_ids: Vec<DocumentId> = actions
        .iter()
        .filter_map(|a| match a {
            Action::Delete { id } => Some(id.clone()),
            _ => None,
        })
        .collect();

    assert_eq!(upsert_ids.len(), 4, "3 inserts + 1 update = 4 upserts");
    assert_eq!(delete_ids.len(), 1, "1 delete");

    // Verify specific IDs
    assert!(upsert_ids.contains(&DocumentId::String("0006".to_string())));
    assert!(upsert_ids.contains(&DocumentId::String("0007".to_string())));
    assert!(upsert_ids.contains(&DocumentId::String("0008".to_string())));
    assert!(upsert_ids.contains(&DocumentId::String("0001".to_string())));
    assert_eq!(delete_ids, vec![DocumentId::String("0002".to_string())]);

    // Write to sink to verify end-to-end
    let cdc_sink = CollectingSink::new();
    cdc_sink
        .write("test_ns", &actions)
        .await
        .expect("Sink write failed");
    assert_eq!(cdc_sink.writes.lock().unwrap()[0].len(), 5);
}

/// Verifies there's no gap between backfill and CDC: the watermark LSN captured
/// before backfill is the exact point where CDC picks up. Boundary rows are
/// inserted WHILE backfill is still in progress, exercising the critical window
/// between watermark capture and backfill completion. A regression that starts
/// CDC from a post-backfill LSN (instead of the pre-backfill watermark) would
/// miss these boundary rows.
#[tokio::test]
async fn no_gap_between_backfill_watermark_and_cdc_start() {
    let (ctx, client) = setup_replication_test().await;

    let slot = "nogap_slot";
    let pub_name = "nogap_pub";
    ensure_slot(&client, slot).await.unwrap();
    ensure_publication(&client, pub_name, &["public.backfill_items".to_string()])
        .await
        .unwrap();

    // Insert seed data BEFORE watermark
    insert_rows(&client, 3).await;

    // Capture watermark — this is the boundary between backfill and CDC
    let watermark_lsn = get_current_wal_lsn(&client).await.unwrap();

    // Separate connection for boundary inserts during backfill
    let insert_client = connect(&ctx.connection_string).await.unwrap();

    // Sink that notifies after the first batch, letting us insert during backfill
    let (first_write_tx, first_write_rx) = tokio::sync::oneshot::channel::<()>();
    let notify_sink = NotifyingSink::new(first_write_tx);
    let checkpointer = MemCheckpointer::new(None);
    // batch_size=2 ensures multiple batches over 3 seed rows
    let config = make_config(2);

    let backfill_fut = run_backfill(
        &config,
        &client,
        &notify_sink,
        &checkpointer,
        &PassthroughTransformer,
    );
    tokio::pin!(backfill_fut);

    // Drive backfill until first batch is written
    tokio::select! {
        _ = first_write_rx => {}
        result = &mut backfill_fut => {
            panic!(
                "Backfill finished before first batch notification ({} rows)",
                result.processed_rows
            );
        }
    }

    // Insert boundary rows WHILE backfill is in progress — after watermark,
    // before backfill completes. Their WAL positions sit between the watermark
    // and the post-backfill LSN, so CDC must start from the watermark to see them.
    for i in 4..=6 {
        let id = format!("{:04}", i);
        insert_client
            .execute(
                "INSERT INTO backfill_items (id, name, value) VALUES ($1, $2, $3)",
                &[&id, &format!("name_{i}"), &format!("value_{i}")],
            )
            .await
            .unwrap();
    }

    // Let backfill finish — it will also pick up 0004-0006 (after cursor)
    let result = backfill_fut.await;
    assert!(matches!(result.status, BackfillOutcome::Completed));

    // CDC from the watermark must capture the boundary inserts
    let mut stream = ReplicationStream::connect(ReplicationStreamConfig {
        connection_string: ctx.connection_url.clone(),
        slot_name: slot.to_string(),
        publication_name: pub_name.to_string(),
        start_lsn: Some(watermark_lsn),
        status_interval: Duration::from_secs(10),
    })
    .await
    .unwrap();

    let cdc_events = collect_cdc_events(&mut stream, 3, 10).await;
    assert_eq!(
        cdc_events.len(),
        3,
        "CDC should capture all 3 boundary inserts from watermark onward"
    );

    // Route and transform
    let router = Router::new(vec![make_mapping()]);
    let routed = router.route_batch(&cdc_events, stream.relation_cache());
    let events = &routed["test"];
    let actions = PassthroughTransformer
        .transform_batch(events.as_slice())
        .await
        .unwrap();

    let cdc_ids: Vec<DocumentId> = actions
        .iter()
        .filter_map(|a| match a {
            Action::Upsert { id, .. } => Some(id.clone()),
            _ => None,
        })
        .collect();

    let expected_cdc: Vec<DocumentId> = (4..=6)
        .map(|i| DocumentId::String(format!("{:04}", i)))
        .collect();
    assert_eq!(
        cdc_ids, expected_cdc,
        "CDC must capture exactly boundary IDs 0004-0006"
    );

    // Verify backfill captured at least the seed rows
    let backfill_ids: Vec<DocumentId> = notify_sink
        .inner
        .writes
        .lock()
        .unwrap()
        .iter()
        .flatten()
        .filter_map(|a| match a {
            Action::Upsert { id, .. } => Some(id.clone()),
            _ => None,
        })
        .collect();

    let expected_seed: Vec<DocumentId> = (1..=3)
        .map(|i| DocumentId::String(format!("{:04}", i)))
        .collect();
    for id in &expected_seed {
        assert!(
            backfill_ids.contains(id),
            "Backfill must capture seed row {id:?}"
        );
    }

    // Full coverage: backfill + CDC covers all 6 unique rows, no gaps
    let mut all_ids: Vec<DocumentId> = backfill_ids.into_iter().chain(cdc_ids).collect();
    all_ids.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
    all_ids.dedup();
    let expected_all: Vec<DocumentId> = (1..=6)
        .map(|i| DocumentId::String(format!("{:04}", i)))
        .collect();
    assert_eq!(
        all_ids, expected_all,
        "No gap: backfill + CDC covers all 6 unique rows"
    );
}

/// Multi-batch backfill followed by CDC. Verifies that cursor-based pagination
/// across multiple batches doesn't interfere with the CDC watermark.
#[tokio::test]
async fn backfill_multiple_batches_then_cdc() {
    let (ctx, client) = setup_replication_test().await;
    insert_rows(&client, 7).await;

    let slot = "multi_batch_slot";
    let pub_name = "multi_batch_pub";
    ensure_slot(&client, slot).await.unwrap();
    ensure_publication(&client, pub_name, &["public.backfill_items".to_string()])
        .await
        .unwrap();

    let watermark_lsn = get_current_wal_lsn(&client).await.unwrap();

    // Backfill with batch_size=3 → 3 batches (3 + 3 + 1)
    let backfill_sink = CollectingSink::new();
    let checkpointer = MemCheckpointer::new(None);
    let config = make_config(3);

    let result = run_backfill(
        &config,
        &client,
        &backfill_sink,
        &checkpointer,
        &PassthroughTransformer,
    )
    .await;

    assert!(matches!(result.status, BackfillOutcome::Completed));
    assert_eq!(result.processed_rows, 7);
    assert_eq!(backfill_sink.writes.lock().unwrap().len(), 3);

    // CDC: insert 2 more rows after watermark
    for i in 8..=9 {
        let id = format!("{:04}", i);
        client
            .execute(
                "INSERT INTO backfill_items (id, name, value) VALUES ($1, $2, $3)",
                &[&id, &format!("name_{i}"), &format!("value_{i}")],
            )
            .await
            .unwrap();
    }

    let mut stream = ReplicationStream::connect(ReplicationStreamConfig {
        connection_string: ctx.connection_url.clone(),
        slot_name: slot.to_string(),
        publication_name: pub_name.to_string(),
        start_lsn: Some(watermark_lsn),
        status_interval: Duration::from_secs(10),
    })
    .await
    .unwrap();

    let cdc_events = collect_cdc_events(&mut stream, 2, 10).await;

    assert_eq!(cdc_events.len(), 2);

    let router = Router::new(vec![make_mapping()]);
    let routed = router.route_batch(&cdc_events, stream.relation_cache());
    let events = &routed["test"];
    let actions = PassthroughTransformer
        .transform_batch(events.as_slice())
        .await
        .unwrap();

    assert_eq!(actions.len(), 2);
    assert!(actions.iter().all(|a| matches!(a, Action::Upsert { .. })));

    let cdc_ids: Vec<DocumentId> = actions
        .iter()
        .filter_map(|a| match a {
            Action::Upsert { id, .. } => Some(id.clone()),
            _ => None,
        })
        .collect();
    let expected_cdc: Vec<DocumentId> = (8..=9)
        .map(|i| DocumentId::String(format!("{:04}", i)))
        .collect();
    assert_eq!(
        cdc_ids, expected_cdc,
        "CDC must capture exactly IDs 0008-0009"
    );
}

/// Empty table at backfill time — all data arrives through CDC only.
#[tokio::test]
async fn empty_backfill_then_cdc_only() {
    let (ctx, client) = setup_replication_test().await;

    let slot = "empty_bf_slot";
    let pub_name = "empty_bf_pub";
    ensure_slot(&client, slot).await.unwrap();
    ensure_publication(&client, pub_name, &["public.backfill_items".to_string()])
        .await
        .unwrap();

    let watermark_lsn = get_current_wal_lsn(&client).await.unwrap();

    // Backfill on empty table
    let backfill_sink = CollectingSink::new();
    let checkpointer = MemCheckpointer::new(None);
    let config = make_config(10);

    let result = run_backfill(
        &config,
        &client,
        &backfill_sink,
        &checkpointer,
        &PassthroughTransformer,
    )
    .await;

    assert!(matches!(result.status, BackfillOutcome::Completed));
    assert_eq!(result.processed_rows, 0);
    assert!(backfill_sink.writes.lock().unwrap().is_empty());

    // All data via CDC
    for i in 1..=3 {
        let id = format!("{:04}", i);
        client
            .execute(
                "INSERT INTO backfill_items (id, name, value) VALUES ($1, $2, $3)",
                &[&id, &format!("name_{i}"), &format!("value_{i}")],
            )
            .await
            .unwrap();
    }

    let mut stream = ReplicationStream::connect(ReplicationStreamConfig {
        connection_string: ctx.connection_url.clone(),
        slot_name: slot.to_string(),
        publication_name: pub_name.to_string(),
        start_lsn: Some(watermark_lsn),
        status_interval: Duration::from_secs(10),
    })
    .await
    .unwrap();

    let cdc_events = collect_cdc_events(&mut stream, 3, 10).await;

    assert_eq!(cdc_events.len(), 3);

    let router = Router::new(vec![make_mapping()]);
    let routed = router.route_batch(&cdc_events, stream.relation_cache());
    let events = &routed["test"];
    let actions = PassthroughTransformer
        .transform_batch(events.as_slice())
        .await
        .unwrap();

    assert_eq!(actions.len(), 3);
    assert!(actions.iter().all(|a| matches!(a, Action::Upsert { .. })));

    let cdc_ids: Vec<DocumentId> = actions
        .iter()
        .filter_map(|a| match a {
            Action::Upsert { id, .. } => Some(id.clone()),
            _ => None,
        })
        .collect();
    let expected_cdc: Vec<DocumentId> = (1..=3)
        .map(|i| DocumentId::String(format!("{:04}", i)))
        .collect();
    assert_eq!(
        cdc_ids, expected_cdc,
        "CDC must capture exactly IDs 0001-0003"
    );
}
