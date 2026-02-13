use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::json;

use config::IdType;
use pg::batch::BatchQueryConfig;
use pg::connect::connect;
use pg::test_utils::{TestContext, setup_postgres};
use puffgres_core::{
    Action, BackfillCheckpointer, BackfillConfig, BackfillOutcome, BackfillSink, CoreError,
    DocumentId, Transformer, run_backfill,
};
use replication::{Operation, RowEvent};

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

#[async_trait]
impl BackfillCheckpointer for MemCheckpointer {
    async fn load_progress(&self, _config_name: &str) -> Result<Option<(String, u64)>, CoreError> {
        Ok(self.progress.lock().unwrap().clone())
    }

    async fn save_progress(
        &self,
        _config_name: &str,
        last_id: &str,
        processed_rows: u64,
    ) -> Result<(), CoreError> {
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
