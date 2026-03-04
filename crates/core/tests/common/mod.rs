use std::ops::RangeInclusive;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::json;

use config::IdType;
use pg::batch::BatchQueryConfig;
use pg::connect::connect;
use pg::test_utils::{TestContext, setup_postgres_logical};
use puffgres_core::{
    Action, BackfillConfig, BackfillSink, CoreError, DocumentId, Mapping, Router, Transformer,
};
use replication::{Operation, RelationCache, ReplicationStream, RowEvent};

pub struct CollectingSink {
    pub writes: Arc<Mutex<Vec<Vec<Action>>>>,
}

impl CollectingSink {
    pub fn new() -> Self {
        Self {
            writes: Arc::new(Mutex::new(vec![])),
        }
    }

    pub fn total_actions(&self) -> usize {
        self.writes
            .lock()
            .expect("lock poisoned")
            .iter()
            .map(|batch| batch.len())
            .sum()
    }
}

#[async_trait]
impl BackfillSink for CollectingSink {
    async fn write(&self, _namespace: &str, actions: &[Action]) -> Result<(), CoreError> {
        self.writes
            .lock()
            .expect("lock poisoned")
            .push(actions.to_vec());
        Ok(())
    }
}

/// A sink that succeeds for the first `max_writes` calls, then fails.
/// Useful for simulating a crash mid-backfill so `run_backfill`'s own
/// checkpointing is exercised.
pub struct FailingAfterSink {
    pub inner: CollectingSink,
    writes_remaining: std::sync::atomic::AtomicUsize,
}

impl FailingAfterSink {
    pub fn new(max_writes: usize) -> Self {
        Self {
            inner: CollectingSink::new(),
            writes_remaining: std::sync::atomic::AtomicUsize::new(max_writes),
        }
    }
}

#[async_trait]
impl BackfillSink for FailingAfterSink {
    async fn write(&self, namespace: &str, actions: &[Action]) -> Result<(), CoreError> {
        let prev = self.writes_remaining.fetch_update(
            std::sync::atomic::Ordering::SeqCst,
            std::sync::atomic::Ordering::SeqCst,
            |n| if n > 0 { Some(n - 1) } else { None },
        );
        if prev.is_err() {
            return Err(CoreError::Pipeline("simulated crash".to_string()));
        }
        self.inner.write(namespace, actions).await
    }
}

pub struct PassthroughTransformer;

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
                    schema: None,
                },
            })
            .collect())
    }
}

pub async fn create_test_table(client: &tokio_postgres::Client, table_name: &str) {
    client
        .execute(
            &format!(
                "CREATE TABLE IF NOT EXISTS {table_name} (id TEXT PRIMARY KEY, name TEXT, value TEXT)"
            ),
            &[],
        )
        .await
        .expect("Failed to create table");
}

pub async fn insert_rows(
    client: &tokio_postgres::Client,
    table_name: &str,
    range: RangeInclusive<usize>,
) {
    for i in range {
        let id = format!("{:04}", i);
        let name = format!("name_{}", i);
        let value = format!("value_{}", i);
        client
            .execute(
                &format!("INSERT INTO {table_name} (id, name, value) VALUES ($1, $2, $3)"),
                &[&id, &name, &value],
            )
            .await
            .expect("Failed to insert row");
    }
}

pub fn make_config(table_name: &str, batch_size: u32) -> BackfillConfig {
    BackfillConfig {
        batch_size,
        max_retries: 3,
        config_name: "test".to_string(),
        namespace: "test_ns".to_string(),
        query_config: BatchQueryConfig {
            schema: "public".to_string(),
            table: table_name.to_string(),
            id_column: "id".to_string(),
            columns: None,
            batch_size,
        },
        id_type: IdType::String,
    }
}

pub fn make_mapping(table_name: &str) -> Mapping {
    Mapping {
        name: "test".to_string(),
        namespace: "test_ns".to_string(),
        source_schema: "public".to_string(),
        source_table: table_name.to_string(),
        id_column: "id".to_string(),
        id_type: IdType::String,
        columns: None,
    }
}

pub async fn setup_replication_test(table_name: &str) -> (TestContext, tokio_postgres::Client) {
    let ctx = setup_postgres_logical().await;
    let client = connect(&ctx.connection_string)
        .await
        .expect("Failed to connect");
    create_test_table(&client, table_name).await;
    (ctx, client)
}

/// Collect CDC events from a replication stream with a timeout.
/// Returns tuples of (event, ack_lsn) for each event collected.
pub async fn collect_cdc_events(
    stream: &mut ReplicationStream,
    expected: usize,
    timeout_secs: u64,
) -> Vec<(RowEvent, u64)> {
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
                let ack_lsn = batch.ack_lsn;
                for ev in batch.events {
                    events.push((ev, ack_lsn));
                }
                stream.ack();
            }
            Ok(Ok(None)) => break,
            Ok(Err(e)) => panic!("Replication stream error: {e}"),
            Err(_) => break,
        }
    }
    events
}

/// Extract upsert IDs from a list of actions.
pub fn extract_upsert_ids(actions: &[Action]) -> Vec<DocumentId> {
    actions
        .iter()
        .filter_map(|a| match a {
            Action::Upsert { id, .. } => Some(id.clone()),
            _ => None,
        })
        .collect()
}

/// Route events through a Router and transform with PassthroughTransformer.
pub async fn route_and_transform(
    events: &[RowEvent],
    relation_cache: &RelationCache,
    table_name: &str,
) -> Vec<Action> {
    let router = Router::new(vec![make_mapping(table_name)]);
    let routed = router.route_batch(events, relation_cache);
    let events_for_config = routed
        .get("test")
        .expect("Router should match events to 'test' config");
    PassthroughTransformer
        .transform_batch(events_for_config.as_slice())
        .await
        .expect("Transform failed")
}
