use std::collections::HashMap;

use pg::batch::{BatchQueryConfig, fetch_row_by_id, resolve_column_names};
use puff::TurbopufferClient;
use puffgres_core::{DocumentId, Transformer, row_convert::pg_rows_to_events};
use replication::{Operation, RowEvent};
use state::{DlqEntry, DlqOperation, StateStore};
use tokio_util::sync::CancellationToken;

use crate::error::CliError;
use crate::observability::Metrics;
use crate::project_config::ProjectConfig;

/// Map a replication Operation to the DLQ operation enum.
pub(crate) fn operation_to_dlq(op: Operation) -> DlqOperation {
    match op {
        Operation::Insert => DlqOperation::Insert,
        Operation::Update => DlqOperation::Update,
        Operation::Delete => DlqOperation::Delete,
    }
}

/// Map a DLQ operation back to a replication Operation.
fn dlq_to_operation(op: &DlqOperation) -> Operation {
    match op {
        DlqOperation::Insert => Operation::Insert,
        DlqOperation::Update => Operation::Update,
        DlqOperation::Delete => Operation::Delete,
    }
}

/// Insert failed events into the DLQ with only the operation and doc_id
/// (no full event payload).
pub(crate) fn send_events_to_dlq(
    db: &impl StateStore,
    config_name: &str,
    lsn: u64,
    events: &[(&replication::RowEvent, DocumentId)],
    error: &str,
    permanent: bool,
) -> Result<(), CliError> {
    for (event, doc_id) in events {
        let doc_id_json = serde_json::to_string(doc_id)
            .map_err(|e| CliError::Run(format!("failed to serialize doc_id for DLQ: {e}")))?;
        let dlq_op = operation_to_dlq(event.operation);
        let entry = if permanent {
            DlqEntry::permanent(config_name, lsn, dlq_op, Some(doc_id_json), error)
        } else {
            DlqEntry::retryable(config_name, lsn, dlq_op, Some(doc_id_json), error)
        };
        db.insert_dlq_entry(&entry)?;
    }
    Ok(())
}

/// Result of a single DLQ replay pass.
pub(crate) struct ReplayResult {
    /// Number of retryable entries that were fetched.
    pub fetched: usize,
    /// Number of entries successfully replayed and deleted (delivered to Turbopuffer).
    pub succeeded: usize,
}

/// Fetch retryable DLQ entries and attempt to re-process them.
///
/// Every entry is replayed through the configured transformer with the
/// original operation (Insert/Update/Delete) so custom transform logic
/// sees the same event type as the live streaming path.
///
/// On success: delete the entry. On failure: increment retry count, mark
/// permanent if max retries exhausted.
#[tracing::instrument(name = "replay_dlq", skip_all)]
pub(crate) async fn replay_dlq(
    db: &impl StateStore,
    database_url: &str,
    configs: &HashMap<String, &config::Config>,
    transformers: &HashMap<String, Box<dyn Transformer>>,
    namespaces: &HashMap<String, String>,
    puff_client: &TurbopufferClient,
    project_config: &ProjectConfig,
    metrics: Option<&Metrics>,
    token: &CancellationToken,
) -> Result<ReplayResult, CliError> {
    let entries = db.list_retryable_entries(project_config.dlq_replay_batch_size())?;
    if entries.is_empty() {
        return Ok(ReplayResult {
            fetched: 0,
            succeeded: 0,
        });
    }

    tracing::info!(entries = entries.len(), "replaying DLQ entries");

    // Open a fresh connection per replay pass so a stale/dropped connection
    // doesn't burn through retry budgets on otherwise-valid DLQ entries.
    let pg_client = pg::connect::connect(database_url)
        .await
        .map_err(|e| CliError::Run(format!("DLQ replay connection failed: {e}")))?;

    let mut succeeded: usize = 0;

    for entry in &entries {
        if token.is_cancelled() {
            tracing::info!("shutdown requested, aborting DLQ replay");
            return Ok(ReplayResult {
                fetched: 0,
                succeeded: 0,
            });
        }

        let config = match configs.get(&entry.config_name) {
            Some(c) => c,
            None => {
                tracing::warn!(dlq_id = entry.id, config = %entry.config_name, "config no longer exists, marking permanent");
                db.mark_permanent(entry.id, "config no longer exists")?;
                continue;
            }
        };
        let transformer = match transformers.get(&entry.config_name) {
            Some(t) => t,
            None => {
                db.mark_permanent(entry.id, "transformer no longer exists")?;
                continue;
            }
        };
        let namespace = match namespaces.get(&entry.config_name) {
            Some(ns) => ns,
            None => {
                db.mark_permanent(entry.id, "namespace no longer exists")?;
                continue;
            }
        };

        let doc_id = match &entry.doc_id {
            Some(json) => match serde_json::from_str::<DocumentId>(json) {
                Ok(id) => id,
                Err(e) => {
                    db.mark_permanent(entry.id, &format!("doc_id deserialization failed: {e}"))?;
                    continue;
                }
            },
            None => {
                db.mark_permanent(entry.id, "missing doc_id")?;
                continue;
            }
        };

        let operation = match &entry.operation {
            Some(op) => op,
            None => {
                db.mark_permanent(entry.id, "missing operation (legacy entry)")?;
                continue;
            }
        };

        let replay_op = dlq_to_operation(operation);

        let result: Result<(), CliError> = match operation {
            DlqOperation::Delete => {
                replay_delete(transformer.as_ref(), puff_client, namespace, &doc_id).await
            }
            DlqOperation::Insert | DlqOperation::Update => {
                replay_upsert(
                    &pg_client,
                    config,
                    transformer.as_ref(),
                    puff_client,
                    namespace,
                    &doc_id,
                    replay_op,
                )
                .await
            }
        };

        match result {
            Ok(()) => {
                // On success, remove from the DLQ.
                db.delete_dlq_entry(entry.id)?;
                tracing::info!(dlq_id = entry.id, "DLQ entry replayed successfully");
                succeeded += 1;
                if let Some(m) = metrics {
                    m.dlq_replayed.add(1, &[]);
                }
            }
            Err(e) => {
                if entry.retry_count + 1 >= project_config.dlq_max_retries() {
                    tracing::warn!(dlq_id = entry.id, error = %e, "DLQ max retries exhausted, marking permanent");
                    db.mark_permanent(entry.id, &format!("max retries exhausted: {e}"))?;
                } else {
                    db.increment_retry(entry.id)?;
                }
                if let Some(m) = metrics {
                    m.dlq_replay_failed.add(1, &[]);
                }
            }
        }
    }

    Ok(ReplayResult {
        fetched: entries.len(),
        succeeded,
    })
}

/// Replay a DLQ delete through the transformer so custom delete logic
/// (ID remapping, conditional skips, tombstone upserts, etc.) is honoured.
async fn replay_delete(
    transformer: &dyn Transformer,
    puff_client: &TurbopufferClient,
    namespace: &str,
    doc_id: &DocumentId,
) -> Result<(), CliError> {
    let event = RowEvent {
        relation_id: 0,
        operation: Operation::Delete,
        new_tuple: None,
        old_tuple: None,
    };
    let refs: Vec<(&RowEvent, DocumentId)> = vec![(&event, doc_id.clone())];
    let actions = transformer
        .transform_batch(&refs)
        .await
        .map_err(|e| CliError::Run(format!("DLQ delete transform failed: {e}")))?;
    puff_client
        .send_batch(namespace, &actions)
        .await
        .map_err(|e| CliError::Run(format!("DLQ delete failed: {e}")))?;
    Ok(())
}

/// Re-query Postgres for the current row and upsert to Turbopuffer.
/// If the row no longer exists, send a delete instead.
///
/// `operation` is the original replication operation (Insert or Update)
/// so the transformer sees the correct event type.
async fn replay_upsert(
    pg_client: &pg::Client,
    config: &config::Config,
    transformer: &dyn Transformer,
    puff_client: &TurbopufferClient,
    namespace: &str,
    doc_id: &DocumentId,
    operation: Operation,
) -> Result<(), CliError> {
    // Resolve all columns in attnum (WAL) order so the transformer's
    // column_reindex lines up, while still casting every column to ::text
    // (which `columns: None` / SELECT * does not do — causing panics on
    // non-text types like JSONB, arrays, bytea, etc.).
    let column_names = resolve_column_names(pg_client, &config.source.schema, &config.source.table)
        .await
        .map_err(|e| CliError::Run(format!("DLQ column resolution failed: {e}")))?;

    let query_config = BatchQueryConfig {
        schema: config.source.schema.clone(),
        table: config.source.table.clone(),
        id_column: config.id.column.clone(),
        columns: Some(column_names),
        batch_size: 1,
    };

    let id_cast = match config.id.id_type {
        config::IdType::Uint | config::IdType::Int => "::int8",
        config::IdType::Uuid => "::uuid",
        config::IdType::String => "",
    };

    let row = fetch_row_by_id(pg_client, &query_config, &doc_id.to_string(), id_cast)
        .await
        .map_err(|e| CliError::Run(format!("DLQ re-query failed: {e}")))?;

    match row {
        Some(row) => {
            let mut events = pg_rows_to_events(&[row], &config.id.column, &config.id.id_type)
                .map_err(|e| CliError::Run(format!("DLQ row conversion failed: {e}")))?;
            // pg_rows_to_events always sets Insert — restore the original operation.
            for (event, _) in &mut events {
                event.operation = operation;
            }
            let refs: Vec<_> = events.iter().map(|(ev, id)| (ev, id.clone())).collect();
            let actions = transformer
                .transform_batch(&refs)
                .await
                .map_err(|e| CliError::Run(format!("DLQ transform failed: {e}")))?;
            puff_client
                .send_batch(namespace, &actions)
                .await
                .map_err(|e| CliError::Run(format!("DLQ send failed: {e}")))?;
        }
        None => {
            // Row deleted from Postgres — route through transformer as a delete.
            replay_delete(transformer, puff_client, namespace, doc_id).await?;
        }
    }

    Ok(())
}
