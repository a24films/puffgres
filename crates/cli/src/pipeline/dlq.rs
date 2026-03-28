use std::collections::HashMap;

use puff::TurbopufferClient;
use puffgres_core::{DocumentId, Transformer};
use replication::RowEvent;
use state::{DlqEntry, StateDb};
use tokio_util::sync::CancellationToken;

use crate::error::CliError;
use crate::observability::Metrics;
use crate::project_config::ProjectConfig;

/// Serialize routed events and insert them into the DLQ.
/// `permanent` = true for errors that will never succeed on retry,
/// false for errors that should be retried (including transform errors,
/// which are marked permanent after `dlq_max_retries` during DLQ replay).
pub(crate) fn send_events_to_dlq(
    db: &StateDb,
    config_name: &str,
    lsn: u64,
    events: &[(&RowEvent, DocumentId)],
    error: &str,
    permanent: bool,
) -> Result<(), CliError> {
    for (event, doc_id) in events {
        let event_json = serde_json::to_string(event)
            .map_err(|e| CliError::Run(format!("failed to serialize event for DLQ: {e}")))?;
        let doc_id_json = serde_json::to_string(doc_id)
            .map_err(|e| CliError::Run(format!("failed to serialize doc_id for DLQ: {e}")))?;
        let entry = if permanent {
            DlqEntry::permanent(config_name, lsn, event_json, Some(doc_id_json), error)
        } else {
            DlqEntry::retryable(config_name, lsn, event_json, Some(doc_id_json), error)
        };
        db.insert_dlq_entry(&entry)?;
    }
    Ok(())
}

/// Fetch retryable DLQ entries and attempt to re-transform + re-send them.
/// On success: delete the entry. On failure: increment retry count, mark permanent
/// if max retries exhausted.
#[tracing::instrument(name = "replay_dlq", skip_all)]
pub(crate) async fn replay_dlq(
    db: &StateDb,
    transformers: &HashMap<String, Box<dyn Transformer>>,
    namespaces: &HashMap<String, String>,
    puff_client: &TurbopufferClient,
    project_config: &ProjectConfig,
    metrics: Option<&Metrics>,
    token: &CancellationToken,
) -> Result<(), CliError> {
    let entries = db.list_retryable_entries(project_config.dlq_replay_batch_size())?;
    if entries.is_empty() {
        return Ok(());
    }

    tracing::info!(entries = entries.len(), "replaying DLQ entries");

    for entry in &entries {
        if token.is_cancelled() {
            tracing::info!("shutdown requested, aborting DLQ replay");
            return Ok(());
        }

        let transformer = match transformers.get(&entry.config_name) {
            Some(t) => t,
            None => {
                tracing::warn!(dlq_id = entry.id, config = %entry.config_name, "config no longer exists, marking permanent");
                db.mark_permanent(entry.id, "config no longer exists")?;
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

        let event: RowEvent = match serde_json::from_str(&entry.event_json) {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!(dlq_id = entry.id, error = %e, "failed to deserialize DLQ event, marking permanent");
                db.mark_permanent(entry.id, &format!("deserialization failed: {e}"))?;
                continue;
            }
        };

        let doc_id = match &entry.doc_id {
            Some(json) => match serde_json::from_str::<DocumentId>(json) {
                Ok(id) => id,
                Err(e) => {
                    eprintln!(
                        "  DLQ entry {}: failed to deserialize doc_id, marking permanent: {e}",
                        entry.id
                    );
                    db.mark_permanent(entry.id, &format!("doc_id deserialization failed: {e}"))?;
                    continue;
                }
            },
            // Legacy entries written before doc_id was stored.
            None => {
                eprintln!(
                    "  DLQ entry {}: missing doc_id (legacy entry), marking permanent",
                    entry.id
                );
                db.mark_permanent(entry.id, "missing doc_id (legacy entry)")?;
                continue;
            }
        };
        let transform_result = transformer.transform_batch(&[(&event, doc_id)]).await;

        match transform_result {
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
            Ok(actions) => match puff_client.send_batch(namespace, &actions).await {
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
                Ok(()) => {
                    db.delete_dlq_entry(entry.id)?;
                    tracing::info!(dlq_id = entry.id, "DLQ entry replayed successfully");
                    if let Some(m) = metrics {
                        m.dlq_replayed.add(1, &[]);
                    }
                }
            },
        }
    }

    Ok(())
}
