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
/// `permanent` = true for transform errors (bad data won't fix itself on retry),
/// false for sink errors (transient network/server failures).
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

/// Deserialized DLQ entry ready for batched replay.
struct PreparedEntry<'a> {
    entry: &'a DlqEntry,
    event: RowEvent,
    doc_id: DocumentId,
}

/// Fetch retryable DLQ entries and attempt to re-transform + re-send them,
/// batched by config to minimize round-trips to the transformer and sink.
/// On success: delete the entries. On failure: increment retry count, mark
/// permanent if max retries exhausted.
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

    // Group entries by config, filtering out permanently-broken ones.
    let mut by_config: HashMap<&str, Vec<PreparedEntry<'_>>> = HashMap::new();

    for entry in &entries {
        if !transformers.contains_key(&entry.config_name) {
            tracing::warn!(dlq_id = entry.id, config = %entry.config_name, "config no longer exists, marking permanent");
            db.mark_permanent(entry.id, "config no longer exists")?;
            continue;
        }
        if !namespaces.contains_key(&entry.config_name) {
            db.mark_permanent(entry.id, "namespace no longer exists")?;
            continue;
        }

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
                    tracing::warn!(dlq_id = entry.id, error = %e, "failed to deserialize DLQ doc_id, marking permanent");
                    db.mark_permanent(entry.id, &format!("doc_id deserialization failed: {e}"))?;
                    continue;
                }
            },
            None => {
                tracing::warn!(
                    dlq_id = entry.id,
                    "missing doc_id (legacy entry), marking permanent"
                );
                db.mark_permanent(entry.id, "missing doc_id (legacy entry)")?;
                continue;
            }
        };

        by_config
            .entry(&entry.config_name)
            .or_default()
            .push(PreparedEntry {
                entry,
                event,
                doc_id,
            });
    }

    for (config_name, prepared) in &by_config {
        if token.is_cancelled() {
            tracing::info!("shutdown requested, aborting DLQ replay");
            return Ok(());
        }

        let transformer = &transformers[*config_name];
        let namespace = &namespaces[*config_name];

        let batch: Vec<(&RowEvent, DocumentId)> = prepared
            .iter()
            .map(|p| (&p.event, p.doc_id.clone()))
            .collect();

        let transform_result = transformer.transform_batch(&batch).await;

        match transform_result {
            Err(e) => {
                tracing::warn!(config = %config_name, error = %e, entries = prepared.len(), "DLQ batch transform failed");
                for p in prepared {
                    if p.entry.retry_count + 1 >= project_config.dlq_max_retries() {
                        tracing::warn!(
                            dlq_id = p.entry.id,
                            "DLQ max retries exhausted, marking permanent"
                        );
                        db.mark_permanent(p.entry.id, &format!("max retries exhausted: {e}"))?;
                    } else {
                        db.increment_retry(p.entry.id)?;
                    }
                }
                if let Some(m) = metrics {
                    m.dlq_replay_failed.add(prepared.len() as u64, &[]);
                }
            }
            Ok(actions) => match puff_client.send_batch(namespace, &actions).await {
                Err(e) => {
                    tracing::warn!(config = %config_name, error = %e, entries = prepared.len(), "DLQ batch send failed");
                    for p in prepared {
                        if p.entry.retry_count + 1 >= project_config.dlq_max_retries() {
                            tracing::warn!(
                                dlq_id = p.entry.id,
                                "DLQ max retries exhausted, marking permanent"
                            );
                            db.mark_permanent(p.entry.id, &format!("max retries exhausted: {e}"))?;
                        } else {
                            db.increment_retry(p.entry.id)?;
                        }
                    }
                    if let Some(m) = metrics {
                        m.dlq_replay_failed.add(prepared.len() as u64, &[]);
                    }
                }
                Ok(()) => {
                    for p in prepared {
                        db.delete_dlq_entry(p.entry.id)?;
                    }
                    tracing::info!(
                        config = %config_name,
                        entries = prepared.len(),
                        "DLQ entries replayed successfully"
                    );
                    if let Some(m) = metrics {
                        m.dlq_replayed.add(prepared.len() as u64, &[]);
                    }
                }
            },
        }
    }

    Ok(())
}
