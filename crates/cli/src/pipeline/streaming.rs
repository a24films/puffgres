use std::collections::HashMap;
use std::time::{Duration, Instant};

use chrono::Utc;
use puff::TurbopufferClient;
use serde::{Deserialize, Serialize};
use puffgres_core::{DocumentId, Router, Transformer};
use replication::{RelationCache, ReplicationStream, ReplicationStreamConfig, RowEvent};
use state::{PostgresStateStore, StateStore, StreamingCheckpoint};
use tokio_util::sync::CancellationToken;

use super::dlq::send_events_to_dlq;
use super::{PUBLICATION_NAME, SLOT_NAME, STATUS_INTERVAL};
use crate::env::EnvConfig;
use crate::error::CliError;
use crate::observability::Metrics;
use crate::project_config::ProjectConfig;

/// Returns true if the config should skip this batch because it has already
/// been checkpointed past this LSN.
fn should_skip_config(
    config_name: &str,
    batch_lsn: u64,
    config_checkpoint_lsns: &HashMap<String, u64>,
) -> bool {
    config_checkpoint_lsns
        .get(config_name)
        .is_some_and(|&cp_lsn| batch_lsn <= cp_lsn)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SpoolRoutedEvent {
    doc_id: DocumentId,
    event: RowEvent,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SpoolConfigBatch {
    config_name: String,
    events: Vec<SpoolRoutedEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SpoolPayload {
    config_batches: Vec<SpoolConfigBatch>,
}

fn route_spool_payload(
    events: &[RowEvent],
    relation_cache: &RelationCache,
    router: &Router,
    config_checkpoint_lsns: &HashMap<String, u64>,
    batch_lsn: u64,
) -> SpoolPayload {
    let config_batches = router
        .route_batch(events, relation_cache)
        .into_iter()
        .filter_map(|(config_name, routed_events)| {
            if should_skip_config(config_name, batch_lsn, config_checkpoint_lsns) {
                return None;
            }

            let events = routed_events
                .iter()
                .map(|(event, doc_id)| SpoolRoutedEvent {
                    doc_id: doc_id.clone(),
                    event: (*event).clone(),
                })
                .collect::<Vec<_>>();

            Some(SpoolConfigBatch {
                config_name: config_name.to_string(),
                events,
            })
        })
        .collect();

    SpoolPayload { config_batches }
}

fn checkpoint_config_names(
    applied_configs: &[(std::path::PathBuf, config::Config)],
    config_checkpoint_lsns: &HashMap<String, u64>,
    batch_lsn: u64,
) -> Vec<String> {
    applied_configs
        .iter()
        .map(|(_, config)| config.name.clone())
        .filter(|config_name| !should_skip_config(config_name, batch_lsn, config_checkpoint_lsns))
        .collect()
}

async fn insert_spool_entry(
    db: &PostgresStateStore,
    transaction_id: u64,
    ack_lsn: Option<u64>,
    is_final_chunk: bool,
    checkpoint_configs: &[String],
    payload: &SpoolPayload,
) -> Result<i64, CliError> {
    let checkpoint_configs_json = serde_json::to_string(checkpoint_configs)
        .map_err(|e| CliError::Run(format!("failed to serialize spool checkpoints: {e}")))?;
    let payload_json = serde_json::to_string(payload)
        .map_err(|e| CliError::Run(format!("failed to serialize spool payload: {e}")))?;

    db.insert_spool_entry(
        i64::try_from(transaction_id)
            .map_err(|_| CliError::Run("transaction id exceeded i64 range".to_string()))?,
        ack_lsn,
        is_final_chunk,
        &checkpoint_configs_json,
        &payload_json,
    )
    .await
    .map_err(CliError::from)
}

async fn process_config_events(
    config_name: &str,
    events: &[(&RowEvent, DocumentId)],
    namespace: &str,
    transformer: &dyn Transformer,
    puff_client: &TurbopufferClient,
    db: &impl StateStore,
    metrics: Option<&Metrics>,
    events_processed: &mut HashMap<String, u64>,
    dlq_lsn: u64,
) -> Result<(), CliError> {
    let transform_result = transformer.transform_batch(events).await;

    match transform_result {
        Err(e) => {
            tracing::error!(config = %config_name, error = %e, "transform error, sending to DLQ");
            if let Some(m) = metrics {
                m.cdc_events_failed.add(events.len() as u64, &[]);
            }
            send_events_to_dlq(db, config_name, dlq_lsn, events, &e.to_string(), false)?;
        }
        Ok(actions) => {
            let send_start = std::time::Instant::now();
            match puff_client.send_batch(namespace, &actions).await {
                Err(e) => {
                    tracing::error!(config = %config_name, error = %e, "turbopuffer error, sending to DLQ");
                    if let Some(m) = metrics {
                        m.cdc_events_failed.add(events.len() as u64, &[]);
                        m.turbopuffer_requests.add(1, &[]);
                        m.turbopuffer_latency
                            .record(send_start.elapsed().as_millis() as f64, &[]);
                    }
                    send_events_to_dlq(db, config_name, dlq_lsn, events, &e.to_string(), false)?;
                }
                Ok(()) => {
                    let count = events_processed.entry(config_name.to_string()).or_insert(0);
                    *count += events.len() as u64;

                    if let Some(m) = metrics {
                        m.cdc_events_processed.add(events.len() as u64, &[]);
                        m.turbopuffer_requests.add(1, &[]);
                        m.turbopuffer_latency
                            .record(send_start.elapsed().as_millis() as f64, &[]);
                    }

                    tracing::info!(
                        config = %config_name,
                        namespace = %namespace,
                        events = events.len(),
                        total = *count,
                        "batch sent",
                    );
                }
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn process_spool_entry(
    spool_id: i64,
    payload: &SpoolPayload,
    checkpoint_configs: &[String],
    ack_lsn: Option<u64>,
    transformers: &HashMap<String, Box<dyn Transformer>>,
    namespaces: &HashMap<String, String>,
    puff_client: &TurbopufferClient,
    db: &PostgresStateStore,
    metrics: Option<&Metrics>,
    events_processed: &mut HashMap<String, u64>,
    config_checkpoint_lsns: &mut HashMap<String, u64>,
) -> Result<(), CliError> {
    for config_batch in &payload.config_batches {
        let transformer = transformers
            .get(&config_batch.config_name)
            .ok_or_else(|| {
                CliError::Run(format!(
                    "internal error: no transformer for config '{}'",
                    config_batch.config_name
                ))
            })?;
        let namespace = namespaces
            .get(&config_batch.config_name)
            .ok_or_else(|| {
                CliError::Run(format!(
                    "internal error: no namespace for config '{}'",
                    config_batch.config_name
                ))
            })?;
        let events = config_batch
            .events
            .iter()
            .map(|event| (&event.event, event.doc_id.clone()))
            .collect::<Vec<_>>();

        process_config_events(
            &config_batch.config_name,
            &events,
            namespace,
            transformer.as_ref(),
            puff_client,
            db,
            metrics,
            events_processed,
            ack_lsn.unwrap_or(0),
        )
        .await?;
    }

    if let Some(ack_lsn) = ack_lsn {
        for config_name in checkpoint_configs {
            let checkpoint = StreamingCheckpoint {
                config_name: config_name.clone(),
                lsn: ack_lsn,
                events_processed: *events_processed.get(config_name).unwrap_or(&0),
                updated_at: Utc::now(),
            };
            db.save_streaming_checkpoint(&checkpoint).await?;
            config_checkpoint_lsns.insert(config_name.clone(), ack_lsn);
        }
    }

    db.mark_spool_entry_done(spool_id).await?;
    Ok(())
}

/// Outer loop: reconnects the replication stream on schema changes.
/// When Postgres sends a Relation message with changed columns (e.g. ALTER TABLE),
/// we drop the stream and reconnect so the fresh RelationCache picks up the new schema.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_streaming_loop(
    env_config: &EnvConfig,
    applied_configs: &[(std::path::PathBuf, config::Config)],
    router: &Router,
    namespaces: &HashMap<String, String>,
    transformers: &HashMap<String, Box<dyn Transformer>>,
    puff_client: &TurbopufferClient,
    db: &PostgresStateStore,
    project_config: &ProjectConfig,
    metrics: Option<&Metrics>,
    token: CancellationToken,
    mut start_lsn: Option<u64>,
) -> Result<(), CliError> {
    let mut events_processed: HashMap<String, u64> = HashMap::new();
    let mut config_checkpoint_lsns: HashMap<String, u64> = HashMap::new();
    // Build watched columns map: schema.table → columns referenced by any config.
    // Schema changes that only touch columns outside this set are silently accepted.
    // Tables where ANY config has columns = None get no entry, so all changes are breaking.
    let mut watched_columns: HashMap<String, Vec<String>> = HashMap::new();

    // Tables with at least one columns = None config must watch everything.
    let watch_all: std::collections::HashSet<String> = applied_configs
        .iter()
        .filter(|(_, c)| c.columns.is_none())
        .map(|(_, c)| format!("{}.{}", c.source.schema, c.source.table))
        .collect();

    for (_, config) in applied_configs {
        let checkpoint = db.get_streaming_checkpoint(&config.name).await?;
        let count = checkpoint.as_ref().map(|c| c.events_processed).unwrap_or(0);
        if let Some(ref cp) = checkpoint {
            config_checkpoint_lsns.insert(config.name.clone(), cp.lsn);
        } else if let Some(bp) = db.get_backfill_progress(&config.name).await? {
            // Seed skip state for new configs: they have no streaming checkpoint
            // yet but completed backfill with a watermark LSN. Without this,
            // should_skip_config would never skip them and they'd re-process
            // every historical batch from start_lsn up to their watermark.
            if let Some(wlsn) = bp.watermark_lsn {
                config_checkpoint_lsns.insert(config.name.clone(), wlsn);
            }
        }
        events_processed.insert(config.name.clone(), count);

        let key = format!("{}.{}", config.source.schema, config.source.table);
        if watch_all.contains(&key) {
            continue;
        }
        if let Some(ref cols) = config.columns {
            let entry = watched_columns.entry(key).or_default();
            if !entry.contains(&config.id.column) {
                entry.push(config.id.column.clone());
            }
            for col in cols {
                if !entry.contains(col) {
                    entry.push(col.clone());
                }
            }
        }
    }

    // Auto-clean stale permanent DLQ entries
    let dlq_max_age_hours = env_config
        .dlq_max_age_hours
        .unwrap_or_else(|| project_config.dlq_permanent_max_age_hours());
    let cleaned = db.clear_old_permanent_entries(dlq_max_age_hours).await?;
    if cleaned > 0 {
        tracing::info!(
            entries_removed = cleaned,
            max_age_hours = dlq_max_age_hours,
            "cleaned stale permanent DLQ entries",
        );
    }

    if token.is_cancelled() {
        tracing::info!("shutdown requested, skipping DLQ replay and streaming");
        return Ok(());
    }

    // Build config lookup by name for DLQ replay.
    let configs_by_name: HashMap<String, &config::Config> = applied_configs
        .iter()
        .map(|(_, c)| (c.name.clone(), c))
        .collect();

    // Drain all retryable DLQ entries from previous runs before streaming.
    // Process in batches until empty so a large backlog doesn't span dozens
    // of TLS-reconnect cycles. If transforms keep failing (e.g. a downstream
    // service is briefly down), detect the stall and fall through to streaming
    // rather than spinning forever — the periodic replay during streaming will
    // pick up remaining entries once the service recovers.
    //
    // The stall limit is intentionally small (2) so we don't burn through
    // per-entry retry budgets in a tight loop during a temporary outage.
    {
        let stall_limit: usize = 2;
        let mut stalled_passes: usize = 0;
        loop {
            if token.is_cancelled() {
                tracing::info!("shutdown requested, aborting DLQ drain");
                return Ok(());
            }
            let result = match super::dlq::replay_dlq(
                db,
                &env_config.database_url,
                &configs_by_name,
                transformers,
                namespaces,
                puff_client,
                project_config,
                metrics,
                &token,
            )
            .await
            {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "DLQ drain connection failed, deferring to periodic replay",
                    );
                    break;
                }
            };
            if result.fetched == 0 {
                break;
            }
            if result.succeeded == 0 {
                stalled_passes += 1;
                if stalled_passes >= stall_limit {
                    tracing::warn!(
                        stalled_passes,
                        "DLQ drain stalled with no progress, deferring to periodic replay",
                    );
                    break;
                }
            } else {
                stalled_passes = 0;
            }
        }
    }

    let mut batch_count: u64 = 0;
    let mut last_maintenance = Instant::now();

    // Outer loop: reconnects the replication stream on schema changes.
    loop {
        if token.is_cancelled() {
            tracing::info!("shutdown requested, exiting streaming loop");
            return Ok(());
        }
        let stream_config = ReplicationStreamConfig {
            connection_string: env_config.database_url.clone(),
            slot_name: SLOT_NAME.to_string(),
            publication_name: PUBLICATION_NAME.to_string(),
            start_lsn,
            status_interval: STATUS_INTERVAL,
            max_transaction_events: project_config.max_transaction_events(),
            sub_batch_size: project_config.sub_batch_size(),
            watched_columns: watched_columns.clone(),
        };

        let mut stream = ReplicationStream::connect(stream_config).await?;

        let lsn_display = start_lsn
            .map(|l| pg::PgLsn::from(l).to_string())
            .unwrap_or_else(|| "-".to_string());
        tracing::info!(lsn = %lsn_display, "streaming from LSN");

        tracing::info!("listening for changes");

        // Note: delivery to Turbopuffer is at-least-once. If we crash between
        // send_batch and save_streaming_checkpoint, we'll re-send on restart.
        // This is fine because Turbopuffer upserts are idempotent.
        let should_reconnect;
        loop {
            let batch_result = tokio::select! {
                _ = token.cancelled() => {
                    tracing::info!("shutdown requested, finishing current batch loop");
                    return Ok(());
                }
                result = stream.recv_batch() => match result {
                    Ok(Some(result)) => result,
                    Ok(None) => {
                        tracing::info!("replication stream ended");
                        return Ok(());
                    }
                    Err(e) => {
                        return Err(e.into());
                    }
                }
            };

            let batch = match batch_result {
                replication::BatchResult::SchemaChanged(sc) => {
                    tracing::warn!(
                        relation_id = sc.relation_id,
                        schema = %sc.namespace,
                        table = %sc.name,
                        "schema change detected, reconnecting replication stream",
                    );
                    should_reconnect = true;
                    break;
                }
                replication::BatchResult::SubBatch(sub_batch) => {
                    if sub_batch.events.is_empty() {
                        continue;
                    }
                    let _span = tracing::info_span!(
                        "cdc_sub_batch",
                        txn_id = sub_batch.transaction_id,
                        events = sub_batch.events.len()
                    )
                    .entered();
                    let payload = route_spool_payload(
                        &sub_batch.events,
                        stream.relation_cache(),
                        router,
                        &HashMap::new(),
                        0,
                    );
                    let checkpoint_configs = Vec::new();
                    let spool_id = insert_spool_entry(
                        db,
                        sub_batch.transaction_id,
                        None,
                        false,
                        &checkpoint_configs,
                        &payload,
                    )
                    .await?;
                    process_spool_entry(
                        spool_id,
                        &payload,
                        &checkpoint_configs,
                        None,
                        transformers,
                        namespaces,
                        puff_client,
                        db,
                        metrics,
                        &mut events_processed,
                        &mut config_checkpoint_lsns,
                    )
                    .await?;
                    continue;
                }
                replication::BatchResult::TransactionTooLarge {
                    ack_lsn,
                    event_count,
                } => {
                    tracing::warn!(
                        ack_lsn,
                        event_count,
                        "transaction exceeded max_transaction_events limit, skipping",
                    );
                    for (_, config) in applied_configs {
                        let checkpoint = StreamingCheckpoint {
                            config_name: config.name.clone(),
                            lsn: ack_lsn,
                            events_processed: *events_processed.get(&config.name).unwrap_or(&0),
                            updated_at: Utc::now(),
                        };
                        db.save_streaming_checkpoint(&checkpoint).await?;
                    }
                    stream.ack();

                    // Still count toward DLQ replay cadence so oversized-only
                    // runs don't starve retryable DLQ entries.
                    batch_count += 1;
                    if batch_count.is_multiple_of(project_config.dlq_replay_interval()) {
                        if let Err(e) = super::dlq::replay_dlq(
                            db,
                            &env_config.database_url,
                            &configs_by_name,
                            transformers,
                            namespaces,
                            puff_client,
                            project_config,
                            metrics,
                            &token,
                        )
                        .await
                        {
                            tracing::warn!(error = %e, "DLQ replay failed, deferring to next interval");
                        }
                    }
                    continue;
                }
                replication::BatchResult::Batch(batch) => batch,
            };

            let _batch_span = tracing::info_span!(
                "cdc_batch",
                lsn = batch.ack_lsn,
                txn_id = batch.transaction_id,
                events = batch.events.len()
            )
            .entered();
            let batch_start = std::time::Instant::now();

            let payload = route_spool_payload(
                &batch.events,
                stream.relation_cache(),
                router,
                &config_checkpoint_lsns,
                batch.ack_lsn,
            );
            let checkpoint_configs =
                checkpoint_config_names(applied_configs, &config_checkpoint_lsns, batch.ack_lsn);
            let spool_id = insert_spool_entry(
                db,
                batch.transaction_id,
                Some(batch.ack_lsn),
                true,
                &checkpoint_configs,
                &payload,
            )
            .await?;
            stream.ack();
            if let Some(m) = metrics {
                m.replication_acks.add(1, &[]);
                // Replication lag: PG commit timestamp is microseconds since
                // 2000-01-01. Convert to Unix epoch and compare to wall clock.
                const PG_EPOCH_OFFSET_MICROS: i64 = 946_684_800_000_000;
                let commit_unix_micros = batch.commit_time_micros + PG_EPOCH_OFFSET_MICROS;
                let now_micros = Utc::now().timestamp_micros();
                let lag_ms = (now_micros - commit_unix_micros) as f64 / 1000.0;
                m.replication_lag_ms.record(lag_ms, &[]);
            }

            process_spool_entry(
                spool_id,
                &payload,
                &checkpoint_configs,
                Some(batch.ack_lsn),
                transformers,
                namespaces,
                puff_client,
                db,
                metrics,
                &mut events_processed,
                &mut config_checkpoint_lsns,
            )
            .await?;

            if let Some(m) = metrics {
                m.cdc_batch_duration
                    .record(batch_start.elapsed().as_millis() as f64, &[]);
            }

            batch_count += 1;
            if batch_count.is_multiple_of(project_config.dlq_replay_interval()) {
                if let Err(e) = super::dlq::replay_dlq(
                    db,
                    &env_config.database_url,
                    &configs_by_name,
                    transformers,
                    namespaces,
                    puff_client,
                    project_config,
                    metrics,
                    &token,
                )
                .await
                {
                    tracing::warn!(error = %e, "DLQ replay failed, deferring to next interval");
                }
            }

            // Periodic maintenance: clean stale DLQ entries and reclaim disk space.
            let maintenance_interval =
                Duration::from_secs(project_config.maintenance_interval_secs());
            if last_maintenance.elapsed() >= maintenance_interval {
                match db.run_maintenance(dlq_max_age_hours) {
                    Ok(cleaned) if cleaned > 0 => {
                        tracing::info!(
                            entries_removed = cleaned,
                            "maintenance: cleaned stale permanent DLQ entries",
                        );
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "maintenance task failed");
                    }
                    _ => {}
                }
                last_maintenance = Instant::now();
            }
        }

        if !should_reconnect {
            break;
        }

        // Update start_lsn from latest checkpoints before reconnecting
        drop(stream);

        // Wait for the replication slot to be released before reconnecting.
        {
            let slot_client = pg::connect::connect(&env_config.database_url).await?;
            super::setup::terminate_slot_and_wait(&slot_client, token.clone()).await?;
        }

        let mut checkpoint_lsns = Vec::new();
        for (_, config) in applied_configs {
            if let Some(cp) = db.get_streaming_checkpoint(&config.name).await? {
                checkpoint_lsns.push(cp.lsn);
            }
        }
        start_lsn = checkpoint_lsns.into_iter().min();
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use config::Config;
    use puffgres_core::{ColumnValue, Operation, RelationInfo, TupleData};
    use replication::ReplicaIdentity;
    use std::sync::Arc;

    fn load_fixture(name: &str) -> Config {
        let path = format!(
            "{}/../core/tests/fixtures/{name}.toml",
            env!("CARGO_MANIFEST_DIR")
        );
        toml::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
    }

    fn users_relation() -> RelationInfo {
        RelationInfo {
            id: 16384,
            namespace: "public".to_string(),
            name: "users".to_string(),
            replica_identity: ReplicaIdentity::Default,
            columns: vec![replication::ColumnInfo {
                part_of_key: true,
                name: "id".to_string(),
                type_oid: 23,
                type_modifier: -1,
            }],
        }
    }

    fn insert_event(id: &'static [u8]) -> RowEvent {
        RowEvent {
            relation_id: 16384,
            operation: Operation::Insert,
            new_tuple: Some(Arc::new(TupleData {
                columns: vec![ColumnValue::Text(Bytes::from_static(id))],
            })),
            old_tuple: None,
        }
    }

    #[test]
    fn should_skip_when_batch_lsn_below_checkpoint() {
        let mut checkpoints = HashMap::new();
        checkpoints.insert("config_a".to_string(), 1000);

        assert!(should_skip_config("config_a", 500, &checkpoints));
    }

    #[test]
    fn should_skip_when_batch_lsn_equals_checkpoint() {
        let mut checkpoints = HashMap::new();
        checkpoints.insert("config_a".to_string(), 1000);

        assert!(should_skip_config("config_a", 1000, &checkpoints));
    }

    #[test]
    fn should_not_skip_when_batch_lsn_above_checkpoint() {
        let mut checkpoints = HashMap::new();
        checkpoints.insert("config_a".to_string(), 1000);

        assert!(!should_skip_config("config_a", 1001, &checkpoints));
    }

    #[test]
    fn should_not_skip_when_config_has_no_checkpoint() {
        let checkpoints = HashMap::new();

        assert!(!should_skip_config("new_config", 500, &checkpoints));
    }

    #[test]
    fn independent_configs_skip_independently() {
        let mut checkpoints = HashMap::new();
        checkpoints.insert("old_config".to_string(), 5000);

        let batch_lsn = 3000;
        assert!(should_skip_config("old_config", batch_lsn, &checkpoints));
        assert!(!should_skip_config("new_config", batch_lsn, &checkpoints));
    }

    #[test]
    fn route_spool_payload_skips_checkpointed_configs() {
        let router = Router::new(vec![puffgres_core::Mapping::from_config(&load_fixture("valid"))]);
        let mut relation_cache = RelationCache::new();
        relation_cache.insert(users_relation());
        let events = vec![insert_event(b"1"), insert_event(b"2")];
        let checkpoints = HashMap::from([("users".to_string(), 1000)]);

        let payload = route_spool_payload(&events, &relation_cache, &router, &checkpoints, 1000);

        assert!(payload.config_batches.is_empty());
    }

    #[test]
    fn checkpoint_config_names_excludes_skipped_configs() {
        let applied_configs = vec![
            (std::path::PathBuf::from("a.toml"), load_fixture("valid")),
            (
                std::path::PathBuf::from("b.toml"),
                Config {
                    name: "other".to_string(),
                    ..load_fixture("valid")
                },
            ),
        ];
        let checkpoints = HashMap::from([("users".to_string(), 1000)]);

        let checkpoint_configs = checkpoint_config_names(&applied_configs, &checkpoints, 1000);

        assert_eq!(checkpoint_configs, vec!["other".to_string()]);
    }
}
