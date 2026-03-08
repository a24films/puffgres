use std::collections::HashMap;

use chrono::Utc;
use puff::TurbopufferClient;
use puffgres_core::{Backoff, BackoffConfig, Router, Transformer};
use replication::{ReplicationStream, ReplicationStreamConfig};
use state::{StateDb, StreamingCheckpoint};
use tokio_util::sync::CancellationToken;

use super::dlq::send_events_to_dlq;
use super::{PUBLICATION_NAME, SLOT_NAME, STATUS_INTERVAL};
use crate::env::EnvConfig;
use crate::error::CliError;
use crate::observability::Metrics;
use crate::project_config::ProjectConfig;

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
    db: &StateDb,
    project_config: &ProjectConfig,
    metrics: Option<&Metrics>,
    token: CancellationToken,
    mut start_lsn: Option<u64>,
) -> Result<(), CliError> {
    let mut events_processed: HashMap<String, u64> = HashMap::new();
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
        let count = db
            .get_streaming_checkpoint(&config.name)?
            .map(|c| c.events_processed)
            .unwrap_or(0);
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
    let cleaned = db.clear_old_permanent_entries(dlq_max_age_hours)?;
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

    // Replay any retryable DLQ entries from previous runs
    super::dlq::replay_dlq(
        db,
        transformers,
        namespaces,
        puff_client,
        project_config,
        metrics,
        &token,
    )
    .await?;

    let mut batch_count: u64 = 0;

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
                replication::BatchResult::TransactionTooLarge {
                    ack_lsn,
                    event_count,
                } => {
                    tracing::warn!(
                        ack_lsn,
                        event_count,
                        "transaction exceeded max_transaction_events limit, skipping",
                    );
                    // Ack to advance past the oversized transaction
                    stream.ack();

                    // Still count toward DLQ replay cadence so oversized-only
                    // runs don't starve retryable DLQ entries.
                    batch_count += 1;
                    if batch_count.is_multiple_of(project_config.dlq_replay_interval()) {
                        super::dlq::replay_dlq(
                            db,
                            transformers,
                            namespaces,
                            puff_client,
                            project_config,
                            metrics,
                            &token,
                        )
                        .await?;
                    }
                    continue;
                }
                replication::BatchResult::Batch(batch) => batch,
            };

            if batch.events.is_empty() {
                stream.ack();
                continue;
            }

            let _batch_span = tracing::info_span!(
                "cdc_batch",
                lsn = batch.ack_lsn,
                events = batch.events.len()
            )
            .entered();
            let batch_start = std::time::Instant::now();
            let config_events = router.route_batch(&batch.events, stream.relation_cache());

            for (config_name, events) in &config_events {
                let transformer = transformers
                    .get(*config_name)
                    .expect("transformer missing for applied config");
                let namespace = namespaces
                    .get(*config_name)
                    .expect("namespace missing for applied config");

                let transform_result = transformer.transform_batch(events.as_slice()).await;

                match transform_result {
                    Err(e) => {
                        tracing::error!(config = %config_name, error = %e, "transform error, sending to DLQ");
                        if let Some(m) = metrics {
                            m.cdc_events_failed.add(events.len() as u64, &[]);
                        }
                        send_events_to_dlq(
                            db,
                            config_name,
                            batch.ack_lsn,
                            events,
                            &e.to_string(),
                            false,
                        )?;
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
                                send_events_to_dlq(
                                    db,
                                    config_name,
                                    batch.ack_lsn,
                                    events,
                                    &e.to_string(),
                                    false,
                                )?;
                            }
                            Ok(()) => {
                                let count =
                                    events_processed.entry(config_name.to_string()).or_insert(0);
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
            }

            for (_, config) in applied_configs {
                let checkpoint = StreamingCheckpoint {
                    config_name: config.name.clone(),
                    lsn: batch.ack_lsn,
                    events_processed: *events_processed.get(&config.name).unwrap_or(&0),
                    updated_at: Utc::now(),
                };
                db.save_streaming_checkpoint(&checkpoint)?;
            }

            // Ack unconditionally -- failed events are in the DLQ for retry
            stream.ack();
            if let Some(m) = metrics {
                m.replication_acks.add(1, &[]);
                m.cdc_batch_duration
                    .record(batch_start.elapsed().as_millis() as f64, &[]);
            }

            batch_count += 1;
            if batch_count.is_multiple_of(project_config.dlq_replay_interval()) {
                super::dlq::replay_dlq(
                    db,
                    transformers,
                    namespaces,
                    puff_client,
                    project_config,
                    metrics,
                    &token,
                )
                .await?;
            }
        }

        if !should_reconnect {
            break;
        }

        // Update start_lsn from latest checkpoints before reconnecting
        drop(stream);

        // Wait for the replication slot to be released before reconnecting.
        // After dropping the stream, Postgres may still consider the previous
        // backend active for a short window. Without this backoff the new
        // connect() races against slot release and can fail, taking down CDC
        // until a manual restart.
        {
            let slot_client = pg::connect::connect(&env_config.database_url).await?;

            pg::slot::terminate_active_slot_backend(&slot_client, SLOT_NAME).await?;

            let mut backoff = Backoff::new(BackoffConfig {
                initial_delay_ms: 100,
                max_delay_ms: 5_000,
                max_retries: 10,
                multiplier: 2.0,
                jitter: true,
            });
            while pg::slot::get_active_pid(&slot_client, SLOT_NAME)
                .await?
                .is_some()
            {
                match backoff.next_delay() {
                    Some(delay) => {
                        tokio::select! {
                            _ = tokio::time::sleep(delay) => {}
                            _ = token.cancelled() => {
                                tracing::info!("shutdown requested during schema-change slot wait");
                                return Ok(());
                            }
                        }
                        pg::slot::terminate_active_slot_backend(&slot_client, SLOT_NAME).await?;
                    }
                    None => {
                        return Err(CliError::Run(format!(
                            "timed out waiting for replication slot '{}' to be released after schema change",
                            SLOT_NAME
                        )));
                    }
                }
            }
        }

        let mut checkpoint_lsns = Vec::new();
        for (_, config) in applied_configs {
            if let Some(cp) = db.get_streaming_checkpoint(&config.name)? {
                checkpoint_lsns.push(cp.lsn);
            }
        }
        start_lsn = checkpoint_lsns.into_iter().min();
    }

    Ok(())
}
