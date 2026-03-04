use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::time::Duration;

use chrono::Utc;
use config::ConfigLoader;
use pg::batch::BatchQueryConfig;
use puff::TurbopufferClient;
use puffgres_core::{
    BackfillConfig, BackfillOutcome, Backoff, BackoffConfig, DocumentId, JsTransformer, Mapping,
    Router, Transformer, run_backfill,
};
use replication::{ReplicationError, ReplicationStream, ReplicationStreamConfig, RowEvent};
use sha2::{Digest, Sha256};
use state::{BackfillProgress, BackfillStatus, DlqEntry, StateDb, StreamingCheckpoint};

use crate::env::EnvConfig;
use crate::error::CliError;
use crate::observability::Metrics;
use crate::paths::ProjectPaths;
use crate::project_config::ProjectConfig;

const SLOT_NAME: &str = "puffgres";
const PUBLICATION_NAME: &str = "puffgres";
const STATUS_INTERVAL: Duration = Duration::from_secs(10);

pub fn run(
    paths: &ProjectPaths,
    env_config: &EnvConfig,
    project_config: &ProjectConfig,
    metrics: Option<&Metrics>,
) -> Result<(), CliError> {
    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| CliError::Run(format!("failed to create async runtime: {e}")))?;
    rt.block_on(run_async(paths, env_config, project_config, metrics))
}

fn prefixed_namespace(prefix: &Option<String>, namespace: &str) -> String {
    match prefix {
        Some(p) if !p.is_empty() => format!("{}_{}", p, namespace),
        _ => namespace.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[tokio::test]
    async fn retry_loop_succeeds_after_transient_failures() {
        let attempts = AtomicU32::new(0);
        let result = retry_loop(
            BackoffConfig {
                initial_delay_ms: 1,
                max_delay_ms: 1,
                max_retries: 5,
                multiplier: 1.0,
                jitter: false,
            },
            || {
                let n = attempts.fetch_add(1, Ordering::SeqCst);
                async move {
                    if n < 2 {
                        Err(CliError::Run("transient".into()))
                    } else {
                        Ok(())
                    }
                }
            },
        )
        .await;
        assert!(result.is_ok());
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn retry_loop_gives_up_after_max_retries() {
        let attempts = AtomicU32::new(0);
        let result = retry_loop(
            BackoffConfig {
                initial_delay_ms: 1,
                max_delay_ms: 1,
                max_retries: 3,
                multiplier: 1.0,
                jitter: false,
            },
            || {
                attempts.fetch_add(1, Ordering::SeqCst);
                async { Err(CliError::Run("permanent".into())) }
            },
        )
        .await;
        assert!(result.is_err());
        // 1 initial attempt + 3 retries = 4 total
        assert_eq!(attempts.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn retry_loop_skips_non_retryable_errors() {
        let attempts = AtomicU32::new(0);
        let result = retry_loop(
            BackoffConfig {
                initial_delay_ms: 1,
                max_delay_ms: 1,
                max_retries: 5,
                multiplier: 1.0,
                jitter: false,
            },
            || {
                attempts.fetch_add(1, Ordering::SeqCst);
                async { Err(CliError::RunValidation("config drift".into())) }
            },
        )
        .await;
        assert!(result.is_err());
        // Should exit after the first attempt with no retries
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn retry_loop_returns_immediately_on_success() {
        let attempts = AtomicU32::new(0);
        let result = retry_loop(
            BackoffConfig {
                initial_delay_ms: 1,
                max_delay_ms: 1,
                max_retries: 5,
                multiplier: 1.0,
                jitter: false,
            },
            || {
                attempts.fetch_add(1, Ordering::SeqCst);
                async { Ok(()) }
            },
        )
        .await;
        assert!(result.is_ok());
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn prefixed_namespace_with_prefix() {
        let prefix = Some("production".to_string());
        assert_eq!(prefixed_namespace(&prefix, "user"), "production_user");
    }

    #[test]
    fn prefixed_namespace_without_prefix() {
        assert_eq!(prefixed_namespace(&None, "user"), "user");
    }

    #[test]
    fn prefixed_namespace_empty_prefix_treated_as_none() {
        let prefix = Some("".to_string());
        assert_eq!(prefixed_namespace(&prefix, "user"), "user");
    }

    use crate::test_utils::{PASSTHROUGH_TRANSFORM, setup_project, write_config, write_transform};
    use state::ConfigRecord;

    fn dummy_env(state_db_path: PathBuf) -> EnvConfig {
        EnvConfig {
            database_url: "host=invalid".to_string(),
            turbopuffer_api_key: "fake".to_string(),
            turbopuffer_region: None,
            turbopuffer_namespace_prefix: None,
            otel_endpoint: None,
            otel_headers: None,
            state_db_path,
            dlq_max_age_hours: None,
        }
    }

    /// Sync wrapper for run_cdc_inner (bypasses retry loop).
    fn run_no_retry(
        paths: &ProjectPaths,
        env_config: &EnvConfig,
        project_config: &ProjectConfig,
        metrics: Option<&Metrics>,
    ) -> Result<(), CliError> {
        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| CliError::Run(format!("failed to create async runtime: {e}")))?;
        rt.block_on(run_cdc_inner(paths, env_config, project_config, metrics))
    }

    #[test]
    fn test_errors_on_unreadable_transform_for_applied_config() {
        let (_dir, paths, state_db_path) = setup_project();
        let user_dir = write_config(&paths, "user", "public", "users", "id", "uint");
        write_transform(&user_dir, PASSTHROUGH_TRANSFORM);

        let loader = ConfigLoader::new(&paths.configs);
        let (config_path, cfg) = &loader.load_all().unwrap()[0];
        let transform_bytes = fs::read(config_path.parent().unwrap().join("transform.ts")).unwrap();
        let transform_hash = format!("{:x}", Sha256::digest(&transform_bytes));
        let mut db = StateDb::open(&state_db_path).unwrap();
        db.insert_config(&ConfigRecord {
            name: cfg.name.clone(),

            namespace: cfg.namespace.clone(),
            content_hash: cfg.content_hash().unwrap(),
            transform_hash: Some(transform_hash),
            applied_at: Utc::now(),
            tombstone_applied_at: None,
            namespace_prefix: None,
        })
        .unwrap();

        // Delete the transform file so it can't be read
        fs::remove_file(config_path.parent().unwrap().join("transform.ts")).unwrap();

        let err = run_no_retry(
            &paths,
            &dummy_env(state_db_path),
            &ProjectConfig::default(),
            None,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("cannot read transform"),
            "expected unreadable transform error, got: {err}"
        );
    }

    #[test]
    fn test_errors_on_modified_content_hash_for_applied_config() {
        let (_dir, paths, state_db_path) = setup_project();
        let user_dir = write_config(&paths, "user", "public", "users", "id", "uint");
        write_transform(&user_dir, PASSTHROUGH_TRANSFORM);

        let loader = ConfigLoader::new(&paths.configs);
        let (config_path, cfg) = &loader.load_all().unwrap()[0];
        let transform_bytes = fs::read(config_path.parent().unwrap().join("transform.ts")).unwrap();
        let transform_hash = format!("{:x}", Sha256::digest(&transform_bytes));
        let mut db = StateDb::open(&state_db_path).unwrap();
        db.insert_config(&ConfigRecord {
            name: cfg.name.clone(),
            namespace: cfg.namespace.clone(),
            content_hash: "stale_content_hash".to_string(),
            transform_hash: Some(transform_hash),
            applied_at: Utc::now(),
            tombstone_applied_at: None,
            namespace_prefix: None,
        })
        .unwrap();

        let err = run_no_retry(
            &paths,
            &dummy_env(state_db_path),
            &ProjectConfig::default(),
            None,
        )
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("has been modified since last apply"),
            "expected modified content hash error, got: {err}"
        );
        assert!(
            err.to_string().contains("content hash"),
            "expected content hash details in error, got: {err}"
        );
    }

    #[test]
    fn test_errors_on_modified_transform_for_applied_config() {
        let (_dir, paths, state_db_path) = setup_project();
        let user_dir = write_config(&paths, "user", "public", "users", "id", "uint");
        write_transform(&user_dir, PASSTHROUGH_TRANSFORM);

        let loader = ConfigLoader::new(&paths.configs);
        let cfg = &loader.load_all().unwrap()[0].1;
        let mut db = StateDb::open(&state_db_path).unwrap();
        db.insert_config(&ConfigRecord {
            name: cfg.name.clone(),

            namespace: cfg.namespace.clone(),
            content_hash: cfg.content_hash().unwrap(),
            transform_hash: Some("stale_hash".to_string()),
            applied_at: Utc::now(),
            tombstone_applied_at: None,
            namespace_prefix: None,
        })
        .unwrap();

        let err = run_no_retry(
            &paths,
            &dummy_env(state_db_path),
            &ProjectConfig::default(),
            None,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("was modified"),
            "expected modified transform error, got: {err}"
        );
    }
}

/// Run `f` in a loop with exponential backoff on failure.
///
/// Non-retryable errors (e.g. config validation failures) bypass the loop and
/// propagate immediately so the process fails fast for operator action.
async fn retry_loop<F, Fut>(config: BackoffConfig, mut f: F) -> Result<(), CliError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<(), CliError>>,
{
    let mut backoff = Backoff::new(config);
    loop {
        match f().await {
            Ok(()) => break Ok(()),
            Err(e) if !e.is_retryable() => {
                tracing::error!(error = %e, "non-recoverable error, exiting");
                break Err(e);
            }
            Err(e) => {
                tracing::error!(error = %e, "CDC loop exited with error, restarting...");
                if let Some(delay) = backoff.next_delay() {
                    tracing::info!(
                        delay_ms = delay.as_millis() as u64,
                        "waiting before restart"
                    );
                    tokio::time::sleep(delay).await;
                } else {
                    break Err(e);
                }
            }
        }
    }
}

pub async fn run_async(
    paths: &ProjectPaths,
    env_config: &EnvConfig,
    project_config: &ProjectConfig,
    metrics: Option<&Metrics>,
) -> Result<(), CliError> {
    retry_loop(
        BackoffConfig {
            initial_delay_ms: 1_000,
            max_delay_ms: 60_000,
            max_retries: u32::MAX,
            multiplier: 2.0,
            jitter: true,
        },
        || run_cdc_inner(paths, env_config, project_config, metrics),
    )
    .await
}

#[tracing::instrument(name = "run", skip_all)]
async fn run_cdc_inner(
    paths: &ProjectPaths,
    env_config: &EnvConfig,
    project_config: &ProjectConfig,
    metrics: Option<&Metrics>,
) -> Result<(), CliError> {
    let mut db = StateDb::open(&env_config.state_db_path)?;

    let loader = ConfigLoader::new(&paths.configs);
    let all_configs = loader.load_all()?;

    let applied_configs: Vec<_> = all_configs
        .into_iter()
        .filter(|(_, config)| {
            db.get_config(&config.name)
                .ok()
                .flatten()
                .is_some_and(|r| r.tombstone_applied_at.is_none())
        })
        .collect();

    if applied_configs.is_empty() {
        tracing::warn!("no applied configs — run `puffgres apply` first");
        return Ok(());
    }

    // Re-key: detect namespace prefix changes and reset state if needed
    let current_prefix = env_config.turbopuffer_namespace_prefix.clone();
    for (_, config) in &applied_configs {
        let stored_prefix = db.get_namespace_prefix(&config.name)?;
        match &stored_prefix {
            None => {
                // First run or legacy config — just store the current prefix
                let prefix_to_store = current_prefix.as_deref().unwrap_or("");
                db.set_namespace_prefix(&config.name, Some(prefix_to_store))?;
            }
            Some(stored) => {
                let effective_current = current_prefix.as_deref().unwrap_or("");
                if stored != effective_current {
                    tracing::warn!(
                        config = %config.name,
                        old_prefix = %stored,
                        new_prefix = %effective_current,
                        "namespace prefix changed — resetting state, backfill will re-run",
                    );
                    db.delete_streaming_checkpoint(&config.name)?;
                    db.clear_dlq(Some(&config.name))?;
                    db.save_backfill_progress(&BackfillProgress {
                        config_name: config.name.clone(),
                        last_id: None,
                        total_rows: None,
                        processed_rows: 0,
                        status: BackfillStatus::Pending,
                        started_at: None,
                        completed_at: None,
                        error_message: None,
                        watermark_lsn: None,
                    })?;
                    db.set_namespace_prefix(&config.name, Some(effective_current))?;
                }
            }
        }
    }

    let mut mappings = Vec::new();
    let mut transformers: HashMap<String, Box<dyn Transformer>> = HashMap::new();
    let mut namespaces: HashMap<String, String> = HashMap::new();
    let mut tables: BTreeSet<String> = BTreeSet::new();

    for (config_path, config) in &applied_configs {
        mappings.push(Mapping::from_config(config));
        namespaces.insert(
            config.name.clone(),
            prefixed_namespace(&env_config.turbopuffer_namespace_prefix, &config.namespace),
        );
        tables.insert(format!("{}.{}", config.source.schema, config.source.table));

        let transform_path = config_path.parent().unwrap().join("transform.ts");

        // Verify transform file can be read and hasn't drifted from the applied hash
        let transform_content = fs::read(&transform_path).map_err(|e| {
            CliError::RunValidation(format!(
                "cannot read transform file for config '{}': {e}",
                config.name,
            ))
        })?;

        if let Some(record) = db.get_config(&config.name)? {
            let current_content_hash = config.content_hash().map_err(|e| {
                CliError::RunValidation(format!(
                    "failed to compute content hash for config '{}': {e}",
                    config.name,
                ))
            })?;
            if record.content_hash != current_content_hash {
                return Err(CliError::RunValidation(format!(
                    "config '{}' has been modified since last apply.\n  content hash: expected {}, got {}\n  Run 'puffgres apply' to apply the changes.",
                    config.name, record.content_hash, current_content_hash,
                )));
            }

            if let Some(ref stored_hash) = record.transform_hash {
                let current_hash = format!("{:x}", Sha256::digest(&transform_content));
                if *stored_hash != current_hash {
                    return Err(CliError::RunValidation(format!(
                        "config '{}' transform was modified since last apply.\n  transform hash: expected {}, got {}\n  Run 'puffgres apply' to apply the changes.",
                        config.name, stored_hash, current_hash,
                    )));
                }
            }
        }

        transformers.insert(
            config.name.clone(),
            Box::new(JsTransformer::new(
                transform_path,
                config.id.id_type.clone(),
            )),
        );
    }

    let router = Router::new(mappings);
    let tables: Vec<String> = tables.into_iter().collect();

    let pg_client = pg::connect::connect(&env_config.database_url)
        .await
        .map_err(|e| CliError::Run(format!("failed to connect to postgres: {e}")))?;

    crate::validate::preflight_check(
        &env_config.database_url,
        &env_config.state_db_path,
        &applied_configs,
        Some(&pg_client),
    )
    .await
    .map_err(|msg| CliError::Run(format!("pre-flight check failed: {msg}")))?;

    pg::slot::ensure_slot(&pg_client, SLOT_NAME)
        .await
        .map_err(|e| CliError::Run(format!("failed to ensure replication slot: {e}")))?;

    pg::publication::ensure_publication(&pg_client, PUBLICATION_NAME, &tables)
        .await
        .map_err(|e| CliError::Run(format!("failed to ensure publication: {e}")))?;

    pg::publication::ensure_replica_identity_full(&pg_client, &tables)
        .await
        .map_err(|e| CliError::Run(format!("failed to set replica identity: {e}")))?;

    tracing::info!(slot = SLOT_NAME, "replication slot ready");
    tracing::info!(publication = PUBLICATION_NAME, "publication ready");

    // Check backfill status for each config
    let mut needs_backfill: Vec<&config::Config> = Vec::new();
    let mut watermark_lsns: Vec<u64> = Vec::new();

    for (_, config) in &applied_configs {
        match db.get_backfill_progress(&config.name)? {
            Some(bp) if bp.status == BackfillStatus::Completed => {
                if let Some(wlsn) = bp.watermark_lsn {
                    watermark_lsns.push(wlsn);
                }
            }
            Some(bp) if bp.status == BackfillStatus::Failed => {
                // Auto-resume: reset Failed -> InProgress, preserving last_id cursor
                tracing::info!(
                    config = %config.name,
                    processed_rows = bp.processed_rows,
                    last_id = bp.last_id.as_deref().unwrap_or("-"),
                    "auto-resuming failed backfill",
                );
                db.save_backfill_progress(&BackfillProgress {
                    config_name: config.name.clone(),
                    last_id: bp.last_id,
                    total_rows: bp.total_rows,
                    processed_rows: bp.processed_rows,
                    status: BackfillStatus::InProgress,
                    started_at: bp.started_at,
                    completed_at: None,
                    error_message: None,
                    watermark_lsn: bp.watermark_lsn,
                })?;
                needs_backfill.push(config);
            }
            _ => needs_backfill.push(config),
        }
    }

    // Run backfills if needed
    let puff_client = TurbopufferClient::new(
        env_config.turbopuffer_api_key.clone(),
        env_config.turbopuffer_region.clone(),
    )
    .map_err(|e| CliError::Run(format!("failed to create turbopuffer client: {e}")))?;

    if !needs_backfill.is_empty() {
        let _backfill_span = tracing::info_span!("backfill").entered();
        let watermark = pg::slot::get_current_wal_lsn(&pg_client)
            .await
            .map_err(|e| CliError::Run(format!("failed to get current WAL LSN: {e}")))?;
        tracing::info!(watermark_lsn = %pg::PgLsn::from(watermark), "starting backfill");

        for config in &needs_backfill {
            let namespace = namespaces
                .get(&config.name)
                .expect("namespace missing for applied config");
            let transformer = transformers
                .get(&config.name)
                .expect("transformer missing for applied config");

            let backfill_config = BackfillConfig {
                batch_size: project_config.batch_size(),
                max_retries: project_config.max_retries(),
                config_name: config.name.clone(),
                namespace: namespace.clone(),
                query_config: BatchQueryConfig {
                    schema: config.source.schema.clone(),
                    table: config.source.table.clone(),
                    id_column: config.id.column.clone(),
                    columns: config.columns.clone(),
                    batch_size: project_config.batch_size(),
                },
                id_type: config.id.id_type.clone(),
            };

            let result = run_backfill(
                &backfill_config,
                &pg_client,
                &puff_client,
                &mut db,
                transformer.as_ref(),
            )
            .await;

            match result.status {
                BackfillOutcome::Completed => {
                    // Mark completed in state db with watermark
                    db.save_backfill_progress(&BackfillProgress {
                        config_name: config.name.clone(),
                        last_id: None,
                        total_rows: None,
                        processed_rows: result.processed_rows,
                        status: BackfillStatus::Completed,
                        started_at: None,
                        completed_at: Some(Utc::now()),
                        error_message: None,
                        watermark_lsn: Some(watermark),
                    })?;
                    tracing::info!(
                        config = %config.name,
                        rows = result.processed_rows,
                        "backfill complete",
                    );
                    if let Some(m) = metrics {
                        m.backfill_rows_processed.add(result.processed_rows, &[]);
                    }
                    watermark_lsns.push(watermark);
                }
                BackfillOutcome::Failed { error, .. } => {
                    return Err(CliError::Run(format!(
                        "backfill failed for {}: {}",
                        config.name, error
                    )));
                }
            }
        }
    }

    // start_lsn: minimum of all watermark LSNs and streaming checkpoints
    let start_lsn = {
        let mut candidates: Vec<u64> = watermark_lsns;
        for (_, config) in &applied_configs {
            if let Some(cp) = db.get_streaming_checkpoint(&config.name)? {
                candidates.push(cp.lsn);
            }
        }
        candidates.into_iter().min()
    };

    // Here we stop holding the existing slot, and poll w/ exponential backoff for the new one.
    // Need to do this after publication setup / LSN resolution so we don't create
    // (and fail to clean one of these up) if those fail.
    pg::slot::terminate_active_slot_backend(&pg_client, SLOT_NAME)
        .await
        .map_err(|e| CliError::Run(format!("failed to terminate stale slot backend: {e}")))?;

    let mut backoff = Backoff::new(BackoffConfig {
        initial_delay_ms: 100,
        max_delay_ms: 5_000,
        max_retries: 10,
        multiplier: 2.0,
        jitter: true,
    });
    while pg::slot::get_active_pid(&pg_client, SLOT_NAME)
        .await
        .map_err(|e| CliError::Run(format!("failed to check slot active PID: {e}")))?
        .is_some()
    {
        match backoff.next_delay() {
            Some(delay) => {
                tokio::time::sleep(delay).await;
                pg::slot::terminate_active_slot_backend(&pg_client, SLOT_NAME)
                    .await
                    .map_err(|e| {
                        CliError::Run(format!("failed to terminate stale slot backend: {e}"))
                    })?;
            }
            None => {
                return Err(CliError::Run(format!(
                    "timed out waiting for replication slot '{}' to be released",
                    SLOT_NAME
                )));
            }
        }
    }

    drop(pg_client);

    let mut events_processed: HashMap<String, u64> = HashMap::new();
    for (_, config) in &applied_configs {
        let count = db
            .get_streaming_checkpoint(&config.name)?
            .map(|c| c.events_processed)
            .unwrap_or(0);
        events_processed.insert(config.name.clone(), count);
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

    // Replay any retryable DLQ entries from previous runs
    replay_dlq(
        &mut db,
        &transformers,
        &namespaces,
        &puff_client,
        project_config,
        metrics,
    )
    .await?;

    let mut start_lsn = start_lsn;
    let mut batch_count: u64 = 0;

    // Outer loop: reconnects the replication stream on schema changes.
    // When Postgres sends a Relation message with changed columns (e.g. ALTER TABLE),
    // we drop the stream and reconnect so the fresh RelationCache picks up the new schema.
    loop {
        let stream_config = ReplicationStreamConfig {
            connection_string: env_config.database_url.clone(),
            slot_name: SLOT_NAME.to_string(),
            publication_name: PUBLICATION_NAME.to_string(),
            start_lsn,
            status_interval: STATUS_INTERVAL,
        };

        let mut stream = ReplicationStream::connect(stream_config)
            .await
            .map_err(|e| CliError::Run(format!("failed to connect replication stream: {e}")))?;

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
            let batch = match stream.recv_batch().await {
                Ok(Some(batch)) => batch,
                Ok(None) => {
                    tracing::info!("replication stream ended");
                    return Ok(());
                }
                Err(ReplicationError::SchemaChanged {
                    relation_id,
                    ref namespace,
                    ref name,
                }) => {
                    tracing::warn!(
                        relation_id,
                        schema = %namespace,
                        table = %name,
                        "schema change detected, reconnecting replication stream",
                    );
                    should_reconnect = true;
                    break;
                }
                Err(e) => {
                    return Err(CliError::Run(format!("replication stream error: {e}")));
                }
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
                            &mut db,
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
                                    &mut db,
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

            for (_, config) in &applied_configs {
                let checkpoint = StreamingCheckpoint {
                    config_name: config.name.clone(),
                    lsn: batch.ack_lsn,
                    events_processed: *events_processed.get(&config.name).unwrap_or(&0),
                    updated_at: Utc::now(),
                };
                db.save_streaming_checkpoint(&checkpoint)?;
            }

            // Ack unconditionally — failed events are in the DLQ for retry
            stream.ack();
            if let Some(m) = metrics {
                m.replication_acks.add(1, &[]);
                m.cdc_batch_duration
                    .record(batch_start.elapsed().as_millis() as f64, &[]);
            }

            batch_count += 1;
            if batch_count % project_config.dlq_replay_interval() == 0 {
                replay_dlq(
                    &mut db,
                    &transformers,
                    &namespaces,
                    &puff_client,
                    project_config,
                    metrics,
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
            let slot_client = pg::connect::connect(&env_config.database_url)
                .await
                .map_err(|e| {
                    CliError::Run(format!("failed to connect for slot release check: {e}"))
                })?;

            pg::slot::terminate_active_slot_backend(&slot_client, SLOT_NAME)
                .await
                .map_err(|e| {
                    CliError::Run(format!("failed to terminate stale slot backend: {e}"))
                })?;

            let mut backoff = Backoff::new(BackoffConfig {
                initial_delay_ms: 100,
                max_delay_ms: 5_000,
                max_retries: 10,
                multiplier: 2.0,
                jitter: true,
            });
            while pg::slot::get_active_pid(&slot_client, SLOT_NAME)
                .await
                .map_err(|e| CliError::Run(format!("failed to check slot active PID: {e}")))?
                .is_some()
            {
                match backoff.next_delay() {
                    Some(delay) => {
                        tokio::time::sleep(delay).await;
                        pg::slot::terminate_active_slot_backend(&slot_client, SLOT_NAME)
                            .await
                            .map_err(|e| {
                                CliError::Run(format!(
                                    "failed to terminate stale slot backend: {e}"
                                ))
                            })?;
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
        for (_, config) in &applied_configs {
            if let Some(cp) = db.get_streaming_checkpoint(&config.name)? {
                checkpoint_lsns.push(cp.lsn);
            }
        }
        start_lsn = checkpoint_lsns.into_iter().min();
    }

    Ok(())
}

/// Serialize routed events and insert them into the DLQ.
/// `permanent` = true for transform errors (bad data won't fix itself on retry),
/// false for sink errors (transient network/server failures).
fn send_events_to_dlq(
    db: &mut StateDb,
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
async fn replay_dlq(
    db: &mut StateDb,
    transformers: &HashMap<String, Box<dyn Transformer>>,
    namespaces: &HashMap<String, String>,
    puff_client: &TurbopufferClient,
    project_config: &ProjectConfig,
    metrics: Option<&Metrics>,
) -> Result<(), CliError> {
    let entries = db.list_retryable_entries(project_config.dlq_replay_batch_size())?;
    if entries.is_empty() {
        return Ok(());
    }

    tracing::info!(entries = entries.len(), "replaying DLQ entries");

    for entry in &entries {
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
