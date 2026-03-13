use std::collections::{BTreeSet, HashMap};
use std::fs;

use backon::{BackoffBuilder, ExponentialBuilder};
use config::{Config, ConfigLoader};
use puffgres_core::{JsTransformer, Mapping, Router, Transformer};
use sha2::{Digest, Sha256};
use state::{BackfillProgress, BackfillStatus, StateDb};
use tokio_util::sync::CancellationToken;

use super::{ConfigInfo, PUBLICATION_NAME, SLOT_NAME};
use crate::env::EnvConfig;
use crate::error::CliError;
use crate::paths::ProjectPaths;

/// Load configs, validate hashes, build transformers, run preflight checks,
/// and ensure the replication slot and publication are ready.
///
/// Returns everything the pipeline needs to proceed with backfill and streaming.
#[allow(clippy::type_complexity)]
pub(crate) async fn setup_pipeline(
    paths: &ProjectPaths,
    env_config: &EnvConfig,
    db: &StateDb,
) -> Result<
    Option<(
        Vec<(std::path::PathBuf, config::Config)>,
        Router,
        Vec<String>,
        HashMap<String, String>,
        HashMap<String, Box<dyn Transformer>>,
        pg::connect::PgConnection,
    )>,
    CliError,
> {
    let loader = ConfigLoader::new(&paths.configs);
    let all_configs_with_bytes = loader.load_all_with_bytes()?;

    let mut applied_configs = Vec::new();
    let mut config_raw_bytes: HashMap<String, Vec<u8>> = HashMap::new();
    for (path, config, bytes) in all_configs_with_bytes {
        let record = db.get_config(&config.name)?;
        if record.is_some_and(|r| r.tombstone_applied_at.is_none()) {
            config_raw_bytes.insert(config.name.clone(), bytes);
            applied_configs.push((path, config));
        }
    }

    if applied_configs.is_empty() {
        tracing::warn!("no applied configs \u{2014} run `puffgres apply` first");
        return Ok(None);
    }

    // Re-key: detect namespace prefix changes and reset state if needed
    let current_prefix = env_config.turbopuffer_namespace_prefix.clone();
    for (_, config) in &applied_configs {
        let stored_prefix = db.get_namespace_prefix(&config.name)?;
        match &stored_prefix {
            None => {
                // First run or legacy config -- just store the current prefix
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
                        "namespace prefix changed \u{2014} resetting state, backfill will re-run",
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
    let mut namespaces: HashMap<String, String> = HashMap::new();
    let mut tables: BTreeSet<String> = BTreeSet::new();

    let mut config_infos: HashMap<String, ConfigInfo> = HashMap::new();

    for (config_path, config) in &applied_configs {
        mappings.push(Mapping::from_config(config));
        namespaces.insert(
            config.name.clone(),
            super::prefixed_namespace(&env_config.turbopuffer_namespace_prefix, &config.namespace),
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
            let config_bytes = config_raw_bytes
                .get(&config.name)
                .expect("config_raw_bytes populated for every applied config");
            let current_content_hash = Config::content_hash_from_bytes(config_bytes);
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

        config_infos.insert(
            config.name.clone(),
            ConfigInfo {
                transform_path,
                id_type: config.id.id_type.clone(),
                columns: config.columns.clone(),
                schema: config.source.schema.clone(),
                table: config.source.table.clone(),
            },
        );
    }

    let router = Router::new(mappings);
    let tables: Vec<String> = tables.into_iter().collect();

    let pg_client = pg::connect::connect(&env_config.database_url).await?;

    // Build transformers after PG connect so we can compute column reindex
    // mappings for configs that specify a column subset/reorder.
    let mut transformers: HashMap<String, Box<dyn Transformer>> = HashMap::new();
    for (name, info) in &config_infos {
        let transformer: Box<dyn Transformer> = if let Some(ref config_cols) = info.columns {
            // Fetch table columns in their natural (WAL) order
            let all_columns =
                pg::column::resolve_column_info(&pg_client, &info.schema, &info.table).await?;

            // Build reindex: for each config column, find its position in the table
            let reindex: Vec<usize> = config_cols
                .iter()
                .map(|col_name| {
                    all_columns
                        .iter()
                        .position(|c| c.name == *col_name)
                        .ok_or_else(|| {
                            CliError::Run(format!(
                                "config '{}': column '{}' not found in {}.{}",
                                name, col_name, info.schema, info.table
                            ))
                        })
                })
                .collect::<Result<Vec<_>, _>>()?;

            Box::new(JsTransformer::with_column_reindex(
                info.transform_path.clone(),
                info.id_type.clone(),
                reindex,
            ))
        } else {
            Box::new(JsTransformer::new(
                info.transform_path.clone(),
                info.id_type.clone(),
            ))
        };
        transformers.insert(name.clone(), transformer);
    }

    crate::validate::preflight_check(
        &env_config.database_url,
        &env_config.state_db_path,
        &applied_configs,
        Some(&pg_client),
    )
    .await
    .map_err(|msg| CliError::Run(format!("pre-flight check failed: {msg}")))?;

    pg::slot::ensure_slot(&pg_client, SLOT_NAME).await?;

    pg::publication::ensure_publication(&pg_client, PUBLICATION_NAME, &tables).await?;

    pg::publication::ensure_replica_identity_full(&pg_client, &tables).await?;

    tracing::info!(slot = SLOT_NAME, "replication slot ready");
    tracing::info!(publication = PUBLICATION_NAME, "publication ready");

    Ok(Some((
        applied_configs,
        router,
        tables,
        namespaces,
        transformers,
        pg_client,
    )))
}

/// Terminate active slot backend and poll with exponential backoff until released.
pub(crate) async fn terminate_slot_and_wait(
    pg_client: &pg::connect::PgConnection,
    token: CancellationToken,
) -> Result<(), CliError> {
    pg::slot::terminate_active_slot_backend(pg_client, SLOT_NAME).await?;

    let mut backoff = ExponentialBuilder::default()
        .with_min_delay(std::time::Duration::from_millis(100))
        .with_max_delay(std::time::Duration::from_secs(5))
        .with_max_times(10)
        .with_jitter()
        .build();
    while pg::slot::get_active_pid(pg_client, SLOT_NAME)
        .await?
        .is_some()
    {
        if token.is_cancelled() {
            tracing::info!("shutdown requested, aborting slot-release wait");
            return Ok(());
        }
        match backoff.next() {
            Some(delay) => {
                tokio::select! {
                    _ = tokio::time::sleep(delay) => {}
                    _ = token.cancelled() => {
                        tracing::info!("shutdown requested during slot-release backoff");
                        return Ok(());
                    }
                }
                pg::slot::terminate_active_slot_backend(pg_client, SLOT_NAME).await?;
            }
            None => {
                return Err(CliError::Run(format!(
                    "timed out waiting for replication slot '{}' to be released",
                    SLOT_NAME
                )));
            }
        }
    }
    Ok(())
}
