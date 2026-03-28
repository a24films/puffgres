pub(crate) mod backfill;
pub(crate) mod dlq;
pub(crate) mod setup;
pub(crate) mod streaming;

use std::fs;
use std::time::Duration;

use backon::{BackoffBuilder, ExponentialBuilder};
use puff::TurbopufferClient;
use tokio_util::sync::CancellationToken;

use crate::env::EnvConfig;
use crate::error::CliError;
use crate::observability::Metrics;
use crate::paths::ProjectPaths;
use crate::project_config::ProjectConfig;
use crate::shutdown::ShutdownController;

pub(crate) const SLOT_NAME: &str = "puffgres";
pub(crate) const PUBLICATION_NAME: &str = "puffgres";
pub(crate) const STATUS_INTERVAL: Duration = Duration::from_secs(10);

pub(crate) struct ConfigInfo {
    pub(crate) transform_path: std::path::PathBuf,
    pub(crate) id_type: config::IdType,
    pub(crate) columns: Option<Vec<String>>,
    pub(crate) schema: String,
    pub(crate) table: String,
}

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

pub(crate) fn prefixed_namespace(prefix: &Option<String>, namespace: &str) -> String {
    match prefix {
        Some(p) if !p.is_empty() => format!("{}_{}", p, namespace),
        _ => namespace.to_string(),
    }
}

/// Run `f` in a loop with exponential backoff on failure.
///
/// Non-retryable errors (e.g. config validation failures) bypass the loop and
/// propagate immediately so the process fails fast for operator action.
/// If the cancellation token is set, the loop exits cleanly after the current
/// iteration completes.
async fn retry_loop<F, Fut>(
    builder: ExponentialBuilder,
    token: CancellationToken,
    metrics: Option<&Metrics>,
    tls_unclean_close_level: &str,
    mut f: F,
) -> Result<(), CliError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<(), CliError>>,
{
    let mut backoff = builder.build();
    loop {
        if token.is_cancelled() {
            tracing::info!("shutdown requested, exiting retry loop");
            break Ok(());
        }

        match f().await {
            Ok(()) => break Ok(()),
            Err(e) if !e.is_retryable() => {
                tracing::error!(error = %e, "non-recoverable error, exiting");
                break Err(e);
            }
            Err(e) => {
                if token.is_cancelled() {
                    tracing::info!("shutdown requested during error recovery, exiting");
                    break Ok(());
                }
                if e.is_tls_unclean_close() {
                    if let Some(metrics) = metrics {
                        metrics.tls_unclean_close.add(1, &[]);
                    }
                    if tls_unclean_close_level != "silent" {
                        if tls_unclean_close_level == "warn" {
                            tracing::warn!(
                                error = %e,
                                error_debug = ?e,
                                retryable = e.is_retryable(),
                                tls_unclean_close = true,
                                "CDC loop exited due to unclean TLS shutdown, reconnecting"
                            );
                        } else {
                            tracing::error!(
                                error = %e,
                                error_debug = ?e,
                                retryable = e.is_retryable(),
                                tls_unclean_close = true,
                                "CDC loop exited due to unclean TLS shutdown, reconnecting"
                            );
                        }
                    }
                } else {
                    tracing::error!(
                        error = %e,
                        error_debug = ?e,
                        retryable = e.is_retryable(),
                        "CDC loop exited with error, restarting..."
                    );
                }
                if let Some(delay) = backoff.next() {
                    tracing::info!(
                        delay_ms = delay.as_millis() as u64,
                        "waiting before restart"
                    );
                    tokio::select! {
                        _ = tokio::time::sleep(delay) => {}
                        _ = token.cancelled() => {
                            tracing::info!("shutdown requested during backoff, exiting");
                            break Ok(());
                        }
                    }
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
    let shutdown = ShutdownController::new();
    let token = shutdown.token();

    retry_loop(
        ExponentialBuilder::default()
            .with_min_delay(Duration::from_secs(1))
            .with_max_delay(Duration::from_secs(60))
            .with_max_times(usize::MAX)
            .with_jitter(),
        token.clone(),
        metrics,
        project_config.tls_unclean_close_level(),
        || run_cdc_inner(paths, env_config, project_config, metrics, token.clone()),
    )
    .await
}

#[tracing::instrument(name = "run", skip_all)]
async fn run_cdc_inner(
    paths: &ProjectPaths,
    env_config: &EnvConfig,
    project_config: &ProjectConfig,
    metrics: Option<&Metrics>,
    token: CancellationToken,
) -> Result<(), CliError> {
    if let Some(parent) = env_config.state_db_path.parent() {
        if !parent.exists() {
            fs::create_dir_all(parent)?;
        }
    }
    let db = state::StateDb::open(&env_config.state_db_path)?;

    let Some((applied_configs, router, _tables, namespaces, transformers, pg_client)) =
        setup::setup_pipeline(paths, env_config, &db).await?
    else {
        return Ok(());
    };

    let puff_client = TurbopufferClient::new(
        env_config.turbopuffer_api_key.clone(),
        env_config.turbopuffer_region.clone(),
    )?;

    // Check backfill status and run backfills
    let watermark_lsns = backfill::check_and_run_backfills(
        &applied_configs,
        &db,
        &namespaces,
        &transformers,
        &pg_client,
        &puff_client,
        project_config,
        metrics,
        token.clone(),
    )
    .await?;

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

    if token.is_cancelled() {
        tracing::info!("shutdown requested, skipping slot handoff");
        return Ok(());
    }

    // Here we stop holding the existing slot, and poll w/ exponential backoff for the new one.
    // Need to do this after publication setup / LSN resolution so we don't create
    // (and fail to clean one of these up) if those fail.
    setup::terminate_slot_and_wait(&pg_client, token.clone()).await?;

    drop(pg_client);

    streaming::run_streaming_loop(
        env_config,
        &applied_configs,
        &router,
        &namespaces,
        &transformers,
        &puff_client,
        &db,
        project_config,
        metrics,
        token,
        start_lsn,
    )
    .await
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
            ExponentialBuilder::default()
                .with_min_delay(Duration::from_millis(1))
                .with_max_delay(Duration::from_millis(1))
                .with_max_times(5),
            CancellationToken::new(),
            None,
            "error",
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
            ExponentialBuilder::default()
                .with_min_delay(Duration::from_millis(1))
                .with_max_delay(Duration::from_millis(1))
                .with_max_times(3),
            CancellationToken::new(),
            None,
            "error",
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
            ExponentialBuilder::default()
                .with_min_delay(Duration::from_millis(1))
                .with_max_delay(Duration::from_millis(1))
                .with_max_times(5),
            CancellationToken::new(),
            None,
            "error",
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
            ExponentialBuilder::default()
                .with_min_delay(Duration::from_millis(1))
                .with_max_delay(Duration::from_millis(1))
                .with_max_times(5),
            CancellationToken::new(),
            None,
            "error",
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
    use config::ConfigLoader;
    use sha2::{Digest, Sha256};
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
        let token = CancellationToken::new();
        rt.block_on(run_cdc_inner(
            paths,
            env_config,
            project_config,
            metrics,
            token,
        ))
    }

    #[test]
    fn no_applied_configs_returns_ok() {
        let (_dir, paths, state_db_path) = setup_project();
        // Write a config but don't apply it — no record in state db
        let user_dir = write_config(&paths, "user", "public", "users", "id", "uint");
        write_transform(&user_dir, PASSTHROUGH_TRANSFORM);

        let result = run_no_retry(
            &paths,
            &dummy_env(state_db_path),
            &ProjectConfig::default(),
            None,
        );
        assert!(
            result.is_ok(),
            "expected Ok(()) when no configs are applied, got: {:?}",
            result.unwrap_err()
        );
    }

    #[test]
    fn errors_on_unreadable_transform_for_applied_config() {
        let (_dir, paths, state_db_path) = setup_project();
        let user_dir = write_config(&paths, "user", "public", "users", "id", "uint");
        write_transform(&user_dir, PASSTHROUGH_TRANSFORM);

        let loader = ConfigLoader::new(&paths.configs);
        let (config_path, cfg) = &loader.load_all().unwrap()[0];
        let transform_bytes = fs::read(config_path.parent().unwrap().join("transform.ts")).unwrap();
        let transform_hash = format!("{:x}", Sha256::digest(&transform_bytes));
        let db = state::StateDb::open(&state_db_path).unwrap();
        db.insert_config(&ConfigRecord {
            name: cfg.name.clone(),
            namespace: cfg.namespace.clone(),
            content_hash: config::Config::content_hash_from_bytes(&fs::read(config_path).unwrap()),
            transform_hash: Some(transform_hash),
            applied_at: chrono::Utc::now(),
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
    fn errors_on_modified_content_hash_for_applied_config() {
        let (_dir, paths, state_db_path) = setup_project();
        let user_dir = write_config(&paths, "user", "public", "users", "id", "uint");
        write_transform(&user_dir, PASSTHROUGH_TRANSFORM);

        let loader = ConfigLoader::new(&paths.configs);
        let (config_path, cfg) = &loader.load_all().unwrap()[0];
        let transform_bytes = fs::read(config_path.parent().unwrap().join("transform.ts")).unwrap();
        let transform_hash = format!("{:x}", Sha256::digest(&transform_bytes));
        let db = state::StateDb::open(&state_db_path).unwrap();
        db.insert_config(&ConfigRecord {
            name: cfg.name.clone(),
            namespace: cfg.namespace.clone(),
            content_hash: "stale_content_hash".to_string(),
            transform_hash: Some(transform_hash),
            applied_at: chrono::Utc::now(),
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
    fn errors_on_modified_transform_for_applied_config() {
        let (_dir, paths, state_db_path) = setup_project();
        let user_dir = write_config(&paths, "user", "public", "users", "id", "uint");
        write_transform(&user_dir, PASSTHROUGH_TRANSFORM);

        let loader = ConfigLoader::new(&paths.configs);
        let (config_path, cfg) = &loader.load_all().unwrap()[0];
        let db = state::StateDb::open(&state_db_path).unwrap();
        db.insert_config(&ConfigRecord {
            name: cfg.name.clone(),
            namespace: cfg.namespace.clone(),
            content_hash: config::Config::content_hash_from_bytes(&fs::read(config_path).unwrap()),
            transform_hash: Some("stale_hash".to_string()),
            applied_at: chrono::Utc::now(),
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
