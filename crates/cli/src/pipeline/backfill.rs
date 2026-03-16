use std::collections::HashMap;

use chrono::Utc;
use pg::batch::BatchQueryConfig;
use puff::TurbopufferClient;
use puffgres_core::{BackfillConfig, BackfillOutcome, Transformer, run_backfill};
use state::{BackfillProgress, BackfillStatus, StateDb};
use tokio_util::sync::CancellationToken;

use crate::error::CliError;
use crate::observability::Metrics;
use crate::project_config::ProjectConfig;

/// Check backfill status for each config and run backfills as needed.
///
/// Returns the list of watermark LSNs (one per config that has completed backfill).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn check_and_run_backfills(
    applied_configs: &[(std::path::PathBuf, config::Config)],
    db: &StateDb,
    namespaces: &HashMap<String, String>,
    transformers: &HashMap<String, Box<dyn Transformer>>,
    pg_client: &pg::connect::PgConnection,
    puff_client: &TurbopufferClient,
    project_config: &ProjectConfig,
    metrics: Option<&Metrics>,
    token: CancellationToken,
) -> Result<Vec<u64>, CliError> {
    let mut needs_backfill: Vec<&config::Config> = Vec::new();
    let mut watermark_lsns: Vec<u64> = Vec::new();

    for (_, config) in applied_configs {
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

    if !needs_backfill.is_empty() {
        let _backfill_span = tracing::info_span!("backfill").entered();
        let watermark = pg::slot::get_current_wal_lsn(pg_client).await?;
        tracing::info!(watermark_lsn = %pg::PgLsn::from(watermark), "starting backfill");

        for config in &needs_backfill {
            if token.is_cancelled() {
                tracing::info!("shutdown requested, stopping backfill loop");
                return Ok(watermark_lsns);
            }

            let namespace = namespaces.get(&config.name).ok_or_else(|| {
                CliError::Run(format!(
                    "internal error: no namespace for config '{}'",
                    config.name
                ))
            })?;
            let transformer = transformers.get(&config.name).ok_or_else(|| {
                CliError::Run(format!(
                    "internal error: no transformer for config '{}'",
                    config.name
                ))
            })?;

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
                pg_client,
                puff_client,
                db,
                transformer.as_ref(),
                token.clone(),
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
                BackfillOutcome::Cancelled => {
                    tracing::info!(config = %config.name, "backfill cancelled");
                    return Ok(watermark_lsns);
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

    Ok(watermark_lsns)
}
