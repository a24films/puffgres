use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, Instant};

use backon::{BackoffBuilder, ExponentialBuilder};

use tokio_util::sync::CancellationToken;

use crate::row_convert::pg_rows_to_events;
use crate::{Action, CoreError, DocumentId, Transformer};
use config::IdType;
use pg::batch::BatchQueryConfig;
use replication::RowEvent;
use state::BackfillCheckpointer;

/// Retry an async operation with exponential backoff.
///
/// Transient errors are retried until backoff is exhausted. Permanent errors
/// (where `is_transient()` returns false) propagate immediately. Cancellation
/// is checked during retry backoff sleeps so that shutdown is not delayed.
async fn retry_with_backoff<F, Fut, T>(
    builder: &ExponentialBuilder,
    token: &CancellationToken,
    mut f: F,
) -> Result<T, CoreError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, CoreError>>,
{
    let mut backoff = builder.build();
    loop {
        match f().await {
            Ok(val) => return Ok(val),
            Err(e) if !e.is_transient() => return Err(e),
            Err(e) => match backoff.next() {
                Some(delay) => {
                    tracing::warn!(
                        error = %e,
                        retry_delay_ms = delay.as_millis() as u64,
                        "backfill batch error, retrying",
                    );
                    tokio::select! {
                        _ = tokio::time::sleep(delay) => {}
                        _ = token.cancelled() => {
                            tracing::info!("shutdown requested during backfill retry backoff");
                            return Err(CoreError::Cancelled);
                        }
                    }
                }
                None => return Err(e),
            },
        }
    }
}

pub struct BackfillConfig {
    pub batch_size: u32,
    pub max_retries: u32,
    pub config_name: String,
    pub namespace: String,
    pub query_config: BatchQueryConfig,
    pub id_type: IdType,
}

pub struct BackfillResult {
    pub processed_rows: u64,
    pub status: BackfillOutcome,
}

pub enum BackfillOutcome {
    Completed,
    Cancelled,
    Failed { error: String, processed: u64 },
}

pub trait BackfillSink: Send + Sync {
    fn write<'a>(
        &'a self,
        namespace: &'a str,
        actions: &'a [Action],
    ) -> Pin<Box<dyn Future<Output = Result<(), CoreError>> + Send + 'a>>;
}

pub async fn run_backfill(
    config: &BackfillConfig,
    client: &tokio_postgres::Client,
    sink: &dyn BackfillSink,
    checkpointer: &dyn BackfillCheckpointer,
    transformer: &dyn Transformer,
    token: CancellationToken,
) -> BackfillResult {
    // 1. Resolve columns (also validates table reachability)
    let columns = match pg::batch::resolve_column_names(
        client,
        &config.query_config.schema,
        &config.query_config.table,
    )
    .await
    {
        Ok(cols) => cols,
        Err(e) => {
            return BackfillResult {
                processed_rows: 0,
                status: BackfillOutcome::Failed {
                    error: e.to_string(),
                    processed: 0,
                },
            };
        }
    };

    // 2. Validate id column has a non-partial unique index
    if let Err(e) = pg::batch::validate_id_column_uniqueness(client, &config.query_config).await {
        return BackfillResult {
            processed_rows: 0,
            status: BackfillOutcome::Failed {
                error: e.to_string(),
                processed: 0,
            },
        };
    }

    // 3. Build effective query config: honor top-level batch_size, ensure
    //    columns are explicit (so every value is text-cast in the SELECT).
    let mut query_config = config.query_config.clone();
    query_config.batch_size = config.batch_size;
    if query_config.columns.is_none() {
        query_config.columns = Some(columns);
    }

    let cursor_cast = match pg::batch::resolve_cursor_cast(client, &config.query_config).await {
        Ok(cast) => cast,
        Err(e) => {
            return BackfillResult {
                processed_rows: 0,
                status: BackfillOutcome::Failed {
                    error: e.to_string(),
                    processed: 0,
                },
            };
        }
    };

    // 4. Resume from checkpoint or start fresh
    let (mut cursor, mut processed) = match checkpointer.load_progress(&config.config_name) {
        Ok(Some((last_id, count))) => (Some(last_id), count),
        Ok(None) => (None, 0),
        Err(e) => {
            return BackfillResult {
                processed_rows: 0,
                status: BackfillOutcome::Failed {
                    error: e.to_string(),
                    processed: 0,
                },
            };
        }
    };

    // 5. Create backoff
    let backoff_builder = ExponentialBuilder::default()
        .with_min_delay(Duration::from_millis(100))
        .with_max_delay(Duration::from_secs(30))
        .with_max_times(config.max_retries as usize)
        .with_jitter();

    // 6. Main loop
    let mut batch_num: u64 = 0;
    loop {
        if token.is_cancelled() {
            tracing::info!(
                config = %config.config_name,
                processed_rows = processed,
                "shutdown requested, stopping backfill",
            );
            return BackfillResult {
                processed_rows: processed,
                status: BackfillOutcome::Cancelled,
            };
        }

        let batch_start = Instant::now();
        let fetch_result = retry_with_backoff(&backoff_builder, &token, || async {
            pg::batch::fetch_batch(client, &query_config, cursor.as_deref(), &cursor_cast)
                .await
                .map_err(CoreError::from)
        })
        .await;

        let batch_result = match fetch_result {
            Ok(val) => val,
            Err(CoreError::Cancelled) => {
                return BackfillResult {
                    processed_rows: processed,
                    status: BackfillOutcome::Cancelled,
                };
            }
            Err(e) => {
                return BackfillResult {
                    processed_rows: processed,
                    status: BackfillOutcome::Failed {
                        error: e.to_string(),
                        processed,
                    },
                };
            }
        };

        if batch_result.rows.is_empty() {
            break;
        }

        let events = match pg_rows_to_events(
            &batch_result.rows,
            &config.query_config.id_column,
            &config.id_type,
        ) {
            Ok(e) => e,
            Err(e) => {
                return BackfillResult {
                    processed_rows: processed,
                    status: BackfillOutcome::Failed {
                        error: e.to_string(),
                        processed,
                    },
                };
            }
        };

        let refs: Vec<(&RowEvent, DocumentId)> =
            events.iter().map(|(ev, id)| (ev, id.clone())).collect();
        let actions = match retry_with_backoff(&backoff_builder, &token, || {
            transformer.transform_batch(&refs)
        })
        .await
        {
            Ok(val) => val,
            Err(CoreError::Cancelled) => {
                return BackfillResult {
                    processed_rows: processed,
                    status: BackfillOutcome::Cancelled,
                };
            }
            Err(e) => {
                return BackfillResult {
                    processed_rows: processed,
                    status: BackfillOutcome::Failed {
                        error: e.to_string(),
                        processed,
                    },
                };
            }
        };

        match retry_with_backoff(&backoff_builder, &token, || {
            sink.write(&config.namespace, &actions)
        })
        .await
        {
            Ok(()) => {}
            Err(CoreError::Cancelled) => {
                return BackfillResult {
                    processed_rows: processed,
                    status: BackfillOutcome::Cancelled,
                };
            }
            Err(e) => {
                return BackfillResult {
                    processed_rows: processed,
                    status: BackfillOutcome::Failed {
                        error: e.to_string(),
                        processed,
                    },
                };
            }
        }

        // Save checkpoint in a dedicated retry loop so that a transient
        // checkpoint failure does not re-fetch and re-write the batch.
        let batch_len = events.len() as u64;
        let last_id = batch_result
            .last_id
            .as_deref()
            .unwrap_or_else(|| cursor.as_deref().unwrap_or(""));
        {
            let mut cp_backoff = backoff_builder.build();
            loop {
                match checkpointer.save_progress(
                    &config.config_name,
                    last_id,
                    processed + batch_len,
                ) {
                    Ok(()) => break,
                    Err(e) => match cp_backoff.next() {
                        Some(delay) => {
                            tokio::select! {
                                _ = tokio::time::sleep(delay) => {}
                                _ = token.cancelled() => {
                                    tracing::info!("shutdown requested during checkpoint retry");
                                    return BackfillResult {
                                        processed_rows: processed,
                                        status: BackfillOutcome::Cancelled,
                                    };
                                }
                            }
                        }
                        None => {
                            return BackfillResult {
                                processed_rows: processed,
                                status: BackfillOutcome::Failed {
                                    error: e.to_string(),
                                    processed,
                                },
                            };
                        }
                    },
                }
            }
        }
        processed += batch_len;
        cursor = batch_result.last_id;
        batch_num += 1;

        tracing::info!(
            config = %config.config_name,
            batch = batch_num,
            rows = batch_len,
            total_rows = processed,
            cursor = cursor.as_deref().unwrap_or("-"),
            elapsed_ms = batch_start.elapsed().as_millis() as u64,
            has_more = batch_result.has_more,
            "backfill batch complete",
        );

        if !batch_result.has_more {
            break;
        }
    }

    // 7. Completed
    BackfillResult {
        processed_rows: processed,
        status: BackfillOutcome::Completed,
    }
}
