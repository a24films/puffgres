use std::future::Future;
use std::pin::Pin;
use std::time::Instant;

use crate::backoff::{Backoff, BackoffConfig};
use crate::row_convert::pg_rows_to_events;
use crate::{Action, CoreError, DocumentId, Transformer};
use config::IdType;
use pg::batch::BatchQueryConfig;
use replication::RowEvent;
use state::BackfillCheckpointer;

/// Retry an async operation with exponential backoff.
///
/// Transient errors are retried until backoff is exhausted. Permanent errors
/// (where `is_transient()` returns false) propagate immediately.
async fn retry_with_backoff<F, Fut, T>(backoff: &mut Backoff, mut f: F) -> Result<T, CoreError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, CoreError>>,
{
    loop {
        match f().await {
            Ok(val) => return Ok(val),
            Err(e) if !e.is_transient() => return Err(e),
            Err(e) => match backoff.next_delay() {
                Some(delay) => {
                    tracing::warn!(
                        error = %e,
                        retry_delay_ms = delay.as_millis() as u64,
                        "backfill batch error, retrying",
                    );
                    tokio::time::sleep(delay).await;
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
    let mut backoff = Backoff::new(BackoffConfig {
        max_retries: config.max_retries,
        initial_delay_ms: 100,
        jitter: true,
        ..BackoffConfig::default()
    });

    // 6. Main loop
    let mut batch_num: u64 = 0;
    loop {
        let batch_start = Instant::now();
        let fetch_result = retry_with_backoff(&mut backoff, || async {
            pg::batch::fetch_batch(client, &query_config, cursor.as_deref(), &cursor_cast)
                .await
                .map_err(CoreError::from)
        })
        .await;

        let batch_result = match fetch_result {
            Ok(val) => val,
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
        let actions =
            match retry_with_backoff(&mut backoff, || transformer.transform_batch(&refs)).await {
                Ok(val) => val,
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

        if let Err(e) =
            retry_with_backoff(&mut backoff, || sink.write(&config.namespace, &actions)).await
        {
            return BackfillResult {
                processed_rows: processed,
                status: BackfillOutcome::Failed {
                    error: e.to_string(),
                    processed,
                },
            };
        }

        // Save checkpoint in a dedicated retry loop so that a transient
        // checkpoint failure does not re-fetch and re-write the batch.
        let batch_len = events.len() as u64;
        let last_id = batch_result
            .last_id
            .as_deref()
            .unwrap_or_else(|| cursor.as_deref().unwrap_or(""));
        loop {
            match checkpointer.save_progress(&config.config_name, last_id, processed + batch_len) {
                Ok(()) => break,
                Err(e) => match backoff.next_delay() {
                    Some(delay) => tokio::time::sleep(delay).await,
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

        backoff.reset();
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
