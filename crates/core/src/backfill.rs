use std::time::Instant;

use async_trait::async_trait;

use crate::backoff::{Backoff, BackoffConfig};
use crate::row_convert::pg_rows_to_events;
use crate::{Action, CoreError, DocumentId, Transformer};
use config::IdType;
use pg::batch::BatchQueryConfig;
use replication::RowEvent;
use state::BackfillCheckpointer;

/// Unwrap a `Result` inside the batch loop: on error, sleep with backoff and
/// retry. Once retries are exhausted, return a failed `BackfillResult`.
macro_rules! retry_or_fail {
    ($backoff:expr, $processed:expr, $result:expr) => {
        match $result {
            Ok(val) => val,
            Err(e) => match $backoff.next_delay() {
                Some(delay) => {
                    tracing::warn!(
                        error = %e,
                        retry_delay_ms = delay.as_millis() as u64,
                        "backfill batch error, retrying",
                    );
                    tokio::time::sleep(delay).await;
                    continue;
                }
                None => {
                    return BackfillResult {
                        processed_rows: $processed,
                        status: BackfillOutcome::Failed {
                            error: e.to_string(),
                            processed: $processed,
                        },
                    };
                }
            },
        }
    };
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

#[async_trait]
pub trait BackfillSink: Send + Sync {
    async fn write(&self, namespace: &str, actions: &[Action]) -> Result<(), CoreError>;
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

    // 2. Build effective query config: honor top-level batch_size, ensure
    //    columns are explicit (so every value is text-cast in the SELECT).
    let mut query_config = config.query_config.clone();
    query_config.batch_size = config.batch_size;
    if query_config.columns.is_none() {
        query_config.columns = Some(columns);
    }

    let cursor_cast = match &config.id_type {
        IdType::String => "",
        IdType::Uint | IdType::Int => "::int8",
        IdType::Uuid => "::uuid",
    };

    // 3. Resume from checkpoint or start fresh
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

    // 4. Create backoff
    let mut backoff = Backoff::new(BackoffConfig {
        max_retries: config.max_retries,
        initial_delay_ms: 100,
        jitter: true,
        ..BackoffConfig::default()
    });

    // 5. Main loop
    let mut batch_num: u64 = 0;
    loop {
        let batch_start = Instant::now();
        let batch_result = retry_or_fail!(
            backoff,
            processed,
            pg::batch::fetch_batch(client, &query_config, cursor.as_deref(), cursor_cast)
                .await
                .map_err(CoreError::from)
        );

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
        let actions = retry_or_fail!(backoff, processed, transformer.transform_batch(&refs).await);

        retry_or_fail!(
            backoff,
            processed,
            sink.write(&config.namespace, &actions).await
        );

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

    // 6. Completed
    BackfillResult {
        processed_rows: processed,
        status: BackfillOutcome::Completed,
    }
}
