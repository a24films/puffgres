use std::time::{SystemTime, UNIX_EPOCH};

use chrono::Utc;
use pg::connect::{PgConnection, quote_identifier};
use pg::schema_bootstrap::{PUFFGRES_SCHEMA, ensure_schema, ensure_state_tables};

use crate::{
    BackfillProgress, BackfillStatus, ConfigRecord, DlqEntry, DlqOperation, ErrorKind,
    SpoolEntry, SpoolStatus, StateError, StreamingCheckpoint, epoch,
};

pub struct PostgresStateStore {
    connection: PgConnection,
    schema_name: String,
}

impl PostgresStateStore {
    pub async fn connect(connection_string: &str) -> Result<Self, StateError> {
        Self::connect_with_schema(connection_string, PUFFGRES_SCHEMA).await
    }

    pub async fn connect_with_schema(
        connection_string: &str,
        schema_name: &str,
    ) -> Result<Self, StateError> {
        let connection = pg::connect::connect(connection_string).await?;
        ensure_schema(&connection, schema_name).await?;
        ensure_state_tables(&connection, schema_name).await?;

        Ok(Self {
            connection,
            schema_name: schema_name.to_string(),
        })
    }

    pub fn client(&self) -> &pg::Client {
        &self.connection
    }

    pub fn schema_name(&self) -> &str {
        &self.schema_name
    }

    pub async fn verify_startup_roundtrip(&self) -> Result<(), StateError> {
        let pid = std::process::id();
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| StateError::InvalidState(format!("system clock error: {e}")))?
            .as_millis();
        let probe_key = format!("startup_probe_{}_{}", pid, ts);
        let probe_value = format!("{}-{}", pid, ts);
        let schema = quote_identifier(&self.schema_name);

        let upsert = format!(
            "INSERT INTO {schema}.runtime_state (key, value, updated_at)
             VALUES ($1, $2, $3)
             ON CONFLICT(key) DO UPDATE
             SET value = excluded.value, updated_at = excluded.updated_at"
        );
        let select =
            format!("SELECT value FROM {schema}.runtime_state WHERE key = $1");
        let delete =
            format!("DELETE FROM {schema}.runtime_state WHERE key = $1");
        let updated_at = chrono::Utc::now().timestamp_millis();

        self.connection
            .execute(&upsert, &[&probe_key, &probe_value, &updated_at])
            .await
            .map_err(pg::PgError::from)?;

        let stored = self
            .connection
            .query_one(&select, &[&probe_key])
            .await
            .map_err(pg::PgError::from)?
            .get::<_, String>(0);

        let _ = self.connection.execute(&delete, &[&probe_key]).await;

        if stored != probe_value {
            return Err(StateError::InvalidState(format!(
                "postgres state roundtrip verification failed for schema '{}'",
                self.schema_name
            )));
        }

        tracing::info!(
            state_schema = %self.schema_name,
            "postgres state startup roundtrip check passed"
        );

        Ok(())
    }

    pub async fn save_streaming_checkpoint(
        &self,
        checkpoint: &StreamingCheckpoint,
    ) -> Result<(), StateError> {
        let schema = quote_identifier(&self.schema_name);
        let query = format!(
            "INSERT INTO {schema}.streaming_checkpoints
                (config_name, lsn, events_processed, updated_at)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (config_name) DO UPDATE
             SET lsn = excluded.lsn,
                 events_processed = excluded.events_processed,
                 updated_at = excluded.updated_at"
        );

        let lsn = i64::from_ne_bytes(checkpoint.lsn.to_ne_bytes());
        let events_processed = i64::from_ne_bytes(checkpoint.events_processed.to_ne_bytes());
        let updated_at = epoch::to_millis(&checkpoint.updated_at);

        self.connection
            .execute(
                &query,
                &[
                    &checkpoint.config_name,
                    &lsn,
                    &events_processed,
                    &updated_at,
                ],
            )
            .await
            .map_err(pg::PgError::from)?;

        Ok(())
    }

    pub async fn get_streaming_checkpoint(
        &self,
        config_name: &str,
    ) -> Result<Option<StreamingCheckpoint>, StateError> {
        let schema = quote_identifier(&self.schema_name);
        let query = format!(
            "SELECT config_name, lsn, events_processed, updated_at
             FROM {schema}.streaming_checkpoints
             WHERE config_name = $1"
        );

        let row = self
            .connection
            .query_opt(&query, &[&config_name])
            .await
            .map_err(pg::PgError::from)?;

        row.map(|row| {
            let updated_at = epoch::from_millis(row.get::<_, i64>(3)).ok_or_else(|| {
                StateError::InvalidState(format!(
                    "invalid updated_at millis for config '{}'",
                    config_name
                ))
            })?;

            Ok(StreamingCheckpoint {
                config_name: row.get(0),
                lsn: u64::from_ne_bytes(row.get::<_, i64>(1).to_ne_bytes()),
                events_processed: u64::from_ne_bytes(row.get::<_, i64>(2).to_ne_bytes()),
                updated_at,
            })
        })
        .transpose()
    }

    pub async fn delete_streaming_checkpoint(&self, config_name: &str) -> Result<bool, StateError> {
        let schema = quote_identifier(&self.schema_name);
        let query = format!(
            "DELETE FROM {schema}.streaming_checkpoints WHERE config_name = $1"
        );

        let deleted = self
            .connection
            .execute(&query, &[&config_name])
            .await
            .map_err(pg::PgError::from)?;

        Ok(deleted > 0)
    }

    pub async fn save_backfill_progress(
        &self,
        progress: &BackfillProgress,
    ) -> Result<(), StateError> {
        let schema = quote_identifier(&self.schema_name);
        let query = format!(
            "INSERT INTO {schema}.backfill_progress
                (config_name, last_id, total_rows, processed_rows, status, started_at,
                 completed_at, error_message, watermark_lsn)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
             ON CONFLICT (config_name) DO UPDATE
             SET last_id = excluded.last_id,
                 total_rows = excluded.total_rows,
                 processed_rows = excluded.processed_rows,
                 status = excluded.status,
                 started_at = excluded.started_at,
                 completed_at = excluded.completed_at,
                 error_message = excluded.error_message,
                 watermark_lsn = excluded.watermark_lsn"
        );

        let total_rows = progress.total_rows.map(|v| i64::from_ne_bytes(v.to_ne_bytes()));
        let processed_rows = i64::from_ne_bytes(progress.processed_rows.to_ne_bytes());
        let started_at = progress.started_at.as_ref().map(epoch::to_millis);
        let completed_at = progress.completed_at.as_ref().map(epoch::to_millis);
        let watermark_lsn = progress
            .watermark_lsn
            .map(|v| i64::from_ne_bytes(v.to_ne_bytes()));

        self.connection
            .execute(
                &query,
                &[
                    &progress.config_name,
                    &progress.last_id,
                    &total_rows,
                    &processed_rows,
                    &progress.status.to_string(),
                    &started_at,
                    &completed_at,
                    &progress.error_message,
                    &watermark_lsn,
                ],
            )
            .await
            .map_err(pg::PgError::from)?;

        Ok(())
    }

    pub async fn get_backfill_progress(
        &self,
        config_name: &str,
    ) -> Result<Option<BackfillProgress>, StateError> {
        let schema = quote_identifier(&self.schema_name);
        let query = format!(
            "SELECT config_name, last_id, total_rows, processed_rows, status, started_at,
                    completed_at, error_message, watermark_lsn
             FROM {schema}.backfill_progress
             WHERE config_name = $1"
        );

        let row = self
            .connection
            .query_opt(&query, &[&config_name])
            .await
            .map_err(pg::PgError::from)?;

        row.map(|row| {
            let status_text: String = row.get(4);
            let status = status_text.parse::<BackfillStatus>().map_err(|e| {
                StateError::InvalidState(format!("invalid backfill status: {e}"))
            })?;

            Ok(BackfillProgress {
                config_name: row.get(0),
                last_id: row.get(1),
                total_rows: row
                    .get::<_, Option<i64>>(2)
                    .map(|v| u64::from_ne_bytes(v.to_ne_bytes())),
                processed_rows: u64::from_ne_bytes(row.get::<_, i64>(3).to_ne_bytes()),
                status,
                started_at: row.get::<_, Option<i64>>(5).and_then(epoch::from_millis),
                completed_at: row.get::<_, Option<i64>>(6).and_then(epoch::from_millis),
                error_message: row.get(7),
                watermark_lsn: row
                    .get::<_, Option<i64>>(8)
                    .map(|v| u64::from_ne_bytes(v.to_ne_bytes())),
            })
        })
        .transpose()
    }

    pub async fn insert_config(&self, config: &ConfigRecord) -> Result<(), StateError> {
        let schema = quote_identifier(&self.schema_name);
        let query = format!(
            "INSERT INTO {schema}.configs
                (name, namespace, content_hash, transform_hash, applied_at, tombstone_applied_at, namespace_prefix)
             VALUES ($1, $2, $3, $4, $5, $6, $7)"
        );

        let applied_at = epoch::to_millis(&config.applied_at);
        let tombstone_applied_at = config.tombstone_applied_at.as_ref().map(epoch::to_millis);

        self.connection
            .execute(
                &query,
                &[
                    &config.name,
                    &config.namespace,
                    &config.content_hash,
                    &config.transform_hash,
                    &applied_at,
                    &tombstone_applied_at,
                    &config.namespace_prefix,
                ],
            )
            .await
            .map_err(pg::PgError::from)?;

        Ok(())
    }

    pub async fn get_config(&self, name: &str) -> Result<Option<ConfigRecord>, StateError> {
        let schema = quote_identifier(&self.schema_name);
        let query = format!(
            "SELECT name, namespace, content_hash, transform_hash, applied_at, tombstone_applied_at, namespace_prefix
             FROM {schema}.configs
             WHERE name = $1"
        );

        let row = self
            .connection
            .query_opt(&query, &[&name])
            .await
            .map_err(pg::PgError::from)?;

        row.as_ref().map(config_from_row).transpose()
    }

    pub async fn list_configs(&self) -> Result<Vec<ConfigRecord>, StateError> {
        let schema = quote_identifier(&self.schema_name);
        let query = format!(
            "SELECT name, namespace, content_hash, transform_hash, applied_at, tombstone_applied_at, namespace_prefix
             FROM {schema}.configs
             ORDER BY name ASC"
        );

        let rows = self
            .connection
            .query(&query, &[])
            .await
            .map_err(pg::PgError::from)?;

        rows.iter().map(config_from_row).collect()
    }

    pub async fn list_tombstoned_configs(&self) -> Result<Vec<ConfigRecord>, StateError> {
        let schema = quote_identifier(&self.schema_name);
        let query = format!(
            "SELECT name, namespace, content_hash, transform_hash, applied_at, tombstone_applied_at, namespace_prefix
             FROM {schema}.configs
             WHERE tombstone_applied_at IS NOT NULL
             ORDER BY name ASC"
        );

        let rows = self
            .connection
            .query(&query, &[])
            .await
            .map_err(pg::PgError::from)?;

        rows.iter().map(config_from_row).collect()
    }

    pub async fn tombstone_config(&self, name: &str) -> Result<(), StateError> {
        let schema = quote_identifier(&self.schema_name);
        let now = Utc::now().timestamp_millis();
        let query = format!(
            "UPDATE {schema}.configs
             SET tombstone_applied_at = $2
             WHERE name = $1"
        );

        let updated = self
            .connection
            .execute(&query, &[&name, &now])
            .await
            .map_err(pg::PgError::from)?;

        if updated == 0 {
            return Err(StateError::InvalidState(format!(
                "config '{name}' not found"
            )));
        }

        Ok(())
    }

    pub async fn get_namespace_prefix(&self, config_name: &str) -> Result<Option<String>, StateError> {
        let config = self.get_config(config_name).await?;
        match config {
            Some(record) => Ok(record.namespace_prefix),
            None => Err(StateError::InvalidState(format!(
                "config '{config_name}' not found"
            ))),
        }
    }

    pub async fn set_namespace_prefix(
        &self,
        config_name: &str,
        prefix: Option<&str>,
    ) -> Result<(), StateError> {
        let schema = quote_identifier(&self.schema_name);
        let prefix = prefix.map(ToString::to_string);
        let query = format!(
            "UPDATE {schema}.configs
             SET namespace_prefix = $2
             WHERE name = $1"
        );

        let updated = self
            .connection
            .execute(&query, &[&config_name, &prefix])
            .await
            .map_err(pg::PgError::from)?;

        if updated == 0 {
            return Err(StateError::InvalidState(format!(
                "config '{config_name}' not found"
            )));
        }

        Ok(())
    }

    pub async fn insert_dlq_entry(&self, entry: &DlqEntry) -> Result<i64, StateError> {
        let schema = quote_identifier(&self.schema_name);
        let query = format!(
            "INSERT INTO {schema}.dlq
                (config_name, lsn, doc_id, error_message, error_kind, retry_count,
                 created_at, last_retry_at, permanent_at, operation)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
             RETURNING id"
        );

        let lsn = i64::from_ne_bytes(entry.lsn.to_ne_bytes());
        let retry_count = i32::try_from(entry.retry_count).map_err(|_| {
            StateError::InvalidState(format!(
                "retry_count {} exceeds i32::MAX",
                entry.retry_count
            ))
        })?;
        let created_at = epoch::to_millis(&entry.created_at);
        let last_retry_at = entry.last_retry_at.as_ref().map(epoch::to_millis);
        let permanent_at = entry.permanent_at.as_ref().map(epoch::to_millis);
        let operation = entry.operation.as_ref().map(ToString::to_string);

        let id = self
            .connection
            .query_one(
                &query,
                &[
                    &entry.config_name,
                    &lsn,
                    &entry.doc_id,
                    &entry.error_message,
                    &error_kind_to_str(&entry.error_kind),
                    &retry_count,
                    &created_at,
                    &last_retry_at,
                    &permanent_at,
                    &operation,
                ],
            )
            .await
            .map_err(pg::PgError::from)?
            .get::<_, i64>(0);

        Ok(id)
    }

    pub async fn list_retryable_entries(&self, limit: usize) -> Result<Vec<DlqEntry>, StateError> {
        let schema = quote_identifier(&self.schema_name);
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let query = format!(
            "SELECT d.id, d.config_name, d.lsn, d.doc_id, d.error_message, d.error_kind,
                    d.retry_count, d.created_at, d.last_retry_at, d.permanent_at, d.operation
             FROM {schema}.dlq d
             INNER JOIN {schema}.configs c ON c.name = d.config_name
             WHERE d.error_kind = 'retryable'
               AND c.tombstone_applied_at IS NULL
             ORDER BY d.created_at ASC
             LIMIT $1"
        );

        let rows = self
            .connection
            .query(&query, &[&limit])
            .await
            .map_err(pg::PgError::from)?;

        rows.iter().map(dlq_from_row).collect()
    }

    pub async fn clear_dlq(&self, config_name: Option<&str>) -> Result<u64, StateError> {
        let schema = quote_identifier(&self.schema_name);
        let deleted = match config_name {
            Some(config_name) => {
                let query = format!("DELETE FROM {schema}.dlq WHERE config_name = $1");
                self.connection
                    .execute(&query, &[&config_name])
                    .await
                    .map_err(pg::PgError::from)?
            }
            None => {
                let query = format!("DELETE FROM {schema}.dlq");
                self.connection
                    .execute(&query, &[])
                    .await
                    .map_err(pg::PgError::from)?
            }
        };

        Ok(deleted)
    }

    pub async fn mark_permanent(&self, id: i64, error: &str) -> Result<(), StateError> {
        let schema = quote_identifier(&self.schema_name);
        let now = Utc::now().timestamp_millis();
        let query = format!(
            "UPDATE {schema}.dlq
             SET error_kind = 'permanent',
                 error_message = $2,
                 last_retry_at = $3,
                 permanent_at = $3
             WHERE id = $1"
        );

        let updated = self
            .connection
            .execute(&query, &[&id, &error, &now])
            .await
            .map_err(pg::PgError::from)?;

        if updated == 0 {
            return Err(StateError::NotFound(format!("dlq entry with id {id}")));
        }

        Ok(())
    }

    pub async fn delete_dlq_entry(&self, id: i64) -> Result<bool, StateError> {
        let schema = quote_identifier(&self.schema_name);
        let query = format!("DELETE FROM {schema}.dlq WHERE id = $1");

        let deleted = self
            .connection
            .execute(&query, &[&id])
            .await
            .map_err(pg::PgError::from)?;

        Ok(deleted > 0)
    }

    pub async fn increment_retry(&self, id: i64) -> Result<(), StateError> {
        let schema = quote_identifier(&self.schema_name);
        let now = Utc::now().timestamp_millis();
        let query = format!(
            "UPDATE {schema}.dlq
             SET retry_count = retry_count + 1,
                 last_retry_at = $2
             WHERE id = $1"
        );

        let updated = self
            .connection
            .execute(&query, &[&id, &now])
            .await
            .map_err(pg::PgError::from)?;

        if updated == 0 {
            return Err(StateError::NotFound(format!("dlq entry with id {id}")));
        }

        Ok(())
    }

    pub async fn clear_old_permanent_entries(&self, max_age_hours: u64) -> Result<u64, StateError> {
        let schema = quote_identifier(&self.schema_name);
        let cutoff =
            Utc::now() - chrono::Duration::hours(i64::try_from(max_age_hours).unwrap_or(i64::MAX));
        let cutoff_millis = epoch::to_millis(&cutoff);
        let query = format!(
            "DELETE FROM {schema}.dlq
             WHERE error_kind = 'permanent'
               AND permanent_at < $1"
        );

        let deleted = self
            .connection
            .execute(&query, &[&cutoff_millis])
            .await
            .map_err(pg::PgError::from)?;

        Ok(deleted)
    }

    pub async fn insert_spool_entry(
        &self,
        transaction_id: i64,
        ack_lsn: Option<u64>,
        is_final_chunk: bool,
        checkpoint_configs_json: &str,
        payload_json: &str,
    ) -> Result<i64, StateError> {
        let schema = quote_identifier(&self.schema_name);
        let query = format!(
            "INSERT INTO {schema}.cdc_spool
                (transaction_id, ack_lsn, is_final_chunk, checkpoint_configs, payload)
             VALUES ($1, $2, $3, $4::jsonb, $5::jsonb)
             RETURNING id"
        );
        let ack_lsn = ack_lsn.map(|value| i64::from_ne_bytes(value.to_ne_bytes()));

        let id = self
            .connection
            .query_one(
                &query,
                &[
                    &transaction_id,
                    &ack_lsn,
                    &is_final_chunk,
                    &checkpoint_configs_json,
                    &payload_json,
                ],
            )
            .await
            .map_err(pg::PgError::from)?
            .get::<_, i64>(0);

        Ok(id)
    }

    pub async fn claim_pending_spool_entries(
        &self,
        limit: usize,
    ) -> Result<Vec<SpoolEntry>, StateError> {
        let schema = quote_identifier(&self.schema_name);
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let query = format!(
            "WITH claimed AS (
                SELECT id
                FROM {schema}.cdc_spool
                WHERE status = 'pending'
                ORDER BY id ASC
                LIMIT $1
                FOR UPDATE SKIP LOCKED
             )
             UPDATE {schema}.cdc_spool AS spool
             SET status = 'processing', started_at = now()
             FROM claimed
             WHERE spool.id = claimed.id
             RETURNING spool.id, spool.transaction_id, spool.ack_lsn, spool.is_final_chunk,
                       spool.checkpoint_configs::text, spool.payload::text, spool.status,
                       spool.retry_count, spool.last_error"
        );

        let rows = self
            .connection
            .query(&query, &[&limit])
            .await
            .map_err(pg::PgError::from)?;

        rows.iter().map(spool_from_row).collect()
    }

    pub async fn mark_spool_entry_done(&self, id: i64) -> Result<(), StateError> {
        let schema = quote_identifier(&self.schema_name);
        let query = format!(
            "UPDATE {schema}.cdc_spool
             SET status = 'done', completed_at = now(), last_error = NULL
             WHERE id = $1"
        );

        let updated = self
            .connection
            .execute(&query, &[&id])
            .await
            .map_err(pg::PgError::from)?;

        if updated == 0 {
            return Err(StateError::NotFound(format!("spool entry with id {id}")));
        }

        Ok(())
    }

    pub async fn mark_spool_entry_failed(&self, id: i64, error: &str) -> Result<(), StateError> {
        let schema = quote_identifier(&self.schema_name);
        let query = format!(
            "UPDATE {schema}.cdc_spool
             SET status = 'failed',
                 retry_count = retry_count + 1,
                 last_error = $2,
                 completed_at = now()
             WHERE id = $1"
        );

        let updated = self
            .connection
            .execute(&query, &[&id, &error])
            .await
            .map_err(pg::PgError::from)?;

        if updated == 0 {
            return Err(StateError::NotFound(format!("spool entry with id {id}")));
        }

        Ok(())
    }

    pub async fn release_spool_entry(&self, id: i64) -> Result<(), StateError> {
        let schema = quote_identifier(&self.schema_name);
        let query = format!(
            "UPDATE {schema}.cdc_spool
             SET status = 'pending',
                 started_at = NULL,
                 completed_at = NULL
             WHERE id = $1"
        );

        let updated = self
            .connection
            .execute(&query, &[&id])
            .await
            .map_err(pg::PgError::from)?;

        if updated == 0 {
            return Err(StateError::NotFound(format!("spool entry with id {id}")));
        }

        Ok(())
    }

    pub async fn count_pending_spool_entries(&self) -> Result<u64, StateError> {
        let schema = quote_identifier(&self.schema_name);
        let query = format!(
            "SELECT COUNT(*) FROM {schema}.cdc_spool WHERE status = 'pending'"
        );

        let count = self
            .connection
            .query_one(&query, &[])
            .await
            .map_err(pg::PgError::from)?
            .get::<_, i64>(0);

        Ok(u64::try_from(count).unwrap_or(0))
    }
}

fn config_from_row(row: &tokio_postgres::Row) -> Result<ConfigRecord, StateError> {
    let applied_at = epoch::from_millis(row.get::<_, i64>(4)).ok_or_else(|| {
        StateError::InvalidState("invalid applied_at millis".to_string())
    })?;
    let tombstone_applied_at = row.get::<_, Option<i64>>(5).map(epoch::from_millis).ok_or_else(
        || StateError::InvalidState("invalid tombstone_applied_at millis".to_string()),
    )?;

    Ok(ConfigRecord {
        name: row.get(0),
        namespace: row.get(1),
        content_hash: row.get(2),
        transform_hash: row.get(3),
        applied_at,
        tombstone_applied_at,
        namespace_prefix: row.get(6),
    })
}

fn dlq_from_row(row: &tokio_postgres::Row) -> Result<DlqEntry, StateError> {
    let created_at = epoch::from_millis(row.get::<_, i64>(7))
        .ok_or_else(|| StateError::InvalidState("invalid created_at millis".to_string()))?;
    let last_retry_at = row.get::<_, Option<i64>>(8).and_then(epoch::from_millis);
    let permanent_at = row.get::<_, Option<i64>>(9).and_then(epoch::from_millis);
    let error_kind = error_kind_from_str(row.get::<_, String>(5).as_str())?;
    let operation = row
        .get::<_, Option<String>>(10)
        .as_deref()
        .and_then(|value| value.parse::<DlqOperation>().ok());

    Ok(DlqEntry {
        id: row.get(0),
        config_name: row.get(1),
        lsn: u64::from_ne_bytes(row.get::<_, i64>(2).to_ne_bytes()),
        doc_id: row.get(3),
        operation,
        error_message: row.get(4),
        error_kind,
        retry_count: u32::try_from(row.get::<_, i32>(6)).unwrap_or(0),
        created_at,
        last_retry_at,
        permanent_at,
    })
}

fn error_kind_to_str(error_kind: &ErrorKind) -> &'static str {
    match error_kind {
        ErrorKind::Retryable => "retryable",
        ErrorKind::Permanent => "permanent",
    }
}

fn error_kind_from_str(value: &str) -> Result<ErrorKind, StateError> {
    match value {
        "retryable" => Ok(ErrorKind::Retryable),
        "permanent" => Ok(ErrorKind::Permanent),
        _ => Err(StateError::InvalidState(format!(
            "invalid error kind: {value}"
        ))),
    }
}

fn spool_from_row(row: &tokio_postgres::Row) -> Result<SpoolEntry, StateError> {
    Ok(SpoolEntry {
        id: row.get(0),
        transaction_id: row.get(1),
        ack_lsn: row
            .get::<_, Option<i64>>(2)
            .map(|value| u64::from_ne_bytes(value.to_ne_bytes())),
        is_final_chunk: row.get(3),
        checkpoint_configs_json: row.get(4),
        payload_json: row.get(5),
        status: spool_status_from_str(row.get::<_, String>(6).as_str())?,
        retry_count: row.get(7),
        last_error: row.get(8),
    })
}

fn spool_status_from_str(value: &str) -> Result<SpoolStatus, StateError> {
    match value {
        "pending" => Ok(SpoolStatus::Pending),
        "processing" => Ok(SpoolStatus::Processing),
        "done" => Ok(SpoolStatus::Done),
        "failed" => Ok(SpoolStatus::Failed),
        _ => Err(StateError::InvalidState(format!(
            "invalid spool status: {value}"
        ))),
    }
}
