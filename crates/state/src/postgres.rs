use std::time::{SystemTime, UNIX_EPOCH};

use pg::connect::{PgConnection, quote_identifier};
use pg::schema_bootstrap::{PUFFGRES_SCHEMA, ensure_schema, ensure_state_tables};

use crate::{BackfillProgress, BackfillStatus, StateError, StreamingCheckpoint, epoch};

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
}
