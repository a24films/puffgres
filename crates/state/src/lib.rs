mod backfill;
mod configs;
mod dlq;
mod epoch;
mod error;
mod models;
mod schema;
mod streaming_checkpoint;

#[cfg(test)]
mod test_helpers;

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use diesel::SqliteConnection;
use diesel::prelude::*;
use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};

pub use backfill::{BackfillCheckpointer, BackfillProgress, BackfillStatus};
pub use configs::ConfigRecord;
pub use dlq::{DlqEntry, DlqOperation, ErrorKind};
pub use error::StateError;
pub use streaming_checkpoint::StreamingCheckpoint;

const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");

struct Inner {
    conn: Mutex<SqliteConnection>,
    path: PathBuf,
}

/// Thread-safe via `Mutex` so it can be shared across pipeline phases.
/// The public API is async; internally each call defers to `spawn_blocking`
/// because Diesel's SQLite backend is synchronous.
#[derive(Clone)]
pub struct StateDb {
    inner: Arc<Inner>,
}

impl StateDb {
    pub async fn open(path: &Path) -> Result<Self, StateError> {
        let path = path.to_path_buf();
        tokio::task::spawn_blocking(move || Self::open_blocking(&path))
            .await
            .map_err(|e| StateError::InvalidState(format!("blocking task join failed: {e}")))?
    }

    fn open_blocking(path: &Path) -> Result<Self, StateError> {
        let url = path
            .to_str()
            .ok_or_else(|| StateError::InvalidState("database path is not valid UTF-8".into()))?;
        let mut conn = SqliteConnection::establish(url)?;

        // Enable WAL mode, a busy timeout for concurrent access, and foreign
        // keys, matching previous rusqlite behavior.
        diesel::sql_query("PRAGMA journal_mode = WAL")
            .execute(&mut conn)
            .ok();
        diesel::sql_query("PRAGMA busy_timeout = 5000")
            .execute(&mut conn)
            .ok();
        diesel::sql_query("PRAGMA foreign_keys = ON")
            .execute(&mut conn)
            .ok();
        // Request incremental auto-vacuum so freed pages can be reclaimed
        // without a full VACUUM. Takes effect immediately for new databases;
        // existing databases require a one-time VACUUM to convert.
        diesel::sql_query("PRAGMA auto_vacuum = INCREMENTAL")
            .execute(&mut conn)
            .ok();

        let mode: String = diesel::sql_query("PRAGMA journal_mode")
            .get_result::<JournalModeResult>(&mut conn)
            .map(|r| r.journal_mode)
            .unwrap_or_default();
        if mode != "wal" {
            eprintln!("warning: expected SQLite WAL mode but got '{mode}'");
        }

        conn.run_pending_migrations(MIGRATIONS)
            .map_err(|e| StateError::Migration(e.to_string()))?;

        let auto_vacuum: i32 = diesel::sql_query("PRAGMA auto_vacuum")
            .get_result::<AutoVacuumResult>(&mut conn)
            .map(|r| r.auto_vacuum)
            .unwrap_or(0);
        if auto_vacuum != 2 {
            eprintln!(
                "warning: auto_vacuum is not INCREMENTAL (mode={auto_vacuum}); \
                 run VACUUM once to enable incremental space reclamation"
            );
        }

        Ok(Self {
            inner: Arc::new(Inner {
                conn: Mutex::new(conn),
                path: path.to_path_buf(),
            }),
        })
    }

    pub fn path(&self) -> &Path {
        &self.inner.path
    }

    /// Run a synchronous Diesel closure on the blocking pool, holding the
    /// connection mutex for the duration of the call.
    pub(crate) async fn run_blocking<F, T>(&self, f: F) -> Result<T, StateError>
    where
        F: FnOnce(&mut SqliteConnection) -> Result<T, StateError> + Send + 'static,
        T: Send + 'static,
    {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let mut conn = inner
                .conn
                .lock()
                .map_err(|_| StateError::InvalidState("state db mutex poisoned".into()))?;
            f(&mut conn)
        })
        .await
        .map_err(|e| StateError::InvalidState(format!("blocking task join failed: {e}")))?
    }

    pub async fn reset(&self) -> Result<(), StateError> {
        self.run_blocking(|conn| {
            conn.transaction::<_, StateError, _>(|conn| {
                diesel::delete(schema::dlq::table).execute(conn)?;
                diesel::delete(schema::backfill_progress::table).execute(conn)?;
                diesel::delete(schema::runtime_state::table).execute(conn)?;
                diesel::delete(schema::streaming_checkpoints::table).execute(conn)?;
                diesel::delete(schema::configs::table).execute(conn)?;
                Ok(())
            })
        })
        .await
    }

    /// Reclaim free pages from the database file. Only effective when
    /// auto_vacuum is set to INCREMENTAL.
    pub async fn incremental_vacuum(&self) -> Result<(), StateError> {
        self.run_blocking(|conn| {
            diesel::sql_query("PRAGMA incremental_vacuum").execute(conn)?;
            Ok(())
        })
        .await
    }

    /// Run periodic maintenance: clean stale permanent DLQ entries and
    /// reclaim freed disk space via incremental vacuum.
    pub async fn run_maintenance(&self, dlq_max_age_hours: u64) -> Result<u64, StateError> {
        let cleaned = self.clear_old_permanent_entries(dlq_max_age_hours).await?;
        self.incremental_vacuum().await?;
        Ok(cleaned)
    }

    pub async fn verify_startup_roundtrip(&self) -> Result<(), StateError> {
        let pid = std::process::id();
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| StateError::InvalidState(format!("system clock error: {e}")))?
            .as_millis();
        let probe_key = format!("startup_probe_{}_{}", pid, ts);
        let probe_value = format!("{}-{}", pid, ts);
        let updated_at = chrono::Utc::now().timestamp_millis();
        let path_display = self.inner.path.display().to_string();

        let probe_key_clone = probe_key.clone();
        let probe_value_clone = probe_value.clone();
        let stored = self
            .run_blocking(move |conn| {
                diesel::sql_query(
                    "INSERT INTO runtime_state (key, value, updated_at)
                     VALUES (?, ?, ?)
                     ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at",
                )
                .bind::<diesel::sql_types::Text, _>(&probe_key_clone)
                .bind::<diesel::sql_types::Text, _>(&probe_value_clone)
                .bind::<diesel::sql_types::BigInt, _>(updated_at)
                .execute(conn)?;

                let stored = diesel::sql_query("SELECT value FROM runtime_state WHERE key = ?")
                    .bind::<diesel::sql_types::Text, _>(&probe_key_clone)
                    .get_result::<RuntimeStateValue>(conn)?
                    .value;

                // Clean up the probe row — it's only needed for this check.
                diesel::sql_query("DELETE FROM runtime_state WHERE key = ?")
                    .bind::<diesel::sql_types::Text, _>(&probe_key_clone)
                    .execute(conn)
                    .ok();

                Ok(stored)
            })
            .await?;

        if stored != probe_value {
            return Err(StateError::InvalidState(format!(
                "state database roundtrip verification failed for {path_display}"
            )));
        }

        tracing::info!(
            state_db_path = %path_display,
            "state database startup roundtrip check passed"
        );

        Ok(())
    }
}

#[derive(QueryableByName)]
struct JournalModeResult {
    #[diesel(sql_type = diesel::sql_types::Text)]
    journal_mode: String,
}

#[derive(QueryableByName)]
struct AutoVacuumResult {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    auto_vacuum: i32,
}

#[derive(QueryableByName)]
struct RuntimeStateValue {
    #[diesel(sql_type = diesel::sql_types::Text)]
    value: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn open_creates_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");

        assert!(!path.exists());
        let db = StateDb::open(&path).await.unwrap();
        assert!(path.exists());
        assert_eq!(db.path(), path.as_path());
    }

    #[tokio::test]
    async fn open_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        std::fs::write(&path, "").unwrap();

        let db = StateDb::open(&path).await.unwrap();
        assert_eq!(db.path(), path.as_path());
    }

    #[tokio::test]
    async fn open_creates_all_tables() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = StateDb::open(&path).await.unwrap();

        // Verify each table exists by performing a no-op count via the public API.
        assert_eq!(db.list_configs().await.unwrap().len(), 0);
        assert_eq!(db.list_streaming_checkpoints().await.unwrap().len(), 0);
        assert_eq!(db.list_dlq_entries(None, 100).await.unwrap().len(), 0);
        assert!(
            db.get_backfill_progress("nonexistent")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn reset_clears_all_data() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = StateDb::open(&path).await.unwrap();

        let config = ConfigRecord {
            name: "film".to_string(),
            namespace: "film".to_string(),
            content_hash: "abc".to_string(),
            transform_hash: None,
            applied_at: chrono::Utc::now(),
            tombstone_applied_at: None,
            namespace_prefix: None,
        };
        db.insert_config(&config).await.unwrap();
        assert_eq!(db.list_configs().await.unwrap().len(), 1);

        db.reset().await.unwrap();
        assert_eq!(db.list_configs().await.unwrap().len(), 0);
    }

    #[tokio::test]
    async fn reset_on_empty_tables() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = StateDb::open(&path).await.unwrap();

        db.reset().await.unwrap();
        assert_eq!(db.list_configs().await.unwrap().len(), 0);
    }

    #[tokio::test]
    async fn startup_roundtrip_succeeds() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = StateDb::open(&path).await.unwrap();

        db.verify_startup_roundtrip().await.unwrap();
    }

    #[tokio::test]
    async fn reset_clears_dlq_and_backfill() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = StateDb::open(&path).await.unwrap();

        let config = ConfigRecord {
            name: "film".to_string(),
            namespace: "film".to_string(),
            content_hash: "abc".to_string(),
            transform_hash: None,
            applied_at: chrono::Utc::now(),
            tombstone_applied_at: None,
            namespace_prefix: None,
        };
        db.insert_config(&config).await.unwrap();

        let dlq_entry = DlqEntry::retryable(
            "film",
            100,
            DlqOperation::Insert,
            Some(r#"{"Uint":1}"#.to_string()),
            "boom",
        );
        db.insert_dlq_entry(&dlq_entry).await.unwrap();
        assert_eq!(db.dlq_count(None).await.unwrap(), 1);

        let backfill = BackfillProgress {
            config_name: "film".to_string(),
            last_id: None,
            total_rows: None,
            processed_rows: 0,
            status: BackfillStatus::Pending,
            started_at: None,
            completed_at: None,
            error_message: None,
            watermark_lsn: None,
        };
        db.save_backfill_progress(&backfill).await.unwrap();
        assert!(db.get_backfill_progress("film").await.unwrap().is_some());

        db.reset().await.unwrap();

        assert_eq!(db.dlq_count(None).await.unwrap(), 0);
        assert!(db.get_backfill_progress("film").await.unwrap().is_none());
        assert_eq!(db.list_configs().await.unwrap().len(), 0);
    }

    #[tokio::test]
    async fn open_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");

        StateDb::open(&path).await.unwrap();
        StateDb::open(&path).await.unwrap();
        StateDb::open(&path).await.unwrap();
    }
}
