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
pub use dlq::{DlqEntry, ErrorKind};
pub use error::StateError;
pub use streaming_checkpoint::StreamingCheckpoint;

const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");

/// Thread-safe via `Mutex` so it can be shared across pipeline phases.
#[derive(Clone)]
pub struct StateDb {
    conn: Arc<Mutex<SqliteConnection>>,
    path: PathBuf,
}

impl StateDb {
    pub fn open(path: &Path) -> Result<Self, StateError> {
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

        let mode: String = diesel::sql_query("PRAGMA journal_mode")
            .get_result::<JournalModeResult>(&mut conn)
            .map(|r| r.journal_mode)
            .unwrap_or_default();
        if mode != "wal" {
            eprintln!("warning: expected SQLite WAL mode but got '{mode}'");
        }

        conn.run_pending_migrations(MIGRATIONS)
            .map_err(|e| StateError::Migration(e.to_string()))?;

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            path: path.to_path_buf(),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn lock(&self) -> Result<std::sync::MutexGuard<'_, SqliteConnection>, StateError> {
        self.conn
            .lock()
            .map_err(|_| StateError::InvalidState("state db mutex poisoned".into()))
    }

    pub fn transaction<F, T>(&self, f: F) -> Result<T, StateError>
    where
        F: FnOnce(&mut SqliteConnection) -> Result<T, StateError>,
    {
        let mut conn = self.lock()?;
        conn.transaction(|conn| f(conn)).map_err(StateError::from)
    }

    pub fn reset(&self) -> Result<(), StateError> {
        self.transaction(|conn| {
            diesel::delete(schema::dlq::table).execute(conn)?;
            diesel::delete(schema::backfill_progress::table).execute(conn)?;
            diesel::delete(schema::runtime_state::table).execute(conn)?;
            diesel::delete(schema::streaming_checkpoints::table).execute(conn)?;
            diesel::delete(schema::configs::table).execute(conn)?;
            Ok(())
        })
    }

    pub fn verify_startup_roundtrip(&self) -> Result<(), StateError> {
        let pid = std::process::id();
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| StateError::InvalidState(format!("system clock error: {e}")))?
            .as_millis();
        let probe_key = format!("startup_probe_{}_{}", pid, ts);
        let probe_value = format!("{}-{}", pid, ts);
        let updated_at = chrono::Utc::now().timestamp_millis();

        let mut conn = self.lock()?;
        diesel::sql_query(
            "INSERT INTO runtime_state (key, value, updated_at)
             VALUES (?, ?, ?)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at",
        )
        .bind::<diesel::sql_types::Text, _>(&probe_key)
        .bind::<diesel::sql_types::Text, _>(&probe_value)
        .bind::<diesel::sql_types::BigInt, _>(updated_at)
        .execute(&mut *conn)?;

        let stored = diesel::sql_query("SELECT value FROM runtime_state WHERE key = ?")
            .bind::<diesel::sql_types::Text, _>(&probe_key)
            .get_result::<RuntimeStateValue>(&mut *conn)?
            .value;

        // Clean up the probe row — it's only needed for this check.
        diesel::sql_query("DELETE FROM runtime_state WHERE key = ?")
            .bind::<diesel::sql_types::Text, _>(&probe_key)
            .execute(&mut *conn)
            .ok();

        if stored != probe_value {
            return Err(StateError::InvalidState(format!(
                "state database roundtrip verification failed for {}",
                self.path.display()
            )));
        }

        tracing::info!(
            state_db_path = %self.path.display(),
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
struct RuntimeStateValue {
    #[diesel(sql_type = diesel::sql_types::Text)]
    value: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_creates_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");

        assert!(!path.exists());
        let db = StateDb::open(&path).unwrap();
        assert!(path.exists());
        assert_eq!(db.path(), path.as_path());
    }

    #[test]
    fn open_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        std::fs::write(&path, "").unwrap();

        let db = StateDb::open(&path).unwrap();
        assert_eq!(db.path(), path.as_path());
    }

    #[test]
    fn open_creates_all_tables() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = StateDb::open(&path).unwrap();

        // Verify each table exists by querying it via the ORM.
        assert_eq!(
            schema::configs::table
                .count()
                .get_result::<i64>(&mut *db.lock().unwrap())
                .unwrap(),
            0
        );
        assert_eq!(
            schema::streaming_checkpoints::table
                .count()
                .get_result::<i64>(&mut *db.lock().unwrap())
                .unwrap(),
            0
        );
        assert_eq!(
            schema::dlq::table
                .count()
                .get_result::<i64>(&mut *db.lock().unwrap())
                .unwrap(),
            0
        );
        assert_eq!(
            schema::runtime_state::table
                .count()
                .get_result::<i64>(&mut *db.lock().unwrap())
                .unwrap(),
            0
        );
        assert_eq!(
            schema::backfill_progress::table
                .count()
                .get_result::<i64>(&mut *db.lock().unwrap())
                .unwrap(),
            0
        );
    }

    #[test]
    fn reset_clears_all_data() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = StateDb::open(&path).unwrap();

        let config = ConfigRecord {
            name: "film".to_string(),

            namespace: "film".to_string(),
            content_hash: "abc".to_string(),
            transform_hash: None,
            applied_at: chrono::Utc::now(),
            tombstone_applied_at: None,
            namespace_prefix: None,
        };
        db.insert_config(&config).unwrap();
        assert_eq!(db.list_configs().unwrap().len(), 1);

        db.reset().unwrap();
        assert_eq!(db.list_configs().unwrap().len(), 0);
    }

    #[test]
    fn reset_on_empty_tables() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = StateDb::open(&path).unwrap();

        db.reset().unwrap();
        assert_eq!(db.list_configs().unwrap().len(), 0);
    }

    #[test]
    fn startup_roundtrip_succeeds() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = StateDb::open(&path).unwrap();

        // Should succeed without error — probe row is cleaned up after check.
        db.verify_startup_roundtrip().unwrap();

        // Probe row should be cleaned up.
        let count: i64 = schema::runtime_state::table
            .count()
            .get_result(&mut *db.lock().unwrap())
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn reset_clears_dlq_and_backfill() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = StateDb::open(&path).unwrap();

        let config = ConfigRecord {
            name: "film".to_string(),

            namespace: "film".to_string(),
            content_hash: "abc".to_string(),
            transform_hash: None,
            applied_at: chrono::Utc::now(),
            tombstone_applied_at: None,
            namespace_prefix: None,
        };
        db.insert_config(&config).unwrap();

        let dlq_entry = DlqEntry {
            id: 0,
            config_name: "film".to_string(),
            lsn: 100,
            event_json: r#"{"test": true}"#.to_string(),
            doc_id: Some(r#"{"Uint":1}"#.to_string()),
            error_message: "boom".to_string(),
            error_kind: ErrorKind::Retryable,
            retry_count: 0,
            created_at: chrono::Utc::now(),
            last_retry_at: None,
            permanent_at: None,
        };
        db.insert_dlq_entry(&dlq_entry).unwrap();
        assert_eq!(db.dlq_count(None).unwrap(), 1);

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
        db.save_backfill_progress(&backfill).unwrap();
        assert!(db.get_backfill_progress("film").unwrap().is_some());

        db.reset().unwrap();

        assert_eq!(db.dlq_count(None).unwrap(), 0);
        assert!(db.get_backfill_progress("film").unwrap().is_none());
        assert_eq!(db.list_configs().unwrap().len(), 0);
    }

    #[test]
    fn open_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");

        StateDb::open(&path).unwrap();
        StateDb::open(&path).unwrap();
        StateDb::open(&path).unwrap();
    }
}
