mod backfill;
mod configs;
mod dlq;
mod error;
mod models;
mod schema;
mod streaming_checkpoint;

use std::path::{Path, PathBuf};

use diesel::SqliteConnection;
use diesel::prelude::*;
use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};

pub use backfill::{BackfillCheckpointer, BackfillProgress, BackfillStatus};
pub use configs::ConfigRecord;
pub use dlq::{DlqEntry, ErrorKind};
pub use error::StateError;
pub use streaming_checkpoint::StreamingCheckpoint;

const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");

pub struct StateDb {
    conn: SqliteConnection,
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

        conn.run_pending_migrations(MIGRATIONS)
            .map_err(|e| StateError::Migration(e.to_string()))?;

        Ok(Self {
            conn,
            path: path.to_path_buf(),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn reset(&mut self) -> Result<(), StateError> {
        diesel::delete(schema::dlq::table).execute(&mut self.conn)?;
        diesel::delete(schema::backfill_progress::table).execute(&mut self.conn)?;
        diesel::delete(schema::streaming_checkpoints::table).execute(&mut self.conn)?;
        diesel::delete(schema::configs::table).execute(&mut self.conn)?;
        Ok(())
    }
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
        let mut db = StateDb::open(&path).unwrap();

        // Verify each table exists by querying it via the ORM.
        assert_eq!(
            schema::configs::table
                .count()
                .get_result::<i64>(&mut db.conn)
                .unwrap(),
            0
        );
        assert_eq!(
            schema::streaming_checkpoints::table
                .count()
                .get_result::<i64>(&mut db.conn)
                .unwrap(),
            0
        );
        assert_eq!(
            schema::dlq::table
                .count()
                .get_result::<i64>(&mut db.conn)
                .unwrap(),
            0
        );
        assert_eq!(
            schema::backfill_progress::table
                .count()
                .get_result::<i64>(&mut db.conn)
                .unwrap(),
            0
        );
    }

    #[test]
    fn reset_clears_all_data() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let mut db = StateDb::open(&path).unwrap();

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
        let mut db = StateDb::open(&path).unwrap();

        db.reset().unwrap();
        assert_eq!(db.list_configs().unwrap().len(), 0);
    }

    #[test]
    fn reset_clears_dlq_and_backfill() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let mut db = StateDb::open(&path).unwrap();

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
