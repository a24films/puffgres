use std::path::{Path, PathBuf};

use rusqlite::Connection;

use crate::StateError;

pub struct StateDb {
    conn: Connection,
    path: PathBuf,
}

impl StateDb {
    pub fn open(path: &Path) -> Result<Self, StateError> {
        let conn = Connection::open(path)?;
        Ok(Self {
            conn,
            path: path.to_path_buf(),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn conn(&self) -> &Connection {
        &self.conn
    }

    pub fn initialize(&self) -> Result<(), StateError> {
        self.ensure_configs_table()?;
        self.ensure_streaming_checkpoints_table()?;
        self.ensure_dlq_table()?;
        self.ensure_backfill_table()?;
        Ok(())
    }

    pub fn reset(&self) -> Result<(), StateError> {
        self.conn.execute_batch(
            "DELETE FROM dlq; DELETE FROM backfill_progress; DELETE FROM streaming_checkpoints; DELETE FROM configs;",
        )?;
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
    fn initialize_creates_all_tables() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = StateDb::open(&path).unwrap();

        db.initialize().unwrap();

        // Verify each table exists by checking sqlite_master
        let mut stmt = db
            .conn
            .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            .unwrap();
        let tables: Vec<String> = stmt
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert!(tables.contains(&"configs".to_string()));
        assert!(tables.contains(&"streaming_checkpoints".to_string()));
        assert!(tables.contains(&"dlq".to_string()));
        assert!(tables.contains(&"backfill_progress".to_string()));
    }

    #[test]
    fn reset_clears_all_data() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = StateDb::open(&path).unwrap();
        db.initialize().unwrap();

        // Insert a config
        let config = crate::ConfigRecord {
            name: "film".to_string(),
            version: 1,
            namespace: "film_v1".to_string(),
            content_hash: "abc".to_string(),
            transform_hash: None,
            applied_at: chrono::Utc::now(),
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
        db.initialize().unwrap();

        db.reset().unwrap();
        assert_eq!(db.list_configs().unwrap().len(), 0);
    }

    #[test]
    fn reset_clears_dlq_and_backfill() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = StateDb::open(&path).unwrap();
        db.initialize().unwrap();

        // Insert a config, then a DLQ entry and backfill progress
        let config = crate::ConfigRecord {
            name: "film".to_string(),
            version: 1,
            namespace: "film_v1".to_string(),
            content_hash: "abc".to_string(),
            transform_hash: None,
            applied_at: chrono::Utc::now(),
        };
        db.insert_config(&config).unwrap();

        let dlq_entry = crate::DlqEntry {
            id: 0,
            config_name: "film".to_string(),
            lsn: 100,
            event_json: r#"{"test": true}"#.to_string(),
            doc_id: Some(r#"{"Uint":1}"#.to_string()),
            error_message: "boom".to_string(),
            error_kind: crate::ErrorKind::Retryable,
            retry_count: 0,
            created_at: chrono::Utc::now(),
            last_retry_at: None,
            permanent_at: None,
        };
        db.insert_dlq_entry(&dlq_entry).unwrap();
        assert_eq!(db.dlq_count(None).unwrap(), 1);

        let backfill = crate::BackfillProgress {
            config_name: "film".to_string(),
            last_id: None,
            total_rows: None,
            processed_rows: 0,
            status: crate::BackfillStatus::Pending,
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
    fn initialize_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = StateDb::open(&path).unwrap();

        // Should not error with multiple initializations
        db.initialize().unwrap();
        db.initialize().unwrap();
        db.initialize().unwrap();
    }
}
