use chrono::{DateTime, Utc};
use rusqlite::{Row, params};

use crate::{StateDb, StateError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamingCheckpoint {
    pub config_name: String,
    pub lsn: u64,
    pub events_processed: u64,
    pub updated_at: DateTime<Utc>,
}

impl StreamingCheckpoint {
    fn from_row(row: &Row) -> Result<Self, rusqlite::Error> {
        let updated_at_str: String = row.get(3)?;
        let updated_at = DateTime::parse_from_rfc3339(&updated_at_str)
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now());

        Ok(Self {
            config_name: row.get(0)?,
            lsn: row.get::<_, i64>(1)? as u64,
            events_processed: row.get::<_, i64>(2)? as u64,
            updated_at,
        })
    }
}

const CHECKPOINT_SELECT_COLS: &str = "config_name, lsn, events_processed, updated_at";

impl StateDb {
    pub fn ensure_streaming_checkpoints_table(&self) -> Result<(), StateError> {
        self.conn().execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS streaming_checkpoints (
                config_name TEXT PRIMARY KEY,
                lsn INTEGER NOT NULL,
                events_processed INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (config_name) REFERENCES configs(name) ON DELETE CASCADE
            );
            "#,
        )?;
        Ok(())
    }

    pub fn save_streaming_checkpoint(
        &self,
        checkpoint: &StreamingCheckpoint,
    ) -> Result<(), StateError> {
        self.conn().execute(
            &format!(
                "INSERT INTO streaming_checkpoints ({}) VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(config_name) DO UPDATE SET
                 lsn = excluded.lsn,
                 events_processed = excluded.events_processed,
                 updated_at = excluded.updated_at",
                CHECKPOINT_SELECT_COLS
            ),
            params![
                checkpoint.config_name,
                checkpoint.lsn as i64,
                checkpoint.events_processed as i64,
                checkpoint.updated_at.to_rfc3339(),
            ],
        )?;
        Ok(())
    }

    pub fn get_streaming_checkpoint(
        &self,
        config_name: &str,
    ) -> Result<Option<StreamingCheckpoint>, StateError> {
        let mut stmt = self.conn().prepare(&format!(
            "SELECT {} FROM streaming_checkpoints WHERE config_name = ?1",
            CHECKPOINT_SELECT_COLS
        ))?;

        let mut rows = stmt.query(params![config_name])?;
        match rows.next()? {
            Some(row) => Ok(Some(StreamingCheckpoint::from_row(row)?)),
            None => Ok(None),
        }
    }

    pub fn delete_streaming_checkpoint(&self, config_name: &str) -> Result<bool, StateError> {
        let rows_affected = self.conn().execute(
            "DELETE FROM streaming_checkpoints WHERE config_name = ?1",
            params![config_name],
        )?;
        Ok(rows_affected > 0)
    }

    pub fn list_streaming_checkpoints(&self) -> Result<Vec<StreamingCheckpoint>, StateError> {
        let mut stmt = self.conn().prepare(&format!(
            "SELECT {} FROM streaming_checkpoints ORDER BY config_name",
            CHECKPOINT_SELECT_COLS
        ))?;

        let rows = stmt.query_map([], StreamingCheckpoint::from_row)?;

        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ConfigRecord;

    fn setup_streaming_checkpoints_db() -> (tempfile::TempDir, StateDb) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = StateDb::open(&path).unwrap();
        db.ensure_configs_table().unwrap();
        db.ensure_streaming_checkpoints_table().unwrap();
        (dir, db)
    }

    fn sample_config(name: &str) -> ConfigRecord {
        ConfigRecord {
            name: name.to_string(),
            version: 1,
            namespace: format!("{}_v1", name),
            content_hash: "abc123".to_string(),
            transform_hash: None,
            applied_at: Utc::now(),
        }
    }

    fn sample_streaming_checkpoint(
        config_name: &str,
        lsn: u64,
        events: u64,
    ) -> StreamingCheckpoint {
        StreamingCheckpoint {
            config_name: config_name.to_string(),
            lsn,
            events_processed: events,
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn save_and_retrieve_streaming_checkpoint() {
        let (_dir, db) = setup_streaming_checkpoints_db();
        let config = sample_config("film");
        db.insert_config(&config).unwrap();

        let checkpoint = sample_streaming_checkpoint("film", 1000, 50);
        db.save_streaming_checkpoint(&checkpoint).unwrap();

        let retrieved = db.get_streaming_checkpoint("film").unwrap().unwrap();
        assert_eq!(retrieved.config_name, "film");
        assert_eq!(retrieved.lsn, 1000);
        assert_eq!(retrieved.events_processed, 50);
    }

    #[test]
    fn update_existing_streaming_checkpoint() {
        let (_dir, db) = setup_streaming_checkpoints_db();
        let config = sample_config("film");
        db.insert_config(&config).unwrap();

        let checkpoint1 = sample_streaming_checkpoint("film", 1000, 50);
        db.save_streaming_checkpoint(&checkpoint1).unwrap();

        let checkpoint2 = sample_streaming_checkpoint("film", 2000, 100);
        db.save_streaming_checkpoint(&checkpoint2).unwrap();

        let retrieved = db.get_streaming_checkpoint("film").unwrap().unwrap();
        assert_eq!(retrieved.lsn, 2000);
        assert_eq!(retrieved.events_processed, 100);

        let all = db.list_streaming_checkpoints().unwrap();
        assert_eq!(all.len(), 1);
    }

    #[test]
    fn streaming_checkpoint_deleted_when_config_deleted() {
        let (_dir, db) = setup_streaming_checkpoints_db();
        let config = sample_config("film");
        db.insert_config(&config).unwrap();

        let checkpoint = sample_streaming_checkpoint("film", 1000, 50);
        db.save_streaming_checkpoint(&checkpoint).unwrap();

        assert!(db.get_streaming_checkpoint("film").unwrap().is_some());

        db.conn()
            .execute("DELETE FROM configs WHERE name = ?1", params!["film"])
            .unwrap();

        assert!(db.get_streaming_checkpoint("film").unwrap().is_none());
    }

    #[test]
    fn delete_streaming_checkpoint_returns_true_when_exists() {
        let (_dir, db) = setup_streaming_checkpoints_db();
        let config = sample_config("film");
        db.insert_config(&config).unwrap();

        let checkpoint = sample_streaming_checkpoint("film", 1000, 50);
        db.save_streaming_checkpoint(&checkpoint).unwrap();

        let deleted = db.delete_streaming_checkpoint("film").unwrap();
        assert!(deleted);
        assert!(db.get_streaming_checkpoint("film").unwrap().is_none());
    }

    #[test]
    fn delete_streaming_checkpoint_returns_false_when_not_exists() {
        let (_dir, db) = setup_streaming_checkpoints_db();
        let deleted = db.delete_streaming_checkpoint("nonexistent").unwrap();
        assert!(!deleted);
    }

    #[test]
    fn list_multiple_streaming_checkpoints() {
        let (_dir, db) = setup_streaming_checkpoints_db();

        db.insert_config(&sample_config("alpha")).unwrap();
        db.insert_config(&sample_config("beta")).unwrap();
        db.insert_config(&sample_config("gamma")).unwrap();

        db.save_streaming_checkpoint(&sample_streaming_checkpoint("alpha", 100, 10))
            .unwrap();
        db.save_streaming_checkpoint(&sample_streaming_checkpoint("beta", 200, 20))
            .unwrap();
        db.save_streaming_checkpoint(&sample_streaming_checkpoint("gamma", 300, 30))
            .unwrap();

        let checkpoints = db.list_streaming_checkpoints().unwrap();
        assert_eq!(checkpoints.len(), 3);
        assert_eq!(checkpoints[0].config_name, "alpha");
        assert_eq!(checkpoints[0].lsn, 100);
        assert_eq!(checkpoints[1].config_name, "beta");
        assert_eq!(checkpoints[1].lsn, 200);
        assert_eq!(checkpoints[2].config_name, "gamma");
        assert_eq!(checkpoints[2].lsn, 300);
    }

    #[test]
    fn get_nonexistent_streaming_checkpoint_returns_none() {
        let (_dir, db) = setup_streaming_checkpoints_db();
        assert!(
            db.get_streaming_checkpoint("nonexistent")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn streaming_checkpoint_requires_valid_config() {
        let (_dir, db) = setup_streaming_checkpoints_db();
        let checkpoint = sample_streaming_checkpoint("nonexistent_config", 1000, 50);

        let result = db.save_streaming_checkpoint(&checkpoint);
        assert!(result.is_err());
    }
}
