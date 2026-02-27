use std::str::FromStr;

use chrono::{DateTime, Utc};
use rusqlite::{Row, params};
use strum::{AsRefStr, Display, EnumString};

use crate::{StateDb, StateError};

const BACKFILL_SELECT_COLS: &str = "config_name, last_id, total_rows, processed_rows, status, started_at, completed_at, error_message, watermark_lsn";
const COL_CONFIG_NAME: usize = 0;
const COL_LAST_ID: usize = 1;
const COL_TOTAL_ROWS: usize = 2;
const COL_PROCESSED_ROWS: usize = 3;
const COL_STATUS: usize = 4;
const COL_STARTED_AT: usize = 5;
const COL_COMPLETED_AT: usize = 6;
const COL_ERROR_MESSAGE: usize = 7;
const COL_WATERMARK_LSN: usize = 8;

#[derive(Debug, Clone, PartialEq, Eq, Display, EnumString, AsRefStr)]
#[strum(serialize_all = "snake_case")]
pub enum BackfillStatus {
    Pending,
    InProgress,
    Completed,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackfillProgress {
    pub config_name: String,
    pub last_id: Option<String>,
    pub total_rows: Option<u64>,
    pub processed_rows: u64,
    pub status: BackfillStatus,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub error_message: Option<String>,
    pub watermark_lsn: Option<u64>,
}

impl BackfillProgress {
    fn from_row(row: &Row) -> Result<Self, rusqlite::Error> {
        let status_str: String = row.get(COL_STATUS)?;
        let status = BackfillStatus::from_str(&status_str)
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;

        let started_at = row
            .get::<_, Option<String>>(COL_STARTED_AT)?
            .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
            .map(|dt| dt.with_timezone(&Utc));

        let completed_at = row
            .get::<_, Option<String>>(COL_COMPLETED_AT)?
            .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
            .map(|dt| dt.with_timezone(&Utc));

        Ok(Self {
            config_name: row.get(COL_CONFIG_NAME)?,
            last_id: row.get(COL_LAST_ID)?,
            total_rows: row.get::<_, Option<i64>>(COL_TOTAL_ROWS)?.map(|v| v as u64),
            processed_rows: row.get::<_, i64>(COL_PROCESSED_ROWS)? as u64,
            status,
            started_at,
            completed_at,
            error_message: row.get(COL_ERROR_MESSAGE)?,
            watermark_lsn: row
                .get::<_, Option<i64>>(COL_WATERMARK_LSN)?
                .map(|v| v as u64),
        })
    }
}

impl StateDb {
    pub fn ensure_backfill_table(&self) -> Result<(), StateError> {
        self.conn().execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS backfill_progress (
                config_name TEXT PRIMARY KEY,
                last_id TEXT,
                total_rows INTEGER,
                processed_rows INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL CHECK (status IN ('pending', 'in_progress', 'completed', 'failed')),
                started_at TEXT,
                completed_at TEXT,
                error_message TEXT,
                watermark_lsn INTEGER,
                FOREIGN KEY (config_name) REFERENCES configs(name) ON DELETE CASCADE
            );
            "#,
        )?;
        Ok(())
    }

    pub fn save_backfill_progress(&self, progress: &BackfillProgress) -> Result<(), StateError> {
        self.conn().execute(
            &format!(
                "INSERT INTO backfill_progress ({}) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                 ON CONFLICT(config_name) DO UPDATE SET
                 last_id = excluded.last_id,
                 total_rows = excluded.total_rows,
                 processed_rows = excluded.processed_rows,
                 status = excluded.status,
                 started_at = excluded.started_at,
                 completed_at = excluded.completed_at,
                 error_message = excluded.error_message,
                 watermark_lsn = excluded.watermark_lsn",
                BACKFILL_SELECT_COLS
            ),
            params![
                progress.config_name,
                progress.last_id,
                progress.total_rows.map(|v| v as i64),
                progress.processed_rows as i64,
                progress.status.as_ref(),
                progress.started_at.as_ref().map(|dt| dt.to_rfc3339()),
                progress.completed_at.as_ref().map(|dt| dt.to_rfc3339()),
                progress.error_message,
                progress.watermark_lsn.map(|v| v as i64),
            ],
        )?;
        Ok(())
    }

    pub fn get_backfill_progress(
        &self,
        config_name: &str,
    ) -> Result<Option<BackfillProgress>, StateError> {
        let mut stmt = self.conn().prepare(&format!(
            "SELECT {} FROM backfill_progress WHERE config_name = ?1",
            BACKFILL_SELECT_COLS
        ))?;

        let mut rows = stmt.query(params![config_name])?;
        match rows.next()? {
            Some(row) => Ok(Some(BackfillProgress::from_row(row)?)),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ConfigRecord;

    fn setup_backfill_db() -> (tempfile::TempDir, StateDb) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = StateDb::open(&path).unwrap();
        db.ensure_configs_table().unwrap();
        db.ensure_backfill_table().unwrap();
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

    fn sample_backfill_progress(config_name: &str) -> BackfillProgress {
        BackfillProgress {
            config_name: config_name.to_string(),
            last_id: Some("12345".to_string()),
            total_rows: Some(1000),
            processed_rows: 100,
            status: BackfillStatus::InProgress,
            started_at: Some(Utc::now()),
            completed_at: None,
            error_message: None,
            watermark_lsn: None,
        }
    }

    #[test]
    fn save_and_retrieve_backfill_progress() {
        let (_dir, db) = setup_backfill_db();
        let config = sample_config("film");
        db.insert_config(&config).unwrap();

        let progress = sample_backfill_progress("film");
        db.save_backfill_progress(&progress).unwrap();

        let retrieved = db.get_backfill_progress("film").unwrap().unwrap();
        assert_eq!(retrieved.config_name, "film");
        assert_eq!(retrieved.last_id, Some("12345".to_string()));
        assert_eq!(retrieved.total_rows, Some(1000));
        assert_eq!(retrieved.processed_rows, 100);
        assert_eq!(retrieved.status, BackfillStatus::InProgress);
        assert!(retrieved.started_at.is_some());
        assert!(retrieved.completed_at.is_none());
    }

    #[test]
    fn update_backfill_progress_upsert() {
        let (_dir, db) = setup_backfill_db();
        let config = sample_config("film");
        db.insert_config(&config).unwrap();

        let mut progress1 = sample_backfill_progress("film");
        progress1.processed_rows = 100;
        progress1.status = BackfillStatus::InProgress;
        db.save_backfill_progress(&progress1).unwrap();

        let mut progress2 = sample_backfill_progress("film");
        progress2.processed_rows = 500;
        progress2.last_id = Some("67890".to_string());
        db.save_backfill_progress(&progress2).unwrap();

        let retrieved = db.get_backfill_progress("film").unwrap().unwrap();
        assert_eq!(retrieved.processed_rows, 500);
        assert_eq!(retrieved.last_id, Some("67890".to_string()));
    }

    #[test]
    fn backfill_progress_deleted_when_config_deleted() {
        let (_dir, db) = setup_backfill_db();
        let config = sample_config("film");
        db.insert_config(&config).unwrap();

        let progress = sample_backfill_progress("film");
        db.save_backfill_progress(&progress).unwrap();

        assert!(db.get_backfill_progress("film").unwrap().is_some());

        db.conn()
            .execute("DELETE FROM configs WHERE name = ?1", params!["film"])
            .unwrap();

        assert!(db.get_backfill_progress("film").unwrap().is_none());
    }

    #[test]
    fn get_nonexistent_backfill_progress_returns_none() {
        let (_dir, db) = setup_backfill_db();
        assert!(db.get_backfill_progress("nonexistent").unwrap().is_none());
    }

    #[test]
    fn backfill_progress_requires_valid_config() {
        let (_dir, db) = setup_backfill_db();
        let progress = sample_backfill_progress("nonexistent_config");

        let result = db.save_backfill_progress(&progress);
        assert!(result.is_err());
    }

    #[test]
    fn watermark_lsn_saved_and_retrieved() {
        let (_dir, db) = setup_backfill_db();
        let config = sample_config("film");
        db.insert_config(&config).unwrap();

        let mut progress = sample_backfill_progress("film");
        progress.watermark_lsn = Some(42_000);
        db.save_backfill_progress(&progress).unwrap();

        let retrieved = db.get_backfill_progress("film").unwrap().unwrap();
        assert_eq!(retrieved.watermark_lsn, Some(42_000));
    }

    #[test]
    fn watermark_lsn_defaults_to_none() {
        let (_dir, db) = setup_backfill_db();
        let config = sample_config("film");
        db.insert_config(&config).unwrap();

        let progress = sample_backfill_progress("film");
        db.save_backfill_progress(&progress).unwrap();

        let retrieved = db.get_backfill_progress("film").unwrap().unwrap();
        assert_eq!(retrieved.watermark_lsn, None);
    }
}
