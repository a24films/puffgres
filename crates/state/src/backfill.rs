use std::str::FromStr;

use chrono::{DateTime, Utc};
use diesel::prelude::*;
use strum::{AsRefStr, Display, EnumString};

use crate::models::{BackfillProgressRow, NewBackfillProgress};
use crate::schema::backfill_progress;
use crate::{StateDb, StateError};

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
    fn from_row(row: &BackfillProgressRow) -> Result<Self, StateError> {
        let status = BackfillStatus::from_str(&row.status)
            .map_err(|e| StateError::InvalidState(format!("invalid status: {e}")))?;

        let started_at = row
            .started_at
            .as_deref()
            .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
            .map(|dt| dt.with_timezone(&Utc));

        let completed_at = row
            .completed_at
            .as_deref()
            .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
            .map(|dt| dt.with_timezone(&Utc));

        Ok(Self {
            config_name: row.config_name.clone(),
            last_id: row.last_id.clone(),
            total_rows: row.total_rows.map(|v| v as u64),
            processed_rows: row.processed_rows as u64,
            status,
            started_at,
            completed_at,
            error_message: row.error_message.clone(),
            watermark_lsn: row.watermark_lsn.map(|v| v as u64),
        })
    }
}

/// Trait for persisting backfill cursor state. Implemented by `StateDb` for
/// production use; tests can supply an in-memory implementation.
pub trait BackfillCheckpointer {
    fn load_progress(&mut self, config_name: &str) -> Result<Option<(String, u64)>, StateError>;

    fn save_progress(
        &mut self,
        config_name: &str,
        last_id: &str,
        processed_rows: u64,
    ) -> Result<(), StateError>;
}

impl BackfillCheckpointer for StateDb {
    fn load_progress(&mut self, config_name: &str) -> Result<Option<(String, u64)>, StateError> {
        self.load_backfill_cursor(config_name)
    }

    fn save_progress(
        &mut self,
        config_name: &str,
        last_id: &str,
        processed_rows: u64,
    ) -> Result<(), StateError> {
        self.save_backfill_cursor(config_name, last_id, processed_rows)
    }
}

impl StateDb {
    pub fn save_backfill_progress(
        &mut self,
        progress: &BackfillProgress,
    ) -> Result<(), StateError> {
        let started_at_str = progress.started_at.as_ref().map(|dt| dt.to_rfc3339());
        let completed_at_str = progress.completed_at.as_ref().map(|dt| dt.to_rfc3339());
        let new = NewBackfillProgress {
            config_name: &progress.config_name,
            last_id: progress.last_id.as_deref(),
            total_rows: progress.total_rows.map(|v| v as i64),
            processed_rows: progress.processed_rows as i64,
            status: progress.status.as_ref(),
            started_at: started_at_str.as_deref(),
            completed_at: completed_at_str.as_deref(),
            error_message: progress.error_message.as_deref(),
            watermark_lsn: progress.watermark_lsn.map(|v| v as i64),
        };

        diesel::replace_into(backfill_progress::table)
            .values(&new)
            .execute(&mut self.conn)?;

        Ok(())
    }

    /// Lightweight cursor load for the backfill loop — returns just (last_id, processed_rows).
    pub fn load_backfill_cursor(
        &mut self,
        config_name: &str,
    ) -> Result<Option<(String, u64)>, StateError> {
        let p = self.get_backfill_progress(config_name)?;
        Ok(p.and_then(|p| p.last_id.map(|id| (id, p.processed_rows))))
    }

    /// Lightweight cursor save for the backfill loop — saves cursor position as InProgress.
    pub fn save_backfill_cursor(
        &mut self,
        config_name: &str,
        last_id: &str,
        processed_rows: u64,
    ) -> Result<(), StateError> {
        self.save_backfill_progress(&BackfillProgress {
            config_name: config_name.to_string(),
            last_id: Some(last_id.to_string()),
            total_rows: None,
            processed_rows,
            status: BackfillStatus::InProgress,
            started_at: Some(Utc::now()),
            completed_at: None,
            error_message: None,
            watermark_lsn: None,
        })
    }

    pub fn get_backfill_progress(
        &mut self,
        config_name: &str,
    ) -> Result<Option<BackfillProgress>, StateError> {
        let row = backfill_progress::table
            .filter(backfill_progress::config_name.eq(config_name))
            .first::<BackfillProgressRow>(&mut self.conn)
            .optional()?;

        match row {
            Some(r) => Ok(Some(BackfillProgress::from_row(&r)?)),
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
        let mut db = StateDb::open(&path).unwrap();
        db.initialize().unwrap();
        (dir, db)
    }

    fn sample_config(name: &str) -> ConfigRecord {
        ConfigRecord {
            name: name.to_string(),
            namespace: name.to_string(),
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
        let (_dir, mut db) = setup_backfill_db();
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
        let (_dir, mut db) = setup_backfill_db();
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
        let (_dir, mut db) = setup_backfill_db();
        let config = sample_config("film");
        db.insert_config(&config).unwrap();

        let progress = sample_backfill_progress("film");
        db.save_backfill_progress(&progress).unwrap();

        assert!(db.get_backfill_progress("film").unwrap().is_some());

        diesel::delete(
            crate::schema::configs::table.filter(crate::schema::configs::name.eq("film")),
        )
        .execute(&mut db.conn)
        .unwrap();

        assert!(db.get_backfill_progress("film").unwrap().is_none());
    }

    #[test]
    fn get_nonexistent_backfill_progress_returns_none() {
        let (_dir, mut db) = setup_backfill_db();
        assert!(db.get_backfill_progress("nonexistent").unwrap().is_none());
    }

    #[test]
    fn backfill_progress_requires_valid_config() {
        let (_dir, mut db) = setup_backfill_db();
        let progress = sample_backfill_progress("nonexistent_config");

        let result = db.save_backfill_progress(&progress);
        assert!(result.is_err());
    }

    #[test]
    fn watermark_lsn_saved_and_retrieved() {
        let (_dir, mut db) = setup_backfill_db();
        let config = sample_config("film");
        db.insert_config(&config).unwrap();

        let mut progress = sample_backfill_progress("film");
        progress.watermark_lsn = Some(42_000);
        db.save_backfill_progress(&progress).unwrap();

        let retrieved = db.get_backfill_progress("film").unwrap().unwrap();
        assert_eq!(retrieved.watermark_lsn, Some(42_000));
    }

    #[test]
    fn load_backfill_cursor_returns_none_when_no_progress() {
        let (_dir, mut db) = setup_backfill_db();
        assert!(db.load_backfill_cursor("nonexistent").unwrap().is_none());
    }

    #[test]
    fn load_backfill_cursor_returns_none_when_no_last_id() {
        let (_dir, mut db) = setup_backfill_db();
        db.insert_config(&sample_config("film")).unwrap();

        let mut progress = sample_backfill_progress("film");
        progress.last_id = None;
        db.save_backfill_progress(&progress).unwrap();

        assert!(db.load_backfill_cursor("film").unwrap().is_none());
    }

    #[test]
    fn load_backfill_cursor_returns_id_and_count() {
        let (_dir, mut db) = setup_backfill_db();
        db.insert_config(&sample_config("film")).unwrap();
        db.save_backfill_progress(&sample_backfill_progress("film"))
            .unwrap();

        let (id, rows) = db.load_backfill_cursor("film").unwrap().unwrap();
        assert_eq!(id, "12345");
        assert_eq!(rows, 100);
    }

    #[test]
    fn save_backfill_cursor_creates_in_progress_record() {
        let (_dir, mut db) = setup_backfill_db();
        db.insert_config(&sample_config("film")).unwrap();

        db.save_backfill_cursor("film", "500", 250).unwrap();

        let p = db.get_backfill_progress("film").unwrap().unwrap();
        assert_eq!(p.last_id, Some("500".to_string()));
        assert_eq!(p.processed_rows, 250);
        assert_eq!(p.status, BackfillStatus::InProgress);
        assert!(p.started_at.is_some());
    }

    #[test]
    fn save_backfill_cursor_updates_existing() {
        let (_dir, mut db) = setup_backfill_db();
        db.insert_config(&sample_config("film")).unwrap();

        db.save_backfill_cursor("film", "100", 50).unwrap();
        db.save_backfill_cursor("film", "200", 100).unwrap();

        let (id, rows) = db.load_backfill_cursor("film").unwrap().unwrap();
        assert_eq!(id, "200");
        assert_eq!(rows, 100);
    }

    #[test]
    fn watermark_lsn_above_i32_max_roundtrips() {
        let (_dir, mut db) = setup_backfill_db();
        let config = sample_config("film");
        db.insert_config(&config).unwrap();

        let big_lsn: u64 = (i32::MAX as u64) + 5_000;
        let mut progress = sample_backfill_progress("film");
        progress.watermark_lsn = Some(big_lsn);
        db.save_backfill_progress(&progress).unwrap();

        let retrieved = db.get_backfill_progress("film").unwrap().unwrap();
        assert_eq!(retrieved.watermark_lsn, Some(big_lsn));
    }

    #[test]
    fn watermark_lsn_defaults_to_none() {
        let (_dir, mut db) = setup_backfill_db();
        let config = sample_config("film");
        db.insert_config(&config).unwrap();

        let progress = sample_backfill_progress("film");
        db.save_backfill_progress(&progress).unwrap();

        let retrieved = db.get_backfill_progress("film").unwrap().unwrap();
        assert_eq!(retrieved.watermark_lsn, None);
    }
}
