use std::str::FromStr;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use diesel::prelude::*;
use strum::{AsRefStr, Display, EnumString};

use crate::epoch;
use crate::models::{BackfillProgressRow, NewBackfillProgress};
use crate::pg_lsn::Lsn;
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

        let started_at = row.started_at.and_then(epoch::from_millis);
        let completed_at = row.completed_at.and_then(epoch::from_millis);

        let total_rows = row
            .total_rows
            .map(|v| {
                u64::try_from(v)
                    .map_err(|_| StateError::InvalidState(format!("negative total_rows: {v}")))
            })
            .transpose()?;
        let processed_rows = u64::try_from(row.processed_rows).map_err(|_| {
            StateError::InvalidState(format!("negative processed_rows: {}", row.processed_rows))
        })?;

        Ok(Self {
            config_name: row.config_name.clone(),
            last_id: row.last_id.clone(),
            total_rows,
            processed_rows,
            status,
            started_at,
            completed_at,
            error_message: row.error_message.clone(),
            watermark_lsn: row.watermark_lsn.map(u64::from),
        })
    }
}

/// Trait for persisting backfill cursor state. Implemented by `StateDb` for
/// production use; tests can supply an in-memory implementation.
#[async_trait]
pub trait BackfillCheckpointer: Send + Sync {
    async fn load_progress(&self, config_name: &str) -> Result<Option<(String, u64)>, StateError>;

    async fn save_progress(
        &self,
        config_name: &str,
        last_id: &str,
        processed_rows: u64,
    ) -> Result<(), StateError>;
}

#[async_trait]
impl BackfillCheckpointer for StateDb {
    async fn load_progress(&self, config_name: &str) -> Result<Option<(String, u64)>, StateError> {
        self.get_backfill_cursor(config_name).await
    }

    async fn save_progress(
        &self,
        config_name: &str,
        last_id: &str,
        processed_rows: u64,
    ) -> Result<(), StateError> {
        self.save_backfill_cursor(config_name, last_id, processed_rows)
            .await
    }
}

impl StateDb {
    pub async fn save_backfill_progress(
        &self,
        progress: &BackfillProgress,
    ) -> Result<(), StateError> {
        let p = progress.clone();
        self.run_blocking(move |conn| {
            let total_rows = p
                .total_rows
                .map(|v| {
                    i64::try_from(v).map_err(|_| {
                        StateError::InvalidState(format!("total_rows {v} exceeds i64::MAX"))
                    })
                })
                .transpose()?;
            let processed_rows = i64::try_from(p.processed_rows).map_err(|_| {
                StateError::InvalidState(format!(
                    "processed_rows {} exceeds i64::MAX",
                    p.processed_rows
                ))
            })?;

            let new = NewBackfillProgress {
                config_name: &p.config_name,
                last_id: p.last_id.as_deref(),
                total_rows,
                processed_rows,
                status: p.status.as_ref(),
                started_at: p.started_at.as_ref().map(epoch::to_millis),
                completed_at: p.completed_at.as_ref().map(epoch::to_millis),
                error_message: p.error_message.as_deref(),
                watermark_lsn: p.watermark_lsn.map(Lsn),
            };

            diesel::insert_into(backfill_progress::table)
                .values(&new)
                .on_conflict(backfill_progress::config_name)
                .do_update()
                .set(&new)
                .execute(conn)?;
            Ok(())
        })
        .await
    }

    /// Lightweight cursor load for the backfill loop — returns just (last_id, processed_rows).
    pub async fn get_backfill_cursor(
        &self,
        config_name: &str,
    ) -> Result<Option<(String, u64)>, StateError> {
        let p = self.get_backfill_progress(config_name).await?;
        Ok(p.and_then(|p| p.last_id.map(|id| (id, p.processed_rows))))
    }

    /// Lightweight cursor save for the backfill loop — saves cursor position as InProgress.
    /// Preserves existing `watermark_lsn` and `started_at` so a resumed backfill
    /// doesn't lose the original watermark.
    ///
    /// The read-modify-write is performed under a single lock acquisition so
    /// concurrent callers on cloned `StateDb` handles cannot regress cursor state.
    pub async fn save_backfill_cursor(
        &self,
        config_name: &str,
        last_id: &str,
        processed_rows: u64,
    ) -> Result<(), StateError> {
        let config_name = config_name.to_string();
        let last_id = last_id.to_string();
        self.run_blocking(move |conn| {
            // Read existing progress under the same lock.
            let existing = backfill_progress::table
                .filter(backfill_progress::config_name.eq(&config_name))
                .first::<BackfillProgressRow>(conn)
                .optional()?
                .map(|r| BackfillProgress::from_row(&r))
                .transpose()?;

            let started_at_millis = existing
                .as_ref()
                .and_then(|p| p.started_at)
                .or_else(|| Some(Utc::now()))
                .map(|dt| epoch::to_millis(&dt));

            let total_rows = existing
                .as_ref()
                .and_then(|p| p.total_rows)
                .map(|v| {
                    i64::try_from(v).map_err(|_| {
                        StateError::InvalidState(format!("total_rows {v} exceeds i64::MAX"))
                    })
                })
                .transpose()?;
            let processed_rows_i64 = i64::try_from(processed_rows).map_err(|_| {
                StateError::InvalidState(format!(
                    "processed_rows {processed_rows} exceeds i64::MAX"
                ))
            })?;

            let new = NewBackfillProgress {
                config_name: &config_name,
                last_id: Some(&last_id),
                total_rows,
                processed_rows: processed_rows_i64,
                status: BackfillStatus::InProgress.as_ref(),
                started_at: started_at_millis,
                completed_at: None,
                error_message: None,
                watermark_lsn: existing.as_ref().and_then(|p| p.watermark_lsn).map(Lsn),
            };

            diesel::insert_into(backfill_progress::table)
                .values(&new)
                .on_conflict(backfill_progress::config_name)
                .do_update()
                .set(&new)
                .execute(conn)?;
            Ok(())
        })
        .await
    }

    pub async fn get_backfill_progress(
        &self,
        config_name: &str,
    ) -> Result<Option<BackfillProgress>, StateError> {
        let config_name = config_name.to_string();
        self.run_blocking(move |conn| {
            let row = backfill_progress::table
                .filter(backfill_progress::config_name.eq(&config_name))
                .first::<BackfillProgressRow>(conn)
                .optional()?;
            match row {
                Some(r) => Ok(Some(BackfillProgress::from_row(&r)?)),
                None => Ok(None),
            }
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::{sample_config, setup_test_db};

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

    #[tokio::test]
    async fn save_and_retrieve_backfill_progress() {
        let db = setup_test_db().await;
        db.insert_config(&sample_config("film")).await.unwrap();

        let progress = sample_backfill_progress("film");
        db.save_backfill_progress(&progress).await.unwrap();

        let retrieved = db.get_backfill_progress("film").await.unwrap().unwrap();
        assert_eq!(retrieved.config_name, "film");
        assert_eq!(retrieved.last_id, Some("12345".to_string()));
        assert_eq!(retrieved.total_rows, Some(1000));
        assert_eq!(retrieved.processed_rows, 100);
        assert_eq!(retrieved.status, BackfillStatus::InProgress);
        assert!(retrieved.started_at.is_some());
        assert!(retrieved.completed_at.is_none());
    }

    #[tokio::test]
    async fn update_backfill_progress_upsert() {
        let db = setup_test_db().await;
        db.insert_config(&sample_config("film")).await.unwrap();

        let mut progress1 = sample_backfill_progress("film");
        progress1.processed_rows = 100;
        progress1.status = BackfillStatus::InProgress;
        db.save_backfill_progress(&progress1).await.unwrap();

        let mut progress2 = sample_backfill_progress("film");
        progress2.processed_rows = 500;
        progress2.last_id = Some("67890".to_string());
        db.save_backfill_progress(&progress2).await.unwrap();

        let retrieved = db.get_backfill_progress("film").await.unwrap().unwrap();
        assert_eq!(retrieved.processed_rows, 500);
        assert_eq!(retrieved.last_id, Some("67890".to_string()));
    }

    #[tokio::test]
    async fn backfill_progress_deleted_when_config_deleted() {
        let db = setup_test_db().await;
        db.insert_config(&sample_config("film")).await.unwrap();

        let progress = sample_backfill_progress("film");
        db.save_backfill_progress(&progress).await.unwrap();

        assert!(db.get_backfill_progress("film").await.unwrap().is_some());

        db.delete_config("film").await.unwrap();

        assert!(db.get_backfill_progress("film").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn get_nonexistent_backfill_progress_returns_none() {
        let db = setup_test_db().await;
        assert!(
            db.get_backfill_progress("nonexistent")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn backfill_progress_requires_valid_config() {
        let db = setup_test_db().await;
        let progress = sample_backfill_progress("nonexistent_config");

        let result = db.save_backfill_progress(&progress).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn watermark_lsn_saved_and_retrieved() {
        let db = setup_test_db().await;
        db.insert_config(&sample_config("film")).await.unwrap();

        let mut progress = sample_backfill_progress("film");
        progress.watermark_lsn = Some(42_000);
        db.save_backfill_progress(&progress).await.unwrap();

        let retrieved = db.get_backfill_progress("film").await.unwrap().unwrap();
        assert_eq!(retrieved.watermark_lsn, Some(42_000));
    }

    #[tokio::test]
    async fn get_backfill_cursor_returns_none_when_no_progress() {
        let db = setup_test_db().await;
        assert!(
            db.get_backfill_cursor("nonexistent")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn get_backfill_cursor_returns_none_when_no_last_id() {
        let db = setup_test_db().await;
        db.insert_config(&sample_config("film")).await.unwrap();

        let mut progress = sample_backfill_progress("film");
        progress.last_id = None;
        db.save_backfill_progress(&progress).await.unwrap();

        assert!(db.get_backfill_cursor("film").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn get_backfill_cursor_returns_id_and_count() {
        let db = setup_test_db().await;
        db.insert_config(&sample_config("film")).await.unwrap();
        db.save_backfill_progress(&sample_backfill_progress("film"))
            .await
            .unwrap();

        let (id, rows) = db.get_backfill_cursor("film").await.unwrap().unwrap();
        assert_eq!(id, "12345");
        assert_eq!(rows, 100);
    }

    #[tokio::test]
    async fn save_backfill_cursor_creates_in_progress_record() {
        let db = setup_test_db().await;
        db.insert_config(&sample_config("film")).await.unwrap();

        db.save_backfill_cursor("film", "500", 250).await.unwrap();

        let p = db.get_backfill_progress("film").await.unwrap().unwrap();
        assert_eq!(p.last_id, Some("500".to_string()));
        assert_eq!(p.processed_rows, 250);
        assert_eq!(p.status, BackfillStatus::InProgress);
        assert!(p.started_at.is_some());
    }

    #[tokio::test]
    async fn save_backfill_cursor_updates_existing() {
        let db = setup_test_db().await;
        db.insert_config(&sample_config("film")).await.unwrap();

        db.save_backfill_cursor("film", "100", 50).await.unwrap();
        db.save_backfill_cursor("film", "200", 100).await.unwrap();

        let (id, rows) = db.get_backfill_cursor("film").await.unwrap().unwrap();
        assert_eq!(id, "200");
        assert_eq!(rows, 100);
    }

    #[tokio::test]
    async fn save_backfill_cursor_preserves_watermark_lsn() {
        let db = setup_test_db().await;
        db.insert_config(&sample_config("film")).await.unwrap();

        // Set initial progress with a watermark
        let mut progress = sample_backfill_progress("film");
        progress.watermark_lsn = Some(99_000);
        progress.last_id = Some("100".to_string());
        progress.processed_rows = 50;
        db.save_backfill_progress(&progress).await.unwrap();

        // save_backfill_cursor should NOT clear watermark_lsn
        db.save_backfill_cursor("film", "200", 100).await.unwrap();

        let p = db.get_backfill_progress("film").await.unwrap().unwrap();
        assert_eq!(p.last_id, Some("200".to_string()));
        assert_eq!(p.processed_rows, 100);
        assert_eq!(
            p.watermark_lsn,
            Some(99_000),
            "watermark_lsn must be preserved across cursor saves"
        );
    }

    #[tokio::test]
    async fn watermark_lsn_above_i32_max_roundtrips() {
        let db = setup_test_db().await;
        db.insert_config(&sample_config("film")).await.unwrap();

        let big_lsn: u64 = (i32::MAX as u64) + 5_000;
        let mut progress = sample_backfill_progress("film");
        progress.watermark_lsn = Some(big_lsn);
        db.save_backfill_progress(&progress).await.unwrap();

        let retrieved = db.get_backfill_progress("film").await.unwrap().unwrap();
        assert_eq!(retrieved.watermark_lsn, Some(big_lsn));
    }

    #[tokio::test]
    async fn watermark_lsn_defaults_to_none() {
        let db = setup_test_db().await;
        db.insert_config(&sample_config("film")).await.unwrap();

        let progress = sample_backfill_progress("film");
        db.save_backfill_progress(&progress).await.unwrap();

        let retrieved = db.get_backfill_progress("film").await.unwrap().unwrap();
        assert_eq!(retrieved.watermark_lsn, None);
    }
}
