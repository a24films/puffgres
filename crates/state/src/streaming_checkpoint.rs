use chrono::{DateTime, Utc};
use diesel::prelude::*;

use crate::epoch;
use crate::models::{NewStreamingCheckpoint, StreamingCheckpointRow};
use crate::pg_lsn::Lsn;
use crate::schema::streaming_checkpoints;
use crate::{StateError, Store};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamingCheckpoint {
    pub config_name: String,
    pub lsn: u64,
    pub events_processed: u64,
    pub updated_at: DateTime<Utc>,
}

impl StreamingCheckpoint {
    fn from_row(row: &StreamingCheckpointRow) -> Result<Self, StateError> {
        let updated_at = epoch::from_millis(row.updated_at).ok_or_else(|| {
            StateError::InvalidState(format!("invalid updated_at millis: {}", row.updated_at))
        })?;

        let events_processed = u64::try_from(row.events_processed).map_err(|_| {
            StateError::InvalidState(format!(
                "negative events_processed: {}",
                row.events_processed
            ))
        })?;

        Ok(Self {
            config_name: row.config_name.clone(),
            lsn: row.lsn.into(),
            events_processed,
            updated_at,
        })
    }
}

impl Store {
    pub async fn save_streaming_checkpoint(
        &self,
        checkpoint: &StreamingCheckpoint,
    ) -> Result<(), StateError> {
        let cp = checkpoint.clone();
        self.run_blocking(move |conn| {
            let events_processed = i64::try_from(cp.events_processed).map_err(|_| {
                StateError::InvalidState(format!(
                    "events_processed {} exceeds i64::MAX",
                    cp.events_processed
                ))
            })?;
            let new = NewStreamingCheckpoint {
                config_name: &cp.config_name,
                lsn: Lsn(cp.lsn),
                events_processed,
                updated_at: epoch::to_millis(&cp.updated_at),
            };
            diesel::insert_into(streaming_checkpoints::table)
                .values(&new)
                .on_conflict(streaming_checkpoints::config_name)
                .do_update()
                .set(&new)
                .execute(conn)?;
            Ok(())
        })
        .await
    }

    pub async fn get_streaming_checkpoint(
        &self,
        config_name: &str,
    ) -> Result<Option<StreamingCheckpoint>, StateError> {
        let name = config_name.to_string();
        self.run_blocking(move |conn| {
            let row = streaming_checkpoints::table
                .filter(streaming_checkpoints::config_name.eq(&name))
                .first::<StreamingCheckpointRow>(conn)
                .optional()?;
            match row {
                Some(r) => Ok(Some(StreamingCheckpoint::from_row(&r)?)),
                None => Ok(None),
            }
        })
        .await
    }

    pub async fn delete_streaming_checkpoint(&self, config_name: &str) -> Result<bool, StateError> {
        let name = config_name.to_string();
        self.run_blocking(move |conn| {
            let rows_affected = diesel::delete(
                streaming_checkpoints::table.filter(streaming_checkpoints::config_name.eq(&name)),
            )
            .execute(conn)?;
            Ok(rows_affected > 0)
        })
        .await
    }

    pub async fn list_streaming_checkpoints(&self) -> Result<Vec<StreamingCheckpoint>, StateError> {
        self.run_blocking(|conn| {
            let rows = streaming_checkpoints::table
                .order(streaming_checkpoints::config_name.asc())
                .load::<StreamingCheckpointRow>(conn)?;
            rows.iter().map(StreamingCheckpoint::from_row).collect()
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::{sample_config, setup_test_db};

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

    #[tokio::test]
    async fn save_and_retrieve_streaming_checkpoint() {
        let db = setup_test_db().await;
        db.insert_config(&sample_config("film")).await.unwrap();

        let checkpoint = sample_streaming_checkpoint("film", 1000, 50);
        db.save_streaming_checkpoint(&checkpoint).await.unwrap();

        let retrieved = db.get_streaming_checkpoint("film").await.unwrap().unwrap();
        assert_eq!(retrieved.config_name, "film");
        assert_eq!(retrieved.lsn, 1000);
        assert_eq!(retrieved.events_processed, 50);
    }

    #[tokio::test]
    async fn update_existing_streaming_checkpoint() {
        let db = setup_test_db().await;
        db.insert_config(&sample_config("film")).await.unwrap();

        db.save_streaming_checkpoint(&sample_streaming_checkpoint("film", 1000, 50))
            .await
            .unwrap();
        db.save_streaming_checkpoint(&sample_streaming_checkpoint("film", 2000, 100))
            .await
            .unwrap();

        let retrieved = db.get_streaming_checkpoint("film").await.unwrap().unwrap();
        assert_eq!(retrieved.lsn, 2000);
        assert_eq!(retrieved.events_processed, 100);

        let all = db.list_streaming_checkpoints().await.unwrap();
        assert_eq!(all.len(), 1);
    }

    #[tokio::test]
    async fn streaming_checkpoint_deleted_when_config_deleted() {
        let db = setup_test_db().await;
        db.insert_config(&sample_config("film")).await.unwrap();
        db.save_streaming_checkpoint(&sample_streaming_checkpoint("film", 1000, 50))
            .await
            .unwrap();

        assert!(db.get_streaming_checkpoint("film").await.unwrap().is_some());
        db.delete_config("film").await.unwrap();
        assert!(db.get_streaming_checkpoint("film").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn delete_streaming_checkpoint_returns_true_when_exists() {
        let db = setup_test_db().await;
        db.insert_config(&sample_config("film")).await.unwrap();
        db.save_streaming_checkpoint(&sample_streaming_checkpoint("film", 1000, 50))
            .await
            .unwrap();

        let deleted = db.delete_streaming_checkpoint("film").await.unwrap();
        assert!(deleted);
        assert!(db.get_streaming_checkpoint("film").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn delete_streaming_checkpoint_returns_false_when_not_exists() {
        let db = setup_test_db().await;
        let deleted = db.delete_streaming_checkpoint("nonexistent").await.unwrap();
        assert!(!deleted);
    }

    #[tokio::test]
    async fn list_multiple_streaming_checkpoints() {
        let db = setup_test_db().await;
        for n in ["alpha", "beta", "gamma"] {
            db.insert_config(&sample_config(n)).await.unwrap();
        }

        db.save_streaming_checkpoint(&sample_streaming_checkpoint("alpha", 100, 10))
            .await
            .unwrap();
        db.save_streaming_checkpoint(&sample_streaming_checkpoint("beta", 200, 20))
            .await
            .unwrap();
        db.save_streaming_checkpoint(&sample_streaming_checkpoint("gamma", 300, 30))
            .await
            .unwrap();

        let checkpoints = db.list_streaming_checkpoints().await.unwrap();
        assert_eq!(checkpoints.len(), 3);
        assert_eq!(checkpoints[0].config_name, "alpha");
        assert_eq!(checkpoints[0].lsn, 100);
        assert_eq!(checkpoints[1].config_name, "beta");
        assert_eq!(checkpoints[1].lsn, 200);
        assert_eq!(checkpoints[2].config_name, "gamma");
        assert_eq!(checkpoints[2].lsn, 300);
    }

    #[tokio::test]
    async fn get_nonexistent_streaming_checkpoint_returns_none() {
        let db = setup_test_db().await;
        assert!(
            db.get_streaming_checkpoint("nonexistent")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn lsn_above_i32_max_roundtrips() {
        let db = setup_test_db().await;
        db.insert_config(&sample_config("film")).await.unwrap();

        let big_lsn: u64 = (i32::MAX as u64) + 1_000;
        let big_events: u64 = (i32::MAX as u64) + 500;
        db.save_streaming_checkpoint(&sample_streaming_checkpoint("film", big_lsn, big_events))
            .await
            .unwrap();

        let retrieved = db.get_streaming_checkpoint("film").await.unwrap().unwrap();
        assert_eq!(retrieved.lsn, big_lsn);
        assert_eq!(retrieved.events_processed, big_events);
    }

    #[tokio::test]
    async fn streaming_checkpoint_requires_valid_config() {
        let db = setup_test_db().await;
        let checkpoint = sample_streaming_checkpoint("nonexistent_config", 1000, 50);

        let result = db.save_streaming_checkpoint(&checkpoint).await;
        assert!(result.is_err());
    }
}
