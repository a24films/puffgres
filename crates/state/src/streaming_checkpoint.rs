use chrono::{DateTime, Utc};
use diesel::prelude::*;

use crate::models::{NewStreamingCheckpoint, StreamingCheckpointRow};
use crate::schema::streaming_checkpoints;
use crate::{StateDb, StateError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamingCheckpoint {
    pub config_name: String,
    pub lsn: u64,
    pub events_processed: u64,
    pub updated_at: DateTime<Utc>,
}

impl StreamingCheckpoint {
    fn from_row(row: &StreamingCheckpointRow) -> Result<Self, StateError> {
        let updated_at = DateTime::parse_from_rfc3339(&row.updated_at)
            .map(|dt| dt.with_timezone(&Utc))
            .map_err(|e| StateError::InvalidState(format!("invalid updated_at: {e}")))?;

        Ok(Self {
            config_name: row.config_name.clone(),
            lsn: u64::from_ne_bytes(row.lsn.to_ne_bytes()),
            events_processed: u64::from_ne_bytes(row.events_processed.to_ne_bytes()),
            updated_at,
        })
    }
}

impl StateDb {
    pub fn save_streaming_checkpoint(
        &self,
        checkpoint: &StreamingCheckpoint,
    ) -> Result<(), StateError> {
        let updated_at_str = checkpoint.updated_at.to_rfc3339();
        let new = NewStreamingCheckpoint {
            config_name: &checkpoint.config_name,
            lsn: i64::from_ne_bytes(checkpoint.lsn.to_ne_bytes()),
            events_processed: i64::from_ne_bytes(checkpoint.events_processed.to_ne_bytes()),
            updated_at: &updated_at_str,
        };

        let mut conn = self.lock()?;
        diesel::replace_into(streaming_checkpoints::table)
            .values(&new)
            .execute(&mut *conn)?;

        Ok(())
    }

    pub fn get_streaming_checkpoint(
        &self,
        config_name: &str,
    ) -> Result<Option<StreamingCheckpoint>, StateError> {
        let mut conn = self.lock()?;
        let row = streaming_checkpoints::table
            .filter(streaming_checkpoints::config_name.eq(config_name))
            .first::<StreamingCheckpointRow>(&mut *conn)
            .optional()?;

        match row {
            Some(r) => Ok(Some(StreamingCheckpoint::from_row(&r)?)),
            None => Ok(None),
        }
    }

    pub fn delete_streaming_checkpoint(&self, config_name: &str) -> Result<bool, StateError> {
        let mut conn = self.lock()?;
        let rows_affected = diesel::delete(
            streaming_checkpoints::table.filter(streaming_checkpoints::config_name.eq(config_name)),
        )
        .execute(&mut *conn)?;

        Ok(rows_affected > 0)
    }

    pub fn list_streaming_checkpoints(&self) -> Result<Vec<StreamingCheckpoint>, StateError> {
        let mut conn = self.lock()?;
        let rows = streaming_checkpoints::table
            .order(streaming_checkpoints::config_name.asc())
            .load::<StreamingCheckpointRow>(&mut *conn)?;

        rows.iter().map(StreamingCheckpoint::from_row).collect()
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

    #[test]
    fn save_and_retrieve_streaming_checkpoint() {
        let (_dir, db) = setup_test_db();
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
        let (_dir, db) = setup_test_db();
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
        let (_dir, db) = setup_test_db();
        let config = sample_config("film");
        db.insert_config(&config).unwrap();

        let checkpoint = sample_streaming_checkpoint("film", 1000, 50);
        db.save_streaming_checkpoint(&checkpoint).unwrap();

        assert!(db.get_streaming_checkpoint("film").unwrap().is_some());

        {
            let mut conn = db.lock().unwrap();
            diesel::delete(
                crate::schema::configs::table.filter(crate::schema::configs::name.eq("film")),
            )
            .execute(&mut *conn)
            .unwrap();
        }

        assert!(db.get_streaming_checkpoint("film").unwrap().is_none());
    }

    #[test]
    fn delete_streaming_checkpoint_returns_true_when_exists() {
        let (_dir, db) = setup_test_db();
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
        let (_dir, db) = setup_test_db();
        let deleted = db.delete_streaming_checkpoint("nonexistent").unwrap();
        assert!(!deleted);
    }

    #[test]
    fn list_multiple_streaming_checkpoints() {
        let (_dir, db) = setup_test_db();

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
        let (_dir, db) = setup_test_db();
        assert!(
            db.get_streaming_checkpoint("nonexistent")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn lsn_above_i32_max_roundtrips() {
        let (_dir, db) = setup_test_db();
        let config = sample_config("film");
        db.insert_config(&config).unwrap();

        let big_lsn: u64 = (i32::MAX as u64) + 1_000;
        let big_events: u64 = (i32::MAX as u64) + 500;
        let checkpoint = sample_streaming_checkpoint("film", big_lsn, big_events);
        db.save_streaming_checkpoint(&checkpoint).unwrap();

        let retrieved = db.get_streaming_checkpoint("film").unwrap().unwrap();
        assert_eq!(retrieved.lsn, big_lsn);
        assert_eq!(retrieved.events_processed, big_events);
    }

    #[test]
    fn streaming_checkpoint_requires_valid_config() {
        let (_dir, db) = setup_test_db();
        let checkpoint = sample_streaming_checkpoint("nonexistent_config", 1000, 50);

        let result = db.save_streaming_checkpoint(&checkpoint);
        assert!(result.is_err());
    }
}
