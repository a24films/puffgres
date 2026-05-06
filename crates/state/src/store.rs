use crate::{
    BackfillCheckpointer, BackfillProgress, ConfigRecord, DlqEntry, StateDb, StateError,
    StreamingCheckpoint,
};

pub trait StateStore: BackfillCheckpointer {
    fn get_config(&self, name: &str) -> Result<Option<ConfigRecord>, StateError>;
    fn tombstone_config(&self, name: &str) -> Result<(), StateError>;
    fn list_tombstoned_configs(&self) -> Result<Vec<ConfigRecord>, StateError>;
    fn get_namespace_prefix(&self, config_name: &str) -> Result<Option<String>, StateError>;
    fn set_namespace_prefix(
        &self,
        config_name: &str,
        prefix: Option<&str>,
    ) -> Result<(), StateError>;
    fn delete_streaming_checkpoint(&self, config_name: &str) -> Result<bool, StateError>;
    fn get_backfill_progress(
        &self,
        config_name: &str,
    ) -> Result<Option<BackfillProgress>, StateError>;
    fn save_backfill_progress(&self, progress: &BackfillProgress) -> Result<(), StateError>;
    fn clear_dlq(&self, config_name: Option<&str>) -> Result<u64, StateError>;
    fn get_streaming_checkpoint(
        &self,
        config_name: &str,
    ) -> Result<Option<StreamingCheckpoint>, StateError>;
    fn save_streaming_checkpoint(&self, checkpoint: &StreamingCheckpoint)
    -> Result<(), StateError>;
    fn clear_old_permanent_entries(&self, max_age_hours: u64) -> Result<u64, StateError>;
    fn run_maintenance(&self, dlq_max_age_hours: u64) -> Result<u64, StateError>;
    fn insert_dlq_entry(&self, entry: &DlqEntry) -> Result<i64, StateError>;
    fn list_retryable_entries(&self, limit: usize) -> Result<Vec<DlqEntry>, StateError>;
    fn mark_permanent(&self, id: i64, error: &str) -> Result<(), StateError>;
    fn delete_dlq_entry(&self, id: i64) -> Result<bool, StateError>;
    fn increment_retry(&self, id: i64) -> Result<(), StateError>;
    fn verify_startup_roundtrip(&self) -> Result<(), StateError>;
}

impl StateStore for StateDb {
    fn get_config(&self, name: &str) -> Result<Option<ConfigRecord>, StateError> {
        StateDb::get_config(self, name)
    }

    fn tombstone_config(&self, name: &str) -> Result<(), StateError> {
        StateDb::tombstone_config(self, name)
    }

    fn list_tombstoned_configs(&self) -> Result<Vec<ConfigRecord>, StateError> {
        StateDb::list_tombstoned_configs(self)
    }

    fn get_namespace_prefix(&self, config_name: &str) -> Result<Option<String>, StateError> {
        StateDb::get_namespace_prefix(self, config_name)
    }

    fn set_namespace_prefix(
        &self,
        config_name: &str,
        prefix: Option<&str>,
    ) -> Result<(), StateError> {
        StateDb::set_namespace_prefix(self, config_name, prefix)
    }

    fn delete_streaming_checkpoint(&self, config_name: &str) -> Result<bool, StateError> {
        StateDb::delete_streaming_checkpoint(self, config_name)
    }

    fn get_backfill_progress(
        &self,
        config_name: &str,
    ) -> Result<Option<BackfillProgress>, StateError> {
        StateDb::get_backfill_progress(self, config_name)
    }

    fn save_backfill_progress(&self, progress: &BackfillProgress) -> Result<(), StateError> {
        StateDb::save_backfill_progress(self, progress)
    }

    fn clear_dlq(&self, config_name: Option<&str>) -> Result<u64, StateError> {
        StateDb::clear_dlq(self, config_name)
    }

    fn get_streaming_checkpoint(
        &self,
        config_name: &str,
    ) -> Result<Option<StreamingCheckpoint>, StateError> {
        StateDb::get_streaming_checkpoint(self, config_name)
    }

    fn save_streaming_checkpoint(
        &self,
        checkpoint: &StreamingCheckpoint,
    ) -> Result<(), StateError> {
        StateDb::save_streaming_checkpoint(self, checkpoint)
    }

    fn clear_old_permanent_entries(&self, max_age_hours: u64) -> Result<u64, StateError> {
        StateDb::clear_old_permanent_entries(self, max_age_hours)
    }

    fn run_maintenance(&self, dlq_max_age_hours: u64) -> Result<u64, StateError> {
        StateDb::run_maintenance(self, dlq_max_age_hours)
    }

    fn insert_dlq_entry(&self, entry: &DlqEntry) -> Result<i64, StateError> {
        StateDb::insert_dlq_entry(self, entry)
    }

    fn list_retryable_entries(&self, limit: usize) -> Result<Vec<DlqEntry>, StateError> {
        StateDb::list_retryable_entries(self, limit)
    }

    fn mark_permanent(&self, id: i64, error: &str) -> Result<(), StateError> {
        StateDb::mark_permanent(self, id, error)
    }

    fn delete_dlq_entry(&self, id: i64) -> Result<bool, StateError> {
        StateDb::delete_dlq_entry(self, id)
    }

    fn increment_retry(&self, id: i64) -> Result<(), StateError> {
        StateDb::increment_retry(self, id)
    }

    fn verify_startup_roundtrip(&self) -> Result<(), StateError> {
        StateDb::verify_startup_roundtrip(self)
    }
}
