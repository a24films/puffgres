use crate::{
    BackfillCheckpointer, BackfillProgress, BackfillStatus, ConfigRecord, DlqEntry,
    PostgresStateStore, StateDb, StateError, StreamingCheckpoint,
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

fn block_on_state<F, T>(future: F) -> Result<T, StateError>
where
    F: std::future::Future<Output = Result<T, StateError>>,
{
    tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(future))
}

impl BackfillCheckpointer for PostgresStateStore {
    fn load_progress(&self, config_name: &str) -> Result<Option<(String, u64)>, StateError> {
        block_on_state(async {
            let progress = self.get_backfill_progress(config_name).await?;
            Ok(progress.and_then(|progress| {
                progress.last_id.map(|last_id| (last_id, progress.processed_rows))
            }))
        })
    }

    fn save_progress(
        &self,
        config_name: &str,
        last_id: &str,
        processed_rows: u64,
    ) -> Result<(), StateError> {
        block_on_state(async {
            let existing = self.get_backfill_progress(config_name).await?;
            let progress = BackfillProgress {
                config_name: config_name.to_string(),
                last_id: Some(last_id.to_string()),
                total_rows: existing.as_ref().and_then(|progress| progress.total_rows),
                processed_rows,
                status: BackfillStatus::InProgress,
                started_at: existing
                    .as_ref()
                    .and_then(|progress| progress.started_at)
                    .or_else(|| Some(chrono::Utc::now())),
                completed_at: None,
                error_message: None,
                watermark_lsn: existing.as_ref().and_then(|progress| progress.watermark_lsn),
            };
            self.save_backfill_progress(&progress).await
        })
    }
}

impl StateStore for PostgresStateStore {
    fn get_config(&self, name: &str) -> Result<Option<ConfigRecord>, StateError> {
        block_on_state(self.get_config(name))
    }

    fn tombstone_config(&self, name: &str) -> Result<(), StateError> {
        block_on_state(self.tombstone_config(name))
    }

    fn list_tombstoned_configs(&self) -> Result<Vec<ConfigRecord>, StateError> {
        block_on_state(self.list_tombstoned_configs())
    }

    fn get_namespace_prefix(&self, config_name: &str) -> Result<Option<String>, StateError> {
        block_on_state(self.get_namespace_prefix(config_name))
    }

    fn set_namespace_prefix(
        &self,
        config_name: &str,
        prefix: Option<&str>,
    ) -> Result<(), StateError> {
        block_on_state(self.set_namespace_prefix(config_name, prefix))
    }

    fn delete_streaming_checkpoint(&self, config_name: &str) -> Result<bool, StateError> {
        block_on_state(self.delete_streaming_checkpoint(config_name))
    }

    fn get_backfill_progress(
        &self,
        config_name: &str,
    ) -> Result<Option<BackfillProgress>, StateError> {
        block_on_state(self.get_backfill_progress(config_name))
    }

    fn save_backfill_progress(&self, progress: &BackfillProgress) -> Result<(), StateError> {
        block_on_state(self.save_backfill_progress(progress))
    }

    fn clear_dlq(&self, config_name: Option<&str>) -> Result<u64, StateError> {
        block_on_state(self.clear_dlq(config_name))
    }

    fn get_streaming_checkpoint(
        &self,
        config_name: &str,
    ) -> Result<Option<StreamingCheckpoint>, StateError> {
        block_on_state(self.get_streaming_checkpoint(config_name))
    }

    fn save_streaming_checkpoint(
        &self,
        checkpoint: &StreamingCheckpoint,
    ) -> Result<(), StateError> {
        block_on_state(self.save_streaming_checkpoint(checkpoint))
    }

    fn clear_old_permanent_entries(&self, max_age_hours: u64) -> Result<u64, StateError> {
        block_on_state(self.clear_old_permanent_entries(max_age_hours))
    }

    fn run_maintenance(&self, dlq_max_age_hours: u64) -> Result<u64, StateError> {
        block_on_state(self.clear_old_permanent_entries(dlq_max_age_hours))
    }

    fn insert_dlq_entry(&self, entry: &DlqEntry) -> Result<i64, StateError> {
        block_on_state(self.insert_dlq_entry(entry))
    }

    fn list_retryable_entries(&self, limit: usize) -> Result<Vec<DlqEntry>, StateError> {
        block_on_state(self.list_retryable_entries(limit))
    }

    fn mark_permanent(&self, id: i64, error: &str) -> Result<(), StateError> {
        block_on_state(self.mark_permanent(id, error))
    }

    fn delete_dlq_entry(&self, id: i64) -> Result<bool, StateError> {
        block_on_state(self.delete_dlq_entry(id))
    }

    fn increment_retry(&self, id: i64) -> Result<(), StateError> {
        block_on_state(self.increment_retry(id))
    }

    fn verify_startup_roundtrip(&self) -> Result<(), StateError> {
        block_on_state(self.verify_startup_roundtrip())
    }
}
