mod backfill;
mod configs;
mod dlq;
mod epoch;
mod error;
mod models;
pub mod pg_lsn;
mod schema;
mod streaming_checkpoint;

#[cfg(test)]
mod test_helpers;

use std::sync::{Arc, Mutex};

use diesel::PgConnection;
use diesel::prelude::*;
use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};

pub use backfill::{BackfillCheckpointer, BackfillProgress, BackfillStatus};
pub use configs::ConfigRecord;
pub use dlq::{DlqEntry, DlqOperation, ErrorKind};
pub use error::StateError;
pub use streaming_checkpoint::StreamingCheckpoint;

const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");

/// Default Postgres schema for puffgres state tables.
pub const DEFAULT_SCHEMA: &str = "puffgres";

struct Inner {
    conn: Mutex<PgConnection>,
    schema: String,
}

/// Thread-safe via `Mutex` so it can be shared across pipeline phases.
/// The public API is async; internally each call defers to `spawn_blocking`
/// because Diesel's Postgres backend is synchronous.
#[derive(Clone)]
pub struct Store {
    inner: Arc<Inner>,
}

/// Validate that `name` is a safe schema identifier.
///
/// We splice the name into `CREATE SCHEMA` / `SET search_path` literally
/// (DDL cannot be parameterized in Postgres), so this guards against SQL
/// injection by accepting only ASCII identifier characters.
fn validate_schema_name(name: &str) -> Result<(), StateError> {
    if name.is_empty() {
        return Err(StateError::InvalidState(
            "schema name must not be empty".into(),
        ));
    }
    let mut chars = name.chars();
    let first = chars.next().unwrap();
    if !(first.is_ascii_alphabetic() || first == '_') {
        return Err(StateError::InvalidState(format!(
            "schema name '{name}' must start with an ASCII letter or underscore"
        )));
    }
    for c in chars {
        if !(c.is_ascii_alphanumeric() || c == '_') {
            return Err(StateError::InvalidState(format!(
                "schema name '{name}' contains invalid character '{c}'; only ASCII alphanumerics and underscore are allowed"
            )));
        }
    }
    Ok(())
}

impl Store {
    pub async fn connect(database_url: &str, schema: &str) -> Result<Self, StateError> {
        validate_schema_name(schema)?;
        let database_url = database_url.to_string();
        let schema = schema.to_string();
        tokio::task::spawn_blocking(move || Self::connect_blocking(&database_url, &schema))
            .await
            .map_err(|e| StateError::InvalidState(format!("blocking task join failed: {e}")))?
    }

    fn connect_blocking(database_url: &str, schema: &str) -> Result<Self, StateError> {
        validate_schema_name(schema)?;
        let mut conn = PgConnection::establish(database_url)?;

        // DDL parameters can't be bound — we already validated the schema name above.
        diesel::sql_query(format!("CREATE SCHEMA IF NOT EXISTS \"{schema}\""))
            .execute(&mut conn)?;
        diesel::sql_query(format!("SET search_path TO \"{schema}\", public")).execute(&mut conn)?;

        conn.run_pending_migrations(MIGRATIONS)
            .map_err(|e| StateError::Migration(e.to_string()))?;

        Ok(Self {
            inner: Arc::new(Inner {
                conn: Mutex::new(conn),
                schema: schema.to_string(),
            }),
        })
    }

    pub fn schema(&self) -> &str {
        &self.inner.schema
    }

    /// Run a synchronous Diesel closure on the blocking pool, holding the
    /// connection mutex for the duration of the call.
    pub(crate) async fn run_blocking<F, T>(&self, f: F) -> Result<T, StateError>
    where
        F: FnOnce(&mut PgConnection) -> Result<T, StateError> + Send + 'static,
        T: Send + 'static,
    {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let mut conn = inner
                .conn
                .lock()
                .map_err(|_| StateError::InvalidState("state db mutex poisoned".into()))?;
            f(&mut conn)
        })
        .await
        .map_err(|e| StateError::InvalidState(format!("blocking task join failed: {e}")))?
    }

    pub async fn reset(&self) -> Result<(), StateError> {
        self.run_blocking(|conn| {
            conn.transaction::<_, StateError, _>(|conn| {
                diesel::delete(schema::dlq::table).execute(conn)?;
                diesel::delete(schema::backfill_progress::table).execute(conn)?;
                diesel::delete(schema::streaming_checkpoints::table).execute(conn)?;
                diesel::delete(schema::configs::table).execute(conn)?;
                Ok(())
            })
        })
        .await
    }

    /// Run periodic maintenance: clean stale permanent DLQ entries.
    pub async fn run_maintenance(&self, dlq_max_age_hours: u64) -> Result<u64, StateError> {
        self.clear_old_permanent_entries(dlq_max_age_hours).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::setup_test_db;

    #[test]
    fn validate_schema_name_accepts_simple() {
        assert!(validate_schema_name("puffgres").is_ok());
        assert!(validate_schema_name("_underscore").is_ok());
        assert!(validate_schema_name("test_123").is_ok());
        assert!(validate_schema_name("A").is_ok());
    }

    #[test]
    fn validate_schema_name_rejects_bad() {
        assert!(validate_schema_name("").is_err());
        assert!(validate_schema_name("1starts_with_digit").is_err());
        assert!(validate_schema_name("has space").is_err());
        assert!(validate_schema_name("has-dash").is_err());
        assert!(validate_schema_name("has\"quote").is_err());
        assert!(validate_schema_name("has;semicolon").is_err());
        assert!(validate_schema_name("schémа").is_err());
    }

    #[tokio::test]
    async fn reset_clears_all_data() {
        let db = setup_test_db().await;

        let config = ConfigRecord {
            name: "film".to_string(),
            namespace: "film".to_string(),
            content_hash: "abc".to_string(),
            transform_hash: None,
            applied_at: chrono::Utc::now(),
            tombstone_applied_at: None,
            namespace_prefix: None,
        };
        db.insert_config(&config).await.unwrap();
        assert_eq!(db.list_configs().await.unwrap().len(), 1);

        db.reset().await.unwrap();
        assert_eq!(db.list_configs().await.unwrap().len(), 0);
    }

    #[tokio::test]
    async fn reset_on_empty_tables() {
        let db = setup_test_db().await;
        db.reset().await.unwrap();
        assert_eq!(db.list_configs().await.unwrap().len(), 0);
    }

    #[tokio::test]
    async fn reset_clears_dlq_and_backfill() {
        let db = setup_test_db().await;

        let config = ConfigRecord {
            name: "film".to_string(),
            namespace: "film".to_string(),
            content_hash: "abc".to_string(),
            transform_hash: None,
            applied_at: chrono::Utc::now(),
            tombstone_applied_at: None,
            namespace_prefix: None,
        };
        db.insert_config(&config).await.unwrap();

        let dlq_entry = DlqEntry::retryable(
            "film",
            100,
            DlqOperation::Insert,
            Some(r#"{"Uint":1}"#.to_string()),
            "boom",
        );
        db.insert_dlq_entry(&dlq_entry).await.unwrap();
        assert_eq!(db.dlq_count(None).await.unwrap(), 1);

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
        db.save_backfill_progress(&backfill).await.unwrap();
        assert!(db.get_backfill_progress("film").await.unwrap().is_some());

        db.reset().await.unwrap();

        assert_eq!(db.dlq_count(None).await.unwrap(), 0);
        assert!(db.get_backfill_progress("film").await.unwrap().is_none());
        assert_eq!(db.list_configs().await.unwrap().len(), 0);
    }
}
