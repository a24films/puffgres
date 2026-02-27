use chrono::{DateTime, Utc};
use rusqlite::{Row, params};

use crate::{StateDb, StateError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DlqEntry {
    pub id: i64,
    pub config_name: String,
    pub lsn: u64,
    pub event_json: String,
    pub doc_id: Option<String>,
    pub error_message: String,
    pub error_kind: ErrorKind,
    pub retry_count: u32,
    pub created_at: DateTime<Utc>,
    pub last_retry_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ErrorKind {
    Retryable,
    Permanent,
}

impl ErrorKind {
    fn to_str(&self) -> &'static str {
        match self {
            ErrorKind::Retryable => "retryable",
            ErrorKind::Permanent => "permanent",
        }
    }

    fn from_str(s: &str) -> Result<Self, StateError> {
        match s {
            "retryable" => Ok(ErrorKind::Retryable),
            "permanent" => Ok(ErrorKind::Permanent),
            _ => Err(StateError::InvalidState(format!(
                "invalid error kind: {}",
                s
            ))),
        }
    }
}

const DLQ_SELECT_COLS: &str = "id, config_name, lsn, event_json, doc_id, error_message, error_kind, retry_count, created_at, last_retry_at";
const COL_ID: usize = 0;
const COL_CONFIG_NAME: usize = 1;
const COL_LSN: usize = 2;
const COL_EVENT_JSON: usize = 3;
const COL_DOC_ID: usize = 4;
const COL_ERROR_MESSAGE: usize = 5;
const COL_ERROR_KIND: usize = 6;
const COL_RETRY_COUNT: usize = 7;
const COL_CREATED_AT: usize = 8;
const COL_LAST_RETRY_AT: usize = 9;

impl DlqEntry {
    pub fn retryable(
        config_name: &str,
        lsn: u64,
        event_json: String,
        doc_id: Option<String>,
        error: &str,
    ) -> Self {
        Self {
            id: 0,
            config_name: config_name.to_string(),
            lsn,
            event_json,
            doc_id,
            error_message: error.to_string(),
            error_kind: ErrorKind::Retryable,
            retry_count: 0,
            created_at: Utc::now(),
            last_retry_at: None,
        }
    }

    pub fn permanent(
        config_name: &str,
        lsn: u64,
        event_json: String,
        doc_id: Option<String>,
        error: &str,
    ) -> Self {
        Self {
            id: 0,
            config_name: config_name.to_string(),
            lsn,
            event_json,
            doc_id,
            error_message: error.to_string(),
            error_kind: ErrorKind::Permanent,
            retry_count: 0,
            created_at: Utc::now(),
            last_retry_at: None,
        }
    }

    fn from_row(row: &Row) -> Result<Self, rusqlite::Error> {
        let created_at_str: String = row.get(COL_CREATED_AT)?;
        let created_at = DateTime::parse_from_rfc3339(&created_at_str)
            .map(|dt| dt.with_timezone(&Utc))
            .map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    COL_CREATED_AT,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })?;

        let last_retry_at: Option<String> = row.get(COL_LAST_RETRY_AT)?;
        let last_retry_at = last_retry_at.and_then(|s| {
            DateTime::parse_from_rfc3339(&s)
                .ok()
                .map(|dt| dt.with_timezone(&Utc))
        });

        let error_kind_str: String = row.get(COL_ERROR_KIND)?;
        let error_kind = ErrorKind::from_str(&error_kind_str).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(
                COL_ERROR_KIND,
                rusqlite::types::Type::Text,
                Box::new(e),
            )
        })?;

        Ok(Self {
            id: row.get(COL_ID)?,
            config_name: row.get(COL_CONFIG_NAME)?,
            lsn: row.get::<_, i64>(COL_LSN)? as u64,
            event_json: row.get(COL_EVENT_JSON)?,
            doc_id: row.get(COL_DOC_ID)?,
            error_message: row.get(COL_ERROR_MESSAGE)?,
            error_kind,
            retry_count: row.get::<_, i64>(COL_RETRY_COUNT)? as u32,
            created_at,
            last_retry_at,
        })
    }
}

impl StateDb {
    pub fn ensure_dlq_table(&self) -> Result<(), StateError> {
        self.conn().execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS dlq (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                config_name TEXT NOT NULL,
                lsn INTEGER NOT NULL,
                event_json TEXT NOT NULL,
                doc_id TEXT,
                error_message TEXT NOT NULL,
                error_kind TEXT NOT NULL CHECK (error_kind IN ('retryable', 'permanent')),
                retry_count INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                last_retry_at TEXT,
                FOREIGN KEY (config_name) REFERENCES configs(name) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_dlq_config_name ON dlq(config_name);
            CREATE INDEX IF NOT EXISTS idx_dlq_error_kind ON dlq(error_kind);
            "#,
        )?;
        Ok(())
    }

    pub fn insert_dlq_entry(&self, entry: &DlqEntry) -> Result<i64, StateError> {
        self.conn().execute(
            "INSERT INTO dlq (config_name, lsn, event_json, doc_id, error_message, error_kind, retry_count, created_at, last_retry_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                entry.config_name,
                entry.lsn as i64,
                entry.event_json,
                entry.doc_id,
                entry.error_message,
                entry.error_kind.to_str(),
                entry.retry_count as i64,
                entry.created_at.to_rfc3339(),
                entry.last_retry_at.as_ref().map(|dt| dt.to_rfc3339()),
            ],
        )?;
        Ok(self.conn().last_insert_rowid())
    }

    pub fn get_dlq_entry(&self, id: i64) -> Result<Option<DlqEntry>, StateError> {
        let mut stmt = self.conn().prepare(&format!(
            "SELECT {} FROM dlq WHERE id = ?1",
            DLQ_SELECT_COLS
        ))?;

        let mut rows = stmt.query(params![id])?;
        match rows.next()? {
            Some(row) => Ok(Some(DlqEntry::from_row(row)?)),
            None => Ok(None),
        }
    }

    pub fn list_dlq_entries(
        &self,
        config_name: Option<&str>,
        limit: usize,
    ) -> Result<Vec<DlqEntry>, StateError> {
        let query = match config_name {
            Some(_) => format!(
                "SELECT {} FROM dlq WHERE config_name = ?1 ORDER BY created_at DESC LIMIT ?2",
                DLQ_SELECT_COLS
            ),
            None => format!(
                "SELECT {} FROM dlq ORDER BY created_at DESC LIMIT ?1",
                DLQ_SELECT_COLS
            ),
        };

        let mut stmt = self.conn().prepare(&query)?;

        let rows = match config_name {
            Some(name) => stmt.query_map(params![name, limit as i64], DlqEntry::from_row)?,
            None => stmt.query_map(params![limit as i64], DlqEntry::from_row)?,
        };

        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn increment_retry(&self, id: i64) -> Result<(), StateError> {
        let rows_affected = self.conn().execute(
            "UPDATE dlq SET retry_count = retry_count + 1, last_retry_at = ?1 WHERE id = ?2",
            params![Utc::now().to_rfc3339(), id],
        )?;

        if rows_affected == 0 {
            return Err(StateError::NotFound(format!("dlq entry with id {}", id)));
        }

        Ok(())
    }

    pub fn delete_dlq_entry(&self, id: i64) -> Result<bool, StateError> {
        let rows_affected = self
            .conn()
            .execute("DELETE FROM dlq WHERE id = ?1", params![id])?;
        Ok(rows_affected > 0)
    }

    pub fn clear_dlq(&self, config_name: Option<&str>) -> Result<u64, StateError> {
        let rows_affected = match config_name {
            Some(name) => self
                .conn()
                .execute("DELETE FROM dlq WHERE config_name = ?1", params![name])?,
            None => self.conn().execute("DELETE FROM dlq", [])?,
        };
        Ok(rows_affected as u64)
    }

    pub fn list_retryable_entries(&self, limit: usize) -> Result<Vec<DlqEntry>, StateError> {
        let query = format!(
            "SELECT {} FROM dlq WHERE error_kind = 'retryable' ORDER BY created_at ASC LIMIT ?1",
            DLQ_SELECT_COLS
        );
        let mut stmt = self.conn().prepare(&query)?;
        let rows = stmt.query_map(params![limit as i64], DlqEntry::from_row)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn mark_permanent(&self, id: i64, error: &str) -> Result<(), StateError> {
        let rows_affected = self.conn().execute(
            "UPDATE dlq SET error_kind = 'permanent', error_message = ?1, last_retry_at = ?2 WHERE id = ?3",
            params![error, Utc::now().to_rfc3339(), id],
        )?;
        if rows_affected == 0 {
            return Err(StateError::NotFound(format!("dlq entry with id {}", id)));
        }
        Ok(())
    }

    pub fn dlq_count(&self, config_name: Option<&str>) -> Result<u64, StateError> {
        let count: i64 = match config_name {
            Some(name) => {
                let mut stmt = self
                    .conn()
                    .prepare("SELECT COUNT(*) FROM dlq WHERE config_name = ?1")?;
                stmt.query_row(params![name], |row| row.get(0))?
            }
            None => {
                let mut stmt = self.conn().prepare("SELECT COUNT(*) FROM dlq")?;
                stmt.query_row([], |row| row.get(0))?
            }
        };
        Ok(count as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ConfigRecord;

    fn setup_dlq_db() -> (tempfile::TempDir, StateDb) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = StateDb::open(&path).unwrap();
        db.ensure_configs_table().unwrap();
        db.ensure_dlq_table().unwrap();
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

    fn sample_dlq_entry(config_name: &str, lsn: u64, error_kind: ErrorKind) -> DlqEntry {
        DlqEntry {
            id: 0, // Will be set by database
            config_name: config_name.to_string(),
            lsn,
            event_json: r#"{"event": "test"}"#.to_string(),
            doc_id: Some(r#"{"Uint":42}"#.to_string()),
            error_message: "Test error".to_string(),
            error_kind,
            retry_count: 0,
            created_at: Utc::now(),
            last_retry_at: None,
        }
    }

    #[test]
    fn insert_and_retrieve_entry() {
        let (_dir, db) = setup_dlq_db();
        let config = sample_config("film");
        db.insert_config(&config).unwrap();

        let entry = sample_dlq_entry("film", 1000, ErrorKind::Retryable);
        let id = db.insert_dlq_entry(&entry).unwrap();

        let retrieved = db.get_dlq_entry(id).unwrap().unwrap();
        assert_eq!(retrieved.config_name, "film");
        assert_eq!(retrieved.lsn, 1000);
        assert_eq!(retrieved.doc_id, Some(r#"{"Uint":42}"#.to_string()));
        assert_eq!(retrieved.error_kind, ErrorKind::Retryable);
        assert_eq!(retrieved.retry_count, 0);
        assert!(retrieved.last_retry_at.is_none());
    }

    #[test]
    fn insert_and_retrieve_entry_without_doc_id() {
        let (_dir, db) = setup_dlq_db();
        let config = sample_config("film");
        db.insert_config(&config).unwrap();

        let mut entry = sample_dlq_entry("film", 1000, ErrorKind::Retryable);
        entry.doc_id = None;
        let id = db.insert_dlq_entry(&entry).unwrap();

        let retrieved = db.get_dlq_entry(id).unwrap().unwrap();
        assert_eq!(retrieved.doc_id, None);
    }

    #[test]
    fn list_with_config_filter() {
        let (_dir, db) = setup_dlq_db();
        db.insert_config(&sample_config("film")).unwrap();
        db.insert_config(&sample_config("actor")).unwrap();

        db.insert_dlq_entry(&sample_dlq_entry("film", 100, ErrorKind::Retryable))
            .unwrap();
        db.insert_dlq_entry(&sample_dlq_entry("film", 200, ErrorKind::Permanent))
            .unwrap();
        db.insert_dlq_entry(&sample_dlq_entry("actor", 300, ErrorKind::Retryable))
            .unwrap();

        let film_entries = db.list_dlq_entries(Some("film"), 100).unwrap();
        assert_eq!(film_entries.len(), 2);
        assert!(film_entries.iter().all(|e| e.config_name == "film"));
    }

    #[test]
    fn list_without_config_filter() {
        let (_dir, db) = setup_dlq_db();
        db.insert_config(&sample_config("film")).unwrap();
        db.insert_config(&sample_config("actor")).unwrap();

        db.insert_dlq_entry(&sample_dlq_entry("film", 100, ErrorKind::Retryable))
            .unwrap();
        db.insert_dlq_entry(&sample_dlq_entry("actor", 200, ErrorKind::Permanent))
            .unwrap();

        let all_entries = db.list_dlq_entries(None, 100).unwrap();
        assert_eq!(all_entries.len(), 2);
    }

    #[test]
    fn increment_retry_count() {
        let (_dir, db) = setup_dlq_db();
        let config = sample_config("film");
        db.insert_config(&config).unwrap();

        let entry = sample_dlq_entry("film", 1000, ErrorKind::Retryable);
        let id = db.insert_dlq_entry(&entry).unwrap();

        // Increment retry
        db.increment_retry(id).unwrap();

        let retrieved = db.get_dlq_entry(id).unwrap().unwrap();
        assert_eq!(retrieved.retry_count, 1);
        assert!(retrieved.last_retry_at.is_some());

        // Increment again
        db.increment_retry(id).unwrap();

        let retrieved = db.get_dlq_entry(id).unwrap().unwrap();
        assert_eq!(retrieved.retry_count, 2);
    }

    #[test]
    fn clear_by_config_name() {
        let (_dir, db) = setup_dlq_db();
        db.insert_config(&sample_config("film")).unwrap();
        db.insert_config(&sample_config("actor")).unwrap();

        db.insert_dlq_entry(&sample_dlq_entry("film", 100, ErrorKind::Retryable))
            .unwrap();
        db.insert_dlq_entry(&sample_dlq_entry("film", 200, ErrorKind::Permanent))
            .unwrap();
        db.insert_dlq_entry(&sample_dlq_entry("actor", 300, ErrorKind::Retryable))
            .unwrap();

        let deleted = db.clear_dlq(Some("film")).unwrap();
        assert_eq!(deleted, 2);

        let remaining = db.list_dlq_entries(None, 100).unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].config_name, "actor");
    }

    #[test]
    fn clear_all() {
        let (_dir, db) = setup_dlq_db();
        db.insert_config(&sample_config("film")).unwrap();
        db.insert_config(&sample_config("actor")).unwrap();

        db.insert_dlq_entry(&sample_dlq_entry("film", 100, ErrorKind::Retryable))
            .unwrap();
        db.insert_dlq_entry(&sample_dlq_entry("actor", 200, ErrorKind::Permanent))
            .unwrap();

        let deleted = db.clear_dlq(None).unwrap();
        assert_eq!(deleted, 2);

        let remaining = db.list_dlq_entries(None, 100).unwrap();
        assert_eq!(remaining.len(), 0);
    }

    #[test]
    fn delete_dlq_entry_returns_true_when_exists() {
        let (_dir, db) = setup_dlq_db();
        let config = sample_config("film");
        db.insert_config(&config).unwrap();

        let entry = sample_dlq_entry("film", 1000, ErrorKind::Retryable);
        let id = db.insert_dlq_entry(&entry).unwrap();

        let deleted = db.delete_dlq_entry(id).unwrap();
        assert!(deleted);
        assert!(db.get_dlq_entry(id).unwrap().is_none());
    }

    #[test]
    fn delete_dlq_entry_returns_false_when_not_exists() {
        let (_dir, db) = setup_dlq_db();
        let deleted = db.delete_dlq_entry(999).unwrap();
        assert!(!deleted);
    }

    #[test]
    fn dlq_count_with_config_filter() {
        let (_dir, db) = setup_dlq_db();
        db.insert_config(&sample_config("film")).unwrap();
        db.insert_config(&sample_config("actor")).unwrap();

        db.insert_dlq_entry(&sample_dlq_entry("film", 100, ErrorKind::Retryable))
            .unwrap();
        db.insert_dlq_entry(&sample_dlq_entry("film", 200, ErrorKind::Permanent))
            .unwrap();
        db.insert_dlq_entry(&sample_dlq_entry("actor", 300, ErrorKind::Retryable))
            .unwrap();

        assert_eq!(db.dlq_count(Some("film")).unwrap(), 2);
        assert_eq!(db.dlq_count(Some("actor")).unwrap(), 1);
    }

    #[test]
    fn dlq_count_without_filter() {
        let (_dir, db) = setup_dlq_db();
        db.insert_config(&sample_config("film")).unwrap();
        db.insert_config(&sample_config("actor")).unwrap();

        db.insert_dlq_entry(&sample_dlq_entry("film", 100, ErrorKind::Retryable))
            .unwrap();
        db.insert_dlq_entry(&sample_dlq_entry("actor", 200, ErrorKind::Permanent))
            .unwrap();

        assert_eq!(db.dlq_count(None).unwrap(), 2);
    }

    #[test]
    fn dlq_entry_deleted_when_config_deleted() {
        let (_dir, db) = setup_dlq_db();
        let config = sample_config("film");
        db.insert_config(&config).unwrap();

        let entry = sample_dlq_entry("film", 1000, ErrorKind::Retryable);
        let id = db.insert_dlq_entry(&entry).unwrap();

        // Verify entry exists
        assert!(db.get_dlq_entry(id).unwrap().is_some());

        // Delete the config (should cascade to dlq)
        db.conn()
            .execute("DELETE FROM configs WHERE name = ?1", params!["film"])
            .unwrap();

        // DLQ entry should be gone
        assert!(db.get_dlq_entry(id).unwrap().is_none());
    }

    #[test]
    fn dlq_entry_requires_valid_config() {
        let (_dir, db) = setup_dlq_db();
        let entry = sample_dlq_entry("nonexistent_config", 1000, ErrorKind::Retryable);

        // Should fail due to foreign key constraint
        let result = db.insert_dlq_entry(&entry);
        assert!(result.is_err());
    }

    #[test]
    fn get_nonexistent_dlq_entry_returns_none() {
        let (_dir, db) = setup_dlq_db();
        assert!(db.get_dlq_entry(999).unwrap().is_none());
    }

    #[test]
    fn increment_retry_fails_for_nonexistent_entry() {
        let (_dir, db) = setup_dlq_db();
        let result = db.increment_retry(999);
        assert!(result.is_err());
    }

    #[test]
    fn from_row_rejects_malformed_created_at() {
        let (_dir, db) = setup_dlq_db();
        db.insert_config(&sample_config("film")).unwrap();

        // Insert a row with a malformed created_at directly via SQL.
        // The created_at column has no CHECK constraint so corrupt text
        // values can exist (e.g. manual edits, migrations).
        db.conn()
            .execute(
                "INSERT INTO dlq (config_name, lsn, event_json, error_message, error_kind, retry_count, created_at)
                 VALUES ('film', 1000, '{}', 'err', 'retryable', 0, 'not-a-timestamp')",
                [],
            )
            .unwrap();

        let result = db.list_dlq_entries(None, 100);
        assert!(result.is_err());
    }

    #[test]
    fn list_respects_limit() {
        let (_dir, db) = setup_dlq_db();
        db.insert_config(&sample_config("film")).unwrap();

        for i in 0..10 {
            db.insert_dlq_entry(&sample_dlq_entry("film", i, ErrorKind::Retryable))
                .unwrap();
        }

        let entries = db.list_dlq_entries(Some("film"), 5).unwrap();
        assert_eq!(entries.len(), 5);
    }

    #[test]
    fn test_dlq_entry_retryable_constructor() {
        let entry = DlqEntry::retryable(
            "film",
            1000,
            r#"{"test":true}"#.to_string(),
            Some(r#"{"Uint":1}"#.to_string()),
            "network timeout",
        );
        assert_eq!(entry.config_name, "film");
        assert_eq!(entry.lsn, 1000);
        assert_eq!(entry.error_kind, ErrorKind::Retryable);
        assert_eq!(entry.retry_count, 0);
        assert_eq!(entry.error_message, "network timeout");
        assert_eq!(entry.doc_id, Some(r#"{"Uint":1}"#.to_string()));
        assert!(entry.last_retry_at.is_none());
    }

    #[test]
    fn test_dlq_entry_permanent_constructor() {
        let entry = DlqEntry::permanent(
            "film",
            2000,
            r#"{"test":true}"#.to_string(),
            Some(r#"{"Uint":2}"#.to_string()),
            "bad transform",
        );
        assert_eq!(entry.config_name, "film");
        assert_eq!(entry.lsn, 2000);
        assert_eq!(entry.error_kind, ErrorKind::Permanent);
        assert_eq!(entry.retry_count, 0);
        assert_eq!(entry.error_message, "bad transform");
        assert_eq!(entry.doc_id, Some(r#"{"Uint":2}"#.to_string()));
    }

    #[test]
    fn test_list_retryable_entries() {
        let (_dir, db) = setup_dlq_db();
        db.insert_config(&sample_config("film")).unwrap();

        db.insert_dlq_entry(&sample_dlq_entry("film", 100, ErrorKind::Retryable))
            .unwrap();
        db.insert_dlq_entry(&sample_dlq_entry("film", 200, ErrorKind::Permanent))
            .unwrap();
        db.insert_dlq_entry(&sample_dlq_entry("film", 300, ErrorKind::Retryable))
            .unwrap();

        let retryable = db.list_retryable_entries(100).unwrap();
        assert_eq!(retryable.len(), 2);
        assert!(
            retryable
                .iter()
                .all(|e| e.error_kind == ErrorKind::Retryable)
        );
    }

    #[test]
    fn test_mark_permanent() {
        let (_dir, db) = setup_dlq_db();
        db.insert_config(&sample_config("film")).unwrap();

        let entry = sample_dlq_entry("film", 100, ErrorKind::Retryable);
        let id = db.insert_dlq_entry(&entry).unwrap();

        db.mark_permanent(id, "max retries exhausted").unwrap();

        let updated = db.get_dlq_entry(id).unwrap().unwrap();
        assert_eq!(updated.error_kind, ErrorKind::Permanent);
        assert_eq!(updated.error_message, "max retries exhausted");
        assert!(updated.last_retry_at.is_some());
    }

    #[test]
    fn test_mark_permanent_nonexistent_fails() {
        let (_dir, db) = setup_dlq_db();
        assert!(db.mark_permanent(999, "error").is_err());
    }
}
