use chrono::{DateTime, Utc};
use diesel::Connection;
use diesel::prelude::*;

use crate::models::{DlqRow, NewDlqEntry};
use crate::schema::dlq;
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
    pub permanent_at: Option<DateTime<Utc>>,
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
            permanent_at: None,
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
            permanent_at: Some(Utc::now()),
        }
    }

    fn from_row(row: &DlqRow) -> Result<Self, StateError> {
        let created_at = DateTime::parse_from_rfc3339(&row.created_at)
            .map(|dt| dt.with_timezone(&Utc))
            .map_err(|e| StateError::InvalidState(format!("invalid created_at: {e}")))?;

        let last_retry_at = row
            .last_retry_at
            .as_deref()
            .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
            .map(|dt| dt.with_timezone(&Utc));

        let permanent_at = row
            .permanent_at
            .as_deref()
            .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
            .map(|dt| dt.with_timezone(&Utc));

        let error_kind = ErrorKind::from_str(&row.error_kind)?;

        Ok(Self {
            id: row.id,
            config_name: row.config_name.clone(),
            lsn: row.lsn as u64,
            event_json: row.event_json.clone(),
            doc_id: row.doc_id.clone(),
            error_message: row.error_message.clone(),
            error_kind,
            retry_count: row.retry_count as u32,
            created_at,
            last_retry_at,
            permanent_at,
        })
    }
}

impl StateDb {
    pub fn insert_dlq_entry(&mut self, entry: &DlqEntry) -> Result<i64, StateError> {
        let created_at_str = entry.created_at.to_rfc3339();
        let last_retry_at_str = entry.last_retry_at.as_ref().map(|dt| dt.to_rfc3339());
        let permanent_at_str = entry.permanent_at.as_ref().map(|dt| dt.to_rfc3339());

        let new = NewDlqEntry {
            config_name: &entry.config_name,
            lsn: entry.lsn as i64,
            event_json: &entry.event_json,
            doc_id: entry.doc_id.as_deref(),
            error_message: &entry.error_message,
            error_kind: entry.error_kind.to_str(),
            retry_count: entry.retry_count as i32,
            created_at: &created_at_str,
            last_retry_at: last_retry_at_str.as_deref(),
            permanent_at: permanent_at_str.as_deref(),
        };

        let id = self
            .conn
            .transaction::<i64, diesel::result::Error, _>(|conn| {
                diesel::insert_into(dlq::table).values(&new).execute(conn)?;

                diesel::select(diesel::dsl::sql::<diesel::sql_types::BigInt>(
                    "last_insert_rowid()",
                ))
                .get_result(conn)
            })?;

        Ok(id)
    }

    pub fn get_dlq_entry(&mut self, id: i64) -> Result<Option<DlqEntry>, StateError> {
        let row = dlq::table
            .filter(dlq::id.eq(id))
            .first::<DlqRow>(&mut self.conn)
            .optional()?;

        match row {
            Some(r) => Ok(Some(DlqEntry::from_row(&r)?)),
            None => Ok(None),
        }
    }

    pub fn list_dlq_entries(
        &mut self,
        config_name: Option<&str>,
        limit: usize,
    ) -> Result<Vec<DlqEntry>, StateError> {
        let rows: Vec<DlqRow> = match config_name {
            Some(name) => dlq::table
                .filter(dlq::config_name.eq(name))
                .order(dlq::created_at.desc())
                .limit(limit as i64)
                .load(&mut self.conn)?,
            None => dlq::table
                .order(dlq::created_at.desc())
                .limit(limit as i64)
                .load(&mut self.conn)?,
        };

        rows.iter().map(DlqEntry::from_row).collect()
    }

    pub fn increment_retry(&mut self, id: i64) -> Result<(), StateError> {
        let now = Utc::now().to_rfc3339();
        let rows_affected = diesel::update(dlq::table.filter(dlq::id.eq(id)))
            .set((
                dlq::retry_count.eq(dlq::retry_count + 1),
                dlq::last_retry_at.eq(&now),
            ))
            .execute(&mut self.conn)?;

        if rows_affected == 0 {
            return Err(StateError::NotFound(format!("dlq entry with id {}", id)));
        }

        Ok(())
    }

    pub fn delete_dlq_entry(&mut self, id: i64) -> Result<bool, StateError> {
        let rows_affected =
            diesel::delete(dlq::table.filter(dlq::id.eq(id))).execute(&mut self.conn)?;
        Ok(rows_affected > 0)
    }

    pub fn clear_dlq(&mut self, config_name: Option<&str>) -> Result<u64, StateError> {
        let rows_affected = match config_name {
            Some(name) => diesel::delete(dlq::table.filter(dlq::config_name.eq(name)))
                .execute(&mut self.conn)?,
            None => diesel::delete(dlq::table).execute(&mut self.conn)?,
        };
        Ok(rows_affected as u64)
    }

    pub fn list_retryable_entries(&mut self, limit: usize) -> Result<Vec<DlqEntry>, StateError> {
        let rows = dlq::table
            .filter(dlq::error_kind.eq("retryable"))
            .order(dlq::created_at.asc())
            .limit(limit as i64)
            .load::<DlqRow>(&mut self.conn)?;

        rows.iter().map(DlqEntry::from_row).collect()
    }

    pub fn mark_permanent(&mut self, id: i64, error: &str) -> Result<(), StateError> {
        let now = Utc::now().to_rfc3339();
        let rows_affected = diesel::update(dlq::table.filter(dlq::id.eq(id)))
            .set((
                dlq::error_kind.eq("permanent"),
                dlq::error_message.eq(error),
                dlq::last_retry_at.eq(&now),
                dlq::permanent_at.eq(&now),
            ))
            .execute(&mut self.conn)?;

        if rows_affected == 0 {
            return Err(StateError::NotFound(format!("dlq entry with id {}", id)));
        }

        Ok(())
    }

    /// Returns (retryable_count, permanent_count) for a given config or globally.
    pub fn dlq_count_by_kind(
        &mut self,
        config_name: Option<&str>,
    ) -> Result<(u64, u64), StateError> {
        let rows: Vec<(String, i64)> = match config_name {
            Some(name) => dlq::table
                .filter(dlq::config_name.eq(name))
                .group_by(dlq::error_kind)
                .select((dlq::error_kind, diesel::dsl::count(dlq::id)))
                .load(&mut self.conn)?,
            None => dlq::table
                .group_by(dlq::error_kind)
                .select((dlq::error_kind, diesel::dsl::count(dlq::id)))
                .load(&mut self.conn)?,
        };

        let mut retryable = 0u64;
        let mut permanent = 0u64;
        for (kind, count) in rows {
            match kind.as_str() {
                "retryable" => retryable = count as u64,
                "permanent" => permanent = count as u64,
                _ => {}
            }
        }
        Ok((retryable, permanent))
    }

    /// Delete permanent DLQ entries whose permanence is older than `max_age_hours` hours.
    pub fn clear_old_permanent_entries(&mut self, max_age_hours: u64) -> Result<u64, StateError> {
        let cutoff = Utc::now() - chrono::Duration::hours(max_age_hours as i64);
        let cutoff_str = cutoff.to_rfc3339();

        let rows_affected = diesel::delete(
            dlq::table
                .filter(dlq::error_kind.eq("permanent"))
                .filter(dlq::permanent_at.lt(&cutoff_str)),
        )
        .execute(&mut self.conn)?;

        Ok(rows_affected as u64)
    }

    pub fn dlq_count(&mut self, config_name: Option<&str>) -> Result<u64, StateError> {
        let count: i64 = match config_name {
            Some(name) => dlq::table
                .filter(dlq::config_name.eq(name))
                .count()
                .get_result(&mut self.conn)?,
            None => dlq::table.count().get_result(&mut self.conn)?,
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
        let mut db = StateDb::open(&path).unwrap();
        db.initialize().unwrap();
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
        let permanent_at = match error_kind {
            ErrorKind::Permanent => Some(Utc::now()),
            ErrorKind::Retryable => None,
        };
        DlqEntry {
            id: 0,
            config_name: config_name.to_string(),
            lsn,
            event_json: r#"{"event": "test"}"#.to_string(),
            doc_id: Some(r#"{"Uint":42}"#.to_string()),
            error_message: "Test error".to_string(),
            error_kind,
            retry_count: 0,
            created_at: Utc::now(),
            last_retry_at: None,
            permanent_at,
        }
    }

    #[test]
    fn insert_and_retrieve_entry() {
        let (_dir, mut db) = setup_dlq_db();
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
        let (_dir, mut db) = setup_dlq_db();
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
        let (_dir, mut db) = setup_dlq_db();
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
        let (_dir, mut db) = setup_dlq_db();
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
        let (_dir, mut db) = setup_dlq_db();
        let config = sample_config("film");
        db.insert_config(&config).unwrap();

        let entry = sample_dlq_entry("film", 1000, ErrorKind::Retryable);
        let id = db.insert_dlq_entry(&entry).unwrap();

        db.increment_retry(id).unwrap();

        let retrieved = db.get_dlq_entry(id).unwrap().unwrap();
        assert_eq!(retrieved.retry_count, 1);
        assert!(retrieved.last_retry_at.is_some());

        db.increment_retry(id).unwrap();

        let retrieved = db.get_dlq_entry(id).unwrap().unwrap();
        assert_eq!(retrieved.retry_count, 2);
    }

    #[test]
    fn clear_by_config_name() {
        let (_dir, mut db) = setup_dlq_db();
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
        let (_dir, mut db) = setup_dlq_db();
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
        let (_dir, mut db) = setup_dlq_db();
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
        let (_dir, mut db) = setup_dlq_db();
        let deleted = db.delete_dlq_entry(999).unwrap();
        assert!(!deleted);
    }

    #[test]
    fn dlq_count_with_config_filter() {
        let (_dir, mut db) = setup_dlq_db();
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
        let (_dir, mut db) = setup_dlq_db();
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
        let (_dir, mut db) = setup_dlq_db();
        let config = sample_config("film");
        db.insert_config(&config).unwrap();

        let entry = sample_dlq_entry("film", 1000, ErrorKind::Retryable);
        let id = db.insert_dlq_entry(&entry).unwrap();

        assert!(db.get_dlq_entry(id).unwrap().is_some());

        diesel::delete(
            crate::schema::configs::table.filter(crate::schema::configs::name.eq("film")),
        )
        .execute(&mut db.conn)
        .unwrap();

        assert!(db.get_dlq_entry(id).unwrap().is_none());
    }

    #[test]
    fn dlq_entry_requires_valid_config() {
        let (_dir, mut db) = setup_dlq_db();
        let entry = sample_dlq_entry("nonexistent_config", 1000, ErrorKind::Retryable);

        let result = db.insert_dlq_entry(&entry);
        assert!(result.is_err());
    }

    #[test]
    fn get_nonexistent_dlq_entry_returns_none() {
        let (_dir, mut db) = setup_dlq_db();
        assert!(db.get_dlq_entry(999).unwrap().is_none());
    }

    #[test]
    fn increment_retry_fails_for_nonexistent_entry() {
        let (_dir, mut db) = setup_dlq_db();
        let result = db.increment_retry(999);
        assert!(result.is_err());
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
        let (_dir, mut db) = setup_dlq_db();
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
        let (_dir, mut db) = setup_dlq_db();
        db.insert_config(&sample_config("film")).unwrap();

        let entry = sample_dlq_entry("film", 100, ErrorKind::Retryable);
        let id = db.insert_dlq_entry(&entry).unwrap();

        db.mark_permanent(id, "max retries exhausted").unwrap();

        let updated = db.get_dlq_entry(id).unwrap().unwrap();
        assert_eq!(updated.error_kind, ErrorKind::Permanent);
        assert_eq!(updated.error_message, "max retries exhausted");
        assert!(updated.last_retry_at.is_some());
        assert!(updated.permanent_at.is_some());
    }

    #[test]
    fn test_mark_permanent_nonexistent_fails() {
        let (_dir, mut db) = setup_dlq_db();
        assert!(db.mark_permanent(999, "error").is_err());
    }

    #[test]
    fn test_dlq_count_by_kind_empty() {
        let (_dir, mut db) = setup_dlq_db();
        let (r, p) = db.dlq_count_by_kind(None).unwrap();
        assert_eq!(r, 0);
        assert_eq!(p, 0);
    }

    #[test]
    fn test_dlq_count_by_kind_mixed() {
        let (_dir, mut db) = setup_dlq_db();
        db.insert_config(&sample_config("film")).unwrap();
        db.insert_config(&sample_config("actor")).unwrap();

        db.insert_dlq_entry(&sample_dlq_entry("film", 100, ErrorKind::Retryable))
            .unwrap();
        db.insert_dlq_entry(&sample_dlq_entry("film", 200, ErrorKind::Permanent))
            .unwrap();
        db.insert_dlq_entry(&sample_dlq_entry("film", 300, ErrorKind::Retryable))
            .unwrap();
        db.insert_dlq_entry(&sample_dlq_entry("actor", 400, ErrorKind::Permanent))
            .unwrap();

        let (r, p) = db.dlq_count_by_kind(None).unwrap();
        assert_eq!(r, 2);
        assert_eq!(p, 2);

        let (r, p) = db.dlq_count_by_kind(Some("film")).unwrap();
        assert_eq!(r, 2);
        assert_eq!(p, 1);

        let (r, p) = db.dlq_count_by_kind(Some("actor")).unwrap();
        assert_eq!(r, 0);
        assert_eq!(p, 1);
    }

    #[test]
    fn test_dlq_count_by_kind_nonexistent_config() {
        let (_dir, mut db) = setup_dlq_db();
        let (r, p) = db.dlq_count_by_kind(Some("nonexistent")).unwrap();
        assert_eq!(r, 0);
        assert_eq!(p, 0);
    }

    #[test]
    fn test_clear_old_permanent_entries_removes_old() {
        let (_dir, mut db) = setup_dlq_db();
        db.insert_config(&sample_config("film")).unwrap();

        let old_time = Utc::now() - chrono::Duration::hours(100);
        let mut old_entry = DlqEntry::permanent(
            "film",
            100,
            r#"{"old":true}"#.to_string(),
            None,
            "old error",
        );
        old_entry.permanent_at = Some(old_time);
        db.insert_dlq_entry(&old_entry).unwrap();

        db.insert_dlq_entry(&DlqEntry::permanent(
            "film",
            200,
            r#"{"new":true}"#.to_string(),
            None,
            "new error",
        ))
        .unwrap();

        db.insert_dlq_entry(&DlqEntry::retryable(
            "film",
            300,
            r#"{"retry":true}"#.to_string(),
            None,
            "retry error",
        ))
        .unwrap();

        let cleaned = db.clear_old_permanent_entries(72).unwrap();
        assert_eq!(cleaned, 1);

        assert_eq!(db.dlq_count(None).unwrap(), 2);
    }

    #[test]
    fn test_clear_old_permanent_entries_leaves_recent() {
        let (_dir, mut db) = setup_dlq_db();
        db.insert_config(&sample_config("film")).unwrap();

        db.insert_dlq_entry(&DlqEntry::permanent(
            "film",
            100,
            r#"{"test":true}"#.to_string(),
            None,
            "error",
        ))
        .unwrap();
        db.insert_dlq_entry(&DlqEntry::permanent(
            "film",
            200,
            r#"{"test":true}"#.to_string(),
            None,
            "error",
        ))
        .unwrap();

        let cleaned = db.clear_old_permanent_entries(72).unwrap();
        assert_eq!(cleaned, 0);
        assert_eq!(db.dlq_count(None).unwrap(), 2);
    }

    #[test]
    fn test_clear_old_permanent_entries_uses_permanent_at_not_created_at() {
        let (_dir, mut db) = setup_dlq_db();
        db.insert_config(&sample_config("film")).unwrap();

        let mut entry = sample_dlq_entry("film", 100, ErrorKind::Retryable);
        entry.created_at = Utc::now() - chrono::Duration::hours(200);
        let id = db.insert_dlq_entry(&entry).unwrap();
        db.mark_permanent(id, "max retries exhausted").unwrap();

        let cleaned = db.clear_old_permanent_entries(72).unwrap();
        assert_eq!(cleaned, 0);
        assert_eq!(db.dlq_count(None).unwrap(), 1);
    }

    #[test]
    fn test_clear_old_permanent_entries_empty_dlq() {
        let (_dir, mut db) = setup_dlq_db();
        let cleaned = db.clear_old_permanent_entries(72).unwrap();
        assert_eq!(cleaned, 0);
    }

    #[test]
    fn list_respects_limit() {
        let (_dir, mut db) = setup_dlq_db();
        db.insert_config(&sample_config("film")).unwrap();

        for i in 0..10 {
            db.insert_dlq_entry(&sample_dlq_entry("film", i, ErrorKind::Retryable))
                .unwrap();
        }

        let entries = db.list_dlq_entries(Some("film"), 5).unwrap();
        assert_eq!(entries.len(), 5);
    }
}
