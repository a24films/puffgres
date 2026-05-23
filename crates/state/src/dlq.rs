use std::str::FromStr;

use chrono::{DateTime, Utc};
use diesel::Connection;
use diesel::prelude::*;
use strum::{Display, EnumString};

use crate::epoch;
use crate::models::{DlqRow, NewDlqEntry};
use crate::schema::dlq;
use crate::{StateDb, StateError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DlqEntry {
    pub id: i64,
    pub config_name: String,
    pub lsn: u64,
    pub doc_id: Option<String>,
    pub operation: Option<DlqOperation>,
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

#[derive(Debug, Clone, PartialEq, Eq, Display, EnumString)]
#[strum(serialize_all = "lowercase")]
pub enum DlqOperation {
    Insert,
    Update,
    Delete,
}

impl DlqEntry {
    pub fn retryable(
        config_name: &str,
        lsn: u64,
        operation: DlqOperation,
        doc_id: Option<String>,
        error: &str,
    ) -> Self {
        Self {
            id: 0,
            config_name: config_name.to_string(),
            lsn,
            doc_id,
            operation: Some(operation),
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
        operation: DlqOperation,
        doc_id: Option<String>,
        error: &str,
    ) -> Self {
        Self {
            id: 0,
            config_name: config_name.to_string(),
            lsn,
            doc_id,
            operation: Some(operation),
            error_message: error.to_string(),
            error_kind: ErrorKind::Permanent,
            retry_count: 0,
            created_at: Utc::now(),
            last_retry_at: None,
            permanent_at: Some(Utc::now()),
        }
    }

    fn from_row(row: &DlqRow) -> Result<Self, StateError> {
        let created_at = epoch::from_millis(row.created_at).ok_or_else(|| {
            StateError::InvalidState(format!("invalid created_at millis: {}", row.created_at))
        })?;

        let last_retry_at = row.last_retry_at.and_then(epoch::from_millis);

        let permanent_at = row.permanent_at.and_then(epoch::from_millis);

        let error_kind = ErrorKind::from_str(&row.error_kind)?;

        Ok(Self {
            id: row.id,
            config_name: row.config_name.clone(),
            lsn: u64::from_ne_bytes(row.lsn.to_ne_bytes()),
            doc_id: row.doc_id.clone(),
            operation: row
                .operation
                .as_deref()
                .and_then(|s| DlqOperation::from_str(s).ok()),
            error_message: row.error_message.clone(),
            error_kind,
            retry_count: u32::try_from(row.retry_count).unwrap_or(0),
            created_at,
            last_retry_at,
            permanent_at,
        })
    }
}

impl StateDb {
    pub async fn insert_dlq_entry(&self, entry: &DlqEntry) -> Result<i64, StateError> {
        let e = entry.clone();
        self.run_blocking(move |conn| {
            let op_str = e.operation.as_ref().map(|o| o.to_string());
            let new = NewDlqEntry {
                config_name: &e.config_name,
                lsn: i64::from_ne_bytes(e.lsn.to_ne_bytes()),
                doc_id: e.doc_id.as_deref(),
                error_message: &e.error_message,
                error_kind: e.error_kind.to_str(),
                retry_count: i32::try_from(e.retry_count).map_err(|_| {
                    StateError::InvalidState(format!(
                        "retry_count {} exceeds i32::MAX",
                        e.retry_count
                    ))
                })?,
                created_at: epoch::to_millis(&e.created_at),
                last_retry_at: e.last_retry_at.as_ref().map(epoch::to_millis),
                permanent_at: e.permanent_at.as_ref().map(epoch::to_millis),
                operation: op_str.as_deref(),
            };

            let id = conn.transaction::<i64, diesel::result::Error, _>(|conn| {
                diesel::insert_into(dlq::table).values(&new).execute(conn)?;
                diesel::select(diesel::dsl::sql::<diesel::sql_types::BigInt>(
                    "last_insert_rowid()",
                ))
                .get_result(conn)
            })?;

            Ok(id)
        })
        .await
    }

    pub async fn get_dlq_entry(&self, id: i64) -> Result<Option<DlqEntry>, StateError> {
        self.run_blocking(move |conn| {
            let row = dlq::table
                .filter(dlq::id.eq(id))
                .first::<DlqRow>(conn)
                .optional()?;
            match row {
                Some(r) => Ok(Some(DlqEntry::from_row(&r)?)),
                None => Ok(None),
            }
        })
        .await
    }

    pub async fn list_dlq_entries(
        &self,
        config_name: Option<&str>,
        limit: usize,
    ) -> Result<Vec<DlqEntry>, StateError> {
        let name = config_name.map(|s| s.to_string());
        self.run_blocking(move |conn| {
            let limit_i64 = i64::try_from(limit).unwrap_or(i64::MAX);
            let rows: Vec<DlqRow> = match name {
                Some(n) => dlq::table
                    .filter(dlq::config_name.eq(&n))
                    .order(dlq::created_at.desc())
                    .limit(limit_i64)
                    .load(conn)?,
                None => dlq::table
                    .order(dlq::created_at.desc())
                    .limit(limit_i64)
                    .load(conn)?,
            };
            rows.iter().map(DlqEntry::from_row).collect()
        })
        .await
    }

    pub async fn increment_retry(&self, id: i64) -> Result<(), StateError> {
        self.run_blocking(move |conn| {
            let now = epoch::to_millis(&Utc::now());
            let rows_affected = diesel::update(dlq::table.filter(dlq::id.eq(id)))
                .set((
                    dlq::retry_count.eq(dlq::retry_count + 1),
                    dlq::last_retry_at.eq(now),
                ))
                .execute(conn)?;
            if rows_affected == 0 {
                return Err(StateError::NotFound(format!("dlq entry with id {id}")));
            }
            Ok(())
        })
        .await
    }

    pub async fn delete_dlq_entry(&self, id: i64) -> Result<bool, StateError> {
        self.run_blocking(move |conn| {
            let rows_affected = diesel::delete(dlq::table.filter(dlq::id.eq(id))).execute(conn)?;
            Ok(rows_affected > 0)
        })
        .await
    }

    pub async fn clear_dlq(&self, config_name: Option<&str>) -> Result<u64, StateError> {
        let name = config_name.map(|s| s.to_string());
        self.run_blocking(move |conn| {
            let rows_affected = match name {
                Some(n) => {
                    diesel::delete(dlq::table.filter(dlq::config_name.eq(&n))).execute(conn)?
                }
                None => diesel::delete(dlq::table).execute(conn)?,
            };
            Ok(u64::try_from(rows_affected).unwrap_or(0))
        })
        .await
    }

    pub async fn list_retryable_entries(&self, limit: usize) -> Result<Vec<DlqEntry>, StateError> {
        use crate::schema::configs;
        self.run_blocking(move |conn| {
            let limit_i64 = i64::try_from(limit).unwrap_or(i64::MAX);
            let rows = dlq::table
                .inner_join(configs::table)
                .filter(dlq::error_kind.eq("retryable"))
                .filter(configs::tombstone_applied_at.is_null())
                .order(dlq::created_at.asc())
                .limit(limit_i64)
                .select(dlq::all_columns)
                .load::<DlqRow>(conn)?;
            rows.iter().map(DlqEntry::from_row).collect()
        })
        .await
    }

    pub async fn mark_permanent(&self, id: i64, error: &str) -> Result<(), StateError> {
        let error = error.to_string();
        self.run_blocking(move |conn| {
            let now = epoch::to_millis(&Utc::now());
            let rows_affected = diesel::update(dlq::table.filter(dlq::id.eq(id)))
                .set((
                    dlq::error_kind.eq("permanent"),
                    dlq::error_message.eq(&error),
                    dlq::last_retry_at.eq(now),
                    dlq::permanent_at.eq(now),
                ))
                .execute(conn)?;
            if rows_affected == 0 {
                return Err(StateError::NotFound(format!("dlq entry with id {id}")));
            }
            Ok(())
        })
        .await
    }

    /// Mark all retryable DLQ entries as permanent in one shot.
    /// Returns the number of entries updated.
    pub async fn mark_all_retryable_permanent(&self, error: &str) -> Result<u64, StateError> {
        let error = error.to_string();
        self.run_blocking(move |conn| {
            let now = epoch::to_millis(&Utc::now());
            let rows_affected = diesel::update(dlq::table.filter(dlq::error_kind.eq("retryable")))
                .set((
                    dlq::error_kind.eq("permanent"),
                    dlq::error_message.eq(&error),
                    dlq::last_retry_at.eq(now),
                    dlq::permanent_at.eq(now),
                ))
                .execute(conn)?;
            Ok(u64::try_from(rows_affected).unwrap_or(0))
        })
        .await
    }

    /// Returns (retryable_count, permanent_count) for a given config or globally.
    pub async fn dlq_count_by_kind(
        &self,
        config_name: Option<&str>,
    ) -> Result<(u64, u64), StateError> {
        let name = config_name.map(|s| s.to_string());
        self.run_blocking(move |conn| {
            let rows: Vec<(String, i64)> = match name {
                Some(n) => dlq::table
                    .filter(dlq::config_name.eq(&n))
                    .group_by(dlq::error_kind)
                    .select((dlq::error_kind, diesel::dsl::count(dlq::id)))
                    .load(conn)?,
                None => dlq::table
                    .group_by(dlq::error_kind)
                    .select((dlq::error_kind, diesel::dsl::count(dlq::id)))
                    .load(conn)?,
            };

            let mut retryable = 0u64;
            let mut permanent = 0u64;
            for (kind, count) in rows {
                match kind.as_str() {
                    "retryable" => retryable = u64::try_from(count).unwrap_or(0),
                    "permanent" => permanent = u64::try_from(count).unwrap_or(0),
                    _ => {}
                }
            }
            Ok((retryable, permanent))
        })
        .await
    }

    /// Delete permanent DLQ entries whose permanence is older than `max_age_hours` hours.
    pub async fn clear_old_permanent_entries(&self, max_age_hours: u64) -> Result<u64, StateError> {
        self.run_blocking(move |conn| {
            let cutoff = Utc::now()
                - chrono::Duration::hours(i64::try_from(max_age_hours).unwrap_or(i64::MAX));
            let cutoff_millis = epoch::to_millis(&cutoff);

            let rows_affected = diesel::delete(
                dlq::table
                    .filter(dlq::error_kind.eq("permanent"))
                    .filter(dlq::permanent_at.lt(cutoff_millis)),
            )
            .execute(conn)?;
            Ok(u64::try_from(rows_affected).unwrap_or(0))
        })
        .await
    }

    pub async fn dlq_count(&self, config_name: Option<&str>) -> Result<u64, StateError> {
        let name = config_name.map(|s| s.to_string());
        self.run_blocking(move |conn| {
            let count: i64 = match name {
                Some(n) => dlq::table
                    .filter(dlq::config_name.eq(&n))
                    .count()
                    .get_result(conn)?,
                None => dlq::table.count().get_result(conn)?,
            };
            Ok(u64::try_from(count).unwrap_or(0))
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::{sample_config, setup_test_db};

    fn sample_dlq_entry(config_name: &str, lsn: u64, error_kind: ErrorKind) -> DlqEntry {
        let permanent_at = match error_kind {
            ErrorKind::Permanent => Some(Utc::now()),
            ErrorKind::Retryable => None,
        };
        DlqEntry {
            id: 0,
            config_name: config_name.to_string(),
            lsn,
            doc_id: Some(r#"{"Uint":42}"#.to_string()),
            operation: Some(DlqOperation::Insert),
            error_message: "Test error".to_string(),
            error_kind,
            retry_count: 0,
            created_at: Utc::now(),
            last_retry_at: None,
            permanent_at,
        }
    }

    #[tokio::test]
    async fn insert_and_retrieve_entry() {
        let (_dir, db) = setup_test_db().await;
        db.insert_config(&sample_config("film")).await.unwrap();

        let entry = sample_dlq_entry("film", 1000, ErrorKind::Retryable);
        let id = db.insert_dlq_entry(&entry).await.unwrap();

        let retrieved = db.get_dlq_entry(id).await.unwrap().unwrap();
        assert_eq!(retrieved.config_name, "film");
        assert_eq!(retrieved.lsn, 1000);
        assert_eq!(retrieved.doc_id, Some(r#"{"Uint":42}"#.to_string()));
        assert_eq!(retrieved.operation, Some(DlqOperation::Insert));
        assert_eq!(retrieved.error_kind, ErrorKind::Retryable);
        assert_eq!(retrieved.retry_count, 0);
        assert!(retrieved.last_retry_at.is_none());
    }

    #[tokio::test]
    async fn insert_and_retrieve_entry_without_doc_id() {
        let (_dir, db) = setup_test_db().await;
        db.insert_config(&sample_config("film")).await.unwrap();

        let mut entry = sample_dlq_entry("film", 1000, ErrorKind::Retryable);
        entry.doc_id = None;
        let id = db.insert_dlq_entry(&entry).await.unwrap();

        let retrieved = db.get_dlq_entry(id).await.unwrap().unwrap();
        assert_eq!(retrieved.doc_id, None);
    }

    #[tokio::test]
    async fn insert_and_retrieve_delete_operation() {
        let (_dir, db) = setup_test_db().await;
        db.insert_config(&sample_config("film")).await.unwrap();

        let entry = DlqEntry::retryable(
            "film",
            1000,
            DlqOperation::Delete,
            Some(r#"{"Uint":42}"#.to_string()),
            "network error",
        );
        let id = db.insert_dlq_entry(&entry).await.unwrap();

        let retrieved = db.get_dlq_entry(id).await.unwrap().unwrap();
        assert_eq!(retrieved.operation, Some(DlqOperation::Delete));
    }

    #[tokio::test]
    async fn list_with_config_filter() {
        let (_dir, db) = setup_test_db().await;
        db.insert_config(&sample_config("film")).await.unwrap();
        db.insert_config(&sample_config("actor")).await.unwrap();

        db.insert_dlq_entry(&sample_dlq_entry("film", 100, ErrorKind::Retryable))
            .await
            .unwrap();
        db.insert_dlq_entry(&sample_dlq_entry("film", 200, ErrorKind::Permanent))
            .await
            .unwrap();
        db.insert_dlq_entry(&sample_dlq_entry("actor", 300, ErrorKind::Retryable))
            .await
            .unwrap();

        let film_entries = db.list_dlq_entries(Some("film"), 100).await.unwrap();
        assert_eq!(film_entries.len(), 2);
        assert!(film_entries.iter().all(|e| e.config_name == "film"));
    }

    #[tokio::test]
    async fn list_without_config_filter() {
        let (_dir, db) = setup_test_db().await;
        db.insert_config(&sample_config("film")).await.unwrap();
        db.insert_config(&sample_config("actor")).await.unwrap();

        db.insert_dlq_entry(&sample_dlq_entry("film", 100, ErrorKind::Retryable))
            .await
            .unwrap();
        db.insert_dlq_entry(&sample_dlq_entry("actor", 200, ErrorKind::Permanent))
            .await
            .unwrap();

        let all_entries = db.list_dlq_entries(None, 100).await.unwrap();
        assert_eq!(all_entries.len(), 2);
    }

    #[tokio::test]
    async fn increment_retry_count() {
        let (_dir, db) = setup_test_db().await;
        db.insert_config(&sample_config("film")).await.unwrap();

        let entry = sample_dlq_entry("film", 1000, ErrorKind::Retryable);
        let id = db.insert_dlq_entry(&entry).await.unwrap();

        db.increment_retry(id).await.unwrap();

        let retrieved = db.get_dlq_entry(id).await.unwrap().unwrap();
        assert_eq!(retrieved.retry_count, 1);
        assert!(retrieved.last_retry_at.is_some());

        db.increment_retry(id).await.unwrap();

        let retrieved = db.get_dlq_entry(id).await.unwrap().unwrap();
        assert_eq!(retrieved.retry_count, 2);
    }

    #[tokio::test]
    async fn clear_by_config_name() {
        let (_dir, db) = setup_test_db().await;
        db.insert_config(&sample_config("film")).await.unwrap();
        db.insert_config(&sample_config("actor")).await.unwrap();

        db.insert_dlq_entry(&sample_dlq_entry("film", 100, ErrorKind::Retryable))
            .await
            .unwrap();
        db.insert_dlq_entry(&sample_dlq_entry("film", 200, ErrorKind::Permanent))
            .await
            .unwrap();
        db.insert_dlq_entry(&sample_dlq_entry("actor", 300, ErrorKind::Retryable))
            .await
            .unwrap();

        let deleted = db.clear_dlq(Some("film")).await.unwrap();
        assert_eq!(deleted, 2);

        let remaining = db.list_dlq_entries(None, 100).await.unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].config_name, "actor");
    }

    #[tokio::test]
    async fn clear_all() {
        let (_dir, db) = setup_test_db().await;
        db.insert_config(&sample_config("film")).await.unwrap();
        db.insert_config(&sample_config("actor")).await.unwrap();

        db.insert_dlq_entry(&sample_dlq_entry("film", 100, ErrorKind::Retryable))
            .await
            .unwrap();
        db.insert_dlq_entry(&sample_dlq_entry("actor", 200, ErrorKind::Permanent))
            .await
            .unwrap();

        let deleted = db.clear_dlq(None).await.unwrap();
        assert_eq!(deleted, 2);

        let remaining = db.list_dlq_entries(None, 100).await.unwrap();
        assert_eq!(remaining.len(), 0);
    }

    #[tokio::test]
    async fn delete_dlq_entry_returns_true_when_exists() {
        let (_dir, db) = setup_test_db().await;
        db.insert_config(&sample_config("film")).await.unwrap();

        let entry = sample_dlq_entry("film", 1000, ErrorKind::Retryable);
        let id = db.insert_dlq_entry(&entry).await.unwrap();

        let deleted = db.delete_dlq_entry(id).await.unwrap();
        assert!(deleted);
        assert!(db.get_dlq_entry(id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn delete_dlq_entry_returns_false_when_not_exists() {
        let (_dir, db) = setup_test_db().await;
        let deleted = db.delete_dlq_entry(999).await.unwrap();
        assert!(!deleted);
    }

    #[tokio::test]
    async fn dlq_count_with_config_filter() {
        let (_dir, db) = setup_test_db().await;
        db.insert_config(&sample_config("film")).await.unwrap();
        db.insert_config(&sample_config("actor")).await.unwrap();

        db.insert_dlq_entry(&sample_dlq_entry("film", 100, ErrorKind::Retryable))
            .await
            .unwrap();
        db.insert_dlq_entry(&sample_dlq_entry("film", 200, ErrorKind::Permanent))
            .await
            .unwrap();
        db.insert_dlq_entry(&sample_dlq_entry("actor", 300, ErrorKind::Retryable))
            .await
            .unwrap();

        assert_eq!(db.dlq_count(Some("film")).await.unwrap(), 2);
        assert_eq!(db.dlq_count(Some("actor")).await.unwrap(), 1);
    }

    #[tokio::test]
    async fn dlq_count_without_filter() {
        let (_dir, db) = setup_test_db().await;
        db.insert_config(&sample_config("film")).await.unwrap();
        db.insert_config(&sample_config("actor")).await.unwrap();

        db.insert_dlq_entry(&sample_dlq_entry("film", 100, ErrorKind::Retryable))
            .await
            .unwrap();
        db.insert_dlq_entry(&sample_dlq_entry("actor", 200, ErrorKind::Permanent))
            .await
            .unwrap();

        assert_eq!(db.dlq_count(None).await.unwrap(), 2);
    }

    #[tokio::test]
    async fn dlq_entry_deleted_when_config_deleted() {
        let (_dir, db) = setup_test_db().await;
        db.insert_config(&sample_config("film")).await.unwrap();

        let entry = sample_dlq_entry("film", 1000, ErrorKind::Retryable);
        let id = db.insert_dlq_entry(&entry).await.unwrap();

        assert!(db.get_dlq_entry(id).await.unwrap().is_some());

        db.delete_config("film").await.unwrap();

        assert!(db.get_dlq_entry(id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn dlq_entry_requires_valid_config() {
        let (_dir, db) = setup_test_db().await;
        let entry = sample_dlq_entry("nonexistent_config", 1000, ErrorKind::Retryable);

        let result = db.insert_dlq_entry(&entry).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn get_nonexistent_dlq_entry_returns_none() {
        let (_dir, db) = setup_test_db().await;
        assert!(db.get_dlq_entry(999).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn increment_retry_fails_for_nonexistent_entry() {
        let (_dir, db) = setup_test_db().await;
        let result = db.increment_retry(999).await;
        assert!(result.is_err());
    }

    #[test]
    fn dlq_entry_retryable_constructor() {
        let entry = DlqEntry::retryable(
            "film",
            1000,
            DlqOperation::Insert,
            Some(r#"{"Uint":1}"#.to_string()),
            "network timeout",
        );
        assert_eq!(entry.config_name, "film");
        assert_eq!(entry.lsn, 1000);
        assert_eq!(entry.error_kind, ErrorKind::Retryable);
        assert_eq!(entry.operation, Some(DlqOperation::Insert));
        assert_eq!(entry.retry_count, 0);
        assert_eq!(entry.error_message, "network timeout");
        assert_eq!(entry.doc_id, Some(r#"{"Uint":1}"#.to_string()));
        assert!(entry.last_retry_at.is_none());
    }

    #[test]
    fn dlq_entry_permanent_constructor() {
        let entry = DlqEntry::permanent(
            "film",
            2000,
            DlqOperation::Update,
            Some(r#"{"Uint":2}"#.to_string()),
            "bad transform",
        );
        assert_eq!(entry.config_name, "film");
        assert_eq!(entry.lsn, 2000);
        assert_eq!(entry.error_kind, ErrorKind::Permanent);
        assert_eq!(entry.operation, Some(DlqOperation::Update));
        assert_eq!(entry.retry_count, 0);
        assert_eq!(entry.error_message, "bad transform");
        assert_eq!(entry.doc_id, Some(r#"{"Uint":2}"#.to_string()));
    }

    #[tokio::test]
    async fn list_retryable_entries() {
        let (_dir, db) = setup_test_db().await;
        db.insert_config(&sample_config("film")).await.unwrap();

        db.insert_dlq_entry(&sample_dlq_entry("film", 100, ErrorKind::Retryable))
            .await
            .unwrap();
        db.insert_dlq_entry(&sample_dlq_entry("film", 200, ErrorKind::Permanent))
            .await
            .unwrap();
        db.insert_dlq_entry(&sample_dlq_entry("film", 300, ErrorKind::Retryable))
            .await
            .unwrap();

        let retryable = db.list_retryable_entries(100).await.unwrap();
        assert_eq!(retryable.len(), 2);
        assert!(
            retryable
                .iter()
                .all(|e| e.error_kind == ErrorKind::Retryable)
        );
    }

    #[tokio::test]
    async fn list_retryable_entries_excludes_tombstoned() {
        let (_dir, db) = setup_test_db().await;
        db.insert_config(&sample_config("film")).await.unwrap();
        db.insert_config(&sample_config("actor")).await.unwrap();

        db.insert_dlq_entry(&sample_dlq_entry("film", 100, ErrorKind::Retryable))
            .await
            .unwrap();
        db.insert_dlq_entry(&sample_dlq_entry("actor", 200, ErrorKind::Retryable))
            .await
            .unwrap();

        db.tombstone_config("actor").await.unwrap();

        let retryable = db.list_retryable_entries(100).await.unwrap();
        assert_eq!(retryable.len(), 1);
        assert_eq!(retryable[0].config_name, "film");
    }

    #[tokio::test]
    async fn mark_permanent() {
        let (_dir, db) = setup_test_db().await;
        db.insert_config(&sample_config("film")).await.unwrap();

        let entry = sample_dlq_entry("film", 100, ErrorKind::Retryable);
        let id = db.insert_dlq_entry(&entry).await.unwrap();

        db.mark_permanent(id, "max retries exhausted")
            .await
            .unwrap();

        let updated = db.get_dlq_entry(id).await.unwrap().unwrap();
        assert_eq!(updated.error_kind, ErrorKind::Permanent);
        assert_eq!(updated.error_message, "max retries exhausted");
        assert!(updated.last_retry_at.is_some());
        assert!(updated.permanent_at.is_some());
    }

    #[tokio::test]
    async fn mark_permanent_nonexistent_fails() {
        let (_dir, db) = setup_test_db().await;
        assert!(db.mark_permanent(999, "error").await.is_err());
    }

    #[tokio::test]
    async fn dlq_count_by_kind_empty() {
        let (_dir, db) = setup_test_db().await;
        let (r, p) = db.dlq_count_by_kind(None).await.unwrap();
        assert_eq!(r, 0);
        assert_eq!(p, 0);
    }

    #[tokio::test]
    async fn dlq_count_by_kind_mixed() {
        let (_dir, db) = setup_test_db().await;
        db.insert_config(&sample_config("film")).await.unwrap();
        db.insert_config(&sample_config("actor")).await.unwrap();

        db.insert_dlq_entry(&sample_dlq_entry("film", 100, ErrorKind::Retryable))
            .await
            .unwrap();
        db.insert_dlq_entry(&sample_dlq_entry("film", 200, ErrorKind::Permanent))
            .await
            .unwrap();
        db.insert_dlq_entry(&sample_dlq_entry("film", 300, ErrorKind::Retryable))
            .await
            .unwrap();
        db.insert_dlq_entry(&sample_dlq_entry("actor", 400, ErrorKind::Permanent))
            .await
            .unwrap();

        let (r, p) = db.dlq_count_by_kind(None).await.unwrap();
        assert_eq!(r, 2);
        assert_eq!(p, 2);

        let (r, p) = db.dlq_count_by_kind(Some("film")).await.unwrap();
        assert_eq!(r, 2);
        assert_eq!(p, 1);

        let (r, p) = db.dlq_count_by_kind(Some("actor")).await.unwrap();
        assert_eq!(r, 0);
        assert_eq!(p, 1);
    }

    #[tokio::test]
    async fn dlq_count_by_kind_nonexistent_config() {
        let (_dir, db) = setup_test_db().await;
        let (r, p) = db.dlq_count_by_kind(Some("nonexistent")).await.unwrap();
        assert_eq!(r, 0);
        assert_eq!(p, 0);
    }

    #[tokio::test]
    async fn clear_old_permanent_entries_removes_old() {
        let (_dir, db) = setup_test_db().await;
        db.insert_config(&sample_config("film")).await.unwrap();

        let old_time = Utc::now() - chrono::Duration::hours(100);
        let mut old_entry =
            DlqEntry::permanent("film", 100, DlqOperation::Insert, None, "old error");
        old_entry.permanent_at = Some(old_time);
        db.insert_dlq_entry(&old_entry).await.unwrap();

        db.insert_dlq_entry(&DlqEntry::permanent(
            "film",
            200,
            DlqOperation::Insert,
            None,
            "new error",
        ))
        .await
        .unwrap();

        db.insert_dlq_entry(&DlqEntry::retryable(
            "film",
            300,
            DlqOperation::Insert,
            None,
            "retry error",
        ))
        .await
        .unwrap();

        let cleaned = db.clear_old_permanent_entries(72).await.unwrap();
        assert_eq!(cleaned, 1);

        assert_eq!(db.dlq_count(None).await.unwrap(), 2);
    }

    #[tokio::test]
    async fn clear_old_permanent_entries_leaves_recent() {
        let (_dir, db) = setup_test_db().await;
        db.insert_config(&sample_config("film")).await.unwrap();

        db.insert_dlq_entry(&DlqEntry::permanent(
            "film",
            100,
            DlqOperation::Insert,
            None,
            "error",
        ))
        .await
        .unwrap();
        db.insert_dlq_entry(&DlqEntry::permanent(
            "film",
            200,
            DlqOperation::Insert,
            None,
            "error",
        ))
        .await
        .unwrap();

        let cleaned = db.clear_old_permanent_entries(72).await.unwrap();
        assert_eq!(cleaned, 0);
        assert_eq!(db.dlq_count(None).await.unwrap(), 2);
    }

    #[tokio::test]
    async fn clear_old_permanent_entries_uses_permanent_at_not_created_at() {
        let (_dir, db) = setup_test_db().await;
        db.insert_config(&sample_config("film")).await.unwrap();

        let mut entry = sample_dlq_entry("film", 100, ErrorKind::Retryable);
        entry.created_at = Utc::now() - chrono::Duration::hours(200);
        let id = db.insert_dlq_entry(&entry).await.unwrap();
        db.mark_permanent(id, "max retries exhausted")
            .await
            .unwrap();

        let cleaned = db.clear_old_permanent_entries(72).await.unwrap();
        assert_eq!(cleaned, 0);
        assert_eq!(db.dlq_count(None).await.unwrap(), 1);
    }

    #[tokio::test]
    async fn clear_old_permanent_entries_empty_dlq() {
        let (_dir, db) = setup_test_db().await;
        let cleaned = db.clear_old_permanent_entries(72).await.unwrap();
        assert_eq!(cleaned, 0);
    }

    #[tokio::test]
    async fn list_respects_limit() {
        let (_dir, db) = setup_test_db().await;
        db.insert_config(&sample_config("film")).await.unwrap();

        for i in 0..10 {
            db.insert_dlq_entry(&sample_dlq_entry("film", i, ErrorKind::Retryable))
                .await
                .unwrap();
        }

        let entries = db.list_dlq_entries(Some("film"), 5).await.unwrap();
        assert_eq!(entries.len(), 5);
    }
}
