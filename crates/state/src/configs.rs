use chrono::{DateTime, Utc};
use diesel::prelude::*;

use crate::epoch;
use crate::models::{ConfigRow, NewConfig};
use crate::schema::configs;
use crate::{StateError, Store};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigRecord {
    pub name: String,
    pub namespace: String,
    pub content_hash: String,
    pub transform_hash: Option<String>,
    pub applied_at: DateTime<Utc>,
    pub tombstone_applied_at: Option<DateTime<Utc>>,
    pub namespace_prefix: Option<String>,
}

impl ConfigRecord {
    fn from_row(row: &ConfigRow) -> Result<Self, StateError> {
        let applied_at = epoch::from_millis(row.applied_at).ok_or_else(|| {
            StateError::InvalidState(format!("invalid applied_at millis: {}", row.applied_at))
        })?;

        let tombstone_applied_at = row
            .tombstone_applied_at
            .map(|ms| {
                epoch::from_millis(ms).ok_or_else(|| {
                    StateError::InvalidState(format!("invalid tombstone_applied_at millis: {ms}"))
                })
            })
            .transpose()?;

        Ok(Self {
            name: row.name.clone(),
            namespace: row.namespace.clone(),
            content_hash: row.content_hash.clone(),
            transform_hash: row.transform_hash.clone(),
            applied_at,
            tombstone_applied_at,
            namespace_prefix: row.namespace_prefix.clone(),
        })
    }
}

impl Store {
    pub async fn insert_config(&self, config: &ConfigRecord) -> Result<(), StateError> {
        let c = config.clone();
        self.run_blocking(move |conn| {
            let new = NewConfig {
                name: &c.name,
                namespace: &c.namespace,
                content_hash: &c.content_hash,
                transform_hash: c.transform_hash.as_deref(),
                applied_at: epoch::to_millis(&c.applied_at),
                tombstone_applied_at: None,
                namespace_prefix: c.namespace_prefix.as_deref(),
            };
            diesel::insert_into(configs::table)
                .values(&new)
                .execute(conn)?;
            Ok(())
        })
        .await
    }

    /// Insert multiple configs atomically in a single transaction.
    pub async fn insert_configs(&self, records: &[ConfigRecord]) -> Result<(), StateError> {
        let records = records.to_vec();
        self.run_blocking(move |conn| {
            conn.transaction::<_, StateError, _>(|conn| {
                for c in &records {
                    let new = NewConfig {
                        name: &c.name,
                        namespace: &c.namespace,
                        content_hash: &c.content_hash,
                        transform_hash: c.transform_hash.as_deref(),
                        applied_at: epoch::to_millis(&c.applied_at),
                        tombstone_applied_at: None,
                        namespace_prefix: c.namespace_prefix.as_deref(),
                    };
                    diesel::insert_into(configs::table)
                        .values(&new)
                        .execute(conn)?;
                }
                Ok(())
            })
        })
        .await
    }

    pub async fn get_config(&self, name: &str) -> Result<Option<ConfigRecord>, StateError> {
        let name = name.to_string();
        self.run_blocking(move |conn| {
            let row = configs::table
                .filter(configs::name.eq(&name))
                .first::<ConfigRow>(conn)
                .optional()?;
            match row {
                Some(r) => Ok(Some(ConfigRecord::from_row(&r)?)),
                None => Ok(None),
            }
        })
        .await
    }

    pub async fn list_configs(&self) -> Result<Vec<ConfigRecord>, StateError> {
        self.run_blocking(|conn| {
            let rows = configs::table
                .order(configs::name.asc())
                .load::<ConfigRow>(conn)?;
            rows.iter().map(ConfigRecord::from_row).collect()
        })
        .await
    }

    pub async fn tombstone_config(&self, name: &str) -> Result<(), StateError> {
        let name = name.to_string();
        self.run_blocking(move |conn| {
            let now = epoch::to_millis(&Utc::now());
            let updated = diesel::update(configs::table.filter(configs::name.eq(&name)))
                .set(configs::tombstone_applied_at.eq(now))
                .execute(conn)?;
            if updated == 0 {
                return Err(StateError::InvalidState(format!(
                    "config '{name}' not found"
                )));
            }
            Ok(())
        })
        .await
    }

    pub async fn is_tombstoned(&self, name: &str) -> Result<bool, StateError> {
        let name = name.to_string();
        self.run_blocking(move |conn| {
            let row = configs::table
                .filter(configs::name.eq(&name))
                .filter(configs::tombstone_applied_at.is_not_null())
                .first::<ConfigRow>(conn)
                .optional()?;
            Ok(row.is_some())
        })
        .await
    }

    pub async fn list_active_configs(&self) -> Result<Vec<ConfigRecord>, StateError> {
        self.run_blocking(|conn| {
            let rows = configs::table
                .filter(configs::tombstone_applied_at.is_null())
                .order(configs::name.asc())
                .load::<ConfigRow>(conn)?;
            rows.iter().map(ConfigRecord::from_row).collect()
        })
        .await
    }

    pub async fn list_tombstoned_configs(&self) -> Result<Vec<ConfigRecord>, StateError> {
        self.run_blocking(|conn| {
            let rows = configs::table
                .filter(configs::tombstone_applied_at.is_not_null())
                .order(configs::name.asc())
                .load::<ConfigRow>(conn)?;
            rows.iter().map(ConfigRecord::from_row).collect()
        })
        .await
    }

    pub async fn get_namespace_prefix(
        &self,
        config_name: &str,
    ) -> Result<Option<String>, StateError> {
        let name = config_name.to_string();
        self.run_blocking(move |conn| {
            let row = configs::table
                .filter(configs::name.eq(&name))
                .first::<ConfigRow>(conn)
                .optional()?;
            match row {
                Some(r) => Ok(r.namespace_prefix),
                None => Err(StateError::InvalidState(format!(
                    "config '{name}' not found"
                ))),
            }
        })
        .await
    }

    pub async fn delete_config(&self, name: &str) -> Result<bool, StateError> {
        let name = name.to_string();
        self.run_blocking(move |conn| {
            let rows_affected =
                diesel::delete(configs::table.filter(configs::name.eq(&name))).execute(conn)?;
            Ok(rows_affected > 0)
        })
        .await
    }

    pub async fn get_last_applied_config(&self) -> Result<Option<ConfigRecord>, StateError> {
        self.run_blocking(|conn| {
            let row = configs::table
                .order(configs::applied_at.desc())
                .first::<ConfigRow>(conn)
                .optional()?;
            match row {
                Some(r) => Ok(Some(ConfigRecord::from_row(&r)?)),
                None => Ok(None),
            }
        })
        .await
    }

    pub async fn set_namespace_prefix(
        &self,
        config_name: &str,
        prefix: Option<&str>,
    ) -> Result<(), StateError> {
        let name = config_name.to_string();
        let prefix = prefix.map(|s| s.to_string());
        self.run_blocking(move |conn| {
            let updated = diesel::update(configs::table.filter(configs::name.eq(&name)))
                .set(configs::namespace_prefix.eq(prefix.as_deref()))
                .execute(conn)?;
            if updated == 0 {
                return Err(StateError::InvalidState(format!(
                    "config '{name}' not found"
                )));
            }
            Ok(())
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use crate::test_helpers::{sample_config, setup_test_db};

    #[tokio::test]
    async fn insert_and_retrieve_config() {
        let db = setup_test_db().await;
        let config = sample_config("film");

        db.insert_config(&config).await.unwrap();

        let retrieved = db.get_config("film").await.unwrap().unwrap();
        assert_eq!(retrieved.name, "film");
        assert_eq!(retrieved.namespace, "film");
        assert_eq!(retrieved.content_hash, "abc123");
        assert!(retrieved.transform_hash.is_none());
        assert!(retrieved.tombstone_applied_at.is_none());
    }

    #[tokio::test]
    async fn list_multiple_configs() {
        let db = setup_test_db().await;

        db.insert_config(&sample_config("alpha")).await.unwrap();
        db.insert_config(&sample_config("beta")).await.unwrap();
        db.insert_config(&sample_config("gamma")).await.unwrap();

        let configs = db.list_configs().await.unwrap();
        assert_eq!(configs.len(), 3);
        assert_eq!(configs[0].name, "alpha");
        assert_eq!(configs[1].name, "beta");
        assert_eq!(configs[2].name, "gamma");
    }

    #[tokio::test]
    async fn duplicate_name_fails() {
        let db = setup_test_db().await;
        db.insert_config(&sample_config("film")).await.unwrap();

        let dup = sample_config("film");
        let result = db.insert_config(&dup).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn duplicate_namespace_fails() {
        let db = setup_test_db().await;
        db.insert_config(&sample_config("film")).await.unwrap();

        let mut dup = sample_config("movie");
        dup.namespace = "film".to_string();

        let result = db.insert_config(&dup).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn get_nonexistent_returns_none() {
        let db = setup_test_db().await;
        assert!(db.get_config("nonexistent").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn config_with_transform_hash() {
        let db = setup_test_db().await;
        let mut config = sample_config("film");
        config.transform_hash = Some("transform_abc".to_string());

        db.insert_config(&config).await.unwrap();

        let retrieved = db.get_config("film").await.unwrap().unwrap();
        assert_eq!(retrieved.transform_hash, Some("transform_abc".to_string()));
    }

    #[tokio::test]
    async fn tombstone_config_sets_timestamp() {
        let db = setup_test_db().await;
        db.insert_config(&sample_config("film")).await.unwrap();

        db.tombstone_config("film").await.unwrap();

        let config = db.get_config("film").await.unwrap().unwrap();
        assert!(config.tombstone_applied_at.is_some());
    }

    #[tokio::test]
    async fn tombstone_nonexistent_errors() {
        let db = setup_test_db().await;
        let result = db.tombstone_config("nonexistent").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn is_tombstoned_returns_false_for_active() {
        let db = setup_test_db().await;
        db.insert_config(&sample_config("film")).await.unwrap();
        assert!(!db.is_tombstoned("film").await.unwrap());
    }

    #[tokio::test]
    async fn is_tombstoned_returns_true_after_tombstone() {
        let db = setup_test_db().await;
        db.insert_config(&sample_config("film")).await.unwrap();
        db.tombstone_config("film").await.unwrap();
        assert!(db.is_tombstoned("film").await.unwrap());
    }

    #[tokio::test]
    async fn list_active_excludes_tombstoned() {
        let db = setup_test_db().await;
        db.insert_config(&sample_config("alpha")).await.unwrap();
        db.insert_config(&sample_config("beta")).await.unwrap();
        db.insert_config(&sample_config("gamma")).await.unwrap();

        db.tombstone_config("beta").await.unwrap();

        let active = db.list_active_configs().await.unwrap();
        assert_eq!(active.len(), 2);
        assert_eq!(active[0].name, "alpha");
        assert_eq!(active[1].name, "gamma");
    }

    #[tokio::test]
    async fn list_tombstoned_returns_only_tombstoned() {
        let db = setup_test_db().await;
        db.insert_config(&sample_config("alpha")).await.unwrap();
        db.insert_config(&sample_config("beta")).await.unwrap();

        db.tombstone_config("alpha").await.unwrap();

        let tombstoned = db.list_tombstoned_configs().await.unwrap();
        assert_eq!(tombstoned.len(), 1);
        assert_eq!(tombstoned[0].name, "alpha");
    }

    #[tokio::test]
    async fn delete_config_removes_row() {
        let db = setup_test_db().await;
        db.insert_config(&sample_config("film")).await.unwrap();
        assert!(db.get_config("film").await.unwrap().is_some());

        let deleted = db.delete_config("film").await.unwrap();
        assert!(deleted);
        assert!(db.get_config("film").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn delete_config_nonexistent_returns_false() {
        let db = setup_test_db().await;
        let deleted = db.delete_config("nonexistent").await.unwrap();
        assert!(!deleted);
    }

    #[tokio::test]
    async fn delete_config_does_not_affect_others() {
        let db = setup_test_db().await;
        db.insert_config(&sample_config("alpha")).await.unwrap();
        db.insert_config(&sample_config("beta")).await.unwrap();

        db.delete_config("alpha").await.unwrap();

        assert!(db.get_config("alpha").await.unwrap().is_none());
        assert!(db.get_config("beta").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn get_last_applied_config_returns_most_recent() {
        let db = setup_test_db().await;

        let mut c1 = sample_config("alpha");
        c1.applied_at = chrono::Utc::now() - chrono::Duration::hours(2);
        db.insert_config(&c1).await.unwrap();

        let mut c2 = sample_config("beta");
        c2.applied_at = chrono::Utc::now() - chrono::Duration::hours(1);
        db.insert_config(&c2).await.unwrap();

        let mut c3 = sample_config("gamma");
        c3.applied_at = chrono::Utc::now();
        db.insert_config(&c3).await.unwrap();

        let last = db.get_last_applied_config().await.unwrap().unwrap();
        assert_eq!(last.name, "gamma");
    }

    #[tokio::test]
    async fn get_last_applied_config_empty_returns_none() {
        let db = setup_test_db().await;
        assert!(db.get_last_applied_config().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn get_last_applied_config_single() {
        let db = setup_test_db().await;
        db.insert_config(&sample_config("only")).await.unwrap();

        let last = db.get_last_applied_config().await.unwrap().unwrap();
        assert_eq!(last.name, "only");
    }

    #[tokio::test]
    async fn insert_configs_batch_is_atomic() {
        let db = setup_test_db().await;
        let configs = vec![
            sample_config("alpha"),
            sample_config("beta"),
            sample_config("gamma"),
        ];
        db.insert_configs(&configs).await.unwrap();
        assert_eq!(db.list_configs().await.unwrap().len(), 3);
    }

    #[tokio::test]
    async fn insert_configs_rolls_back_on_duplicate() {
        let db = setup_test_db().await;
        db.insert_config(&sample_config("alpha")).await.unwrap();

        // Second batch includes "alpha" again — should fail and roll back "beta"
        let configs = vec![sample_config("beta"), sample_config("alpha")];
        assert!(db.insert_configs(&configs).await.is_err());
        // Only the original "alpha" should exist
        assert_eq!(db.list_configs().await.unwrap().len(), 1);
        assert!(db.get_config("beta").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn timestamp_roundtrips_correctly() {
        let db = setup_test_db().await;
        let config = sample_config("film");
        let original_ts = config.applied_at;
        db.insert_config(&config).await.unwrap();

        let retrieved = db.get_config("film").await.unwrap().unwrap();
        // Epoch millis has millisecond precision, so truncate to millis for comparison
        let diff = (original_ts - retrieved.applied_at)
            .num_milliseconds()
            .abs();
        assert!(diff == 0, "timestamps should roundtrip within millis");
    }
}
