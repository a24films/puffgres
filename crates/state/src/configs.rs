use chrono::{DateTime, Utc};
use diesel::prelude::*;

use crate::models::{ConfigRow, NewConfig};
use crate::schema::configs;
use crate::{StateDb, StateError};

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
        let applied_at = DateTime::parse_from_rfc3339(&row.applied_at)
            .map(|dt| dt.with_timezone(&Utc))
            .map_err(|e| StateError::InvalidState(format!("invalid applied_at: {e}")))?;

        let tombstone_applied_at = row
            .tombstone_applied_at
            .as_deref()
            .map(|s| {
                DateTime::parse_from_rfc3339(s)
                    .map(|dt| dt.with_timezone(&Utc))
                    .map_err(|e| {
                        StateError::InvalidState(format!("invalid tombstone_applied_at: {e}"))
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

impl StateDb {
    pub fn insert_config(&mut self, config: &ConfigRecord) -> Result<(), StateError> {
        let applied_at_str = config.applied_at.to_rfc3339();
        let new = NewConfig {
            name: &config.name,
            namespace: &config.namespace,
            content_hash: &config.content_hash,
            transform_hash: config.transform_hash.as_deref(),
            applied_at: &applied_at_str,
            tombstone_applied_at: None,
            namespace_prefix: config.namespace_prefix.as_deref(),
        };

        diesel::insert_into(configs::table)
            .values(&new)
            .execute(&mut self.conn)?;

        Ok(())
    }

    pub fn get_config(&mut self, name: &str) -> Result<Option<ConfigRecord>, StateError> {
        let row = configs::table
            .filter(configs::name.eq(name))
            .first::<ConfigRow>(&mut self.conn)
            .optional()?;

        match row {
            Some(r) => Ok(Some(ConfigRecord::from_row(&r)?)),
            None => Ok(None),
        }
    }

    pub fn list_configs(&mut self) -> Result<Vec<ConfigRecord>, StateError> {
        let rows = configs::table
            .order(configs::name.asc())
            .load::<ConfigRow>(&mut self.conn)?;

        rows.iter().map(ConfigRecord::from_row).collect()
    }

    pub fn tombstone_config(&mut self, name: &str) -> Result<(), StateError> {
        let now = Utc::now().to_rfc3339();
        let updated = diesel::update(configs::table.filter(configs::name.eq(name)))
            .set(configs::tombstone_applied_at.eq(&now))
            .execute(&mut self.conn)?;

        if updated == 0 {
            return Err(StateError::InvalidState(format!(
                "config '{name}' not found"
            )));
        }

        Ok(())
    }

    pub fn is_tombstoned(&mut self, name: &str) -> Result<bool, StateError> {
        let row = configs::table
            .filter(configs::name.eq(name))
            .filter(configs::tombstone_applied_at.is_not_null())
            .first::<ConfigRow>(&mut self.conn)
            .optional()?;

        Ok(row.is_some())
    }

    pub fn list_active_configs(&mut self) -> Result<Vec<ConfigRecord>, StateError> {
        let rows = configs::table
            .filter(configs::tombstone_applied_at.is_null())
            .order(configs::name.asc())
            .load::<ConfigRow>(&mut self.conn)?;

        rows.iter().map(ConfigRecord::from_row).collect()
    }

    pub fn list_tombstoned_configs(&mut self) -> Result<Vec<ConfigRecord>, StateError> {
        let rows = configs::table
            .filter(configs::tombstone_applied_at.is_not_null())
            .order(configs::name.asc())
            .load::<ConfigRow>(&mut self.conn)?;

        rows.iter().map(ConfigRecord::from_row).collect()
    }

    pub fn get_namespace_prefix(
        &mut self,
        config_name: &str,
    ) -> Result<Option<String>, StateError> {
        let row = configs::table
            .filter(configs::name.eq(config_name))
            .first::<ConfigRow>(&mut self.conn)
            .optional()?;

        match row {
            Some(r) => Ok(r.namespace_prefix),
            None => Err(StateError::InvalidState(format!(
                "config '{config_name}' not found"
            ))),
        }
    }

    pub fn delete_config(&mut self, name: &str) -> Result<bool, StateError> {
        let rows_affected = diesel::delete(configs::table.filter(configs::name.eq(name)))
            .execute(&mut self.conn)?;
        Ok(rows_affected > 0)
    }

    pub fn get_last_applied_config(&mut self) -> Result<Option<ConfigRecord>, StateError> {
        let row = configs::table
            .order(configs::applied_at.desc())
            .first::<ConfigRow>(&mut self.conn)
            .optional()?;

        match row {
            Some(r) => Ok(Some(ConfigRecord::from_row(&r)?)),
            None => Ok(None),
        }
    }

    pub fn set_namespace_prefix(
        &mut self,
        config_name: &str,
        prefix: Option<&str>,
    ) -> Result<(), StateError> {
        let updated = diesel::update(configs::table.filter(configs::name.eq(config_name)))
            .set(configs::namespace_prefix.eq(prefix))
            .execute(&mut self.conn)?;

        if updated == 0 {
            return Err(StateError::InvalidState(format!(
                "config '{config_name}' not found"
            )));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::test_helpers::{sample_config, setup_test_db};

    #[test]
    fn insert_and_retrieve_config() {
        let (_dir, mut db) = setup_test_db();
        let config = sample_config("film");

        db.insert_config(&config).unwrap();

        let retrieved = db.get_config("film").unwrap().unwrap();
        assert_eq!(retrieved.name, "film");
        assert_eq!(retrieved.namespace, "film");
        assert_eq!(retrieved.content_hash, "abc123");
        assert!(retrieved.transform_hash.is_none());
        assert!(retrieved.tombstone_applied_at.is_none());
    }

    #[test]
    fn list_multiple_configs() {
        let (_dir, mut db) = setup_test_db();

        db.insert_config(&sample_config("alpha")).unwrap();
        db.insert_config(&sample_config("beta")).unwrap();
        db.insert_config(&sample_config("gamma")).unwrap();

        let configs = db.list_configs().unwrap();
        assert_eq!(configs.len(), 3);
        assert_eq!(configs[0].name, "alpha");
        assert_eq!(configs[1].name, "beta");
        assert_eq!(configs[2].name, "gamma");
    }

    #[test]
    fn duplicate_name_fails() {
        let (_dir, mut db) = setup_test_db();
        db.insert_config(&sample_config("film")).unwrap();

        let dup = sample_config("film");
        let result = db.insert_config(&dup);
        assert!(result.is_err());
    }

    #[test]
    fn duplicate_namespace_fails() {
        let (_dir, mut db) = setup_test_db();
        db.insert_config(&sample_config("film")).unwrap();

        let mut dup = sample_config("movie");
        dup.namespace = "film".to_string();

        let result = db.insert_config(&dup);
        assert!(result.is_err());
    }

    #[test]
    fn get_nonexistent_returns_none() {
        let (_dir, mut db) = setup_test_db();
        assert!(db.get_config("nonexistent").unwrap().is_none());
    }

    #[test]
    fn config_with_transform_hash() {
        let (_dir, mut db) = setup_test_db();
        let mut config = sample_config("film");
        config.transform_hash = Some("transform_abc".to_string());

        db.insert_config(&config).unwrap();

        let retrieved = db.get_config("film").unwrap().unwrap();
        assert_eq!(retrieved.transform_hash, Some("transform_abc".to_string()));
    }

    #[test]
    fn tombstone_config_sets_timestamp() {
        let (_dir, mut db) = setup_test_db();
        db.insert_config(&sample_config("film")).unwrap();

        db.tombstone_config("film").unwrap();

        let config = db.get_config("film").unwrap().unwrap();
        assert!(config.tombstone_applied_at.is_some());
    }

    #[test]
    fn tombstone_nonexistent_errors() {
        let (_dir, mut db) = setup_test_db();
        let result = db.tombstone_config("nonexistent");
        assert!(result.is_err());
    }

    #[test]
    fn is_tombstoned_returns_false_for_active() {
        let (_dir, mut db) = setup_test_db();
        db.insert_config(&sample_config("film")).unwrap();
        assert!(!db.is_tombstoned("film").unwrap());
    }

    #[test]
    fn is_tombstoned_returns_true_after_tombstone() {
        let (_dir, mut db) = setup_test_db();
        db.insert_config(&sample_config("film")).unwrap();
        db.tombstone_config("film").unwrap();
        assert!(db.is_tombstoned("film").unwrap());
    }

    #[test]
    fn list_active_excludes_tombstoned() {
        let (_dir, mut db) = setup_test_db();
        db.insert_config(&sample_config("alpha")).unwrap();
        db.insert_config(&sample_config("beta")).unwrap();
        db.insert_config(&sample_config("gamma")).unwrap();

        db.tombstone_config("beta").unwrap();

        let active = db.list_active_configs().unwrap();
        assert_eq!(active.len(), 2);
        assert_eq!(active[0].name, "alpha");
        assert_eq!(active[1].name, "gamma");
    }

    #[test]
    fn list_tombstoned_returns_only_tombstoned() {
        let (_dir, mut db) = setup_test_db();
        db.insert_config(&sample_config("alpha")).unwrap();
        db.insert_config(&sample_config("beta")).unwrap();

        db.tombstone_config("alpha").unwrap();

        let tombstoned = db.list_tombstoned_configs().unwrap();
        assert_eq!(tombstoned.len(), 1);
        assert_eq!(tombstoned[0].name, "alpha");
    }

    #[test]
    fn delete_config_removes_row() {
        let (_dir, mut db) = setup_test_db();
        db.insert_config(&sample_config("film")).unwrap();
        assert!(db.get_config("film").unwrap().is_some());

        let deleted = db.delete_config("film").unwrap();
        assert!(deleted);
        assert!(db.get_config("film").unwrap().is_none());
    }

    #[test]
    fn delete_config_nonexistent_returns_false() {
        let (_dir, mut db) = setup_test_db();
        let deleted = db.delete_config("nonexistent").unwrap();
        assert!(!deleted);
    }

    #[test]
    fn delete_config_does_not_affect_others() {
        let (_dir, mut db) = setup_test_db();
        db.insert_config(&sample_config("alpha")).unwrap();
        db.insert_config(&sample_config("beta")).unwrap();

        db.delete_config("alpha").unwrap();

        assert!(db.get_config("alpha").unwrap().is_none());
        assert!(db.get_config("beta").unwrap().is_some());
    }

    #[test]
    fn get_last_applied_config_returns_most_recent() {
        let (_dir, mut db) = setup_test_db();

        let mut c1 = sample_config("alpha");
        c1.applied_at = chrono::Utc::now() - chrono::Duration::hours(2);
        db.insert_config(&c1).unwrap();

        let mut c2 = sample_config("beta");
        c2.applied_at = chrono::Utc::now() - chrono::Duration::hours(1);
        db.insert_config(&c2).unwrap();

        let mut c3 = sample_config("gamma");
        c3.applied_at = chrono::Utc::now();
        db.insert_config(&c3).unwrap();

        let last = db.get_last_applied_config().unwrap().unwrap();
        assert_eq!(last.name, "gamma");
    }

    #[test]
    fn get_last_applied_config_empty_returns_none() {
        let (_dir, mut db) = setup_test_db();
        assert!(db.get_last_applied_config().unwrap().is_none());
    }

    #[test]
    fn get_last_applied_config_single() {
        let (_dir, mut db) = setup_test_db();
        db.insert_config(&sample_config("only")).unwrap();

        let last = db.get_last_applied_config().unwrap().unwrap();
        assert_eq!(last.name, "only");
    }
}
