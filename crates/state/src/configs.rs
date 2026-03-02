use chrono::{DateTime, Utc};
use diesel::dsl::max;
use diesel::prelude::*;

use crate::models::{ConfigRow, NewConfig};
use crate::schema::configs;
use crate::{StateDb, StateError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigRecord {
    pub name: String,
    pub version: u64,
    pub namespace: String,
    pub content_hash: String,
    pub transform_hash: Option<String>,
    pub applied_at: DateTime<Utc>,
}

impl ConfigRecord {
    fn from_row(row: &ConfigRow) -> Result<Self, StateError> {
        let applied_at = DateTime::parse_from_rfc3339(&row.applied_at)
            .map(|dt| dt.with_timezone(&Utc))
            .map_err(|e| StateError::InvalidState(format!("invalid applied_at: {e}")))?;

        Ok(Self {
            name: row.name.clone(),
            version: row.version as u64,
            namespace: row.namespace.clone(),
            content_hash: row.content_hash.clone(),
            transform_hash: row.transform_hash.clone(),
            applied_at,
        })
    }
}

impl StateDb {
    pub fn insert_config(&mut self, config: &ConfigRecord) -> Result<(), StateError> {
        let applied_at_str = config.applied_at.to_rfc3339();
        let new = NewConfig {
            name: &config.name,
            version: config.version as i64,
            namespace: &config.namespace,
            content_hash: &config.content_hash,
            transform_hash: config.transform_hash.as_deref(),
            applied_at: &applied_at_str,
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

    /// Return the highest version among configs whose name starts with `<prefix>_`.
    /// Returns 0 if no matching configs exist.
    pub fn max_version_for_prefix(&mut self, prefix: &str) -> Result<i64, StateError> {
        let pattern = format!("{prefix}\\_%");
        let result: Option<i64> = configs::table
            .filter(configs::name.like(&pattern).escape('\\'))
            .select(max(configs::version))
            .first(&mut self.conn)?;

        Ok(result.unwrap_or(0))
    }

    pub fn list_configs(&mut self) -> Result<Vec<ConfigRecord>, StateError> {
        let rows = configs::table
            .order(configs::name.asc())
            .load::<ConfigRow>(&mut self.conn)?;

        rows.iter().map(ConfigRecord::from_row).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_configs_db() -> (tempfile::TempDir, StateDb) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let mut db = StateDb::open(&path).unwrap();
        db.initialize().unwrap();
        (dir, db)
    }

    fn sample_config(name: &str, version: u64) -> ConfigRecord {
        ConfigRecord {
            name: name.to_string(),
            version,
            namespace: format!("{}_v{}", name, version),
            content_hash: "abc123".to_string(),
            transform_hash: None,
            applied_at: Utc::now(),
        }
    }

    #[test]
    fn insert_and_retrieve_config() {
        let (_dir, mut db) = setup_configs_db();
        let config = sample_config("film", 2);

        db.insert_config(&config).unwrap();

        let retrieved = db.get_config("film").unwrap().unwrap();
        assert_eq!(retrieved.name, "film");
        assert_eq!(retrieved.version, 2);
        assert_eq!(retrieved.namespace, "film_v2");
        assert_eq!(retrieved.content_hash, "abc123");
        assert!(retrieved.transform_hash.is_none());
    }

    #[test]
    fn list_multiple_configs() {
        let (_dir, mut db) = setup_configs_db();

        db.insert_config(&sample_config("alpha", 1)).unwrap();
        db.insert_config(&sample_config("beta", 1)).unwrap();
        db.insert_config(&sample_config("gamma", 2)).unwrap();

        let configs = db.list_configs().unwrap();
        assert_eq!(configs.len(), 3);
        assert_eq!(configs[0].name, "alpha");
        assert_eq!(configs[1].name, "beta");
        assert_eq!(configs[2].name, "gamma");
    }

    #[test]
    fn duplicate_name_fails() {
        let (_dir, mut db) = setup_configs_db();
        db.insert_config(&sample_config("film", 1)).unwrap();

        let mut dup = sample_config("film", 2);
        dup.namespace = "film_v2".to_string();

        let result = db.insert_config(&dup);
        assert!(result.is_err());
    }

    #[test]
    fn duplicate_namespace_fails() {
        let (_dir, mut db) = setup_configs_db();
        db.insert_config(&sample_config("film", 1)).unwrap();

        let mut dup = sample_config("movie", 1);
        dup.namespace = "film_v1".to_string();

        let result = db.insert_config(&dup);
        assert!(result.is_err());
    }

    #[test]
    fn get_nonexistent_returns_none() {
        let (_dir, mut db) = setup_configs_db();
        assert!(db.get_config("nonexistent").unwrap().is_none());
    }

    #[test]
    fn max_version_no_matches() {
        let (_dir, mut db) = setup_configs_db();
        assert_eq!(db.max_version_for_prefix("film").unwrap(), 0);
    }

    #[test]
    fn max_version_single_match() {
        let (_dir, mut db) = setup_configs_db();
        let mut config = sample_config("film_0001", 1);
        config.namespace = "film_v1".to_string();
        db.insert_config(&config).unwrap();

        assert_eq!(db.max_version_for_prefix("film").unwrap(), 1);
    }

    #[test]
    fn max_version_multiple_matches() {
        let (_dir, mut db) = setup_configs_db();
        let mut c1 = sample_config("film_0001", 1);
        c1.namespace = "film_v1".to_string();
        let mut c2 = sample_config("film_0002", 2);
        c2.namespace = "film_v2".to_string();
        let mut c3 = sample_config("film_0003", 3);
        c3.namespace = "film_v3".to_string();

        db.insert_config(&c1).unwrap();
        db.insert_config(&c2).unwrap();
        db.insert_config(&c3).unwrap();

        assert_eq!(db.max_version_for_prefix("film").unwrap(), 3);
    }

    #[test]
    fn max_version_ignores_other_prefixes() {
        let (_dir, mut db) = setup_configs_db();
        let mut c1 = sample_config("film_0001", 1);
        c1.namespace = "film_v1".to_string();
        let mut c2 = sample_config("actor_0001", 5);
        c2.namespace = "actor_v5".to_string();

        db.insert_config(&c1).unwrap();
        db.insert_config(&c2).unwrap();

        assert_eq!(db.max_version_for_prefix("film").unwrap(), 1);
        assert_eq!(db.max_version_for_prefix("actor").unwrap(), 5);
    }

    #[test]
    fn config_with_transform_hash() {
        let (_dir, mut db) = setup_configs_db();
        let mut config = sample_config("film", 1);
        config.transform_hash = Some("transform_abc".to_string());

        db.insert_config(&config).unwrap();

        let retrieved = db.get_config("film").unwrap().unwrap();
        assert_eq!(retrieved.transform_hash, Some("transform_abc".to_string()));
    }
}
