use chrono::{DateTime, Utc};
use rusqlite::{Row, params};

use crate::{StateDb, StateError};

const CONFIG_SELECT_COLS: &str =
    "name, version, namespace, content_hash, transform_hash, applied_at";
const COL_APPLIED_AT: usize = 5;

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
    fn from_row(row: &Row) -> Result<Self, rusqlite::Error> {
        let applied_at_str: String = row.get(COL_APPLIED_AT)?;
        let applied_at = DateTime::parse_from_rfc3339(&applied_at_str)
            .map(|dt| dt.with_timezone(&Utc))
            .map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    COL_APPLIED_AT,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })?;

        Ok(Self {
            name: row.get(0)?,
            version: row.get::<_, i64>(1)? as u64,
            namespace: row.get(2)?,
            content_hash: row.get(3)?,
            transform_hash: row.get(4)?,
            applied_at,
        })
    }
}

impl StateDb {
    pub fn ensure_configs_table(&self) -> Result<(), StateError> {
        self.conn().execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS configs (
                name TEXT PRIMARY KEY,
                version INTEGER NOT NULL,
                namespace TEXT NOT NULL UNIQUE,
                content_hash TEXT NOT NULL,
                transform_hash TEXT,
                applied_at TEXT NOT NULL
            );
            "#,
        )?;
        Ok(())
    }

    pub fn insert_config(&self, config: &ConfigRecord) -> Result<(), StateError> {
        self.conn().execute(
            &format!(
                "INSERT INTO configs ({}) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                CONFIG_SELECT_COLS
            ),
            params![
                config.name,
                config.version as i64,
                config.namespace,
                config.content_hash,
                config.transform_hash,
                config.applied_at.to_rfc3339(),
            ],
        )?;
        Ok(())
    }

    pub fn get_config(&self, name: &str) -> Result<Option<ConfigRecord>, StateError> {
        let mut stmt = self.conn().prepare(&format!(
            "SELECT {} FROM configs WHERE name = ?1",
            CONFIG_SELECT_COLS
        ))?;

        let mut rows = stmt.query(params![name])?;
        match rows.next()? {
            Some(row) => Ok(Some(ConfigRecord::from_row(row)?)),
            None => Ok(None),
        }
    }

    /// Return the highest version among configs whose name starts with `<prefix>_`.
    /// Returns 0 if no matching configs exist.
    pub fn max_version_for_prefix(&self, prefix: &str) -> Result<i64, StateError> {
        let pattern = format!("{prefix}_*");
        let mut stmt = self
            .conn()
            .prepare("SELECT MAX(version) FROM configs WHERE name GLOB ?1")?;
        let max: Option<i64> = stmt.query_row(params![pattern], |row| row.get(0))?;
        Ok(max.unwrap_or(0))
    }

    pub fn list_configs(&self) -> Result<Vec<ConfigRecord>, StateError> {
        let mut stmt = self.conn().prepare(&format!(
            "SELECT {} FROM configs ORDER BY name",
            CONFIG_SELECT_COLS
        ))?;

        let rows = stmt.query_map([], ConfigRecord::from_row)?;

        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_configs_db() -> (tempfile::TempDir, StateDb) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = StateDb::open(&path).unwrap();
        db.ensure_configs_table().unwrap();
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
        let (_dir, db) = setup_configs_db();
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
        let (_dir, db) = setup_configs_db();

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
        let (_dir, db) = setup_configs_db();
        db.insert_config(&sample_config("film", 1)).unwrap();

        let mut dup = sample_config("film", 2);
        dup.namespace = "film_v2".to_string();

        let result = db.insert_config(&dup);
        assert!(result.is_err());
    }

    #[test]
    fn duplicate_namespace_fails() {
        let (_dir, db) = setup_configs_db();
        db.insert_config(&sample_config("film", 1)).unwrap();

        let mut dup = sample_config("movie", 1);
        dup.namespace = "film_v1".to_string();

        let result = db.insert_config(&dup);
        assert!(result.is_err());
    }

    #[test]
    fn get_nonexistent_returns_none() {
        let (_dir, db) = setup_configs_db();
        assert!(db.get_config("nonexistent").unwrap().is_none());
    }

    #[test]
    fn max_version_no_matches() {
        let (_dir, db) = setup_configs_db();
        assert_eq!(db.max_version_for_prefix("film").unwrap(), 0);
    }

    #[test]
    fn max_version_single_match() {
        let (_dir, db) = setup_configs_db();
        let mut config = sample_config("film_0001", 1);
        config.namespace = "film_v1".to_string();
        db.insert_config(&config).unwrap();

        assert_eq!(db.max_version_for_prefix("film").unwrap(), 1);
    }

    #[test]
    fn max_version_multiple_matches() {
        let (_dir, db) = setup_configs_db();
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
        let (_dir, db) = setup_configs_db();
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
        let (_dir, db) = setup_configs_db();
        let mut config = sample_config("film", 1);
        config.transform_hash = Some("transform_abc".to_string());

        db.insert_config(&config).unwrap();

        let retrieved = db.get_config("film").unwrap().unwrap();
        assert_eq!(retrieved.transform_hash, Some("transform_abc".to_string()));
    }
}
