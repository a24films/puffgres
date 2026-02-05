use std::path::{Path, PathBuf};

use rusqlite::Connection;

use crate::StateError;

pub struct StateDb {
    conn: Connection,
    path: PathBuf,
}

impl StateDb {
    pub fn open(path: &Path) -> Result<Self, StateError> {
        let conn = Connection::open(path)?;
        Ok(Self {
            conn,
            path: path.to_path_buf(),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn conn(&self) -> &Connection {
        &self.conn
    }

    pub fn initialize(&self) -> Result<(), StateError> {
        self.ensure_configs_table()?;
        self.ensure_streaming_checkpoints_table()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_creates_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");

        assert!(!path.exists());
        let db = StateDb::open(&path).unwrap();
        assert!(path.exists());
        assert_eq!(db.path(), path.as_path());
    }

    #[test]
    fn open_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        std::fs::write(&path, "").unwrap();

        let db = StateDb::open(&path).unwrap();
        assert_eq!(db.path(), path.as_path());
    }

    #[test]
    fn initialize_creates_all_tables() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = StateDb::open(&path).unwrap();

        db.initialize().unwrap();

        // Verify each table exists by checking sqlite_master
        let mut stmt = db
            .conn
            .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            .unwrap();
        let tables: Vec<String> = stmt
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert!(tables.contains(&"configs".to_string()));
        assert!(tables.contains(&"streaming_checkpoints".to_string()));
    }

    #[test]
    fn initialize_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = StateDb::open(&path).unwrap();

        // Should not error with multiple initializations
        db.initialize().unwrap();
        db.initialize().unwrap();
        db.initialize().unwrap();
    }
}
