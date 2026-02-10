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
}
