use std::path::Path;

use state::StateDb;

use crate::error::CliError;

pub fn run(state_db_path: &Path) -> Result<(), CliError> {
    if let Some(parent) = state_db_path.parent() {
        if !parent.exists() {
            std::fs::create_dir_all(parent)?;
        }
    }

    StateDb::open(state_db_path)?;

    println!("Created state database at {}", state_db_path.display());

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_state_db() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("state.db");

        run(&db_path).unwrap();

        assert!(db_path.exists());
    }

    #[test]
    fn creates_parent_directories() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("nested").join("dir").join("state.db");

        run(&db_path).unwrap();

        assert!(db_path.exists());
    }

    #[test]
    fn migrations_run() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("state.db");

        run(&db_path).unwrap();

        // Verify the DB is usable by opening and listing configs
        let mut db = StateDb::open(&db_path).unwrap();
        assert!(db.list_configs().unwrap().is_empty());
    }

    #[test]
    fn idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("state.db");

        run(&db_path).unwrap();
        run(&db_path).unwrap();

        let mut db = StateDb::open(&db_path).unwrap();
        assert!(db.list_configs().unwrap().is_empty());
    }
}
