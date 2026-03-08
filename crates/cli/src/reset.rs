use std::path::Path;

use state::StateDb;

use crate::error::CliError;

pub fn run(state_db_path: &Path) -> Result<(), CliError> {
    if !state_db_path.exists() {
        return Err(CliError::NotInitialized("state.db".to_string()));
    }
    let db = StateDb::open(state_db_path)?;
    db.reset()?;
    println!("Reset: cleared all configs and checkpoints");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::setup_project;
    use chrono::Utc;
    use state::ConfigRecord;

    #[test]
    fn reset_clears_configs() {
        let (_dir, _paths, state_db_path) = setup_project();

        let db = StateDb::open(&state_db_path).unwrap();
        db.insert_config(&ConfigRecord {
            name: "user".to_string(),

            namespace: "user".to_string(),
            content_hash: "abc".to_string(),
            transform_hash: None,
            applied_at: Utc::now(),
            tombstone_applied_at: None,
            namespace_prefix: None,
        })
        .unwrap();
        assert_eq!(db.list_configs().unwrap().len(), 1);
        drop(db);

        run(&state_db_path).unwrap();

        let db = StateDb::open(&state_db_path).unwrap();
        assert_eq!(db.list_configs().unwrap().len(), 0);
    }

    #[test]
    fn reset_on_empty_db() {
        let (_dir, _paths, state_db_path) = setup_project();
        run(&state_db_path).unwrap();
    }

    #[test]
    fn reset_rejects_uninitialized_project() {
        let dir = tempfile::tempdir().unwrap();
        let missing_db = dir.path().join("nonexistent.db");

        let err = run(&missing_db).unwrap_err();
        assert!(
            err.to_string().contains("not found"),
            "expected NotInitialized error, got: {err}"
        );
    }
}
