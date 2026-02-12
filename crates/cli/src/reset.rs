use state::StateDb;

use crate::error::CliError;
use crate::paths::ProjectPaths;

pub fn run(paths: &ProjectPaths) -> Result<(), CliError> {
    let db = StateDb::open(&paths.state_db)?;
    db.reset()?;
    eprintln!("Reset: cleared all configs and checkpoints");
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
        let (_dir, paths) = setup_project();

        let db = StateDb::open(&paths.state_db).unwrap();
        db.insert_config(&ConfigRecord {
            name: "user_0001".to_string(),
            version: 1,
            namespace: "user_v1".to_string(),
            content_hash: "abc".to_string(),
            transform_hash: None,
            applied_at: Utc::now(),
        })
        .unwrap();
        assert_eq!(db.list_configs().unwrap().len(), 1);
        drop(db);

        run(&paths).unwrap();

        let db = StateDb::open(&paths.state_db).unwrap();
        assert_eq!(db.list_configs().unwrap().len(), 0);
    }

    #[test]
    fn reset_on_empty_db() {
        let (_dir, paths) = setup_project();
        run(&paths).unwrap();
    }
}
