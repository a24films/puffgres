use std::fs;
use std::path::Path;

use chrono::Utc;
use config::ConfigLoader;
use state::StateDb;

use crate::error::CliError;
use crate::paths::ProjectPaths;

pub fn run(paths: &ProjectPaths, state_db_path: &Path, name: &str) -> Result<(), CliError> {
    if !state_db_path.exists() {
        return Err(CliError::NotInitialized(
            "state.db — run `puffgres setup` first".to_string(),
        ));
    }
    let mut db = StateDb::open(state_db_path)?;

    let config = db.get_config(name)?.ok_or_else(|| {
        CliError::Tombstone(format!("config '{name}' not found in state database"))
    })?;

    if config.tombstone_applied_at.is_some() {
        println!("Config '{}' is already tombstoned", name);
        return Ok(());
    }

    // Write the marker file before updating the DB so that a filesystem
    // error leaves the DB untouched and a retry can succeed cleanly.
    let loader = ConfigLoader::new(&paths.configs);
    let all_configs = loader.load_all()?;
    for (config_path, cfg) in &all_configs {
        if cfg.name == name {
            let config_dir = config_path.parent().unwrap();
            let now = Utc::now().to_rfc3339();
            let tombstone_content = format!(
                "# This config has been tombstoned.\n\
                 # It will not be included in CDC or backfill.\n\
                 tombstoned_at = \"{now}\"\n"
            );
            fs::write(config_dir.join("tombstone.toml"), tombstone_content)?;
            break;
        }
    }

    db.tombstone_config(name)?;

    println!("Tombstoned config '{}'", name);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use state::ConfigRecord;

    use crate::test_utils::{PASSTHROUGH_TRANSFORM, setup_project, write_config, write_transform};

    fn sample_config(name: &str) -> ConfigRecord {
        ConfigRecord {
            name: name.to_string(),
            namespace: name.to_string(),
            content_hash: "abc123".to_string(),
            transform_hash: None,
            applied_at: Utc::now(),
            tombstone_applied_at: None,
        }
    }

    #[test]
    fn tombstone_sets_timestamp() {
        let (_dir, paths, state_db_path) = setup_project();
        let mut db = StateDb::open(&state_db_path).unwrap();
        db.insert_config(&sample_config("film")).unwrap();

        run(&paths, &state_db_path, "film").unwrap();

        let config = db.get_config("film").unwrap().unwrap();
        assert!(config.tombstone_applied_at.is_some());
    }

    #[test]
    fn tombstone_nonexistent_errors() {
        let (_dir, paths, state_db_path) = setup_project();
        let err = run(&paths, &state_db_path, "nonexistent").unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn tombstone_already_tombstoned_is_idempotent() {
        let (_dir, paths, state_db_path) = setup_project();
        let mut db = StateDb::open(&state_db_path).unwrap();
        db.insert_config(&sample_config("film")).unwrap();

        run(&paths, &state_db_path, "film").unwrap();
        // Second call should succeed (skip with message)
        run(&paths, &state_db_path, "film").unwrap();
    }

    #[test]
    fn tombstone_writes_marker_file() {
        let (_dir, paths, state_db_path) = setup_project();
        let config_dir = write_config(&paths, "film", "public", "films", "id", "uint");
        write_transform(&config_dir, PASSTHROUGH_TRANSFORM);

        let loader = ConfigLoader::new(&paths.configs);
        let cfg = &loader.load_all().unwrap()[0].1;

        let mut db = StateDb::open(&state_db_path).unwrap();
        db.insert_config(&ConfigRecord {
            name: cfg.name.clone(),
            namespace: cfg.namespace.clone(),
            content_hash: cfg.content_hash().unwrap(),
            transform_hash: None,
            applied_at: Utc::now(),
            tombstone_applied_at: None,
        })
        .unwrap();

        run(&paths, &state_db_path, "film").unwrap();

        let tombstone_path = config_dir.join("tombstone.toml");
        assert!(tombstone_path.exists());
        let content = fs::read_to_string(&tombstone_path).unwrap();
        assert!(content.contains("tombstoned_at"));
    }
}
