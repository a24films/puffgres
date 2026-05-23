use std::fs;

use chrono::Utc;
use config::ConfigLoader;
use state::StateDb;

use crate::error::CliError;
use crate::paths::ProjectPaths;

pub async fn run(
    paths: &ProjectPaths,
    database_url: &str,
    state_schema: &str,
    name: &str,
) -> Result<(), CliError> {
    let db = StateDb::connect(database_url, state_schema).await?;

    let config = db.get_config(name).await?.ok_or_else(|| {
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

    db.tombstone_config(name).await?;

    println!("Tombstoned config '{}'", name);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use state::ConfigRecord;

    use crate::test_utils::{
        PASSTHROUGH_TRANSFORM, setup_project_with_state, write_config, write_transform,
    };

    fn sample_config(name: &str) -> ConfigRecord {
        ConfigRecord {
            name: name.to_string(),
            namespace: name.to_string(),
            content_hash: "abc123".to_string(),
            transform_hash: None,
            applied_at: Utc::now(),
            tombstone_applied_at: None,
            namespace_prefix: None,
        }
    }

    #[tokio::test]
    async fn tombstone_sets_timestamp() {
        let (_dir, paths, url, schema) = setup_project_with_state().await;
        let db = StateDb::connect(&url, &schema).await.unwrap();
        db.insert_config(&sample_config("film")).await.unwrap();

        run(&paths, &url, &schema, "film").await.unwrap();

        let config = db.get_config("film").await.unwrap().unwrap();
        assert!(config.tombstone_applied_at.is_some());
    }

    #[tokio::test]
    async fn tombstone_nonexistent_errors() {
        let (_dir, paths, url, schema) = setup_project_with_state().await;
        let err = run(&paths, &url, &schema, "nonexistent").await.unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[tokio::test]
    async fn tombstone_already_tombstoned_is_idempotent() {
        let (_dir, paths, url, schema) = setup_project_with_state().await;
        let db = StateDb::connect(&url, &schema).await.unwrap();
        db.insert_config(&sample_config("film")).await.unwrap();

        run(&paths, &url, &schema, "film").await.unwrap();
        // Second call should succeed (skip with message)
        run(&paths, &url, &schema, "film").await.unwrap();
    }

    #[tokio::test]
    async fn tombstone_writes_marker_file() {
        let (_dir, paths, url, schema) = setup_project_with_state().await;
        let config_dir = write_config(&paths, "film", "public", "films", "id", "uint");
        write_transform(&config_dir, PASSTHROUGH_TRANSFORM);

        let loader = ConfigLoader::new(&paths.configs);
        let (config_path, cfg) = &loader.load_all().unwrap()[0];

        let db = StateDb::connect(&url, &schema).await.unwrap();
        db.insert_config(&ConfigRecord {
            name: cfg.name.clone(),
            namespace: cfg.namespace.clone(),
            content_hash: config::Config::content_hash_from_bytes(
                &std::fs::read(config_path).unwrap(),
            ),
            transform_hash: None,
            applied_at: Utc::now(),
            tombstone_applied_at: None,
            namespace_prefix: None,
        })
        .await
        .unwrap();

        run(&paths, &url, &schema, "film").await.unwrap();

        let tombstone_path = config_dir.join("tombstone.toml");
        assert!(tombstone_path.exists());
        let content = fs::read_to_string(&tombstone_path).unwrap();
        assert!(content.contains("tombstoned_at"));
    }
}
