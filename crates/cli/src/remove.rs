use std::io::{self, Write};

use config::ConfigLoader;
use state::StateDb;

use crate::env::EnvConfig;
use crate::error::CliError;
use crate::paths::ProjectPaths;

pub async fn run_async(
    paths: &ProjectPaths,
    env_config: &EnvConfig,
    name: Option<&str>,
    last: bool,
) -> Result<(), CliError> {
    let db = StateDb::connect(&env_config.database_url, &env_config.state_schema).await?;

    let config_name = resolve_config_name(&db, name, last).await?;

    let config = db.get_config(&config_name).await?;

    match config {
        Some(config) => {
            let full_namespace = match config.namespace_prefix.as_deref() {
                Some(prefix) if !prefix.is_empty() => {
                    format!("{}_{}", prefix, config.namespace)
                }
                _ => match &env_config.turbopuffer_namespace_prefix {
                    Some(prefix) if !prefix.is_empty() => {
                        format!("{}_{}", prefix, config.namespace)
                    }
                    _ => config.namespace.clone(),
                },
            };

            println!("Removing config '{}'...", config_name);
            println!("  Namespace: {}", full_namespace);

            run_remove_saga(&db, paths, env_config, &config_name, &full_namespace).await
        }
        None => {
            // State record already deleted — but only treat as idempotent success
            // if there's evidence this config existed (e.g. config dir still on disk
            // from a partial previous removal). Otherwise, the name is likely a typo.
            let loader = ConfigLoader::new(&paths.configs);
            let has_config_dir = loader
                .load_all()
                .map(|all| all.iter().any(|(_, cfg)| cfg.name == config_name))
                .unwrap_or(false);

            if has_config_dir {
                // Partial previous removal: step 3 (DB delete) completed but step 2
                // (dir delete) didn't. Clean up the remaining directory.
                println!(
                    "Config '{}' already removed from state DB, cleaning up remaining files...",
                    config_name
                );
                cleanup_config_dir(&loader, &config_name)?;
                println!("Removed config '{}'", config_name);
                Ok(())
            } else {
                Err(CliError::Remove(format!(
                    "config '{}' not found in state database or on disk",
                    config_name
                )))
            }
        }
    }
}

/// Idempotent saga: each step tolerates the resource already being gone.
/// Steps:
///   1. Delete turbopuffer namespace (ignore 404)
///   2. Delete config directory (ignore not-found)
///   3. Delete config from state DB (the final "commit")
///
/// If the process crashes at any point, re-running `puffgres remove --name X`
/// picks up where it left off because each step is idempotent.
async fn run_remove_saga(
    db: &StateDb,
    paths: &ProjectPaths,
    env_config: &EnvConfig,
    config_name: &str,
    full_namespace: &str,
) -> Result<(), CliError> {
    // Step 1: Delete the turbopuffer namespace (idempotent — 404 is fine)
    let puff_client = puff::TurbopufferClient::new(
        env_config.turbopuffer_api_key.clone(),
        env_config.turbopuffer_region.clone(),
    )
    .map_err(|e| CliError::Remove(format!("failed to create turbopuffer client: {e}")))?;

    match puff_client.delete_namespace(full_namespace).await {
        Ok(()) => {
            println!("  Deleted turbopuffer namespace '{}'", full_namespace);
        }
        Err(puff::PuffError::Api { status: 404, .. }) => {
            println!(
                "  Turbopuffer namespace '{}' already deleted (not found)",
                full_namespace
            );
        }
        Err(e) => {
            return Err(CliError::Remove(format!(
                "failed to delete namespace '{}': {e}",
                full_namespace
            )));
        }
    }

    // Step 2: Delete the config directory from the filesystem (idempotent)
    let loader = ConfigLoader::new(&paths.configs);
    match loader.load_all() {
        Ok(all_configs) => {
            let mut found = false;
            for (config_path, cfg) in &all_configs {
                if cfg.name == config_name {
                    let config_dir = config_path.parent().unwrap();
                    match std::fs::remove_dir_all(config_dir) {
                        Ok(()) => {
                            println!("  Deleted config directory: {}", config_dir.display());
                        }
                        Err(e) if e.kind() == io::ErrorKind::NotFound => {
                            println!("  Config directory already deleted");
                        }
                        Err(e) => {
                            return Err(CliError::Remove(format!(
                                "failed to delete config directory: {e}"
                            )));
                        }
                    }
                    found = true;
                    break;
                }
            }
            if !found {
                println!("  No config directory found on disk (already removed)");
            }
        }
        Err(config::ConfigError::IoError(ref e)) if e.kind() == io::ErrorKind::NotFound => {
            // configs dir doesn't exist — that's fine
            println!("  No config directory found on disk");
        }
        Err(config::ConfigError::FileError {
            ref path,
            ref source,
        }) if matches!(
            source.as_ref(),
            config::ConfigError::IoError(e) if e.kind() == io::ErrorKind::NotFound
        ) =>
        {
            // Only treat as idempotent success if the missing file belongs to the
            // config we're removing. A different config's missing file is a real error.
            let belongs_to_target = path
                .parent()
                .and_then(|dir| dir.file_name())
                .and_then(|name| name.to_str())
                .is_some_and(|dir_name| dir_name.ends_with(&format!("_{config_name}")));

            if belongs_to_target {
                println!("  Config directory partially deleted (config.toml missing), continuing");
            } else {
                return Err(CliError::Remove(format!(
                    "failed to load configs: {}: {source}",
                    path.display()
                )));
            }
        }
        Err(e) => {
            return Err(CliError::Remove(format!("failed to load configs: {e}")));
        }
    }

    // Step 3: Delete from state DB (the final commit)
    let deleted = db.delete_config(config_name).await?;
    if deleted {
        println!(
            "  Deleted config, checkpoints, backfill progress, and DLQ entries from state database"
        );
    } else {
        println!("  Config already removed from state database");
    }

    println!("Removed config '{}'", config_name);
    Ok(())
}

/// Delete the config directory for the given config name, if it exists on disk.
fn cleanup_config_dir(loader: &ConfigLoader, config_name: &str) -> Result<(), CliError> {
    match loader.load_all() {
        Ok(all_configs) => {
            for (config_path, cfg) in &all_configs {
                if cfg.name == config_name {
                    let config_dir = config_path.parent().unwrap();
                    match std::fs::remove_dir_all(config_dir) {
                        Ok(()) => {
                            println!("  Deleted config directory: {}", config_dir.display());
                        }
                        Err(e) if e.kind() == io::ErrorKind::NotFound => {
                            println!("  Config directory already deleted");
                        }
                        Err(e) => {
                            return Err(CliError::Remove(format!(
                                "failed to delete config directory: {e}"
                            )));
                        }
                    }
                    return Ok(());
                }
            }
            println!("  No config directory found on disk (already removed)");
            Ok(())
        }
        Err(e) => Err(CliError::Remove(format!("failed to load configs: {e}"))),
    }
}

async fn resolve_config_name(
    db: &StateDb,
    name: Option<&str>,
    last: bool,
) -> Result<String, CliError> {
    if let Some(name) = name {
        return Ok(name.to_string());
    }

    if !last {
        return Err(CliError::Remove(
            "provide a config name or use --last to remove the most recently applied config"
                .to_string(),
        ));
    }

    let config = db
        .get_last_applied_config()
        .await?
        .ok_or_else(|| CliError::Remove("no configs found in state database".to_string()))?;

    print!(
        "Last applied config was '{}' (applied at {}) — remove? [y/N] ",
        config.name,
        config.applied_at.format("%Y-%m-%d %H:%M:%S UTC")
    );
    io::stdout().flush()?;

    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    let input = input.trim().to_lowercase();

    if input != "y" && input != "yes" {
        return Err(CliError::Remove("aborted".to_string()));
    }

    Ok(config.name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{
        PASSTHROUGH_TRANSFORM, setup_project, setup_project_with_state, write_config,
        write_transform,
    };
    use chrono::Utc;
    use state::{
        BackfillProgress, BackfillStatus, ConfigRecord, DlqEntry, DlqOperation, StreamingCheckpoint,
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
    async fn resolve_config_name_with_explicit_name() {
        let (_dir, _paths, url, schema) = setup_project_with_state().await;
        let db = StateDb::connect(&url, &schema).await.unwrap();
        let name = resolve_config_name(&db, Some("film"), false).await.unwrap();
        assert_eq!(name, "film");
    }

    #[tokio::test]
    async fn resolve_config_name_errors_without_name_or_last() {
        let (_dir, _paths, url, schema) = setup_project_with_state().await;
        let db = StateDb::connect(&url, &schema).await.unwrap();
        let err = resolve_config_name(&db, None, false).await.unwrap_err();
        assert!(err.to_string().contains("provide a config name"));
    }

    #[tokio::test]
    async fn resolve_config_name_last_errors_on_empty_db() {
        let (_dir, _paths, url, schema) = setup_project_with_state().await;
        let db = StateDb::connect(&url, &schema).await.unwrap();
        let err = resolve_config_name(&db, None, true).await.unwrap_err();
        assert!(err.to_string().contains("no configs found"));
    }

    #[tokio::test]
    async fn delete_config_cascades_to_all_tables() {
        let (_dir, _paths, url, schema) = setup_project_with_state().await;
        let db = StateDb::connect(&url, &schema).await.unwrap();

        db.insert_config(&sample_config("film")).await.unwrap();

        // Insert DLQ entries
        db.insert_dlq_entry(&DlqEntry::retryable(
            "film",
            100,
            DlqOperation::Insert,
            None,
            "error",
        ))
        .await
        .unwrap();
        db.insert_dlq_entry(&DlqEntry::permanent(
            "film",
            200,
            DlqOperation::Insert,
            None,
            "error",
        ))
        .await
        .unwrap();

        // Insert backfill progress
        db.save_backfill_progress(&BackfillProgress {
            config_name: "film".to_string(),
            last_id: Some("100".to_string()),
            total_rows: Some(1000),
            processed_rows: 100,
            status: BackfillStatus::InProgress,
            started_at: Some(Utc::now()),
            completed_at: None,
            error_message: None,
            watermark_lsn: None,
        })
        .await
        .unwrap();

        // Insert streaming checkpoint
        db.save_streaming_checkpoint(&StreamingCheckpoint {
            config_name: "film".to_string(),
            lsn: 5000,
            events_processed: 50,
            updated_at: Utc::now(),
        })
        .await
        .unwrap();

        // Verify everything exists
        assert_eq!(db.dlq_count(Some("film")).await.unwrap(), 2);
        assert!(db.get_backfill_progress("film").await.unwrap().is_some());
        assert!(db.get_streaming_checkpoint("film").await.unwrap().is_some());

        // Delete the config
        let deleted = db.delete_config("film").await.unwrap();
        assert!(deleted);

        // Verify everything is cleaned up via FK cascades
        assert!(db.get_config("film").await.unwrap().is_none());
        assert_eq!(db.dlq_count(Some("film")).await.unwrap(), 0);
        assert!(db.get_backfill_progress("film").await.unwrap().is_none());
        assert!(db.get_streaming_checkpoint("film").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn delete_config_does_not_affect_other_configs() {
        let (_dir, _paths, url, schema) = setup_project_with_state().await;
        let db = StateDb::connect(&url, &schema).await.unwrap();

        db.insert_config(&sample_config("film")).await.unwrap();
        db.insert_config(&sample_config("actor")).await.unwrap();

        db.insert_dlq_entry(&DlqEntry::retryable(
            "film",
            100,
            DlqOperation::Insert,
            None,
            "error",
        ))
        .await
        .unwrap();
        db.insert_dlq_entry(&DlqEntry::retryable(
            "actor",
            200,
            DlqOperation::Insert,
            None,
            "error",
        ))
        .await
        .unwrap();

        db.delete_config("film").await.unwrap();

        assert!(db.get_config("film").await.unwrap().is_none());
        assert!(db.get_config("actor").await.unwrap().is_some());
        assert_eq!(db.dlq_count(Some("film")).await.unwrap(), 0);
        assert_eq!(db.dlq_count(Some("actor")).await.unwrap(), 1);
    }

    #[tokio::test]
    async fn delete_nonexistent_config_returns_false() {
        let (_dir, _paths, url, schema) = setup_project_with_state().await;
        let db = StateDb::connect(&url, &schema).await.unwrap();
        let deleted = db.delete_config("nonexistent").await.unwrap();
        assert!(!deleted);
    }

    #[tokio::test]
    async fn get_last_applied_config_returns_most_recent() {
        let (_dir, _paths, url, schema) = setup_project_with_state().await;
        let db = StateDb::connect(&url, &schema).await.unwrap();

        let mut config1 = sample_config("alpha");
        config1.applied_at = Utc::now() - chrono::Duration::hours(2);
        db.insert_config(&config1).await.unwrap();

        let mut config2 = sample_config("beta");
        config2.applied_at = Utc::now() - chrono::Duration::hours(1);
        db.insert_config(&config2).await.unwrap();

        let mut config3 = sample_config("gamma");
        config3.applied_at = Utc::now();
        db.insert_config(&config3).await.unwrap();

        let last = db.get_last_applied_config().await.unwrap().unwrap();
        assert_eq!(last.name, "gamma");
    }

    #[tokio::test]
    async fn get_last_applied_config_returns_none_on_empty_db() {
        let (_dir, _paths, url, schema) = setup_project_with_state().await;
        let db = StateDb::connect(&url, &schema).await.unwrap();
        assert!(db.get_last_applied_config().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn delete_config_removes_filesystem_directory() {
        let (_dir, paths) = setup_project();

        let config_dir = write_config(&paths, "film", "public", "films", "id", "uint");
        write_transform(&config_dir, PASSTHROUGH_TRANSFORM);

        assert!(config_dir.exists());

        // Load via ConfigLoader and delete the directory
        let loader = ConfigLoader::new(&paths.configs);
        let all_configs = loader.load_all().unwrap();
        for (config_path, cfg) in &all_configs {
            if cfg.name == "film" {
                let dir = config_path.parent().unwrap();
                std::fs::remove_dir_all(dir).unwrap();
                break;
            }
        }

        assert!(!config_dir.exists());
    }
}
