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
    if !env_config.state_db_path.exists() {
        return Err(CliError::NotInitialized("state.db".to_string()));
    }

    let mut db = StateDb::open(&env_config.state_db_path)?;

    let config_name = resolve_config_name(&mut db, name, last)?;

    let config = db.get_config(&config_name)?.ok_or_else(|| {
        CliError::Remove(format!(
            "config '{}' not found in state database",
            config_name
        ))
    })?;

    let full_namespace = match &config.namespace_prefix {
        Some(prefix) => format!("{}_{}", prefix, config.namespace),
        None => match &env_config.turbopuffer_namespace_prefix {
            Some(prefix) => format!("{}_{}", prefix, config.namespace),
            None => config.namespace.clone(),
        },
    };

    println!("Removing config '{}'...", config_name);
    println!("  Namespace: {}", full_namespace);

    // Delete the turbopuffer namespace
    let puff_client = puff::TurbopufferClient::new(
        env_config.turbopuffer_api_key.clone(),
        env_config.turbopuffer_region.clone(),
    )
    .map_err(|e| CliError::Remove(format!("failed to create turbopuffer client: {e}")))?;

    puff_client
        .delete_namespace(&full_namespace)
        .await
        .map_err(|e| {
            CliError::Remove(format!(
                "failed to delete namespace '{}': {e}",
                full_namespace
            ))
        })?;

    println!("  Deleted turbopuffer namespace '{}'", full_namespace);

    // Delete config from state DB (FK cascades handle DLQ, backfill, checkpoints)
    db.delete_config(&config_name)?;
    println!(
        "  Deleted config, checkpoints, backfill progress, and DLQ entries from state database"
    );

    // Delete the config directory from the filesystem
    let loader = ConfigLoader::new(&paths.configs);
    let all_configs = loader.load_all()?;
    for (config_path, cfg) in &all_configs {
        if cfg.name == config_name {
            let config_dir = config_path.parent().unwrap();
            std::fs::remove_dir_all(config_dir)?;
            println!("  Deleted config directory: {}", config_dir.display());
            break;
        }
    }

    println!("Removed config '{}'", config_name);
    Ok(())
}

fn resolve_config_name(
    db: &mut StateDb,
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
        .get_last_applied_config()?
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
    use crate::test_utils::{PASSTHROUGH_TRANSFORM, setup_project, write_config, write_transform};
    use chrono::Utc;
    use state::{BackfillProgress, BackfillStatus, ConfigRecord, DlqEntry, StreamingCheckpoint};

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

    #[test]
    fn resolve_config_name_with_explicit_name() {
        let (_dir, _paths, state_db_path) = setup_project();
        let mut db = StateDb::open(&state_db_path).unwrap();
        let name = resolve_config_name(&mut db, Some("film"), false).unwrap();
        assert_eq!(name, "film");
    }

    #[test]
    fn resolve_config_name_errors_without_name_or_last() {
        let (_dir, _paths, state_db_path) = setup_project();
        let mut db = StateDb::open(&state_db_path).unwrap();
        let err = resolve_config_name(&mut db, None, false).unwrap_err();
        assert!(err.to_string().contains("provide a config name"));
    }

    #[test]
    fn resolve_config_name_last_errors_on_empty_db() {
        let (_dir, _paths, state_db_path) = setup_project();
        let mut db = StateDb::open(&state_db_path).unwrap();
        let err = resolve_config_name(&mut db, None, true).unwrap_err();
        assert!(err.to_string().contains("no configs found"));
    }

    #[test]
    fn delete_config_cascades_to_all_tables() {
        let (_dir, _paths, state_db_path) = setup_project();
        let mut db = StateDb::open(&state_db_path).unwrap();

        db.insert_config(&sample_config("film")).unwrap();

        // Insert DLQ entries
        db.insert_dlq_entry(&DlqEntry::retryable(
            "film",
            100,
            r#"{"test":true}"#.to_string(),
            None,
            "error",
        ))
        .unwrap();
        db.insert_dlq_entry(&DlqEntry::permanent(
            "film",
            200,
            r#"{"test":true}"#.to_string(),
            None,
            "error",
        ))
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
        .unwrap();

        // Insert streaming checkpoint
        db.save_streaming_checkpoint(&StreamingCheckpoint {
            config_name: "film".to_string(),
            lsn: 5000,
            events_processed: 50,
            updated_at: Utc::now(),
        })
        .unwrap();

        // Verify everything exists
        assert_eq!(db.dlq_count(Some("film")).unwrap(), 2);
        assert!(db.get_backfill_progress("film").unwrap().is_some());
        assert!(db.get_streaming_checkpoint("film").unwrap().is_some());

        // Delete the config
        let deleted = db.delete_config("film").unwrap();
        assert!(deleted);

        // Verify everything is cleaned up via FK cascades
        assert!(db.get_config("film").unwrap().is_none());
        assert_eq!(db.dlq_count(Some("film")).unwrap(), 0);
        assert!(db.get_backfill_progress("film").unwrap().is_none());
        assert!(db.get_streaming_checkpoint("film").unwrap().is_none());
    }

    #[test]
    fn delete_config_does_not_affect_other_configs() {
        let (_dir, _paths, state_db_path) = setup_project();
        let mut db = StateDb::open(&state_db_path).unwrap();

        db.insert_config(&sample_config("film")).unwrap();
        db.insert_config(&sample_config("actor")).unwrap();

        db.insert_dlq_entry(&DlqEntry::retryable(
            "film",
            100,
            r#"{"test":true}"#.to_string(),
            None,
            "error",
        ))
        .unwrap();
        db.insert_dlq_entry(&DlqEntry::retryable(
            "actor",
            200,
            r#"{"test":true}"#.to_string(),
            None,
            "error",
        ))
        .unwrap();

        db.delete_config("film").unwrap();

        assert!(db.get_config("film").unwrap().is_none());
        assert!(db.get_config("actor").unwrap().is_some());
        assert_eq!(db.dlq_count(Some("film")).unwrap(), 0);
        assert_eq!(db.dlq_count(Some("actor")).unwrap(), 1);
    }

    #[test]
    fn delete_nonexistent_config_returns_false() {
        let (_dir, _paths, state_db_path) = setup_project();
        let mut db = StateDb::open(&state_db_path).unwrap();
        let deleted = db.delete_config("nonexistent").unwrap();
        assert!(!deleted);
    }

    #[test]
    fn get_last_applied_config_returns_most_recent() {
        let (_dir, _paths, state_db_path) = setup_project();
        let mut db = StateDb::open(&state_db_path).unwrap();

        let mut config1 = sample_config("alpha");
        config1.applied_at = Utc::now() - chrono::Duration::hours(2);
        db.insert_config(&config1).unwrap();

        let mut config2 = sample_config("beta");
        config2.applied_at = Utc::now() - chrono::Duration::hours(1);
        db.insert_config(&config2).unwrap();

        let mut config3 = sample_config("gamma");
        config3.applied_at = Utc::now();
        db.insert_config(&config3).unwrap();

        let last = db.get_last_applied_config().unwrap().unwrap();
        assert_eq!(last.name, "gamma");
    }

    #[test]
    fn get_last_applied_config_returns_none_on_empty_db() {
        let (_dir, _paths, state_db_path) = setup_project();
        let mut db = StateDb::open(&state_db_path).unwrap();
        assert!(db.get_last_applied_config().unwrap().is_none());
    }

    #[test]
    fn delete_config_removes_filesystem_directory() {
        let (_dir, paths, _state_db_path) = setup_project();

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
