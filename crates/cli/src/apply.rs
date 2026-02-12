use std::fs;
use std::path::PathBuf;

use chrono::Utc;
use config::Config;
use sha2::{Digest, Sha256};
use state::{ConfigRecord, StateDb};

use crate::env::EnvConfig;
use crate::error::CliError;
use crate::paths::ProjectPaths;
use crate::validate::validate_schema;

pub fn run(paths: &ProjectPaths, env_config: &EnvConfig) -> Result<(), CliError> {
    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| CliError::Apply(format!("failed to create async runtime: {e}")))?;
    rt.block_on(run_async(paths, env_config))
}

async fn run_async(paths: &ProjectPaths, env_config: &EnvConfig) -> Result<(), CliError> {
    let db = StateDb::open(&paths.state_db)?;

    let loader = config::ConfigLoader::new(&paths.configs);
    let configs = loader.load_all()?;

    if configs.is_empty() {
        eprintln!("No config files found in configs/");
        return Ok(());
    }

    let mut errors: Vec<String> = Vec::new();
    let mut skipped = 0;

    // First pass: basic validation and immutability check
    let mut new_configs: Vec<(PathBuf, Config, String)> = Vec::new();

    for (path, config) in &configs {
        // 1. Validate structure
        if let Err(validation_errors) = config.validate() {
            for err in &validation_errors {
                errors.push(format!(
                    "{}: {} - {}",
                    path.display(),
                    err.field,
                    err.message
                ));
            }
            continue;
        }

        // 2. Check immutability
        let content_hash = config.content_hash();
        if let Some(existing) = db.get_config(&config.name)? {
            if existing.content_hash == content_hash {
                skipped += 1;
                continue;
            } else {
                errors.push(format!(
                    "{}: config '{}' was modified after being applied",
                    path.display(),
                    config.name,
                ));
                continue;
            }
        }

        // 3. Check transform file exists
        let transform_path = paths.root.join(&config.transform.path);
        if !transform_path.exists() {
            errors.push(format!(
                "{}: transform file '{}' does not exist",
                path.display(),
                config.transform.path,
            ));
            continue;
        }

        new_configs.push((path.clone(), config.clone(), content_hash));
    }

    // Bail if any errors — nothing gets applied
    if !errors.is_empty() {
        for err in &errors {
            eprintln!("Error: {}", err);
        }
        return Err(CliError::Apply(format!(
            "{} config(s) had errors",
            errors.len()
        )));
    }

    // Second pass: live validation against Postgres and apply
    let mut applied = 0;
    if !new_configs.is_empty() {
        let schema_configs: Vec<(PathBuf, Config)> = new_configs
            .iter()
            .map(|(p, c, _)| (p.clone(), c.clone()))
            .collect();

        let validated = validate_schema(env_config, &schema_configs)
            .await
            .map_err(|errors| {
                for err in &errors {
                    eprintln!("Error: {}", err);
                }
                CliError::Apply(format!("{} config(s) had errors", errors.len()))
            })?;

        for (i, (_path, config, content_hash)) in new_configs.iter().enumerate() {
            if !validated.contains(&i) {
                continue;
            }

            let transform_path = paths.root.join(&config.transform.path);
            let transform_hash = {
                let content = fs::read(&transform_path)?;
                let hash = Sha256::digest(&content);
                Some(format!("{:x}", hash))
            };

            let record = ConfigRecord {
                name: config.name.clone(),
                version: config.version,
                namespace: config.full_namespace(),
                content_hash: content_hash.clone(),
                transform_hash,
                applied_at: Utc::now(),
            };

            db.insert_config(&record)?;
            applied += 1;
            eprintln!("Applied: {}", config.name);
        }
    }

    eprintln!("{} applied, {} unchanged", applied, skipped);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::test_utils::{
        setup_project, start_postgres, write_config, write_passthrough_transform,
    };

    fn dummy_env() -> EnvConfig {
        EnvConfig {
            database_url: "host=invalid".to_string(),
            turbopuffer_api_key: "fake".to_string(),
            turbopuffer_region: None,
        }
    }

    // --- Tests that fail during structural validation (no Postgres needed) ---

    #[test]
    fn test_errors_on_invalid_config() {
        let (_dir, paths) = setup_project();
        write_config(&paths, "bad", 0, "public", "bad", "id", "uint");

        let err = run(&paths, &dummy_env()).unwrap_err();
        assert!(err.to_string().contains("error"));
    }

    #[test]
    fn test_no_configs_succeeds() {
        let (_dir, paths) = setup_project();
        run(&paths, &dummy_env()).unwrap();
    }

    #[test]
    fn test_errors_on_missing_transform() {
        let (_dir, paths) = setup_project();
        write_config(&paths, "user", 1, "public", "users", "id", "uint");
        // Don't write transform file

        let err = run(&paths, &dummy_env()).unwrap_err();
        assert!(err.to_string().contains("error"));
    }

    #[test]
    fn test_invalid_config_prevents_all_applies() {
        let (_dir, paths) = setup_project();

        // Valid config
        write_config(&paths, "user", 1, "public", "users", "id", "uint");
        write_passthrough_transform(&paths, "user");

        // Invalid config (version 0)
        write_config(&paths, "bad", 0, "public", "bad", "id", "uint");

        let err = run(&paths, &dummy_env()).unwrap_err();
        assert!(err.to_string().contains("error"));

        // Valid config should NOT have been applied
        let db = StateDb::open(&paths.state_db).unwrap();
        let record = db.get_config("user_0001").unwrap();
        assert!(record.is_none());
    }

    // --- Tests that need Postgres for successful apply ---

    #[tokio::test]
    async fn test_applies_new_config() {
        let (_container, env_config) = start_postgres().await;
        let pg_client = pg::connect::connect(&env_config.database_url)
            .await
            .unwrap();
        pg_client
            .execute("CREATE TABLE users (id SERIAL PRIMARY KEY)", &[])
            .await
            .unwrap();
        drop(pg_client);

        let (_dir, paths) = setup_project();
        write_config(&paths, "user", 1, "public", "users", "id", "uint");
        write_passthrough_transform(&paths, "user");

        run_async(&paths, &env_config).await.unwrap();

        let db = StateDb::open(&paths.state_db).unwrap();
        let record = db.get_config("user_0001").unwrap().unwrap();
        assert_eq!(record.name, "user_0001");
        assert_eq!(record.version, 1);
        assert_eq!(record.namespace, "user_v1");
        assert!(record.transform_hash.is_some());
    }

    #[tokio::test]
    async fn test_applies_multiple_configs() {
        let (_container, env_config) = start_postgres().await;
        let pg_client = pg::connect::connect(&env_config.database_url)
            .await
            .unwrap();
        pg_client
            .execute("CREATE TABLE users (id SERIAL PRIMARY KEY)", &[])
            .await
            .unwrap();
        pg_client
            .execute("CREATE TABLE films (id SERIAL PRIMARY KEY)", &[])
            .await
            .unwrap();
        drop(pg_client);

        let (_dir, paths) = setup_project();
        write_config(&paths, "user", 1, "public", "users", "id", "uint");
        write_config(&paths, "film", 1, "public", "films", "id", "uint");
        write_passthrough_transform(&paths, "user");
        write_passthrough_transform(&paths, "film");

        run_async(&paths, &env_config).await.unwrap();

        let db = StateDb::open(&paths.state_db).unwrap();
        let configs = db.list_configs().unwrap();
        assert_eq!(configs.len(), 2);
    }

    #[tokio::test]
    async fn test_skips_already_applied_unchanged() {
        let (_container, env_config) = start_postgres().await;
        let pg_client = pg::connect::connect(&env_config.database_url)
            .await
            .unwrap();
        pg_client
            .execute("CREATE TABLE users (id SERIAL PRIMARY KEY)", &[])
            .await
            .unwrap();
        drop(pg_client);

        let (_dir, paths) = setup_project();
        write_config(&paths, "user", 1, "public", "users", "id", "uint");
        write_passthrough_transform(&paths, "user");

        run_async(&paths, &env_config).await.unwrap();
        // Second apply should skip (unchanged)
        run_async(&paths, &env_config).await.unwrap();

        let db = StateDb::open(&paths.state_db).unwrap();
        let configs = db.list_configs().unwrap();
        assert_eq!(configs.len(), 1);
    }

    #[tokio::test]
    async fn test_errors_on_modified_config() {
        let (_container, env_config) = start_postgres().await;
        let pg_client = pg::connect::connect(&env_config.database_url)
            .await
            .unwrap();
        pg_client
            .execute("CREATE TABLE users (id SERIAL PRIMARY KEY)", &[])
            .await
            .unwrap();
        pg_client
            .execute("CREATE TABLE accounts (id SERIAL PRIMARY KEY)", &[])
            .await
            .unwrap();
        drop(pg_client);

        let (_dir, paths) = setup_project();
        write_config(&paths, "user", 1, "public", "users", "id", "uint");
        write_passthrough_transform(&paths, "user");

        run_async(&paths, &env_config).await.unwrap();

        // Modify the config file (change the table)
        write_config(&paths, "user", 1, "public", "accounts", "id", "uint");

        let err = run_async(&paths, &env_config).await.unwrap_err();
        assert!(err.to_string().contains("error"));
    }

    #[tokio::test]
    async fn test_content_hash_is_stored() {
        let (_container, env_config) = start_postgres().await;
        let pg_client = pg::connect::connect(&env_config.database_url)
            .await
            .unwrap();
        pg_client
            .execute("CREATE TABLE users (id SERIAL PRIMARY KEY)", &[])
            .await
            .unwrap();
        drop(pg_client);

        let (_dir, paths) = setup_project();
        write_config(&paths, "user", 1, "public", "users", "id", "uint");
        write_passthrough_transform(&paths, "user");

        run_async(&paths, &env_config).await.unwrap();

        let db = StateDb::open(&paths.state_db).unwrap();
        let record = db.get_config("user_0001").unwrap().unwrap();
        assert!(!record.content_hash.is_empty());
        assert_eq!(record.content_hash.len(), 64);
    }

    #[tokio::test]
    async fn test_namespace_uses_full_namespace() {
        let (_container, env_config) = start_postgres().await;
        let pg_client = pg::connect::connect(&env_config.database_url)
            .await
            .unwrap();
        pg_client
            .execute("CREATE TABLE films (id SERIAL PRIMARY KEY)", &[])
            .await
            .unwrap();
        drop(pg_client);

        let (_dir, paths) = setup_project();
        write_config(&paths, "film", 2, "public", "films", "id", "uint");
        write_passthrough_transform(&paths, "film");

        run_async(&paths, &env_config).await.unwrap();

        let db = StateDb::open(&paths.state_db).unwrap();
        let record = db.get_config("film_0002").unwrap().unwrap();
        assert_eq!(record.namespace, "film_v2");
    }

    // --- New integration tests for live validation ---

    #[tokio::test]
    async fn test_rejects_nonexistent_table() {
        let (_container, env_config) = start_postgres().await;

        let (_dir, paths) = setup_project();
        write_config(
            &paths,
            "ghost",
            1,
            "public",
            "nonexistent_table",
            "id",
            "uint",
        );
        write_passthrough_transform(&paths, "ghost");

        let err = run_async(&paths, &env_config).await.unwrap_err();
        assert!(
            err.to_string().contains("error"),
            "expected apply error, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_rejects_nonexistent_id_column() {
        let (_container, env_config) = start_postgres().await;

        let pg_client = pg::connect::connect(&env_config.database_url)
            .await
            .unwrap();
        pg_client
            .execute(
                "CREATE TABLE col_test (id SERIAL PRIMARY KEY, name TEXT)",
                &[],
            )
            .await
            .unwrap();
        drop(pg_client);

        let (_dir, paths) = setup_project();
        write_config(
            &paths,
            "col",
            1,
            "public",
            "col_test",
            "missing_col",
            "uint",
        );
        write_passthrough_transform(&paths, "col");

        let err = run_async(&paths, &env_config).await.unwrap_err();
        assert!(
            err.to_string().contains("error"),
            "expected apply error for missing column, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_rejects_incompatible_id_type() {
        let (_container, env_config) = start_postgres().await;

        let pg_client = pg::connect::connect(&env_config.database_url)
            .await
            .unwrap();
        pg_client
            .execute(
                "CREATE TABLE type_test (id TEXT PRIMARY KEY, name TEXT)",
                &[],
            )
            .await
            .unwrap();
        drop(pg_client);

        let (_dir, paths) = setup_project();
        write_config(&paths, "typed", 1, "public", "type_test", "id", "uint");
        write_passthrough_transform(&paths, "typed");

        let err = run_async(&paths, &env_config).await.unwrap_err();
        assert!(
            err.to_string().contains("error"),
            "expected apply error for incompatible id type, got: {err}"
        );
    }
}
