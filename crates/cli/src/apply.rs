use std::fs;
use std::path::PathBuf;
use std::time::Duration;

use chrono::Utc;
use config::Config;
use sha2::{Digest, Sha256};
use state::{ConfigRecord, StateDb};

use crate::env::EnvConfig;
use crate::error::CliError;
use crate::paths::ProjectPaths;
use crate::project_config::ProjectConfig;
use crate::tombstones::reconcile_on_disk_tombstones;
use crate::validate::preflight_check;

fn summarize_config_errors(errors: &[String]) -> String {
    format!(
        "{} config(s) had errors:\n{}",
        errors.len(),
        errors.join("\n")
    )
}

pub fn run(
    paths: &ProjectPaths,
    env_config: &EnvConfig,
    project_config: &ProjectConfig,
) -> Result<(), CliError> {
    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| CliError::Apply(format!("failed to create async runtime: {e}")))?;
    rt.block_on(run_async(paths, env_config, project_config))
}

pub async fn run_async(
    paths: &ProjectPaths,
    env_config: &EnvConfig,
    project_config: &ProjectConfig,
) -> Result<(), CliError> {
    let transform_timeout = Duration::from_secs(project_config.transform_timeout_secs());
    let db = StateDb::connect(&env_config.database_url, &env_config.state_schema).await?;
    reconcile_on_disk_tombstones(paths, &db).await?;

    let loader = config::ConfigLoader::new(&paths.configs);
    let configs = loader.load_all()?;

    if configs.is_empty() {
        println!("No config files found in configs/");
        return Ok(());
    }

    // Immutability check — filter to only new configs
    let mut errors: Vec<String> = Vec::new();
    let mut skipped = 0;

    let mut new_configs: Vec<(PathBuf, Config, String, String)> = Vec::new();

    for (config_path, config) in &configs {
        if db.is_tombstoned(&config.name).await? {
            skipped += 1;
            continue;
        }

        let config_bytes = match fs::read(config_path) {
            Ok(b) => b,
            Err(e) => {
                errors.push(format!("{}: {e}", config_path.display()));
                continue;
            }
        };
        let content_hash = Config::content_hash_from_bytes(&config_bytes);
        if let Some(existing) = db.get_config(&config.name).await? {
            if existing.content_hash == content_hash {
                // Also verify transform file hasn't been modified
                if let Some(ref stored_hash) = existing.transform_hash {
                    let transform_path = config_path.parent().unwrap().join("transform.ts");
                    let transform_content = match fs::read(&transform_path) {
                        Ok(bytes) => bytes,
                        Err(e) => {
                            errors.push(format!(
                                "{}: cannot read transform file for applied config '{}': {e}",
                                config_path.display(),
                                config.name,
                            ));
                            continue;
                        }
                    };
                    let current_hash = format!("{:x}", Sha256::digest(&transform_content));
                    if *stored_hash != current_hash {
                        errors.push(format!(
                            "{}: transform file was modified after config '{}' was applied",
                            config_path.display(),
                            config.name,
                        ));
                        continue;
                    }
                }
                skipped += 1;
                continue;
            } else {
                errors.push(format!(
                    "{}: config '{}' was modified after being applied",
                    config_path.display(),
                    config.name,
                ));
                continue;
            }
        }

        // New config: verify schema.ts exists
        let schema_path = config_path.parent().unwrap().join("schema.ts");
        if !schema_path.exists() {
            errors.push(format!(
                "{}: schema.ts is missing. Run `puffgres generate`",
                config_path.display(),
            ));
            continue;
        }

        // New config: read transform file and compute hash
        let transform_path = config_path.parent().unwrap().join("transform.ts");
        let transform_content = match fs::read(&transform_path) {
            Ok(bytes) => bytes,
            Err(_) => {
                errors.push(format!(
                    "{}: transform file 'transform.ts' does not exist",
                    config_path.display(),
                ));
                continue;
            }
        };
        let transform_hash = format!("{:x}", Sha256::digest(&transform_content));

        new_configs.push((
            config_path.clone(),
            config.clone(),
            content_hash,
            transform_hash,
        ));
    }

    if !errors.is_empty() {
        for err in &errors {
            println!("Error: {}", err);
        }
        return Err(CliError::Apply(summarize_config_errors(&errors)));
    }

    // Pre-flight validation on new configs (static + transforms + namespaces + Postgres)
    let mut applied = 0;
    if !new_configs.is_empty() {
        let new_config_refs: Vec<(PathBuf, Config)> = new_configs
            .iter()
            .map(|(p, c, _, _)| (p.clone(), c.clone()))
            .collect();

        preflight_check(
            &env_config.database_url,
            &env_config.state_schema,
            &new_config_refs,
            None,
            transform_timeout,
        )
        .await
        .map_err(CliError::Apply)?;

        // Set REPLICA IDENTITY FULL before persisting to state DB.
        let mut applied_tables: Vec<String> = new_configs
            .iter()
            .map(|(_, config, _, _)| format!("{}.{}", config.source.schema, config.source.table))
            .collect();
        applied_tables.sort();
        applied_tables.dedup();

        let pg_client = pg::connect::connect(&env_config.database_url)
            .await
            .map_err(|e| CliError::Apply(format!("failed to connect to postgres: {e}")))?;

        pg::publication::ensure_replica_identity_full(&pg_client, &applied_tables)
            .await
            .map_err(|e| CliError::Apply(format!("failed to set replica identity: {e}")))?;

        // Persist all configs atomically after replica identity is set.
        {
            let records: Vec<ConfigRecord> = new_configs
                .iter()
                .map(|(_, config, content_hash, transform_hash)| ConfigRecord {
                    name: config.name.clone(),
                    namespace: config.namespace.clone(),
                    content_hash: content_hash.clone(),
                    transform_hash: Some(transform_hash.clone()),
                    applied_at: Utc::now(),
                    tombstone_applied_at: None,
                    namespace_prefix: None,
                })
                .collect();
            db.insert_configs(&records).await?;
        }

        for (_path, config, _, _) in &new_configs {
            applied += 1;
            println!("Applied: {}", config.name);
        }
    }

    println!("{} applied, {} unchanged", applied, skipped);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::test_utils::{
        PASSTHROUGH_TRANSFORM, setup_project_with_state, write_config, write_transform,
    };

    fn dummy_env(database_url: String, state_schema: String) -> EnvConfig {
        EnvConfig {
            database_url,
            turbopuffer_api_key: "fake".to_string(),
            turbopuffer_region: None,
            turbopuffer_namespace_prefix: None,
            otel_endpoint: None,
            otel_headers: None,
            state_schema,
            dlq_max_age_hours: None,
            inspect_port: None,
        }
    }

    #[tokio::test]
    async fn no_configs_succeeds() {
        let (_dir, paths, url, schema) = setup_project_with_state().await;
        run_async(&paths, &dummy_env(url, schema), &ProjectConfig::default())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn errors_on_missing_transform() {
        let (_dir, paths, url, schema) = setup_project_with_state().await;
        write_config(&paths, "user", "public", "users", "id", "uint");

        let err = run_async(&paths, &dummy_env(url, schema), &ProjectConfig::default())
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("had errors"),
            "expected missing transform error, got: {err}"
        );
    }

    #[tokio::test]
    async fn any_error_prevents_all_applies() {
        let (_dir, paths, url, schema) = setup_project_with_state().await;

        let user_dir = write_config(&paths, "user", "public", "users", "id", "uint");
        write_transform(&user_dir, PASSTHROUGH_TRANSFORM);
        // Create a config without a transform
        write_config(&paths, "bad", "public", "bad", "id", "uint");

        run_async(
            &paths,
            &dummy_env(url.clone(), schema.clone()),
            &ProjectConfig::default(),
        )
        .await
        .unwrap_err();

        let db = StateDb::connect(&url, &schema).await.unwrap();
        assert!(db.get_config("user").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn skips_already_applied_unchanged() {
        let (_dir, paths, url, schema) = setup_project_with_state().await;
        let user_dir = write_config(&paths, "user", "public", "users", "id", "uint");
        write_transform(&user_dir, PASSTHROUGH_TRANSFORM);

        // Load to get the content hash, then pre-seed the state DB
        let loader = config::ConfigLoader::new(&paths.configs);
        let all = loader.load_all().unwrap();
        let (config_path, cfg) = &all[0];
        let transform_bytes = fs::read(config_path.parent().unwrap().join("transform.ts")).unwrap();
        let transform_hash = format!("{:x}", Sha256::digest(&transform_bytes));
        let db = StateDb::connect(&url, &schema).await.unwrap();
        db.insert_config(&ConfigRecord {
            name: cfg.name.clone(),
            namespace: cfg.namespace.clone(),
            content_hash: Config::content_hash_from_bytes(&fs::read(config_path).unwrap()),
            transform_hash: Some(transform_hash),
            applied_at: Utc::now(),
            tombstone_applied_at: None,
            namespace_prefix: None,
        })
        .await
        .unwrap();

        // Config is unchanged → skipped, no PG validation needed
        run_async(&paths, &dummy_env(url, schema), &ProjectConfig::default())
            .await
            .unwrap();
        assert_eq!(db.list_configs().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn apply_auto_tombstones_configs_marked_on_disk() {
        let (_dir, paths, url, schema) = setup_project_with_state().await;
        let user_dir = write_config(&paths, "user", "public", "users", "id", "uint");
        write_transform(&user_dir, PASSTHROUGH_TRANSFORM);

        let loader = config::ConfigLoader::new(&paths.configs);
        let (config_path, cfg) = &loader.load_all().unwrap()[0];
        let transform_bytes = fs::read(config_path.parent().unwrap().join("transform.ts")).unwrap();
        let transform_hash = format!("{:x}", Sha256::digest(&transform_bytes));
        let db = StateDb::connect(&url, &schema).await.unwrap();
        db.insert_config(&ConfigRecord {
            name: cfg.name.clone(),
            namespace: cfg.namespace.clone(),
            content_hash: Config::content_hash_from_bytes(&fs::read(config_path).unwrap()),
            transform_hash: Some(transform_hash),
            applied_at: Utc::now(),
            tombstone_applied_at: None,
            namespace_prefix: None,
        })
        .await
        .unwrap();

        fs::write(
            user_dir.join("tombstone.toml"),
            "tombstoned_at = \"2026-04-15T00:00:00Z\"\n",
        )
        .unwrap();

        run_async(&paths, &dummy_env(url, schema), &ProjectConfig::default())
            .await
            .unwrap();

        let updated = db.get_config(&cfg.name).await.unwrap().unwrap();
        assert!(updated.tombstone_applied_at.is_some());
    }

    #[tokio::test]
    async fn skips_multiple_already_applied() {
        let (_dir, paths, url, schema) = setup_project_with_state().await;
        let user_dir = write_config(&paths, "user", "public", "users", "id", "uint");
        write_transform(&user_dir, PASSTHROUGH_TRANSFORM);
        let film_dir = write_config(&paths, "film", "public", "films", "id", "uint");
        write_transform(&film_dir, PASSTHROUGH_TRANSFORM);

        let loader = config::ConfigLoader::new(&paths.configs);
        let all = loader.load_all().unwrap();
        let db = StateDb::connect(&url, &schema).await.unwrap();
        for (config_path, cfg) in &all {
            let transform_bytes =
                fs::read(config_path.parent().unwrap().join("transform.ts")).unwrap();
            let transform_hash = format!("{:x}", Sha256::digest(&transform_bytes));
            db.insert_config(&ConfigRecord {
                name: cfg.name.clone(),
                namespace: cfg.namespace.clone(),
                content_hash: Config::content_hash_from_bytes(&fs::read(config_path).unwrap()),
                transform_hash: Some(transform_hash),
                applied_at: Utc::now(),
                tombstone_applied_at: None,
                namespace_prefix: None,
            })
            .await
            .unwrap();
        }

        run_async(&paths, &dummy_env(url, schema), &ProjectConfig::default())
            .await
            .unwrap();
        assert_eq!(db.list_configs().await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn errors_on_modified_config() {
        let (_dir, paths, url, schema) = setup_project_with_state().await;
        let user_dir = write_config(&paths, "user", "public", "users", "id", "uint");
        write_transform(&user_dir, PASSTHROUGH_TRANSFORM);

        let loader = config::ConfigLoader::new(&paths.configs);
        let (config_path, cfg) = &loader.load_all().unwrap()[0];
        let db = StateDb::connect(&url, &schema).await.unwrap();
        db.insert_config(&ConfigRecord {
            name: cfg.name.clone(),
            namespace: cfg.namespace.clone(),
            content_hash: Config::content_hash_from_bytes(&fs::read(config_path).unwrap()),
            transform_hash: Some("abc".into()),
            applied_at: Utc::now(),
            tombstone_applied_at: None,
            namespace_prefix: None,
        })
        .await
        .unwrap();

        // Mutate the config on disk
        let content = r#"name = "user"
namespace = "user"

[source]
schema = "public"
table = "accounts"

[id]
column = "id"
type = "uint"
"#
        .to_string();
        fs::write(config_path, content).unwrap();

        let err = run_async(&paths, &dummy_env(url, schema), &ProjectConfig::default())
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("had errors"),
            "expected immutability error, got: {err}"
        );
    }

    #[tokio::test]
    async fn errors_on_unreadable_transform_for_applied_config() {
        let (_dir, paths, url, schema) = setup_project_with_state().await;
        let user_dir = write_config(&paths, "user", "public", "users", "id", "uint");
        write_transform(&user_dir, PASSTHROUGH_TRANSFORM);

        let loader = config::ConfigLoader::new(&paths.configs);
        let (config_path, cfg) = &loader.load_all().unwrap()[0];
        let transform_bytes = fs::read(config_path.parent().unwrap().join("transform.ts")).unwrap();
        let transform_hash = format!("{:x}", Sha256::digest(&transform_bytes));
        let db = StateDb::connect(&url, &schema).await.unwrap();
        db.insert_config(&ConfigRecord {
            name: cfg.name.clone(),
            namespace: cfg.namespace.clone(),
            content_hash: Config::content_hash_from_bytes(&fs::read(config_path).unwrap()),
            transform_hash: Some(transform_hash),
            applied_at: Utc::now(),
            tombstone_applied_at: None,
            namespace_prefix: None,
        })
        .await
        .unwrap();

        // Delete the transform file so it can't be read
        fs::remove_file(config_path.parent().unwrap().join("transform.ts")).unwrap();

        let err = run_async(&paths, &dummy_env(url, schema), &ProjectConfig::default())
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("had errors"),
            "expected unreadable transform error, got: {err}"
        );
    }

    #[tokio::test]
    async fn stored_record_fields() {
        let (_dir, paths, url, schema) = setup_project_with_state().await;
        let film_dir = write_config(&paths, "film", "public", "films", "id", "uint");
        write_transform(&film_dir, PASSTHROUGH_TRANSFORM);

        let loader = config::ConfigLoader::new(&paths.configs);
        let (config_path, cfg) = &loader.load_all().unwrap()[0];
        let content_hash = Config::content_hash_from_bytes(&fs::read(config_path).unwrap());

        let db = StateDb::connect(&url, &schema).await.unwrap();
        db.insert_config(&ConfigRecord {
            name: cfg.name.clone(),
            namespace: cfg.namespace.clone(),
            content_hash: content_hash.clone(),
            transform_hash: Some("t_hash".into()),
            applied_at: Utc::now(),
            tombstone_applied_at: None,
            namespace_prefix: None,
        })
        .await
        .unwrap();

        let record = db.get_config("film").await.unwrap().unwrap();
        assert_eq!(record.namespace, "film");
        assert_eq!(record.content_hash.len(), 64);
        assert_eq!(record.content_hash, content_hash);
        assert!(record.transform_hash.is_some());
    }
}
