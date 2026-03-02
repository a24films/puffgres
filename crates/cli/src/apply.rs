use std::fs;
use std::path::PathBuf;

use chrono::Utc;
use config::Config;
use sha2::{Digest, Sha256};
use state::{ConfigRecord, StateDb};

use crate::env::EnvConfig;
use crate::error::CliError;
use crate::paths::ProjectPaths;
use crate::validate::{validate_live, validate_static};

pub fn run(paths: &ProjectPaths, env_config: &EnvConfig) -> Result<(), CliError> {
    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| CliError::Apply(format!("failed to create async runtime: {e}")))?;
    rt.block_on(run_async(paths, env_config))
}

pub async fn run_async(paths: &ProjectPaths, env_config: &EnvConfig) -> Result<(), CliError> {
    let mut db = StateDb::open(&env_config.state_db_path)?;

    let loader = config::ConfigLoader::new(&paths.configs);
    let configs = loader.load_all()?;

    if configs.is_empty() {
        println!("No config files found in configs/");
        return Ok(());
    }

    // Static validation (no DB connection needed)
    let static_passed = validate_static(&configs).map_err(|errors| {
        for err in &errors {
            println!("Error: {}", err);
        }
        CliError::Apply(format!("{} config(s) had errors", errors.len()))
    })?;

    // Immutability check — filter to only new configs
    let mut errors: Vec<String> = Vec::new();
    let mut skipped = 0;

    // First pass: basic validation, immutability check, and transform hashing
    let mut new_configs: Vec<(PathBuf, Config, String, String)> = Vec::new();

    for &i in &static_passed {
        let (config_path, config) = &configs[i];

        let content_hash = config.content_hash()?;
        if let Some(existing) = db.get_config(&config.name)? {
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

        // 3. Read transform file and compute hash
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
        return Err(CliError::Apply(format!(
            "{} config(s) had errors",
            errors.len()
        )));
    }

    // Live validation against Postgres and apply
    let mut applied = 0;
    if !new_configs.is_empty() {
        let live_configs: Vec<(PathBuf, Config)> = new_configs
            .iter()
            .map(|(p, c, _, _)| (p.clone(), c.clone()))
            .collect();

        let validated = validate_live(env_config, &live_configs).await?;

        // Collect tables that need REPLICA IDENTITY FULL before persisting
        // to the state DB. This ensures that if the ALTER fails, configs are
        // not marked as applied and will be retried on the next run.
        let mut applied_tables: Vec<String> = Vec::new();
        for (i, (_path, config, _, _)) in new_configs.iter().enumerate() {
            if !validated.contains(&i) {
                continue;
            }
            applied_tables.push(format!("{}.{}", config.source.schema, config.source.table));
        }

        if !applied_tables.is_empty() {
            applied_tables.sort();
            applied_tables.dedup();

            let pg_client = pg::connect::connect(&env_config.database_url)
                .await
                .map_err(|e| CliError::Apply(format!("failed to connect to postgres: {e}")))?;

            pg::publication::ensure_replica_identity_full(&pg_client, &applied_tables)
                .await
                .map_err(|e| CliError::Apply(format!("failed to set replica identity: {e}")))?;
        }

        // Persist configs only after replica identity is set successfully.
        for (i, (_path, config, content_hash, transform_hash)) in new_configs.iter().enumerate() {
            if !validated.contains(&i) {
                continue;
            }

            let record = ConfigRecord {
                name: config.name.clone(),

                namespace: config.namespace.clone(),
                content_hash: content_hash.clone(),
                transform_hash: Some(transform_hash.clone()),
                applied_at: Utc::now(),
                tombstone_applied_at: None,
            };

            db.insert_config(&record)?;
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

    use crate::test_utils::{PASSTHROUGH_TRANSFORM, setup_project, write_config, write_transform};

    fn dummy_env(state_db_path: PathBuf) -> EnvConfig {
        EnvConfig {
            database_url: "host=invalid".to_string(),
            turbopuffer_api_key: "fake".to_string(),
            turbopuffer_region: None,
            turbopuffer_namespace_prefix: None,
            otel_endpoint: None,
            otel_headers: None,
            state_db_path,
        }
    }

    #[test]
    fn test_no_configs_succeeds() {
        let (_dir, paths, state_db_path) = setup_project();
        run(&paths, &dummy_env(state_db_path)).unwrap();
    }

    #[test]
    fn test_errors_on_missing_transform() {
        let (_dir, paths, state_db_path) = setup_project();
        write_config(&paths, "user", "public", "users", "id", "uint");

        let err = run(&paths, &dummy_env(state_db_path)).unwrap_err();
        assert!(
            err.to_string().contains("had errors"),
            "expected missing transform error, got: {err}"
        );
    }

    #[test]
    fn test_any_error_prevents_all_applies() {
        let (_dir, paths, state_db_path) = setup_project();

        let user_dir = write_config(&paths, "user", "public", "users", "id", "uint");
        write_transform(&user_dir, PASSTHROUGH_TRANSFORM);
        // Create a config with an invalid name (starts with number) — but we need it to parse
        // So instead, create a config without a transform
        write_config(&paths, "bad", "public", "bad", "id", "uint");

        run(&paths, &dummy_env(state_db_path.clone())).unwrap_err();

        let mut db = StateDb::open(&state_db_path).unwrap();
        assert!(db.get_config("user").unwrap().is_none());
    }

    #[test]
    fn test_skips_already_applied_unchanged() {
        let (_dir, paths, state_db_path) = setup_project();
        let user_dir = write_config(&paths, "user", "public", "users", "id", "uint");
        write_transform(&user_dir, PASSTHROUGH_TRANSFORM);

        // Load to get the content hash, then pre-seed the state DB
        let loader = config::ConfigLoader::new(&paths.configs);
        let all = loader.load_all().unwrap();
        let (config_path, cfg) = &all[0];
        let transform_bytes = fs::read(config_path.parent().unwrap().join("transform.ts")).unwrap();
        let transform_hash = format!("{:x}", Sha256::digest(&transform_bytes));
        let mut db = StateDb::open(&state_db_path).unwrap();
        db.insert_config(&ConfigRecord {
            name: cfg.name.clone(),

            namespace: cfg.namespace.clone(),
            content_hash: cfg.content_hash().unwrap(),
            transform_hash: Some(transform_hash),
            applied_at: Utc::now(),
            tombstone_applied_at: None,
        })
        .unwrap();

        // Config is unchanged → skipped, no PG validation needed
        run(&paths, &dummy_env(state_db_path)).unwrap();
        assert_eq!(db.list_configs().unwrap().len(), 1);
    }

    #[test]
    fn test_skips_multiple_already_applied() {
        let (_dir, paths, state_db_path) = setup_project();
        let user_dir = write_config(&paths, "user", "public", "users", "id", "uint");
        write_transform(&user_dir, PASSTHROUGH_TRANSFORM);
        let film_dir = write_config(&paths, "film", "public", "films", "id", "uint");
        write_transform(&film_dir, PASSTHROUGH_TRANSFORM);

        let loader = config::ConfigLoader::new(&paths.configs);
        let all = loader.load_all().unwrap();
        let mut db = StateDb::open(&state_db_path).unwrap();
        for (config_path, cfg) in &all {
            let transform_bytes =
                fs::read(config_path.parent().unwrap().join("transform.ts")).unwrap();
            let transform_hash = format!("{:x}", Sha256::digest(&transform_bytes));
            db.insert_config(&ConfigRecord {
                name: cfg.name.clone(),

                namespace: cfg.namespace.clone(),
                content_hash: cfg.content_hash().unwrap(),
                transform_hash: Some(transform_hash),
                applied_at: Utc::now(),
                tombstone_applied_at: None,
            })
            .unwrap();
        }

        run(&paths, &dummy_env(state_db_path)).unwrap();
        assert_eq!(db.list_configs().unwrap().len(), 2);
    }

    #[test]
    fn test_errors_on_modified_config() {
        let (_dir, paths, state_db_path) = setup_project();
        let user_dir = write_config(&paths, "user", "public", "users", "id", "uint");
        write_transform(&user_dir, PASSTHROUGH_TRANSFORM);

        let loader = config::ConfigLoader::new(&paths.configs);
        let (config_path, cfg) = &loader.load_all().unwrap()[0];
        let mut db = StateDb::open(&state_db_path).unwrap();
        db.insert_config(&ConfigRecord {
            name: cfg.name.clone(),

            namespace: cfg.namespace.clone(),
            content_hash: cfg.content_hash().unwrap(),
            transform_hash: Some("abc".into()),
            applied_at: Utc::now(),
            tombstone_applied_at: None,
        })
        .unwrap();

        // Mutate the config on disk
        let content = format!(
            r#"name = "user"
namespace = "user"

[source]
schema = "public"
table = "accounts"

[id]
column = "id"
type = "uint"
"#
        );
        fs::write(config_path, content).unwrap();

        let err = run(&paths, &dummy_env(state_db_path)).unwrap_err();
        assert!(
            err.to_string().contains("had errors"),
            "expected immutability error, got: {err}"
        );
    }

    #[test]
    fn test_errors_on_unreadable_transform_for_applied_config() {
        let (_dir, paths, state_db_path) = setup_project();
        let user_dir = write_config(&paths, "user", "public", "users", "id", "uint");
        write_transform(&user_dir, PASSTHROUGH_TRANSFORM);

        let loader = config::ConfigLoader::new(&paths.configs);
        let (config_path, cfg) = &loader.load_all().unwrap()[0];
        let transform_bytes = fs::read(config_path.parent().unwrap().join("transform.ts")).unwrap();
        let transform_hash = format!("{:x}", Sha256::digest(&transform_bytes));
        let mut db = StateDb::open(&state_db_path).unwrap();
        db.insert_config(&ConfigRecord {
            name: cfg.name.clone(),

            namespace: cfg.namespace.clone(),
            content_hash: cfg.content_hash().unwrap(),
            transform_hash: Some(transform_hash),
            applied_at: Utc::now(),
            tombstone_applied_at: None,
        })
        .unwrap();

        // Delete the transform file so it can't be read
        fs::remove_file(config_path.parent().unwrap().join("transform.ts")).unwrap();

        let err = run(&paths, &dummy_env(state_db_path)).unwrap_err();
        assert!(
            err.to_string().contains("had errors"),
            "expected unreadable transform error, got: {err}"
        );
    }

    #[test]
    fn test_stored_record_fields() {
        let (_dir, paths, state_db_path) = setup_project();
        let film_dir = write_config(&paths, "film", "public", "films", "id", "uint");
        write_transform(&film_dir, PASSTHROUGH_TRANSFORM);

        let loader = config::ConfigLoader::new(&paths.configs);
        let cfg = &loader.load_all().unwrap()[0].1;
        let content_hash = cfg.content_hash().unwrap();

        let mut db = StateDb::open(&state_db_path).unwrap();
        db.insert_config(&ConfigRecord {
            name: cfg.name.clone(),

            namespace: cfg.namespace.clone(),
            content_hash: content_hash.clone(),
            transform_hash: Some("t_hash".into()),
            applied_at: Utc::now(),
            tombstone_applied_at: None,
        })
        .unwrap();

        let record = db.get_config("film").unwrap().unwrap();
        assert_eq!(record.namespace, "film");
        assert_eq!(record.content_hash.len(), 64);
        assert_eq!(record.content_hash, content_hash);
        assert!(record.transform_hash.is_some());
    }
}
