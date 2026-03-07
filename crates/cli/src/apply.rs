use std::fs;
use std::path::PathBuf;

use chrono::Utc;
use config::Config;
use sha2::{Digest, Sha256};
use state::{ConfigRecord, StateDb};

use crate::env::EnvConfig;
use crate::error::CliError;
use crate::paths::ProjectPaths;
use crate::validate::preflight_check;

pub fn run(paths: &ProjectPaths, env_config: &EnvConfig) -> Result<(), CliError> {
    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| CliError::Apply(format!("failed to create async runtime: {e}")))?;
    rt.block_on(run_async(paths, env_config))
}

pub async fn run_async(paths: &ProjectPaths, env_config: &EnvConfig) -> Result<(), CliError> {
    if let Some(parent) = env_config.state_db_path.parent() {
        if !parent.exists() {
            std::fs::create_dir_all(parent)?;
        }
    }
    let mut db = StateDb::open(&env_config.state_db_path)?;

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
        let content_hash = match config.content_hash() {
            Ok(h) => h,
            Err(e) => {
                errors.push(format!("{}: {e}", config_path.display()));
                continue;
            }
        };
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
        return Err(CliError::Apply(format!(
            "{} config(s) had errors",
            errors.len()
        )));
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
            &env_config.state_db_path,
            &new_config_refs,
            None,
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

        // Persist configs only after replica identity is set successfully.
        for (_path, config, content_hash, transform_hash) in &new_configs {
            let record = ConfigRecord {
                name: config.name.clone(),

                namespace: config.namespace.clone(),
                content_hash: content_hash.clone(),
                transform_hash: Some(transform_hash.clone()),
                applied_at: Utc::now(),
                tombstone_applied_at: None,
                namespace_prefix: None,
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
            dlq_max_age_hours: None,
        }
    }

    #[test]
    fn no_configs_succeeds() {
        let (_dir, paths, state_db_path) = setup_project();
        run(&paths, &dummy_env(state_db_path)).unwrap();
    }

    #[test]
    fn errors_on_missing_transform() {
        let (_dir, paths, state_db_path) = setup_project();
        write_config(&paths, "user", "public", "users", "id", "uint");

        let err = run(&paths, &dummy_env(state_db_path)).unwrap_err();
        assert!(
            err.to_string().contains("had errors"),
            "expected missing transform error, got: {err}"
        );
    }

    #[test]
    fn any_error_prevents_all_applies() {
        let (_dir, paths, state_db_path) = setup_project();

        let user_dir = write_config(&paths, "user", "public", "users", "id", "uint");
        write_transform(&user_dir, PASSTHROUGH_TRANSFORM);
        // Create a config without a transform
        write_config(&paths, "bad", "public", "bad", "id", "uint");

        run(&paths, &dummy_env(state_db_path.clone())).unwrap_err();

        let mut db = StateDb::open(&state_db_path).unwrap();
        assert!(db.get_config("user").unwrap().is_none());
    }

    #[test]
    fn skips_already_applied_unchanged() {
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
            namespace_prefix: None,
        })
        .unwrap();

        // Config is unchanged → skipped, no PG validation needed
        run(&paths, &dummy_env(state_db_path)).unwrap();
        assert_eq!(db.list_configs().unwrap().len(), 1);
    }

    #[test]
    fn skips_multiple_already_applied() {
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
                namespace_prefix: None,
            })
            .unwrap();
        }

        run(&paths, &dummy_env(state_db_path)).unwrap();
        assert_eq!(db.list_configs().unwrap().len(), 2);
    }

    #[test]
    fn errors_on_modified_config() {
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
            namespace_prefix: None,
        })
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

        let err = run(&paths, &dummy_env(state_db_path)).unwrap_err();
        assert!(
            err.to_string().contains("had errors"),
            "expected immutability error, got: {err}"
        );
    }

    #[test]
    fn errors_on_unreadable_transform_for_applied_config() {
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
            namespace_prefix: None,
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
    fn stored_record_fields() {
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
            namespace_prefix: None,
        })
        .unwrap();

        let record = db.get_config("film").unwrap().unwrap();
        assert_eq!(record.namespace, "film");
        assert_eq!(record.content_hash.len(), 64);
        assert_eq!(record.content_hash, content_hash);
        assert!(record.transform_hash.is_some());
    }
}
