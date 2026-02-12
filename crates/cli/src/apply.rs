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

pub async fn run_async(paths: &ProjectPaths, env_config: &EnvConfig) -> Result<(), CliError> {
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
        let content_hash = config.content_hash()?;
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

    use crate::test_utils::{setup_project, write_config, write_passthrough_transform};

    fn dummy_env() -> EnvConfig {
        EnvConfig {
            database_url: "host=invalid".to_string(),
            turbopuffer_api_key: "fake".to_string(),
            turbopuffer_region: None,
        }
    }

    #[test]
    fn test_no_configs_succeeds() {
        let (_dir, paths) = setup_project();
        run(&paths, &dummy_env()).unwrap();
    }

    #[test]
    fn test_errors_on_invalid_config() {
        let (_dir, paths) = setup_project();
        write_config(&paths, "bad", 0, "public", "bad", "id", "uint");

        let err = run(&paths, &dummy_env()).unwrap_err();
        assert!(
            err.to_string().contains("had errors"),
            "expected validation error, got: {err}"
        );
    }

    #[test]
    fn test_errors_on_missing_transform() {
        let (_dir, paths) = setup_project();
        write_config(&paths, "user", 1, "public", "users", "id", "uint");

        let err = run(&paths, &dummy_env()).unwrap_err();
        assert!(
            err.to_string().contains("had errors"),
            "expected missing transform error, got: {err}"
        );
    }

    #[test]
    fn test_any_error_prevents_all_applies() {
        let (_dir, paths) = setup_project();

        write_config(&paths, "user", 1, "public", "users", "id", "uint");
        write_passthrough_transform(&paths, "user");
        write_config(&paths, "bad", 0, "public", "bad", "id", "uint");

        run(&paths, &dummy_env()).unwrap_err();

        let db = StateDb::open(&paths.state_db).unwrap();
        assert!(db.get_config("user_0001").unwrap().is_none());
    }

    #[test]
    fn test_skips_already_applied_unchanged() {
        let (_dir, paths) = setup_project();
        write_config(&paths, "user", 1, "public", "users", "id", "uint");
        write_passthrough_transform(&paths, "user");

        // Load to get the content hash, then pre-seed the state DB
        let loader = config::ConfigLoader::new(&paths.configs);
        let cfg = &loader.load_all().unwrap()[0].1;
        let db = StateDb::open(&paths.state_db).unwrap();
        db.insert_config(&ConfigRecord {
            name: cfg.name.clone(),
            version: cfg.version,
            namespace: cfg.full_namespace(),
            content_hash: cfg.content_hash().unwrap(),
            transform_hash: Some("abc".into()),
            applied_at: Utc::now(),
        })
        .unwrap();

        // Config is unchanged → skipped, no PG validation needed
        run(&paths, &dummy_env()).unwrap();
        assert_eq!(db.list_configs().unwrap().len(), 1);
    }

    #[test]
    fn test_skips_multiple_already_applied() {
        let (_dir, paths) = setup_project();
        write_config(&paths, "user", 1, "public", "users", "id", "uint");
        write_passthrough_transform(&paths, "user");
        write_config(&paths, "film", 2, "public", "films", "id", "uint");
        write_passthrough_transform(&paths, "film");

        let loader = config::ConfigLoader::new(&paths.configs);
        let all = loader.load_all().unwrap();
        let db = StateDb::open(&paths.state_db).unwrap();
        for (_, cfg) in &all {
            db.insert_config(&ConfigRecord {
                name: cfg.name.clone(),
                version: cfg.version,
                namespace: cfg.full_namespace(),
                content_hash: cfg.content_hash().unwrap(),
                transform_hash: Some("abc".into()),
                applied_at: Utc::now(),
            })
            .unwrap();
        }

        run(&paths, &dummy_env()).unwrap();
        assert_eq!(db.list_configs().unwrap().len(), 2);
    }

    #[test]
    fn test_errors_on_modified_config() {
        let (_dir, paths) = setup_project();
        write_config(&paths, "user", 1, "public", "users", "id", "uint");
        write_passthrough_transform(&paths, "user");

        let loader = config::ConfigLoader::new(&paths.configs);
        let cfg = &loader.load_all().unwrap()[0].1;
        let db = StateDb::open(&paths.state_db).unwrap();
        db.insert_config(&ConfigRecord {
            name: cfg.name.clone(),
            version: cfg.version,
            namespace: cfg.full_namespace(),
            content_hash: cfg.content_hash().unwrap(),
            transform_hash: Some("abc".into()),
            applied_at: Utc::now(),
        })
        .unwrap();

        // Mutate the config on disk
        write_config(&paths, "user", 1, "public", "accounts", "id", "uint");

        let err = run(&paths, &dummy_env()).unwrap_err();
        assert!(
            err.to_string().contains("had errors"),
            "expected immutability error, got: {err}"
        );
    }

    #[test]
    fn test_stored_record_fields() {
        let (_dir, paths) = setup_project();
        write_config(&paths, "film", 2, "public", "films", "id", "uint");
        write_passthrough_transform(&paths, "film");

        let loader = config::ConfigLoader::new(&paths.configs);
        let cfg = &loader.load_all().unwrap()[0].1;
        let content_hash = cfg.content_hash().unwrap();

        let db = StateDb::open(&paths.state_db).unwrap();
        db.insert_config(&ConfigRecord {
            name: cfg.name.clone(),
            version: cfg.version,
            namespace: cfg.full_namespace(),
            content_hash: content_hash.clone(),
            transform_hash: Some("t_hash".into()),
            applied_at: Utc::now(),
        })
        .unwrap();

        let record = db.get_config("film_0002").unwrap().unwrap();
        assert_eq!(record.version, 2);
        assert_eq!(record.namespace, "film_v2");
        assert_eq!(record.content_hash.len(), 64);
        assert_eq!(record.content_hash, content_hash);
        assert!(record.transform_hash.is_some());
    }
}
