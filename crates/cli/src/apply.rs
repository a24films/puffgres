use std::fs;

use chrono::Utc;
use config::ConfigLoader;
use sha2::{Digest, Sha256};
use state::{ConfigRecord, StateDb};

use crate::error::CliError;
use crate::paths::ProjectPaths;

pub fn run(paths: &ProjectPaths) -> Result<(), CliError> {
    let db = StateDb::open(&paths.state_db)?;

    let loader = ConfigLoader::new(&paths.configs);
    let configs = loader.load_all()?;

    if configs.is_empty() {
        eprintln!("No config files found in configs/");
        return Ok(());
    }

    let mut errors: Vec<String> = Vec::new();
    let mut to_apply: Vec<ConfigRecord> = Vec::new();
    let mut skipped = 0;

    // Pass 1: validate all configs and prepare records
    for (path, config) in &configs {
        // 1. Validate
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

        // 3. Compute transform hash (resolve relative to project root)
        let transform_path = paths.root.join(&config.transform.path);
        if !transform_path.exists() {
            errors.push(format!(
                "{}: transform file '{}' not found",
                path.display(),
                config.transform.path,
            ));
            continue;
        }
        let transform_content = fs::read(&transform_path)?;
        let transform_hash = format!("{:x}", Sha256::digest(&transform_content));

        to_apply.push(ConfigRecord {
            name: config.name.clone(),
            version: config.version,
            namespace: config.full_namespace(),
            content_hash,
            transform_hash: Some(transform_hash),
            applied_at: Utc::now(),
        });
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

    // Pass 2: apply all validated configs
    for record in &to_apply {
        db.insert_config(record)?;
        eprintln!("Applied: {}", record.name);
    }

    eprintln!("{} applied, {} unchanged", to_apply.len(), skipped);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    use crate::test_utils::setup_project;

    fn write_config(paths: &ProjectPaths, name: &str, version: i64, table: &str) {
        let config_name = format!("{name}_{version:04}");
        let content = format!(
            r#"name = "{config_name}"
version = {version}
namespace = "{name}"

[source]
schema = "public"
table = "{table}"

[id]
column = "id"
type = "uint"

[transform]
path = "transforms/{name}.ts"
"#
        );
        fs::write(paths.configs.join(format!("{config_name}.toml")), content).unwrap();
    }

    fn write_transform(paths: &ProjectPaths, name: &str) {
        fs::write(
            paths.transforms.join(format!("{name}.ts")),
            format!("export function transform(row: any) {{ return row; }}"),
        )
        .unwrap();
    }

    #[test]
    fn applies_new_config() {
        let (_dir, paths) = setup_project();
        write_config(&paths, "user", 1, "users");
        write_transform(&paths, "user");

        run(&paths).unwrap();

        let db = StateDb::open(&paths.state_db).unwrap();
        let record = db.get_config("user_0001").unwrap().unwrap();
        assert_eq!(record.name, "user_0001");
        assert_eq!(record.version, 1);
        assert_eq!(record.namespace, "user_v1");
        assert!(record.transform_hash.is_some());
    }

    #[test]
    fn applies_multiple_configs() {
        let (_dir, paths) = setup_project();
        write_config(&paths, "user", 1, "users");
        write_config(&paths, "film", 1, "films");
        write_transform(&paths, "user");
        write_transform(&paths, "film");

        run(&paths).unwrap();

        let db = StateDb::open(&paths.state_db).unwrap();
        let configs = db.list_configs().unwrap();
        assert_eq!(configs.len(), 2);
    }

    #[test]
    fn skips_already_applied_unchanged() {
        let (_dir, paths) = setup_project();
        write_config(&paths, "user", 1, "users");
        write_transform(&paths, "user");

        run(&paths).unwrap();
        // Second apply should skip (unchanged)
        run(&paths).unwrap();

        let db = StateDb::open(&paths.state_db).unwrap();
        let configs = db.list_configs().unwrap();
        assert_eq!(configs.len(), 1);
    }

    #[test]
    fn errors_on_modified_config() {
        let (_dir, paths) = setup_project();
        write_config(&paths, "user", 1, "users");
        write_transform(&paths, "user");

        run(&paths).unwrap();

        // Modify the config file (change the table)
        write_config(&paths, "user", 1, "accounts");

        let err = run(&paths).unwrap_err();
        assert!(err.to_string().contains("error"));
    }

    #[test]
    fn errors_on_invalid_config() {
        let (_dir, paths) = setup_project();
        write_config(&paths, "bad", 0, "bad");

        let err = run(&paths).unwrap_err();
        assert!(err.to_string().contains("error"));
    }

    #[test]
    fn no_configs_succeeds() {
        let (_dir, paths) = setup_project();
        run(&paths).unwrap();
    }

    #[test]
    fn errors_on_missing_transform() {
        let (_dir, paths) = setup_project();
        write_config(&paths, "user", 1, "users");
        // Don't write transform file

        let err = run(&paths).unwrap_err();
        assert!(err.to_string().contains("error"));
    }

    #[test]
    fn content_hash_is_stored() {
        let (_dir, paths) = setup_project();
        write_config(&paths, "user", 1, "users");
        write_transform(&paths, "user");

        run(&paths).unwrap();

        let db = StateDb::open(&paths.state_db).unwrap();
        let record = db.get_config("user_0001").unwrap().unwrap();
        assert!(!record.content_hash.is_empty());
        assert_eq!(record.content_hash.len(), 64);
    }

    #[test]
    fn namespace_uses_full_namespace() {
        let (_dir, paths) = setup_project();
        write_config(&paths, "film", 2, "films");
        write_transform(&paths, "film");

        run(&paths).unwrap();

        let db = StateDb::open(&paths.state_db).unwrap();
        let record = db.get_config("film_0002").unwrap().unwrap();
        assert_eq!(record.namespace, "film_v2");
    }

    #[test]
    fn invalid_config_prevents_all_applies() {
        let (_dir, paths) = setup_project();

        // Valid config
        write_config(&paths, "user", 1, "users");
        write_transform(&paths, "user");

        // Invalid config (version 0)
        write_config(&paths, "bad", 0, "bad");

        let err = run(&paths).unwrap_err();
        assert!(err.to_string().contains("error"));

        // Valid config should NOT have been applied
        let db = StateDb::open(&paths.state_db).unwrap();
        let record = db.get_config("user_0001").unwrap();
        assert!(record.is_none());
    }
}
