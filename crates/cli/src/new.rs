use std::fs;

use state::StateDb;

use crate::error::CliError;
use crate::paths::ProjectPaths;

const CONFIG_TEMPLATE: &str = include_str!("../templates/config.toml");
const TRANSFORM_TEMPLATE: &str = include_str!("../templates/transform.ts");

fn render_config(name: &str, version: i64) -> String {
    let config_name = format!("{name}_{version:04}");
    CONFIG_TEMPLATE
        .replace("{{CONFIG_NAME}}", &config_name)
        .replace("{{VERSION}}", &version.to_string())
        .replace("{{NAME}}", name)
}

fn render_transform(name: &str) -> String {
    TRANSFORM_TEMPLATE.replace("{{NAME}}", name)
}

pub fn run(paths: &ProjectPaths, name: &str) -> Result<(), CliError> {
    if !paths.configs.is_dir() {
        return Err(CliError::NotInitialized("configs".to_string()));
    }
    if !paths.transforms.is_dir() {
        return Err(CliError::NotInitialized("transforms".to_string()));
    }

    let mut db = StateDb::open(&paths.state_db)?;
    let db_max = db.max_version_for_prefix(name)?;
    let version = db_max + 1;

    let config_name = format!("{name}_{version:04}");
    let config_path = paths.configs.join(format!("{config_name}.toml"));
    let transform_path = paths.transforms.join(format!("{name}.ts"));

    if config_path.exists() {
        return Err(CliError::AlreadyExists(config_path.display().to_string()));
    }

    fs::write(&config_path, render_config(name, version))?;
    println!("Created config:    {}", config_path.display());

    if !transform_path.exists() {
        fs::write(&transform_path, render_transform(name))?;
        println!("Created transform: {}", transform_path.display());
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use state::ConfigRecord;
    use std::fs;

    use crate::test_utils::setup_project;

    fn insert_applied_config(paths: &ProjectPaths, name: &str, version: u64) {
        let mut db = StateDb::open(&paths.state_db).unwrap();
        db.insert_config(&ConfigRecord {
            name: format!("{name}_{version:04}"),
            version,
            namespace: format!("{name}_v{version}"),
            content_hash: "abc123".to_string(),
            transform_hash: None,
            applied_at: Utc::now(),
        })
        .unwrap();
    }

    #[test]
    fn creates_config_and_transform() {
        let (_dir, paths) = setup_project();

        run(&paths, "user").unwrap();

        assert!(paths.configs.join("user_0001.toml").exists());
        assert!(paths.transforms.join("user.ts").exists());
    }

    #[test]
    fn generated_config_is_valid_toml() {
        let (_dir, paths) = setup_project();

        run(&paths, "film").unwrap();

        let content = fs::read_to_string(paths.configs.join("film_0001.toml")).unwrap();
        let config: config::Config = toml::from_str(&content).unwrap();

        assert_eq!(config.name, "film_0001");
        assert_eq!(config.version, 1);
        assert_eq!(config.namespace, "film");
        assert_eq!(config.source.schema, "public");
        assert_eq!(config.source.table, "film");
        assert_eq!(config.id.column, "id");
        assert_eq!(config.id.id_type, config::IdType::Uint);
        assert_eq!(config.transform.path, "transforms/film.ts");
    }

    #[test]
    fn version_skips_past_db_max() {
        let (_dir, paths) = setup_project();
        insert_applied_config(&paths, "user", 1);
        insert_applied_config(&paths, "user", 3);

        run(&paths, "user").unwrap();

        // Should be 4 since DB max is 3
        assert!(paths.configs.join("user_0004.toml").exists());
    }

    #[test]
    fn errors_if_config_file_already_exists() {
        let (_dir, paths) = setup_project();
        // No DB entries, so next version = 1. Pre-create that file.
        fs::write(paths.configs.join("user_0001.toml"), "existing").unwrap();

        let err = run(&paths, "user").unwrap_err();
        assert!(err.to_string().contains("already exists"));
    }

    #[test]
    fn does_not_overwrite_existing_transform() {
        let (_dir, paths) = setup_project();
        fs::write(paths.transforms.join("user.ts"), "existing").unwrap();

        run(&paths, "user").unwrap();

        let content = fs::read_to_string(paths.transforms.join("user.ts")).unwrap();
        assert_eq!(content, "existing");
    }

    #[test]
    fn fails_if_configs_dir_missing() {
        let dir = tempfile::tempdir().unwrap();
        let paths = ProjectPaths::new(dir.path().to_path_buf()).unwrap();
        fs::create_dir_all(&paths.transforms).unwrap();

        let err = run(&paths, "actor").unwrap_err();
        assert!(err.to_string().contains("configs"));
        assert!(err.to_string().contains("puffgres init"));
    }

    #[test]
    fn fails_if_transforms_dir_missing() {
        let dir = tempfile::tempdir().unwrap();
        let paths = ProjectPaths::new(dir.path().to_path_buf()).unwrap();
        fs::create_dir_all(&paths.configs).unwrap();

        let err = run(&paths, "actor").unwrap_err();
        assert!(err.to_string().contains("transforms"));
        assert!(err.to_string().contains("puffgres init"));
    }

    #[test]
    fn transform_contains_name() {
        let (_dir, paths) = setup_project();

        run(&paths, "product").unwrap();

        let content = fs::read_to_string(paths.transforms.join("product.ts")).unwrap();
        assert!(content.contains("product"));
    }

    #[test]
    fn different_names_independent() {
        let (_dir, paths) = setup_project();
        insert_applied_config(&paths, "user", 2);

        run(&paths, "user").unwrap();
        run(&paths, "film").unwrap();

        assert!(paths.configs.join("user_0003.toml").exists());
        assert!(paths.configs.join("film_0001.toml").exists());
    }
}
