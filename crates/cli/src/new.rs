use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

use config::ConfigLoader;

use crate::error::CliError;
use crate::paths::ProjectPaths;

const CONFIG_TEMPLATE: &str = include_str!("../templates/config.toml");
const TRANSFORM_TEMPLATE: &str = include_str!("../templates/transform.ts");

fn render_config(name: &str) -> String {
    CONFIG_TEMPLATE.replace("{{NAME}}", name)
}

fn render_transform(name: &str) -> String {
    TRANSFORM_TEMPLATE.replace("{{NAME}}", name)
}

pub fn run(paths: &ProjectPaths, name: &str) -> Result<(), CliError> {
    if !paths.configs.is_dir() {
        return Err(CliError::NotInitialized("configs".to_string()));
    }

    let loader = ConfigLoader::new(&paths.configs);
    let existing = loader.load_all()?;
    for (_, config) in &existing {
        if config.name == name {
            return Err(CliError::DuplicateConfig {
                name: name.to_string(),
                field: "name".to_string(),
            });
        }
        if config.namespace == name {
            return Err(CliError::DuplicateConfig {
                name: name.to_string(),
                field: "namespace".to_string(),
            });
        }
    }

    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    let dir_name = format!("{}_{}", timestamp, name);
    let config_dir = paths.configs.join(&dir_name);

    fs::create_dir_all(&config_dir)?;
    fs::write(config_dir.join("config.toml"), render_config(name))?;
    fs::write(config_dir.join("transform.ts"), render_transform(name))?;

    println!(
        "Created config:    {}",
        config_dir.join("config.toml").display()
    );
    println!(
        "Created transform: {}",
        config_dir.join("transform.ts").display()
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    use crate::test_utils::setup_project;

    #[test]
    fn creates_config_and_transform() {
        let (_dir, paths, _state_db_path) = setup_project();

        run(&paths, "user").unwrap();

        // Find the created directory
        let entries: Vec<_> = fs::read_dir(&paths.configs)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_dir())
            .collect();
        assert_eq!(entries.len(), 1);

        let config_dir = entries[0].path();
        assert!(config_dir.join("config.toml").exists());
        assert!(config_dir.join("transform.ts").exists());
    }

    #[test]
    fn generated_config_is_valid_toml() {
        let (_dir, paths, _state_db_path) = setup_project();

        run(&paths, "film").unwrap();

        let entries: Vec<_> = fs::read_dir(&paths.configs)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_dir())
            .collect();
        let config_dir = entries[0].path();
        let content = fs::read_to_string(config_dir.join("config.toml")).unwrap();
        let config: config::Config = toml::from_str(&content).unwrap();

        assert_eq!(config.name, "film");
        assert_eq!(config.namespace, "film");
        assert_eq!(config.source.schema, "public");
        assert_eq!(config.source.table, "film");
        assert_eq!(config.id.column, "id");
        assert_eq!(config.id.id_type, config::IdType::Uint);
    }

    #[test]
    fn dir_name_contains_timestamp_and_name() {
        let (_dir, paths, _state_db_path) = setup_project();

        run(&paths, "user").unwrap();

        let entries: Vec<_> = fs::read_dir(&paths.configs)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_dir())
            .collect();
        let dir_name = entries[0].file_name().into_string().unwrap();
        assert!(dir_name.ends_with("_user"));
        // Timestamp prefix should be a number
        let parts: Vec<&str> = dir_name.rsplitn(2, '_').collect();
        assert!(parts.len() == 2);
    }

    #[test]
    fn fails_if_configs_dir_missing() {
        let dir = tempfile::tempdir().unwrap();
        let paths = ProjectPaths::new(dir.path().to_path_buf()).unwrap();

        let err = run(&paths, "actor").unwrap_err();
        assert!(err.to_string().contains("configs"));
        assert!(err.to_string().contains("puffgres init"));
    }

    #[test]
    fn transform_contains_name() {
        let (_dir, paths, _state_db_path) = setup_project();

        run(&paths, "product").unwrap();

        let entries: Vec<_> = fs::read_dir(&paths.configs)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_dir())
            .collect();
        let config_dir = entries[0].path();
        let content = fs::read_to_string(config_dir.join("transform.ts")).unwrap();
        assert!(content.contains("product"));
    }

    #[test]
    fn different_names_create_separate_dirs() {
        let (_dir, paths, _state_db_path) = setup_project();

        run(&paths, "user").unwrap();
        run(&paths, "film").unwrap();

        let entries: Vec<_> = fs::read_dir(&paths.configs)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_dir())
            .collect();
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn rejects_duplicate_name() {
        let (_dir, paths, _state_db_path) = setup_project();

        run(&paths, "user").unwrap();
        let err = run(&paths, "user").unwrap_err();
        assert!(err.to_string().contains("user"));
        assert!(err.to_string().contains("already exists"));

        // Should still only have one directory
        let entries: Vec<_> = fs::read_dir(&paths.configs)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_dir())
            .collect();
        assert_eq!(entries.len(), 1);
    }

    #[test]
    fn rejects_duplicate_namespace() {
        let (_dir, paths, _state_db_path) = setup_project();

        // Create a config with namespace "user" but name "other"
        let dir = paths.configs.join("1000_other");
        fs::create_dir_all(&dir).unwrap();
        let fixture =
            fs::read_to_string("../../crates/config/tests/fixtures/other_name_user_namespace.toml")
                .unwrap();
        fs::write(dir.join("config.toml"), fixture).unwrap();
        fs::write(dir.join("transform.ts"), "// placeholder").unwrap();

        let err = run(&paths, "user").unwrap_err();
        assert!(err.to_string().contains("user"));
        assert!(err.to_string().contains("already exists"));
    }
}
