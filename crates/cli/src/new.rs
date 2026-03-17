use std::fmt;
use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

use config::ConfigLoader;
use dialoguer::{Input, Select};

use crate::error::CliError;
use crate::paths::ProjectPaths;

const CONFIG_TEMPLATE: &str = include_str!("../templates/config.toml");
const TRANSFORM_TEMPLATE: &str = include_str!("../templates/transform.ts");
const TRANSFORM_TOGETHER_TEMPLATE: &str = include_str!("../templates/transform-together.ts");
const TRANSFORM_BASETEN_TEMPLATE: &str = include_str!("../templates/transform-baseten.ts");
const TRANSFORM_ZEROENTROPY_TEMPLATE: &str = include_str!("../templates/transform-zeroentropy.ts");

/// All the fields collected by the wizard (or passed programmatically in tests).
#[derive(Debug, Clone)]
pub struct NewConfig {
    pub name: String,
    pub namespace: String,
    pub table: String,
    pub id_column: String,
    pub id_type: IdTypeChoice,
    pub embed_provider: EmbedProvider,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdTypeChoice {
    Uint,
    Int,
    Uuid,
    String,
}

impl IdTypeChoice {
    const ALL: [IdTypeChoice; 4] = [
        IdTypeChoice::Uint,
        IdTypeChoice::Int,
        IdTypeChoice::Uuid,
        IdTypeChoice::String,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            IdTypeChoice::Uint => "uint",
            IdTypeChoice::Int => "int",
            IdTypeChoice::Uuid => "uuid",
            IdTypeChoice::String => "string",
        }
    }
}

impl fmt::Display for IdTypeChoice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmbedProvider {
    None,
    Together,
    Baseten,
    ZeroEntropy,
}

impl EmbedProvider {
    const ALL: [EmbedProvider; 4] = [
        EmbedProvider::None,
        EmbedProvider::Together,
        EmbedProvider::Baseten,
        EmbedProvider::ZeroEntropy,
    ];
}

impl fmt::Display for EmbedProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EmbedProvider::None => f.write_str("None (no embeddings)"),
            EmbedProvider::Together => f.write_str("Together AI"),
            EmbedProvider::Baseten => f.write_str("Baseten"),
            EmbedProvider::ZeroEntropy => f.write_str("ZeroEntropy"),
        }
    }
}

fn render_config(cfg: &NewConfig) -> String {
    CONFIG_TEMPLATE
        .replace("{{NAME}}", &cfg.name)
        .replace("{{NAMESPACE}}", &cfg.namespace)
        .replace("{{TABLE}}", &cfg.table)
        .replace("{{ID_COLUMN}}", &cfg.id_column)
        .replace("{{ID_TYPE}}", cfg.id_type.as_str())
}

fn render_transform(cfg: &NewConfig) -> String {
    let template = match cfg.embed_provider {
        EmbedProvider::None => TRANSFORM_TEMPLATE,
        EmbedProvider::Together => TRANSFORM_TOGETHER_TEMPLATE,
        EmbedProvider::Baseten => TRANSFORM_BASETEN_TEMPLATE,
        EmbedProvider::ZeroEntropy => TRANSFORM_ZEROENTROPY_TEMPLATE,
    };
    template.replace("{{NAME}}", &cfg.name)
}

/// Run the interactive wizard, prompting the user for each field.
pub fn wizard() -> Result<NewConfig, CliError> {
    let name: String = Input::new()
        .with_prompt("Config name")
        .interact_text()
        .map_err(|e| CliError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?;

    let namespace: String = Input::new()
        .with_prompt("Turbopuffer namespace")
        .default(name.clone())
        .interact_text()
        .map_err(|e| CliError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?;

    let table: String = Input::new()
        .with_prompt("Source table name")
        .default(name.clone())
        .interact_text()
        .map_err(|e| CliError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?;

    let id_column: String = Input::new()
        .with_prompt("ID column name")
        .default("id".to_string())
        .interact_text()
        .map_err(|e| CliError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?;

    let id_type_idx = Select::new()
        .with_prompt("ID column type")
        .items(&IdTypeChoice::ALL)
        .default(0)
        .interact()
        .map_err(|e| CliError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?;
    let id_type = IdTypeChoice::ALL[id_type_idx];

    let embed_idx = Select::new()
        .with_prompt("Embedding provider")
        .items(&EmbedProvider::ALL)
        .default(0)
        .interact()
        .map_err(|e| CliError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?;
    let embed_provider = EmbedProvider::ALL[embed_idx];

    Ok(NewConfig {
        name,
        namespace,
        table,
        id_column,
        id_type,
        embed_provider,
    })
}

/// Create the config directory and files from a `NewConfig`.
pub fn create(paths: &ProjectPaths, cfg: &NewConfig) -> Result<(), CliError> {
    if !paths.configs.is_dir() {
        return Err(CliError::NotInitialized("configs".to_string()));
    }

    let loader = ConfigLoader::new(&paths.configs);
    let existing = loader.load_all()?;
    for (_, config) in &existing {
        if config.name == cfg.name {
            return Err(CliError::DuplicateConfig {
                name: cfg.name.clone(),
                field: "name".to_string(),
            });
        }
        if config.namespace == cfg.namespace {
            return Err(CliError::DuplicateConfig {
                name: cfg.namespace.clone(),
                field: "namespace".to_string(),
            });
        }
    }

    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    let dir_name = format!("{}_{}", timestamp, cfg.name);
    let config_dir = paths.configs.join(&dir_name);

    fs::create_dir_all(&config_dir)?;
    fs::write(config_dir.join("config.toml"), render_config(cfg))?;
    fs::write(config_dir.join("transform.ts"), render_transform(cfg))?;

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

/// Entry point called from main. If `name` is provided, uses defaults (backwards compat).
/// If `name` is None, runs the interactive wizard.
pub fn run(paths: &ProjectPaths, name: Option<&str>) -> Result<(), CliError> {
    let cfg = match name {
        Some(n) => NewConfig {
            name: n.to_string(),
            namespace: n.to_string(),
            table: n.to_string(),
            id_column: "id".to_string(),
            id_type: IdTypeChoice::Uint,
            embed_provider: EmbedProvider::None,
        },
        None => wizard()?,
    };
    create(paths, &cfg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    use crate::test_utils::setup_project;

    fn default_cfg(name: &str) -> NewConfig {
        NewConfig {
            name: name.to_string(),
            namespace: name.to_string(),
            table: name.to_string(),
            id_column: "id".to_string(),
            id_type: IdTypeChoice::Uint,
            embed_provider: EmbedProvider::None,
        }
    }

    fn find_config_dirs(paths: &ProjectPaths) -> Vec<std::path::PathBuf> {
        fs::read_dir(&paths.configs)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_dir())
            .map(|e| e.path())
            .collect()
    }

    #[test]
    fn creates_config_and_transform() {
        let (_dir, paths, _state_db_path) = setup_project();
        create(&paths, &default_cfg("user")).unwrap();

        let entries = find_config_dirs(&paths);
        assert_eq!(entries.len(), 1);

        let config_dir = &entries[0];
        assert!(config_dir.join("config.toml").exists());
        assert!(config_dir.join("transform.ts").exists());
    }

    #[test]
    fn generated_config_is_valid_toml() {
        let (_dir, paths, _state_db_path) = setup_project();
        create(&paths, &default_cfg("film")).unwrap();

        let entries = find_config_dirs(&paths);
        let config_dir = &entries[0];
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
    fn wizard_fields_serialize_correctly() {
        let (_dir, paths, _state_db_path) = setup_project();

        let cfg = NewConfig {
            name: "product".to_string(),
            namespace: "products_ns".to_string(),
            table: "products".to_string(),
            id_column: "product_id".to_string(),
            id_type: IdTypeChoice::Uuid,
            embed_provider: EmbedProvider::None,
        };
        create(&paths, &cfg).unwrap();

        let entries = find_config_dirs(&paths);
        let content = fs::read_to_string(entries[0].join("config.toml")).unwrap();
        let config: config::Config = toml::from_str(&content).unwrap();

        assert_eq!(config.name, "product");
        assert_eq!(config.namespace, "products_ns");
        assert_eq!(config.source.table, "products");
        assert_eq!(config.source.schema, "public");
        assert_eq!(config.id.column, "product_id");
        assert_eq!(config.id.id_type, config::IdType::Uuid);
    }

    #[test]
    fn all_id_types_serialize() {
        for id_type in IdTypeChoice::ALL {
            let (_dir, paths, _state_db_path) = setup_project();
            let mut cfg = default_cfg("test");
            cfg.id_type = id_type;
            create(&paths, &cfg).unwrap();

            let entries = find_config_dirs(&paths);
            let content = fs::read_to_string(entries[0].join("config.toml")).unwrap();
            let config: config::Config = toml::from_str(&content).unwrap();

            let expected = match id_type {
                IdTypeChoice::Uint => config::IdType::Uint,
                IdTypeChoice::Int => config::IdType::Int,
                IdTypeChoice::Uuid => config::IdType::Uuid,
                IdTypeChoice::String => config::IdType::String,
            };
            assert_eq!(config.id.id_type, expected, "failed for {id_type}");
        }
    }

    #[test]
    fn dir_name_contains_timestamp_and_name() {
        let (_dir, paths, _state_db_path) = setup_project();
        create(&paths, &default_cfg("user")).unwrap();

        let entries = find_config_dirs(&paths);
        let dir_name = entries[0]
            .file_name()
            .unwrap()
            .to_string_lossy()
            .to_string();
        assert!(dir_name.ends_with("_user"));
        let parts: Vec<&str> = dir_name.rsplitn(2, '_').collect();
        assert!(parts.len() == 2);
    }

    #[test]
    fn fails_if_configs_dir_missing() {
        let dir = tempfile::tempdir().unwrap();
        let paths = ProjectPaths::new(dir.path().to_path_buf()).unwrap();

        let err = create(&paths, &default_cfg("actor")).unwrap_err();
        assert!(err.to_string().contains("configs"));
        assert!(err.to_string().contains("puffgres init"));
    }

    #[test]
    fn transform_contains_name() {
        let (_dir, paths, _state_db_path) = setup_project();
        create(&paths, &default_cfg("product")).unwrap();

        let entries = find_config_dirs(&paths);
        let content = fs::read_to_string(entries[0].join("transform.ts")).unwrap();
        assert!(content.contains("product"));
    }

    #[test]
    fn different_names_create_separate_dirs() {
        let (_dir, paths, _state_db_path) = setup_project();
        create(&paths, &default_cfg("user")).unwrap();
        create(&paths, &default_cfg("film")).unwrap();

        let entries = find_config_dirs(&paths);
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn rejects_duplicate_name() {
        let (_dir, paths, _state_db_path) = setup_project();

        create(&paths, &default_cfg("user")).unwrap();
        let err = create(&paths, &default_cfg("user")).unwrap_err();
        assert!(err.to_string().contains("user"));
        assert!(err.to_string().contains("already exists"));

        let entries = find_config_dirs(&paths);
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

        let err = create(&paths, &default_cfg("user")).unwrap_err();
        assert!(err.to_string().contains("user"));
        assert!(err.to_string().contains("already exists"));
    }

    #[test]
    fn run_with_name_uses_defaults() {
        let (_dir, paths, _state_db_path) = setup_project();

        run(&paths, Some("user")).unwrap();

        let entries = find_config_dirs(&paths);
        let content = fs::read_to_string(entries[0].join("config.toml")).unwrap();
        let config: config::Config = toml::from_str(&content).unwrap();

        assert_eq!(config.name, "user");
        assert_eq!(config.namespace, "user");
        assert_eq!(config.source.table, "user");
        assert_eq!(config.id.column, "id");
        assert_eq!(config.id.id_type, config::IdType::Uint);
    }

    #[test]
    fn together_transform_template() {
        let (_dir, paths, _state_db_path) = setup_project();

        let mut cfg = default_cfg("article");
        cfg.embed_provider = EmbedProvider::Together;
        create(&paths, &cfg).unwrap();

        let entries = find_config_dirs(&paths);
        let content = fs::read_to_string(entries[0].join("transform.ts")).unwrap();
        assert!(content.contains("article"));
        assert!(content.contains("embedBatch"));
        assert!(content.contains("../utils/embed"));
        assert!(content.contains("cosine_distance"));
    }

    #[test]
    fn baseten_transform_template() {
        let (_dir, paths, _state_db_path) = setup_project();

        let mut cfg = default_cfg("article");
        cfg.embed_provider = EmbedProvider::Baseten;
        create(&paths, &cfg).unwrap();

        let entries = find_config_dirs(&paths);
        let content = fs::read_to_string(entries[0].join("transform.ts")).unwrap();
        assert!(content.contains("article"));
        assert!(content.contains("embedBatchBaseten"));
        assert!(content.contains("../utils/embed-baseten"));
        assert!(content.contains("cosine_distance"));
    }

    #[test]
    fn zeroentropy_transform_template() {
        let (_dir, paths, _state_db_path) = setup_project();

        let mut cfg = default_cfg("article");
        cfg.embed_provider = EmbedProvider::ZeroEntropy;
        create(&paths, &cfg).unwrap();

        let entries = find_config_dirs(&paths);
        let content = fs::read_to_string(entries[0].join("transform.ts")).unwrap();
        assert!(content.contains("article"));
        assert!(content.contains("embedBatchZeroEntropy"));
        assert!(content.contains("../utils/embed-zeroentropy"));
        assert!(content.contains("cosine_distance"));
    }

    #[test]
    fn none_provider_uses_default_transform() {
        let (_dir, paths, _state_db_path) = setup_project();

        let cfg = default_cfg("user");
        create(&paths, &cfg).unwrap();

        let entries = find_config_dirs(&paths);
        let content = fs::read_to_string(entries[0].join("transform.ts")).unwrap();
        assert!(content.contains("TODO: map row fields"));
        assert!(!content.contains("embedBatch"));
    }
}
