use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

use config::ConfigLoader;
use dialoguer::{Input, Select, theme::ColorfulTheme};

use crate::error::CliError;
use crate::paths::ProjectPaths;

const CONFIG_TEMPLATE: &str = include_str!("../templates/config.toml");
const TRANSFORM_NONE: &str = include_str!("../templates/transform.ts");
const TRANSFORM_TOGETHER: &str = include_str!("../templates/transform-together.ts");
const TRANSFORM_ZEROENTROPY: &str = include_str!("../templates/transform-zeroentropy.ts");
const TRANSFORM_BASETEN: &str = include_str!("../templates/transform-baseten.ts");
const TRANSFORM_CLOUDFLARE: &str = include_str!("../templates/transform-cloudflare.ts");

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Provider {
    None,
    Together,
    ZeroEntropy,
    Baseten,
    Cloudflare,
}

impl Provider {
    fn label(self) -> &'static str {
        match self {
            Provider::None => "None (no embedding)",
            Provider::Together => "Together AI",
            Provider::ZeroEntropy => "ZeroEntropy",
            Provider::Baseten => "Baseten",
            Provider::Cloudflare => "Cloudflare Workers AI",
        }
    }

    fn transform_template(self) -> &'static str {
        match self {
            Provider::None => TRANSFORM_NONE,
            Provider::Together => TRANSFORM_TOGETHER,
            Provider::ZeroEntropy => TRANSFORM_ZEROENTROPY,
            Provider::Baseten => TRANSFORM_BASETEN,
            Provider::Cloudflare => TRANSFORM_CLOUDFLARE,
        }
    }

    fn all() -> [Provider; 5] {
        [
            Provider::None,
            Provider::Together,
            Provider::ZeroEntropy,
            Provider::Baseten,
            Provider::Cloudflare,
        ]
    }
}

#[derive(Debug, Clone)]
pub struct NewOptions {
    pub name: String,
    pub table: String,
    pub namespace: String,
    pub provider: Provider,
}

fn render_config(opts: &NewOptions) -> String {
    CONFIG_TEMPLATE
        .replace("{{NAME}}", &opts.name)
        .replace("{{NAMESPACE}}", &opts.namespace)
        .replace("{{TABLE}}", &opts.table)
}

fn render_transform(opts: &NewOptions) -> String {
    opts.provider
        .transform_template()
        .replace("{{NAME}}", &opts.name)
}

/// Run the interactive `puffgres new` flow.
///
/// Prompts for config name, Postgres table, destination namespace, and the
/// default embedding provider, then writes the config + transform.
pub fn run(paths: &ProjectPaths, name_hint: Option<&str>) -> Result<(), CliError> {
    let opts = prompt_options(paths, name_hint)?;
    create(paths, &opts)
}

fn prompt_options(paths: &ProjectPaths, name_hint: Option<&str>) -> Result<NewOptions, CliError> {
    if !paths.configs.is_dir() {
        return Err(CliError::NotInitialized("configs".to_string()));
    }

    let theme = ColorfulTheme::default();

    let mut name_input = Input::<String>::with_theme(&theme).with_prompt("Config name");
    if let Some(hint) = name_hint {
        name_input = name_input.default(hint.to_string());
    }
    let name: String = name_input
        .interact_text()
        .map_err(|e| CliError::Generate(format!("prompt failed: {e}")))?;
    let name = name.trim().to_string();

    let table: String = Input::with_theme(&theme)
        .with_prompt("Postgres table name")
        .default(name.clone())
        .interact_text()
        .map_err(|e| CliError::Generate(format!("prompt failed: {e}")))?;

    let namespace: String = Input::with_theme(&theme)
        .with_prompt("Destination turbopuffer namespace")
        .default(name.clone())
        .interact_text()
        .map_err(|e| CliError::Generate(format!("prompt failed: {e}")))?;

    let providers = Provider::all();
    let labels: Vec<&str> = providers.iter().map(|p| p.label()).collect();
    let idx = Select::with_theme(&theme)
        .with_prompt("Default embedding provider for the transform")
        .items(&labels)
        .default(0)
        .interact()
        .map_err(|e| CliError::Generate(format!("prompt failed: {e}")))?;
    let provider = providers[idx];

    Ok(NewOptions {
        name: name.trim().to_string(),
        table: table.trim().to_string(),
        namespace: namespace.trim().to_string(),
        provider,
    })
}

/// Write the config + transform for an already-resolved set of options.
///
/// This is the unit-testable entry point — it does no prompting.
pub fn create(paths: &ProjectPaths, opts: &NewOptions) -> Result<(), CliError> {
    if !paths.configs.is_dir() {
        return Err(CliError::NotInitialized("configs".to_string()));
    }

    let loader = ConfigLoader::new(&paths.configs);
    let existing = loader.load_all()?;
    for (_, config) in &existing {
        if config.name == opts.name {
            return Err(CliError::DuplicateConfig {
                name: opts.name.clone(),
                field: "name".to_string(),
            });
        }
        if config.namespace == opts.namespace {
            return Err(CliError::DuplicateConfig {
                name: opts.namespace.clone(),
                field: "namespace".to_string(),
            });
        }
    }

    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    let dir_name = format!("{}_{}", timestamp, opts.name);
    let config_dir = paths.configs.join(&dir_name);

    fs::create_dir_all(&config_dir)?;
    fs::write(config_dir.join("config.toml"), render_config(opts))?;
    fs::write(config_dir.join("transform.ts"), render_transform(opts))?;

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

    fn opts(name: &str) -> NewOptions {
        NewOptions {
            name: name.to_string(),
            table: name.to_string(),
            namespace: name.to_string(),
            provider: Provider::None,
        }
    }

    #[tokio::test]
    async fn creates_config_and_transform() {
        let (_dir, paths) = setup_project();

        create(&paths, &opts("user")).unwrap();

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

    #[tokio::test]
    async fn generated_config_is_valid_toml() {
        let (_dir, paths) = setup_project();

        create(&paths, &opts("film")).unwrap();

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

    #[tokio::test]
    async fn separate_table_and_namespace_are_persisted() {
        let (_dir, paths) = setup_project();

        let options = NewOptions {
            name: "buyer".to_string(),
            table: "smart_buyer".to_string(),
            namespace: "buyers_v2".to_string(),
            provider: Provider::None,
        };
        create(&paths, &options).unwrap();

        let entries: Vec<_> = fs::read_dir(&paths.configs)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_dir())
            .collect();
        let content = fs::read_to_string(entries[0].path().join("config.toml")).unwrap();
        let config: config::Config = toml::from_str(&content).unwrap();

        assert_eq!(config.name, "buyer");
        assert_eq!(config.namespace, "buyers_v2");
        assert_eq!(config.source.table, "smart_buyer");
    }

    #[tokio::test]
    async fn dir_name_contains_timestamp_and_name() {
        let (_dir, paths) = setup_project();

        create(&paths, &opts("user")).unwrap();

        let entries: Vec<_> = fs::read_dir(&paths.configs)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_dir())
            .collect();
        let dir_name = entries[0].file_name().into_string().unwrap();
        assert!(dir_name.ends_with("_user"));
        let parts: Vec<&str> = dir_name.rsplitn(2, '_').collect();
        assert!(parts.len() == 2);
    }

    #[test]
    fn fails_if_configs_dir_missing() {
        let dir = tempfile::tempdir().unwrap();
        let paths = ProjectPaths::new(dir.path().to_path_buf()).unwrap();

        let err = create(&paths, &opts("actor")).unwrap_err();
        assert!(err.to_string().contains("configs"));
        assert!(err.to_string().contains("puffgres init"));
    }

    #[tokio::test]
    async fn transform_contains_name() {
        let (_dir, paths) = setup_project();

        create(&paths, &opts("product")).unwrap();

        let entries: Vec<_> = fs::read_dir(&paths.configs)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_dir())
            .collect();
        let config_dir = entries[0].path();
        let content = fs::read_to_string(config_dir.join("transform.ts")).unwrap();
        assert!(content.contains("product"));
    }

    #[tokio::test]
    async fn different_names_create_separate_dirs() {
        let (_dir, paths) = setup_project();

        create(&paths, &opts("user")).unwrap();
        create(&paths, &opts("film")).unwrap();

        let entries: Vec<_> = fs::read_dir(&paths.configs)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_dir())
            .collect();
        assert_eq!(entries.len(), 2);
    }

    #[tokio::test]
    async fn rejects_duplicate_name() {
        let (_dir, paths) = setup_project();

        create(&paths, &opts("user")).unwrap();
        let err = create(&paths, &opts("user")).unwrap_err();
        assert!(err.to_string().contains("user"));
        assert!(err.to_string().contains("already exists"));

        let entries: Vec<_> = fs::read_dir(&paths.configs)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_dir())
            .collect();
        assert_eq!(entries.len(), 1);
    }

    #[tokio::test]
    async fn rejects_duplicate_namespace() {
        let (_dir, paths) = setup_project();

        let dir = paths.configs.join("1000_other");
        fs::create_dir_all(&dir).unwrap();
        let fixture =
            fs::read_to_string("../../crates/config/tests/fixtures/other_name_user_namespace.toml")
                .unwrap();
        fs::write(dir.join("config.toml"), fixture).unwrap();
        fs::write(dir.join("transform.ts"), "// placeholder").unwrap();

        let err = create(&paths, &opts("user")).unwrap_err();
        assert!(err.to_string().contains("user"));
        assert!(err.to_string().contains("already exists"));
    }

    #[tokio::test]
    async fn provider_together_imports_embed_batch() {
        let (_dir, paths) = setup_project();

        let mut options = opts("films");
        options.provider = Provider::Together;
        create(&paths, &options).unwrap();

        let entries: Vec<_> = fs::read_dir(&paths.configs)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_dir())
            .collect();
        let transform = fs::read_to_string(entries[0].path().join("transform.ts")).unwrap();
        assert!(transform.contains("import { embedBatch }"));
        assert!(transform.contains("../../utils/embed"));
    }

    #[tokio::test]
    async fn provider_zeroentropy_imports_embed_batch() {
        let (_dir, paths) = setup_project();

        let mut options = opts("films");
        options.provider = Provider::ZeroEntropy;
        create(&paths, &options).unwrap();

        let entries: Vec<_> = fs::read_dir(&paths.configs)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_dir())
            .collect();
        let transform = fs::read_to_string(entries[0].path().join("transform.ts")).unwrap();
        assert!(transform.contains("embedBatchZeroEntropy"));
    }

    #[tokio::test]
    async fn provider_baseten_imports_embed_batch() {
        let (_dir, paths) = setup_project();

        let mut options = opts("films");
        options.provider = Provider::Baseten;
        create(&paths, &options).unwrap();

        let entries: Vec<_> = fs::read_dir(&paths.configs)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_dir())
            .collect();
        let transform = fs::read_to_string(entries[0].path().join("transform.ts")).unwrap();
        assert!(transform.contains("embedBatchBaseten"));
    }

    #[tokio::test]
    async fn provider_cloudflare_imports_embed_batch() {
        let (_dir, paths) = setup_project();

        let mut options = opts("films");
        options.provider = Provider::Cloudflare;
        create(&paths, &options).unwrap();

        let entries: Vec<_> = fs::read_dir(&paths.configs)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_dir())
            .collect();
        let transform = fs::read_to_string(entries[0].path().join("transform.ts")).unwrap();
        assert!(transform.contains("embedBatchCloudflare"));
    }
}
