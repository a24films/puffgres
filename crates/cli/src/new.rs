use std::fs;
use std::path::Path;
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
    /// The Postgres column used as the turbopuffer document id. Picked from the
    /// table's columns when a connection is available, otherwise "id".
    pub id_column: String,
    /// The turbopuffer id type, auto-detected from the chosen id column when a
    /// connection is available, otherwise "uint".
    pub id_type: String,
    /// The column whose value is embedded by the generated transform. `None`
    /// when the provider is `None` (no embedding) — the transform then has no
    /// embedding step to fill in.
    pub embed_column: Option<String>,
}

fn render_config(opts: &NewOptions) -> String {
    CONFIG_TEMPLATE
        .replace("{{NAME}}", &opts.name)
        .replace("{{NAMESPACE}}", &opts.namespace)
        .replace("{{TABLE}}", &opts.table)
        .replace("{{ID_COLUMN}}", &opts.id_column)
        .replace("{{ID_TYPE}}", &opts.id_type)
}

/// Map a Postgres `udt_name` to the turbopuffer id type string for the template.
/// Mirrors the compatibility rules in `validate::check_id_type_compat`.
fn id_type_for_udt(udt: &str) -> &'static str {
    match udt {
        "int2" | "int4" | "int8" | "smallint" | "integer" | "bigint" | "serial" | "bigserial"
        | "smallserial" => "uint",
        "uuid" => "uuid",
        // text/varchar/char/citext/etc. and anything we don't recognize.
        _ => "string",
    }
}

/// Best-effort introspection of the table's columns.
///
/// Returns `Err` with a human-readable reason whenever it isn't possible — no
/// `DATABASE_URL`, the connection fails, or the table doesn't exist — so callers
/// can surface why they're falling back to manual entry instead of a picker.
async fn introspect_columns(
    database_url: Option<&str>,
    table: &str,
) -> Result<Vec<pg::column::ColumnInfo>, String> {
    let url = database_url.ok_or("DATABASE_URL not set")?;
    let client = pg::connect::connect(url)
        .await
        .map_err(|e| format!("connection failed: {e}"))?;
    pg::column::resolve_column_info(&client, "public", table)
        .await
        .map_err(|e| e.to_string())
}

/// Pick the turbopuffer id type for the chosen id column from introspected
/// columns.
///
/// Returns "uint" (the historical default) when the columns are unknown or the
/// table has no column with that name.
fn id_type_from_columns(columns: Option<&[pg::column::ColumnInfo]>, id_column: &str) -> String {
    columns
        .and_then(|cols| cols.iter().find(|c| c.name == id_column))
        .map(|c| id_type_for_udt(&c.udt_name))
        .unwrap_or("uint")
        .to_string()
}

fn render_transform(opts: &NewOptions) -> String {
    opts.provider
        .transform_template()
        .replace("{{NAME}}", &opts.name)
        .replace("{{EMBED_EXPR}}", &embed_expr(opts.embed_column.as_deref()))
        .replace("{{ID_DOC_FIELD}}", &id_document_field(&opts.id_column))
}

/// Build the leading `document` field that mirrors the id column so it's stored
/// as a queryable attribute (e.g. `circuit_name: row.circuit_name,`).
///
/// Returns an empty string when the id column is literally `id` — turbopuffer
/// reserves `id` for the top-level document id, so re-adding it as an attribute
/// would conflict.
fn id_document_field(col: &str) -> String {
    if col == "id" {
        return String::new();
    }
    let key = if is_js_identifier(col) {
        col.to_string()
    } else {
        format!("\"{}\"", crate::generate::ts_escape(col))
    };
    format!("{key}: {},\n          ", row_access(col))
}

/// Build the TS expression the transform returns as the text to embed for a row.
///
/// With a column it reads that field (`row.title ?? ""`), coalescing `null` to
/// the empty string. Without one it falls back to `""`, leaving a transform the
/// user can fill in by hand.
fn embed_expr(column: Option<&str>) -> String {
    match column {
        Some(col) => format!("{} ?? \"\"", row_access(col)),
        None => "\"\"".to_string(),
    }
}

/// Access `row.<col>`, falling back to bracket notation for column names that
/// aren't valid JS identifiers (e.g. `user-id` -> `row["user-id"]`).
fn row_access(col: &str) -> String {
    if is_js_identifier(col) {
        format!("row.{col}")
    } else {
        format!("row[\"{}\"]", crate::generate::ts_escape(col))
    }
}

fn is_js_identifier(s: &str) -> bool {
    let mut chars = s.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' || c == '$' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$')
}

/// Run the interactive `puffgres new` flow.
///
/// Prompts for config name, Postgres table, destination namespace, the id
/// column, and the default embedding provider, then writes the config +
/// transform. When a `database_url` is available the id column is picked from
/// the table's columns and its id type auto-detected; otherwise it falls back
/// to `id` / "uint".
pub async fn run(
    paths: &ProjectPaths,
    name_hint: Option<&str>,
    database_url: Option<&str>,
) -> Result<(), CliError> {
    let opts = prompt_options(paths, name_hint, database_url).await?;
    create(paths, &opts)?;

    // Generate the typed schema.ts so the new transform's `./schema` import
    // resolves immediately. Needs a database connection; without one we leave a
    // hint to run it later.
    match database_url {
        Some(url) => {
            println!();
            crate::generate::run_async(paths, url).await?;
        }
        None => {
            println!(
                "\nSkipped schema generation (no DATABASE_URL). Run `puffgres generate` once it's configured."
            );
        }
    }

    Ok(())
}

async fn prompt_options(
    paths: &ProjectPaths,
    name_hint: Option<&str>,
    database_url: Option<&str>,
) -> Result<NewOptions, CliError> {
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

    let table = table.trim().to_string();
    let columns = match introspect_columns(database_url, &table).await {
        Ok(cols) => Some(cols),
        Err(reason) => {
            eprintln!(
                "  ⚠ couldn't read columns for public.{table} ({reason}) — falling back to manual entry"
            );
            None
        }
    };

    let id_column = prompt_id_column(&theme, columns.as_deref())?;
    let id_type = id_type_from_columns(columns.as_deref(), &id_column);

    // Only embedding providers have a text-to-embed step to fill in.
    let embed_column = if provider == Provider::None {
        None
    } else {
        Some(prompt_embed_column(&theme, columns.as_deref())?)
    };

    Ok(NewOptions {
        name: name.trim().to_string(),
        table,
        namespace: namespace.trim().to_string(),
        provider,
        id_column,
        id_type,
        embed_column,
    })
}

/// Ask which column to use as the turbopuffer document id. Presents a picker
/// when the table's columns are known (defaulting to a column named `id` when
/// present), otherwise falls back to free text defaulting to `id`.
fn prompt_id_column(
    theme: &ColorfulTheme,
    columns: Option<&[pg::column::ColumnInfo]>,
) -> Result<String, CliError> {
    let prompt = "Which column is the unique id?";
    match columns {
        Some(cols) if !cols.is_empty() => {
            let names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
            let default = names.iter().position(|n| *n == "id").unwrap_or(0);
            let idx = Select::with_theme(theme)
                .with_prompt(prompt)
                .items(&names)
                .default(default)
                .interact()
                .map_err(|e| CliError::Generate(format!("prompt failed: {e}")))?;
            Ok(names[idx].to_string())
        }
        _ => {
            let col: String = Input::with_theme(theme)
                .with_prompt(prompt)
                .default("id".to_string())
                .interact_text()
                .map_err(|e| CliError::Generate(format!("prompt failed: {e}")))?;
            Ok(col.trim().to_string())
        }
    }
}

/// Ask which column to embed. Presents a picker when the table's columns are
/// known, otherwise falls back to free text.
fn prompt_embed_column(
    theme: &ColorfulTheme,
    columns: Option<&[pg::column::ColumnInfo]>,
) -> Result<String, CliError> {
    let prompt = "Which column should be embedded?";
    match columns {
        Some(cols) if !cols.is_empty() => {
            let names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
            let idx = Select::with_theme(theme)
                .with_prompt(prompt)
                .items(&names)
                .default(0)
                .interact()
                .map_err(|e| CliError::Generate(format!("prompt failed: {e}")))?;
            Ok(names[idx].to_string())
        }
        _ => {
            let col: String = Input::with_theme(theme)
                .with_prompt(prompt)
                .interact_text()
                .map_err(|e| CliError::Generate(format!("prompt failed: {e}")))?;
            Ok(col.trim().to_string())
        }
    }
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

    // Build in a staging dir, then atomically rename into `configs/` so a failed
    // write never leaves a `config.toml`-less dir that poisons `load_all`. The
    // staging dir sits in `paths.root` (same filesystem) to keep rename atomic.
    let staging = paths.root.join(format!(".puffgres-new-{timestamp}"));
    if let Err(e) = write_staging(&staging, opts) {
        let _ = fs::remove_dir_all(&staging);
        return Err(e);
    }
    if let Err(e) = fs::rename(&staging, &config_dir) {
        let _ = fs::remove_dir_all(&staging);
        return Err(CliError::from(e));
    }

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

/// Staging step of the atomic create: write both files into `dir` so a failure
/// can be unwound by removing it wholesale.
fn write_staging(dir: &Path, opts: &NewOptions) -> Result<(), CliError> {
    fs::create_dir_all(dir)?;
    fs::write(dir.join("config.toml"), render_config(opts))?;
    fs::write(dir.join("transform.ts"), render_transform(opts))?;
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
            id_column: "id".to_string(),
            id_type: "uint".to_string(),
            embed_column: None,
        }
    }

    #[test]
    fn maps_udt_to_id_type() {
        assert_eq!(id_type_for_udt("int4"), "uint");
        assert_eq!(id_type_for_udt("bigserial"), "uint");
        assert_eq!(id_type_for_udt("uuid"), "uuid");
        assert_eq!(id_type_for_udt("text"), "string");
        assert_eq!(id_type_for_udt("varchar"), "string");
        // Unknown types fall back to the permissive string type.
        assert_eq!(id_type_for_udt("jsonb"), "string");
    }

    #[tokio::test]
    async fn render_config_uses_detected_id_type() {
        let mut o = opts("film");
        o.id_type = "uuid".to_string();
        let rendered = render_config(&o);
        let config: config::Config = toml::from_str(&rendered).unwrap();
        assert_eq!(config.id.id_type, config::IdType::Uuid);
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
            id_column: "id".to_string(),
            id_type: "uint".to_string(),
            embed_column: None,
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
    async fn create_is_atomic_and_loadable() {
        let (_dir, paths) = setup_project();

        create(&paths, &opts("user")).unwrap();

        // No staging dir left in the root, and load_all sees a complete config.
        let leftover = fs::read_dir(&paths.root)
            .unwrap()
            .filter_map(|e| e.ok())
            .any(|e| {
                e.file_name()
                    .to_string_lossy()
                    .starts_with(".puffgres-new-")
            });
        assert!(!leftover, "staging dir should be renamed away");

        let loaded = config::ConfigLoader::new(&paths.configs)
            .load_all()
            .unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].1.name, "user");
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

    #[test]
    fn id_document_field_skips_literal_id() {
        assert_eq!(id_document_field("id"), "");
    }

    #[test]
    fn id_document_field_mirrors_chosen_column() {
        assert_eq!(
            id_document_field("circuit_name"),
            "circuit_name: row.circuit_name,\n          "
        );
    }

    #[test]
    fn id_document_field_quotes_non_identifier_keys() {
        assert_eq!(
            id_document_field("circuit-name"),
            "\"circuit-name\": row[\"circuit-name\"],\n          "
        );
    }

    #[tokio::test]
    async fn config_uses_chosen_id_column() {
        let (_dir, paths) = setup_project();

        let mut options = opts("circuits");
        options.table = "distinct_circuits".to_string();
        options.id_column = "circuit_name".to_string();
        options.id_type = "string".to_string();
        create(&paths, &options).unwrap();

        let entries: Vec<_> = fs::read_dir(&paths.configs)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_dir())
            .collect();
        let content = fs::read_to_string(entries[0].path().join("config.toml")).unwrap();
        let config: config::Config = toml::from_str(&content).unwrap();

        assert_eq!(config.id.column, "circuit_name");
        assert_eq!(config.id.id_type, config::IdType::String);
    }

    #[tokio::test]
    async fn transform_adds_id_column_to_document() {
        let (_dir, paths) = setup_project();

        let mut options = opts("circuits");
        options.id_column = "circuit_name".to_string();
        create(&paths, &options).unwrap();

        let entries: Vec<_> = fs::read_dir(&paths.configs)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_dir())
            .collect();
        let transform = fs::read_to_string(entries[0].path().join("transform.ts")).unwrap();
        assert!(transform.contains("circuit_name: row.circuit_name,"));
        assert!(!transform.contains("{{ID_DOC_FIELD}}"));
    }

    #[tokio::test]
    async fn transform_omits_id_doc_field_for_literal_id() {
        let (_dir, paths) = setup_project();

        // opts() uses id_column "id" — no redundant document field should appear.
        create(&paths, &opts("films")).unwrap();

        let entries: Vec<_> = fs::read_dir(&paths.configs)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_dir())
            .collect();
        let transform = fs::read_to_string(entries[0].path().join("transform.ts")).unwrap();
        assert!(!transform.contains("{{ID_DOC_FIELD}}"));
        assert!(!transform.contains("id: row.id"));
    }

    #[test]
    fn embed_expr_uses_dot_access_for_identifiers() {
        assert_eq!(embed_expr(Some("title")), "row.title ?? \"\"");
        assert_eq!(embed_expr(Some("_private$1")), "row._private$1 ?? \"\"");
    }

    #[test]
    fn embed_expr_uses_bracket_access_for_non_identifiers() {
        assert_eq!(embed_expr(Some("user-id")), "row[\"user-id\"] ?? \"\"");
        // Leading digit is not a valid identifier start.
        assert_eq!(embed_expr(Some("1col")), "row[\"1col\"] ?? \"\"");
    }

    #[test]
    fn embed_expr_without_column_returns_empty_string() {
        assert_eq!(embed_expr(None), "\"\"");
    }

    #[tokio::test]
    async fn transform_embeds_chosen_column() {
        let (_dir, paths) = setup_project();

        let mut options = opts("films");
        options.provider = Provider::Together;
        options.embed_column = Some("title".to_string());
        create(&paths, &options).unwrap();

        let entries: Vec<_> = fs::read_dir(&paths.configs)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_dir())
            .collect();
        let transform = fs::read_to_string(entries[0].path().join("transform.ts")).unwrap();
        assert!(transform.contains("return row.title ?? \"\";"));
        // The TODO placeholder should be gone.
        assert!(!transform.contains("{{EMBED_EXPR}}"));
        assert!(!transform.contains("return the text you want embedded"));
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
