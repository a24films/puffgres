use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::Duration;

use config::{Config, IdType};
use pg::Client;

use crate::dry_transform::dry_run_transform;
use state::StateDb;

/// Run all pre-flight validation checks on configs.
///
/// Performs: static validation, transform file existence, namespace uniqueness,
/// and per-config Postgres validation (table, columns, id type, unique index,
/// dry-run transform).
///
/// When `pg_client` is `Some`, the provided connection is reused instead of
/// opening a new one.  Pass `None` to have the function connect on its own.
///
/// Returns `Ok(())` if all configs pass, or `Err(message)` on failure.
pub async fn preflight_check(
    database_url: &str,
    state_db_path: &Path,
    configs: &[(PathBuf, Config)],
    pg_client: Option<&Client>,
    transform_timeout: Duration,
) -> Result<(), String> {
    if configs.is_empty() {
        return Ok(());
    }

    println!("Checking {} configs...", configs.len());

    // 1. Static validation
    let mut static_passed: Vec<usize> = Vec::new();
    let mut static_errors: Vec<String> = Vec::new();

    for (i, (path, config)) in configs.iter().enumerate() {
        let display = path.display();
        if let Err(validation_errors) = config.validate() {
            for err in &validation_errors {
                static_errors.push(format!("{display}: {} - {}", err.field, err.message));
            }
            continue;
        }
        static_passed.push(i);
    }

    if !static_errors.is_empty() {
        for err in &static_errors {
            println!("Error: {}", err);
        }
        return Err(format!(
            "{} config(s) had static errors",
            static_errors.len()
        ));
    }

    // 2. Validate transform files exist
    let mut valid_configs: Vec<(PathBuf, Config)> = Vec::new();
    let mut transform_errors: Vec<String> = Vec::new();

    for &i in &static_passed {
        let (path, config) = &configs[i];
        let transform_path = path.parent().unwrap().join("transform.ts");
        if !transform_path.exists() {
            transform_errors.push(format!(
                "{}: transform file 'transform.ts' does not exist",
                path.display(),
            ));
            continue;
        }
        valid_configs.push((path.clone(), config.clone()));
    }

    if !transform_errors.is_empty() {
        for err in &transform_errors {
            println!("Error: {}", err);
        }
        return Err(format!("{} config(s) had errors", transform_errors.len()));
    }

    // 3. Namespace uniqueness
    let mut namespace_errors: Vec<String> = Vec::new();
    let mut seen_namespaces: HashSet<String> = HashSet::new();

    for (path, config) in &valid_configs {
        let ns = config.namespace.clone();
        if !seen_namespaces.insert(ns.clone()) {
            namespace_errors.push(format!("{}: duplicate namespace '{}'", path.display(), ns));
        }
    }

    // Check against state DB for namespace conflicts
    let db =
        StateDb::open(state_db_path).map_err(|e| format!("failed to open state database: {e}"))?;
    let applied = db
        .list_configs()
        .map_err(|e| format!("failed to list applied configs: {e}"))?;
    let applied_namespaces: HashSet<String> = applied.iter().map(|r| r.namespace.clone()).collect();
    for (path, config) in &valid_configs {
        let ns = config.namespace.clone();
        if applied_namespaces.contains(&ns) {
            let conflict = applied.iter().find(|r| r.namespace == ns);
            if let Some(existing) = conflict
                && existing.name != config.name
            {
                namespace_errors.push(format!(
                    "{}: namespace '{}' already used by applied config '{}'",
                    path.display(),
                    ns,
                    existing.name,
                ));
            }
        }
    }

    for (path, config) in &valid_configs {
        if is_reserved_source_schema(&config.source.schema) {
            namespace_errors.push(format!(
                "{}: source schema '{}' is reserved for puffgres state tables",
                path.display(),
                config.source.schema,
            ));
        }
    }

    if !namespace_errors.is_empty() {
        for err in &namespace_errors {
            println!("Error: {}", err);
        }
        return Err(format!(
            "{} config(s) had namespace errors",
            namespace_errors.len()
        ));
    }

    // 4. Live Postgres validation
    let owned_client;
    let pg_client: &Client = match pg_client {
        Some(c) => c,
        None => {
            owned_client = pg::connect::connect(database_url)
                .await
                .map_err(|e| format!("failed to connect to postgres: {e}"))?;
            &owned_client
        }
    };

    let mut passed = 0;
    let mut failed = 0;

    for (path, config) in &valid_configs {
        let qualified = format!("{}.{}", config.source.schema, config.source.table);

        // Table exists
        let table_refs = vec![(config.source.schema.as_str(), config.source.table.as_str())];
        if let Err(e) = pg::connect::validate_tables(pg_client, &table_refs).await {
            println!("  {:<12} FAIL {} -- {}", config.name, qualified, e);
            failed += 1;
            continue;
        }

        // ID column exists + type compatibility
        let pg_type = match pg::column::validate_column(
            pg_client,
            &config.source.schema,
            &config.source.table,
            &config.id.column,
        )
        .await
        {
            Ok(t) => t,
            Err(e) => {
                println!("  {:<12} FAIL {} -- {}", config.name, qualified, e);
                failed += 1;
                continue;
            }
        };

        if let Some(warning) = check_id_type_compat(&config.id.id_type, &pg_type) {
            println!("  {:<12} FAIL {} -- {}", config.name, qualified, warning);
            failed += 1;
            continue;
        }

        // Unique index on id column
        let batch_config = pg::batch::BatchQueryConfig {
            schema: config.source.schema.clone(),
            table: config.source.table.clone(),
            id_column: config.id.column.clone(),
            columns: config.columns.clone(),
            batch_size: 1,
        };

        if let Err(_e) = pg::batch::validate_id_column_uniqueness(pg_client, &batch_config).await {
            println!(
                "  {:<12} FAIL {} -- id column '{}' has no unique index",
                config.name, qualified, config.id.column
            );
            failed += 1;
            continue;
        }

        // Validate columns if specified
        let mut col_ok = true;
        if let Some(columns) = &config.columns {
            for col in columns {
                if let Err(e) = pg::column::validate_column(
                    pg_client,
                    &config.source.schema,
                    &config.source.table,
                    col,
                )
                .await
                {
                    println!("  {:<12} FAIL {} -- {}", config.name, qualified, e);
                    failed += 1;
                    col_ok = false;
                    break;
                }
            }
        }
        if !col_ok {
            continue;
        }

        // Dry-run transform with a sample row
        let mut transform_status = "no sample row";
        let sample = match pg::sample::fetch_sample_row(
            pg_client,
            &config.source.schema,
            &config.source.table,
        )
        .await
        {
            Ok(s) => s,
            Err(e) => {
                println!(
                    "  {:<12} FAIL {} -- failed to fetch sample row: {}",
                    config.name, qualified, e
                );
                failed += 1;
                continue;
            }
        };

        if let Some((column_names, values)) = sample {
            match dry_run_transform(path, config, &column_names, &values, transform_timeout).await {
                Ok(_) => {
                    transform_status = "transform ok";
                }
                Err(e) => {
                    println!("  {:<12} FAIL {} -- {}", config.name, qualified, e);
                    failed += 1;
                    continue;
                }
            }
        }

        // All checks passed for this config
        let id_type_label = id_type_display(&config.id.id_type);
        let ns = &config.namespace;
        println!(
            "  {:<12} ok {qualified} -> {ns} (id: {id_type_label}, unique index ok, {transform_status})",
            config.name,
        );
        passed += 1;
    }

    println!();
    if failed > 0 {
        println!("{passed} passed, {failed} failed");
        Err(format!("{failed} config(s) had errors"))
    } else {
        println!("{passed} passed, {failed} failed");
        Ok(())
    }
}

fn id_type_display(id_type: &IdType) -> &'static str {
    match id_type {
        IdType::Uint => "uint",
        IdType::Int => "int",
        IdType::Uuid => "uuid",
        IdType::String => "string",
    }
}

fn is_reserved_source_schema(schema: &str) -> bool {
    schema == pg::schema_bootstrap::PUFFGRES_SCHEMA
}

/// Check that the config id type is compatible with the Postgres column type.
/// Returns an error message if incompatible, None if OK.
///
/// This is intentionally permissive to match the runtime parsers:
/// - `DocumentId::from_text` parses the WAL text representation, so Uuid works
///   from any text/varchar column and String accepts anything.
/// - `DocumentId::from_value` accepts numbers as strings and strings as UUIDs.
fn check_id_type_compat(id_type: &IdType, pg_udt: &str) -> Option<String> {
    let is_integer = matches!(
        pg_udt,
        "int2"
            | "int4"
            | "int8"
            | "smallint"
            | "integer"
            | "bigint"
            | "serial"
            | "bigserial"
            | "smallserial"
    );
    let is_text = matches!(
        pg_udt,
        "text" | "varchar" | "char" | "bpchar" | "name" | "citext"
    );
    let is_uuid = pg_udt == "uuid";

    let compatible = match id_type {
        IdType::Uint | IdType::Int => is_integer,
        IdType::Uuid => is_uuid || is_text,
        IdType::String => is_text || is_integer || is_uuid,
    };

    if compatible {
        None
    } else {
        Some(format!(
            "id type '{id_type:?}' is not compatible with Postgres column type '{pg_udt}'"
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compatible_pairs() {
        let cases = [
            (IdType::Uint, "int4"),
            (IdType::Uint, "int8"),
            (IdType::Int, "int4"),
            (IdType::Uuid, "uuid"),
            (IdType::Uuid, "text"),
            (IdType::Uuid, "varchar"),
            (IdType::String, "text"),
            (IdType::String, "varchar"),
            (IdType::String, "int4"),
            (IdType::String, "uuid"),
        ];
        for (id_type, pg) in cases {
            assert!(
                check_id_type_compat(&id_type, pg).is_none(),
                "{id_type:?} vs {pg}"
            );
        }
    }

    #[test]
    fn incompatible_pairs() {
        let cases = [
            (IdType::Uint, "uuid"),
            (IdType::Uint, "text"),
            (IdType::Int, "uuid"),
            (IdType::Int, "text"),
        ];
        for (id_type, pg) in cases {
            assert!(
                check_id_type_compat(&id_type, pg).is_some(),
                "{id_type:?} vs {pg}"
            );
        }
    }

    #[test]
    fn id_type_display_values() {
        assert_eq!(id_type_display(&IdType::Uint), "uint");
        assert_eq!(id_type_display(&IdType::Int), "int");
        assert_eq!(id_type_display(&IdType::Uuid), "uuid");
        assert_eq!(id_type_display(&IdType::String), "string");
    }

    #[test]
    fn reserved_source_schema_rejected() {
        assert!(is_reserved_source_schema("puffgres"));
        assert!(!is_reserved_source_schema("public"));
    }
}
