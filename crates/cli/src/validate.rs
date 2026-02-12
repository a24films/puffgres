use std::path::PathBuf;

use config::{Config, IdType};

use crate::env::EnvConfig;

/// Validate configs against Postgres schema: check tables, columns, and id types.
/// Returns the indices of configs that passed, or a list of error messages.
pub async fn validate_schema(
    env_config: &EnvConfig,
    configs: &[(PathBuf, Config)],
) -> Result<Vec<usize>, Vec<String>> {
    let mut passed: Vec<usize> = Vec::new();
    let mut errors: Vec<String> = Vec::new();

    let pg_client = pg::connect::connect(&env_config.database_url)
        .await
        .map_err(|e| vec![format!("failed to connect to postgres: {e}")])?;

    for (i, (path, config)) in configs.iter().enumerate() {
        let display = path.display();

        // 1. Validate table exists
        let table_refs = vec![(config.source.schema.as_str(), config.source.table.as_str())];
        if let Err(e) = pg::connect::validate_tables(&pg_client, &table_refs).await {
            errors.push(format!("{display}: {e}"));
            continue;
        }

        // 2. Validate id column exists and check type compatibility
        let pg_type = match pg::column::validate_column(
            &pg_client,
            &config.source.schema,
            &config.source.table,
            &config.id.column,
        )
        .await
        {
            Ok(t) => t,
            Err(e) => {
                errors.push(format!("{display}: {e}"));
                continue;
            }
        };

        if let Some(warning) = check_id_type_compat(&config.id.id_type, &pg_type) {
            errors.push(format!("{display}: {warning}"));
            continue;
        }

        // 3. Validate columns if specified
        if let Some(columns) = &config.columns {
            let mut col_ok = true;
            for col in columns {
                if let Err(e) = pg::column::validate_column(
                    &pg_client,
                    &config.source.schema,
                    &config.source.table,
                    col,
                )
                .await
                {
                    errors.push(format!("{display}: {e}"));
                    col_ok = false;
                    break;
                }
            }
            if !col_ok {
                continue;
            }
        }

        passed.push(i);
    }

    if !errors.is_empty() {
        return Err(errors);
    }

    Ok(passed)
}

/// Check that the config id type is compatible with the Postgres column type.
/// Returns an error message if incompatible, None if OK.
pub fn check_id_type_compat(id_type: &IdType, pg_udt: &str) -> Option<String> {
    let compatible = match id_type {
        IdType::Uint | IdType::Int => matches!(
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
        ),
        IdType::Uuid => pg_udt == "uuid",
        IdType::String => matches!(
            pg_udt,
            "text" | "varchar" | "char" | "bpchar" | "name" | "citext"
        ),
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
            (IdType::String, "text"),
            (IdType::String, "varchar"),
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
            (IdType::Uuid, "text"),
            (IdType::Uint, "text"),
        ];
        for (id_type, pg) in cases {
            assert!(
                check_id_type_compat(&id_type, pg).is_some(),
                "{id_type:?} vs {pg}"
            );
        }
    }
}
