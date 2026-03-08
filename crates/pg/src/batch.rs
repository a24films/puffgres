// SQL Identifier Safety
//
// Postgres does not support parameterized identifiers (table names, column
// names, schema names). These MUST be interpolated into query strings.
// All identifier interpolation uses `quote_identifier()` which wraps the
// value in double-quotes and escapes embedded double-quotes — the same
// algorithm as PG's `quote_ident()` function.
//
// All *values* (cursor position, LIMIT, schema/table names used in WHERE
// clauses against catalog tables) go through `$1`, `$2`, etc. parameters.

use tokio_postgres::Client;

use crate::connect::quote_identifier;
use crate::{PgError, Result};

const CURSOR_ALIAS: &str = "_puffgres_cursor_id";

#[derive(Clone)]
pub struct BatchQueryConfig {
    pub schema: String,
    pub table: String,
    pub id_column: String,
    pub columns: Option<Vec<String>>,
    pub batch_size: u32,
}

#[derive(Debug)]
pub struct BatchResult {
    pub rows: Vec<tokio_postgres::Row>,
    pub last_id: Option<String>,
    pub has_more: bool,
}

fn build_select_clause(config: &BatchQueryConfig) -> Result<String> {
    let id_col = quote_identifier(&config.id_column);
    let cursor_expr = format!("{}::text AS {}", id_col, quote_identifier(CURSOR_ALIAS));

    match &config.columns {
        Some(cols) if cols.is_empty() => Err(PgError::QueryError(
            "columns list cannot be empty; omit the field to select all columns".to_string(),
        )),
        Some(cols) => {
            let mut parts: Vec<String> = cols
                .iter()
                .map(|c| {
                    let q = quote_identifier(c);
                    format!("{q}::text AS {q}")
                })
                .collect();
            if !cols.iter().any(|c| c == &config.id_column) {
                parts.push(format!("{id_col}::text AS {id_col}"));
            }
            parts.push(cursor_expr);
            Ok(parts.join(", "))
        }
        None => Ok(format!("*, {}", cursor_expr)),
    }
}

fn build_qualified_table(config: &BatchQueryConfig) -> String {
    format!(
        "{}.{}",
        quote_identifier(&config.schema),
        quote_identifier(&config.table),
    )
}

pub async fn validate_id_column_uniqueness(
    client: &Client,
    config: &BatchQueryConfig,
) -> Result<()> {
    let query = r#"
        SELECT EXISTS (
            SELECT 1
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid
                                AND a.attnum = ANY(i.indkey)
            JOIN pg_class c ON c.oid = i.indrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = $1
              AND c.relname = $2
              AND a.attname = $3
              AND i.indisunique
              AND i.indisvalid
              AND i.indpred IS NULL
              AND array_length(i.indkey, 1) = 1
        )
    "#;

    let row = client
        .query_one(query, &[&config.schema, &config.table, &config.id_column])
        .await
        .map_err(|e| {
            PgError::QueryError(format!(
                "failed to check uniqueness of column '{}' on {}.{}: {}",
                config.id_column, config.schema, config.table, e
            ))
        })?;

    let has_unique: bool = row.get(0);
    if !has_unique {
        return Err(PgError::QueryError(format!(
            "id column '{}' on {}.{} must have a non-partial unique index or primary key constraint; \
             cursor-based pagination requires globally unique id values",
            config.id_column, config.schema, config.table
        )));
    }

    Ok(())
}

/// Query the actual PG column type and return the SQL cast expression needed
/// for cursor comparisons. Returns e.g. `"::int8"`, `"::uuid"`, or `""` (no cast).
///
/// Uses `format_type()` to get the human-readable type name, which handles
/// domain types transparently — Postgres resolves the base type for us.
pub async fn resolve_cursor_cast(client: &Client, config: &BatchQueryConfig) -> Result<String> {
    let query = r#"
        SELECT format_type(atttypid, atttypmod)
        FROM pg_catalog.pg_attribute a
        JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = $1
          AND c.relname = $2
          AND a.attname = $3
          AND a.attnum > 0
          AND NOT a.attisdropped
    "#;

    let row = client
        .query_one(query, &[&config.schema, &config.table, &config.id_column])
        .await
        .map_err(|e| {
            PgError::QueryError(format!(
                "failed to resolve type of column '{}' on {}.{}: {}",
                config.id_column, config.schema, config.table, e
            ))
        })?;

    let type_name: String = row.get(0);

    // Map the PG type name to the cast suffix needed for text→type conversion.
    // The cursor value is always stored as text; we cast it back for comparison.
    let cast = match type_name.as_str() {
        "smallint" | "integer" | "bigint" => "::int8",
        "uuid" => "::uuid",
        "text" | "character varying" => "",
        t if t.starts_with("character varying") => "", // varchar(n)
        t if t.starts_with("character") => "",         // char(n)
        _ => {
            return Err(PgError::QueryError(format!(
                "unsupported id column type '{}' for cursor pagination on {}.{}.{}; \
                 supported types: smallint, integer, bigint, uuid, text, varchar, char",
                type_name, config.schema, config.table, config.id_column,
            )));
        }
    };

    Ok(cast.to_string())
}

pub async fn count_rows(client: &Client, config: &BatchQueryConfig) -> Result<u64> {
    let qualified_table = build_qualified_table(config);
    let id_col = quote_identifier(&config.id_column);
    let query = format!(
        "SELECT count(*) FROM {} WHERE {} IS NOT NULL",
        qualified_table, id_col,
    );

    let row = client.query_one(&query, &[]).await.map_err(|e| {
        PgError::QueryError(format!(
            "Failed to count rows in {}.{}: {}",
            config.schema, config.table, e
        ))
    })?;

    let count: i64 = row.get(0);
    Ok(u64::try_from(count).unwrap_or(0))
}

pub async fn resolve_column_names(
    client: &Client,
    schema: &str,
    table: &str,
) -> Result<Vec<String>> {
    let query = r#"
        SELECT a.attname::text
        FROM pg_catalog.pg_attribute a
        JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = $1
          AND c.relname = $2
          AND a.attnum > 0
          AND NOT a.attisdropped
        ORDER BY a.attnum
    "#;
    let rows = client.query(query, &[&schema, &table]).await.map_err(|e| {
        PgError::QueryError(format!(
            "Failed to resolve columns for {}.{}: {}",
            schema, table, e
        ))
    })?;

    if rows.is_empty() {
        return Err(PgError::QueryError(format!(
            "Table {}.{} not found or has no columns",
            schema, table,
        )));
    }

    Ok(rows.iter().map(|r| r.get(0)).collect())
}

pub async fn fetch_batch(
    client: &Client,
    config: &BatchQueryConfig,
    cursor_id: Option<&str>,
    cursor_cast: &str,
) -> Result<BatchResult> {
    if config.batch_size == 0 {
        return Err(PgError::QueryError(
            "batch_size must be greater than 0".to_string(),
        ));
    }

    let qualified_table = build_qualified_table(config);
    let id_col = quote_identifier(&config.id_column);
    let columns_clause = build_select_clause(config)?;

    let limit = config.batch_size + 1;
    let limit_param = i64::from(limit);

    let rows = if let Some(cursor) = cursor_id {
        let query = format!(
            "SELECT {} FROM {} WHERE {} IS NOT NULL AND {} > $1{} ORDER BY {} ASC LIMIT $2",
            columns_clause, qualified_table, id_col, id_col, cursor_cast, id_col,
        );
        client.query(&query, &[&cursor, &limit_param]).await
    } else {
        let query = format!(
            "SELECT {} FROM {} WHERE {} IS NOT NULL ORDER BY {} ASC LIMIT $1",
            columns_clause, qualified_table, id_col, id_col,
        );
        client.query(&query, &[&limit_param]).await
    }
    .map_err(|e| {
        PgError::QueryError(format!(
            "Failed to fetch batch from {}.{}: {}",
            config.schema, config.table, e
        ))
    })?;

    let has_more = rows.len() > usize::try_from(config.batch_size).unwrap_or(usize::MAX);

    let rows: Vec<tokio_postgres::Row> = if has_more {
        rows.into_iter()
            .take(usize::try_from(config.batch_size).unwrap_or(usize::MAX))
            .collect()
    } else {
        rows
    };

    let last_id = rows.last().map(|row| row.get::<&str, String>(CURSOR_ALIAS));

    Ok(BatchResult {
        rows,
        last_id,
        has_more,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> BatchQueryConfig {
        BatchQueryConfig {
            schema: "public".to_string(),
            table: "users".to_string(),
            id_column: "id".to_string(),
            columns: None,
            batch_size: 100,
        }
    }

    #[test]
    fn build_qualified_table_simple() {
        let config = test_config();
        assert_eq!(build_qualified_table(&config), "\"public\".\"users\"");
    }

    #[test]
    fn build_qualified_table_with_special_chars() {
        let config = BatchQueryConfig {
            schema: "my schema".to_string(),
            table: "my\"table".to_string(),
            ..test_config()
        };
        assert_eq!(
            build_qualified_table(&config),
            "\"my schema\".\"my\"\"table\""
        );
    }

    #[test]
    fn build_select_clause_star() {
        let config = test_config();
        assert_eq!(
            build_select_clause(&config).unwrap(),
            "*, \"id\"::text AS \"_puffgres_cursor_id\""
        );
    }

    #[test]
    fn build_select_clause_specific_columns() {
        let config = BatchQueryConfig {
            columns: Some(vec![
                "id".to_string(),
                "name".to_string(),
                "email".to_string(),
            ]),
            ..test_config()
        };
        assert_eq!(
            build_select_clause(&config).unwrap(),
            "\"id\"::text AS \"id\", \"name\"::text AS \"name\", \"email\"::text AS \"email\", \"id\"::text AS \"_puffgres_cursor_id\""
        );
    }

    #[test]
    fn build_select_clause_empty_columns_error() {
        let config = BatchQueryConfig {
            columns: Some(vec![]),
            ..test_config()
        };
        let err = build_select_clause(&config).unwrap_err();
        assert!(err.to_string().contains("columns list cannot be empty"));
    }

    #[test]
    fn build_select_clause_adds_missing_id_column() {
        let config = BatchQueryConfig {
            columns: Some(vec!["name".to_string(), "email".to_string()]),
            ..test_config()
        };
        let clause = build_select_clause(&config).unwrap();
        assert_eq!(
            clause,
            "\"name\"::text AS \"name\", \"email\"::text AS \"email\", \"id\"::text AS \"id\", \"id\"::text AS \"_puffgres_cursor_id\""
        );
    }
}
