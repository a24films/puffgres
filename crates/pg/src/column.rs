use tokio_postgres::Client;

use crate::{PgError, Result};

#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub udt_name: String,
    pub ordinal_position: i32,
}

/// Fetch all columns for a table from `pg_catalog`, ordered by `attnum`.
///
/// Uses `pg_attribute` + `pg_type` instead of `information_schema.columns`:
/// - 10-100x faster on large databases (no view overhead)
/// - More accurate type names (information_schema normalizes them)
/// - Standard approach in Postgres tooling
pub async fn resolve_column_info(
    client: &Client,
    schema: &str,
    table: &str,
) -> Result<Vec<ColumnInfo>> {
    let query = r#"
        SELECT a.attname::text,
               t.typname::text,
               a.attnum::int
        FROM pg_catalog.pg_attribute a
        JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
        JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = $1
          AND c.relname = $2
          AND a.attnum > 0
          AND NOT a.attisdropped
        ORDER BY a.attnum
    "#;

    let rows = client.query(query, &[&schema, &table]).await.map_err(|e| {
        PgError::QueryError(format!("Failed to fetch columns for {schema}.{table}: {e}"))
    })?;

    if rows.is_empty() {
        return Err(PgError::QueryError(format!(
            "No columns found for {schema}.{table} — table may not exist"
        )));
    }

    Ok(rows
        .into_iter()
        .map(|r| ColumnInfo {
            name: r.get(0),
            udt_name: r.get(1),
            ordinal_position: r.get(2),
        })
        .collect())
}

/// Check that a column exists in the given table and return its `udt_name`
/// (the underlying Postgres type, e.g. "int4", "uuid", "text").
pub async fn validate_column(
    client: &Client,
    schema: &str,
    table: &str,
    column: &str,
) -> Result<String> {
    let query = r#"
        SELECT t.typname::text
        FROM pg_catalog.pg_attribute a
        JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
        JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = $1
          AND c.relname = $2
          AND a.attname = $3
          AND a.attnum > 0
          AND NOT a.attisdropped
    "#;

    let row = client
        .query_opt(query, &[&schema, &table, &column])
        .await
        .map_err(|e| {
            PgError::QueryError(format!(
                "Failed to check column {column} in {schema}.{table}: {e}"
            ))
        })?;

    match row {
        Some(r) => Ok(r.get(0)),
        None => Err(PgError::QueryError(format!(
            "Column '{column}' does not exist in {schema}.{table}"
        ))),
    }
}
