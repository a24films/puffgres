use tokio_postgres::Client;

use crate::{PgError, Result};

#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub udt_name: String,
    pub ordinal_position: i32,
}

/// Fetch all columns for a table from `information_schema.columns`,
/// ordered by `ordinal_position`.
pub async fn resolve_column_info(
    client: &Client,
    schema: &str,
    table: &str,
) -> Result<Vec<ColumnInfo>> {
    let query = r#"
        SELECT column_name, udt_name, ordinal_position
        FROM information_schema.columns
        WHERE table_schema = $1
        AND table_name = $2
        ORDER BY ordinal_position
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
        SELECT udt_name
        FROM information_schema.columns
        WHERE table_schema = $1
        AND table_name = $2
        AND column_name = $3
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
