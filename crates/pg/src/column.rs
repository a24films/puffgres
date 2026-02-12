use tokio_postgres::Client;

use crate::{PgError, Result};

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
