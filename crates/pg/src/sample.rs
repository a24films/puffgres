use tokio_postgres::Client;

use crate::connect::quote_identifier;
use crate::{PgError, Result};

/// Fetch a single row from the table with all values cast to text.
/// Returns `(column_names, values)` or `None` if the table is empty.
pub async fn fetch_sample_row(
    client: &Client,
    schema: &str,
    table: &str,
) -> Result<Option<(Vec<String>, Vec<Option<String>>)>> {
    // Get column names in ordinal order via pg_catalog (faster than information_schema)
    let col_query = r#"
        SELECT a.attname::text
        FROM pg_catalog.pg_attribute a
        JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = $1
          AND c.relname = $2
          AND a.attnum > 0
          AND NOT a.attisdropped
          AND has_column_privilege(c.oid, a.attnum, 'SELECT')
        ORDER BY a.attnum
    "#;
    let col_rows = client
        .query(col_query, &[&schema, &table])
        .await
        .map_err(|e| {
            PgError::from_query_err(
                format!("Failed to get columns for {schema}.{table}: {e}"),
                &e,
            )
        })?;

    let column_names: Vec<String> = col_rows.iter().map(|r| r.get(0)).collect();
    if column_names.is_empty() {
        return Err(PgError::QueryError(format!(
            "No columns found for {schema}.{table}"
        )));
    }

    // Build SELECT with all columns cast to text
    let casts: Vec<String> = column_names
        .iter()
        .map(|c| format!("{}::text", quote_identifier(c)))
        .collect();
    let select = format!(
        "SELECT {} FROM {}.{} LIMIT 1",
        casts.join(", "),
        quote_identifier(schema),
        quote_identifier(table)
    );

    let rows = client.query(&select, &[]).await.map_err(|e| {
        PgError::from_query_err(
            format!("Failed to fetch sample row from {schema}.{table}: {e}"),
            &e,
        )
    })?;

    if rows.is_empty() {
        return Ok(None);
    }

    let row = &rows[0];
    let values: Vec<Option<String>> = (0..column_names.len())
        .map(|i| row.get::<_, Option<String>>(i))
        .collect();

    Ok(Some((column_names, values)))
}
