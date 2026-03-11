use tokio_postgres::Client;

use crate::{PgError, Result};

#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub udt_name: String,
    pub ordinal_position: i32,
}

/// Fetch all columns from `pg_catalog`. This is much faster on large databases
/// than `information_schema.columns`, even though it requires more read permissions.
///
/// We resolve domain types down to their base types, which can be recursive —
/// e.g. `item_uuid` -> `store_uuid` -> `uuid`. We recurse down to the base type
/// so we get the underlying primitive PG type we can convert to.
pub async fn resolve_column_info(
    client: &Client,
    schema: &str,
    table: &str,
) -> Result<Vec<ColumnInfo>> {
    let query = r#"
        WITH RECURSIVE base_type(root_oid, typname, typtype, typbasetype) AS (
            SELECT t.oid, t.typname, t.typtype, t.typbasetype
            FROM pg_catalog.pg_type t
            WHERE t.typtype = 'd'
          UNION ALL
            SELECT b.root_oid, bt.typname, bt.typtype, bt.typbasetype
            FROM pg_catalog.pg_type bt
            JOIN base_type b ON b.typbasetype = bt.oid
            WHERE b.typtype = 'd'
        )
        SELECT a.attname::text,
               COALESCE(bt.typname::text, t.typname::text),
               a.attnum::int
        FROM pg_catalog.pg_attribute a
        JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
        LEFT JOIN base_type bt ON bt.root_oid = a.atttypid AND bt.typtype <> 'd'
        JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = $1
          AND c.relname = $2
          AND a.attnum > 0
          AND NOT a.attisdropped
          AND has_column_privilege(c.oid, a.attnum, 'SELECT')
        ORDER BY a.attnum
    "#;

    let rows = client.query(query, &[&schema, &table]).await.map_err(|e| {
        PgError::from_query_err(
            format!("Failed to fetch columns for {schema}.{table}: {e}"),
            &e,
        )
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
///
/// Domain types are recursively resolved to their base scalar type, so
/// even nested domains resolve to the underlying type name.
pub async fn validate_column(
    client: &Client,
    schema: &str,
    table: &str,
    column: &str,
) -> Result<String> {
    let query = r#"
        WITH RECURSIVE base_type(root_oid, typname, typtype, typbasetype) AS (
            SELECT t.oid, t.typname, t.typtype, t.typbasetype
            FROM pg_catalog.pg_type t
            WHERE t.typtype = 'd'
          UNION ALL
            SELECT b.root_oid, bt.typname, bt.typtype, bt.typbasetype
            FROM pg_catalog.pg_type bt
            JOIN base_type b ON b.typbasetype = bt.oid
            WHERE b.typtype = 'd'
        )
        SELECT COALESCE(bt.typname::text, t.typname::text)
        FROM pg_catalog.pg_attribute a
        JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
        LEFT JOIN base_type bt ON bt.root_oid = a.atttypid AND bt.typtype <> 'd'
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
            PgError::from_query_err(
                format!("Failed to check column {column} in {schema}.{table}: {e}"),
                &e,
            )
        })?;

    match row {
        Some(r) => Ok(r.get(0)),
        None => Err(PgError::QueryError(format!(
            "Column '{column}' does not exist in {schema}.{table}"
        ))),
    }
}
