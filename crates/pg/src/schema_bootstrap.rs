use tokio_postgres::Client;

use crate::{PgError, Result, connect::quote_identifier};

pub const PUFFGRES_SCHEMA: &str = "puffgres";

pub async fn ensure_schema(client: &Client, schema_name: &str) -> Result<()> {
    let query = format!(
        "CREATE SCHEMA IF NOT EXISTS {}",
        quote_identifier(schema_name)
    );
    client
        .batch_execute(&query)
        .await
        .map_err(|e| PgError::from_query_err(format!("Failed to ensure schema '{schema_name}'"), &e))
}

pub async fn schema_exists(client: &Client, schema_name: &str) -> Result<bool> {
    let row = client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = $1)",
            &[&schema_name],
        )
        .await
        .map_err(|e| {
            PgError::from_query_err(format!("Failed to check schema '{schema_name}'"), &e)
        })?;
    Ok(row.get(0))
}
