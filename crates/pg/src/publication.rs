use tokio_postgres::Client;

use crate::connect::quote_identifier;
use crate::{PgError, Result};

async fn publication_exists(client: &Client, publication_name: &str) -> Result<bool> {
    let row = client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)",
            &[&publication_name],
        )
        .await
        .map_err(|e| {
            PgError::ReplicationError(format!(
                "Failed to check publication '{}': {}",
                publication_name, e
            ))
        })?;

    Ok(row.get(0))
}

async fn create_publication(
    client: &Client,
    publication_name: &str,
    tables: &[String],
) -> Result<()> {
    let table_list: String = tables
        .iter()
        .filter_map(|t| {
            let (schema, name) = t.split_once('.')?;
            Some(format!(
                "{}.{}",
                quote_identifier(schema),
                quote_identifier(name)
            ))
        })
        .collect::<Vec<_>>()
        .join(", ");

    let query = format!(
        "CREATE PUBLICATION {} FOR TABLE {}",
        quote_identifier(publication_name),
        table_list
    );

    client.execute(&query, &[]).await.map_err(|e| {
        PgError::ReplicationError(format!(
            "Failed to create publication '{}': {}",
            publication_name, e
        ))
    })?;

    Ok(())
}

pub async fn drop_publication(client: &Client, publication_name: &str) -> Result<()> {
    let query = format!("DROP PUBLICATION {}", quote_identifier(publication_name));

    client.execute(&query, &[]).await.map_err(|e| {
        PgError::ReplicationError(format!(
            "Failed to drop publication '{}': {}",
            publication_name, e
        ))
    })?;

    Ok(())
}

pub async fn ensure_publication(
    client: &Client,
    publication_name: &str,
    tables: &[String],
) -> Result<()> {
    if publication_exists(client, publication_name).await? {
        return Ok(());
    }

    create_publication(client, publication_name, tables).await
}

pub async fn get_publication_tables(
    client: &Client,
    publication_name: &str,
) -> Result<Vec<String>> {
    let rows = client
        .query(
            "SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = $1",
            &[&publication_name],
        )
        .await
        .map_err(|e| {
            PgError::ReplicationError(format!(
                "Failed to get tables for publication '{}': {}",
                publication_name, e
            ))
        })?;

    Ok(rows
        .iter()
        .map(|row| {
            let schema: String = row.get(0);
            let table: String = row.get(1);
            format!("{}.{}", schema, table)
        })
        .collect())
}
