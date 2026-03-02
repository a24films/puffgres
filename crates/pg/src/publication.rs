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
        .map(|t| match t.split_once('.') {
            Some((schema, name)) => {
                format!("{}.{}", quote_identifier(schema), quote_identifier(name))
            }
            None => format!("{}.{}", quote_identifier("public"), quote_identifier(t)),
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
        let current = get_publication_tables(client, publication_name).await?;

        let normalize = |t: &str| -> String {
            match t.split_once('.') {
                Some((schema, table)) => format!("{}.{}", schema, table),
                None => format!("public.{}", t),
            }
        };

        let desired: Vec<String> = tables.iter().map(|t| normalize(t)).collect();

        let missing: Vec<String> = tables
            .iter()
            .filter(|t| !current.contains(&normalize(t)))
            .cloned()
            .collect();

        let stale: Vec<String> = current
            .iter()
            .filter(|t| !desired.contains(t))
            .cloned()
            .collect();

        add_tables_to_publication(client, publication_name, &missing).await?;
        remove_tables_from_publication(client, publication_name, &stale).await?;

        return Ok(());
    }

    create_publication(client, publication_name, tables).await
}

pub async fn add_tables_to_publication(
    client: &Client,
    publication_name: &str,
    tables: &[String],
) -> Result<()> {
    if tables.is_empty() {
        return Ok(());
    }

    let table_list: String = tables
        .iter()
        .map(|t| match t.split_once('.') {
            Some((schema, name)) => {
                format!("{}.{}", quote_identifier(schema), quote_identifier(name))
            }
            None => quote_identifier(t),
        })
        .collect::<Vec<_>>()
        .join(", ");

    let query = format!(
        "ALTER PUBLICATION {} ADD TABLE {}",
        quote_identifier(publication_name),
        table_list
    );

    client.execute(&query, &[]).await.map_err(|e| {
        PgError::ReplicationError(format!(
            "Failed to add tables to publication '{}': {}",
            publication_name, e
        ))
    })?;

    Ok(())
}

pub async fn remove_tables_from_publication(
    client: &Client,
    publication_name: &str,
    tables: &[String],
) -> Result<()> {
    if tables.is_empty() {
        return Ok(());
    }

    let table_list: String = tables
        .iter()
        .map(|t| match t.split_once('.') {
            Some((schema, name)) => {
                format!("{}.{}", quote_identifier(schema), quote_identifier(name))
            }
            None => quote_identifier(t),
        })
        .collect::<Vec<_>>()
        .join(", ");

    let query = format!(
        "ALTER PUBLICATION {} DROP TABLE {}",
        quote_identifier(publication_name),
        table_list
    );

    client.execute(&query, &[]).await.map_err(|e| {
        PgError::ReplicationError(format!(
            "Failed to remove tables from publication '{}': {}",
            publication_name, e
        ))
    })?;

    Ok(())
}

/// Set REPLICA IDENTITY FULL on each table so DELETE events include all
/// column values in the old tuple (not just the primary-key columns).
/// Without this, non-PK id columns appear as `Unchanged` in deletes and
/// `extract_id` fails.
pub async fn ensure_replica_identity_full(client: &Client, tables: &[String]) -> Result<()> {
    for table in tables {
        let qualified = match table.split_once('.') {
            Some((schema, name)) => {
                format!("{}.{}", quote_identifier(schema), quote_identifier(name))
            }
            None => format!("{}.{}", quote_identifier("public"), quote_identifier(table)),
        };

        let query = format!("ALTER TABLE {qualified} REPLICA IDENTITY FULL");
        client.execute(&query, &[]).await.map_err(|e| {
            PgError::ReplicationError(format!(
                "Failed to set REPLICA IDENTITY FULL on '{}': {}",
                table, e
            ))
        })?;
    }

    Ok(())
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
