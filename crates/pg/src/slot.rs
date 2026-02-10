use tokio_postgres::Client;

use crate::{PgError, Result};

async fn slot_exists(client: &Client, slot_name: &str) -> Result<bool> {
    let row = client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
            &[&slot_name],
        )
        .await
        .map_err(|e| {
            PgError::ReplicationError(format!("Failed to check slot '{}': {}", slot_name, e))
        })?;

    Ok(row.get(0))
}

async fn get_slot_plugin(client: &Client, slot_name: &str) -> Result<Option<String>> {
    let rows = client
        .query(
            "SELECT plugin FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot_name],
        )
        .await
        .map_err(|e| {
            PgError::ReplicationError(format!(
                "Failed to get plugin for slot '{}': {}",
                slot_name, e
            ))
        })?;

    Ok(rows.first().map(|row| row.get(0)))
}

async fn create_slot(client: &Client, slot_name: &str) -> Result<()> {
    let query = format!(
        "SELECT pg_create_logical_replication_slot('{}', 'pgoutput')",
        slot_name.replace('\'', "''")
    );

    client.execute(&query, &[]).await.map_err(|e| {
        PgError::ReplicationError(format!("Failed to create slot '{}': {}", slot_name, e))
    })?;

    Ok(())
}

pub async fn ensure_slot(client: &Client, slot_name: &str) -> Result<()> {
    if slot_exists(client, slot_name).await? {
        let plugin = get_slot_plugin(client, slot_name).await?;
        match plugin.as_deref() {
            Some("pgoutput") => return Ok(()),
            Some(other) => {
                return Err(PgError::ReplicationError(format!(
                    "Slot '{}' exists but uses plugin '{}', expected 'pgoutput'",
                    slot_name, other
                )));
            }
            None => {
                return Err(PgError::ReplicationError(format!(
                    "Slot '{}' exists but has no plugin",
                    slot_name
                )));
            }
        }
    }

    create_slot(client, slot_name).await
}

pub async fn get_confirmed_flush_lsn(client: &Client, slot_name: &str) -> Result<Option<u64>> {
    let rows = client
        .query(
            "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot_name],
        )
        .await
        .map_err(|e| {
            PgError::ReplicationError(format!(
                "Failed to get confirmed_flush_lsn for slot '{}': {}",
                slot_name, e
            ))
        })?;

    match rows.first() {
        Some(row) => {
            let lsn: Option<tokio_postgres::types::PgLsn> = row.get(0);
            Ok(lsn.map(|l| u64::from(l)))
        }
        None => Ok(None),
    }
}
