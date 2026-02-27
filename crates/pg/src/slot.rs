use tokio_postgres::Client;
use tokio_postgres::error::SqlState;

use crate::{PgError, Result};

const PGOUTPUT: &str = "pgoutput";

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
    match client
        .query_one(
            &format!(
                "SELECT pg_create_logical_replication_slot($1, '{}')",
                PGOUTPUT
            ),
            &[&slot_name],
        )
        .await
    {
        Ok(_) => Ok(()),
        Err(e) if e.code() == Some(&SqlState::DUPLICATE_OBJECT) => {
            match get_slot_plugin(client, slot_name).await?.as_deref() {
                Some(PGOUTPUT) => Ok(()),
                Some(other) => Err(PgError::ReplicationError(format!(
                    "Slot '{}' exists but uses plugin '{}', expected '{}'",
                    slot_name, other, PGOUTPUT
                ))),
                None => Err(PgError::ReplicationError(format!(
                    "Slot '{}' exists but has no plugin",
                    slot_name
                ))),
            }
        }
        Err(e) => Err(PgError::ReplicationError(format!(
            "Failed to create slot '{}': {}",
            slot_name, e
        ))),
    }
}

pub async fn ensure_slot(client: &Client, slot_name: &str) -> Result<()> {
    if slot_exists(client, slot_name).await? {
        let plugin = get_slot_plugin(client, slot_name).await?;
        match plugin.as_deref() {
            Some(PGOUTPUT) => return Ok(()),
            Some(other) => {
                return Err(PgError::ReplicationError(format!(
                    "Slot '{}' exists but uses plugin '{}', expected '{}'",
                    slot_name, other, PGOUTPUT
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

/// Kill any stale backend still holding the replication slot from a previous run.
pub async fn terminate_active_slot_backend(client: &Client, slot_name: &str) -> Result<()> {
    let row = client
        .query_one(
            "SELECT active_pid FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot_name],
        )
        .await
        .map_err(|e| {
            PgError::ReplicationError(format!(
                "Failed to check active PID for slot '{}': {}",
                slot_name, e
            ))
        })?;

    let active_pid: Option<i32> = row.get(0);
    if let Some(pid) = active_pid {
        client
            .execute("SELECT pg_terminate_backend($1)", &[&pid])
            .await
            .map_err(|e| {
                PgError::ReplicationError(format!(
                    "Failed to terminate backend PID {} for slot '{}': {}",
                    pid, slot_name, e
                ))
            })?;
        println!(
            "Terminated stale backend PID {} on slot '{}'",
            pid, slot_name
        );
    }

    Ok(())
}

pub async fn get_active_pid(client: &Client, slot_name: &str) -> Result<Option<i32>> {
    let row = client
        .query_one(
            "SELECT active_pid FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot_name],
        )
        .await
        .map_err(|e| {
            PgError::ReplicationError(format!(
                "Failed to check active PID for slot '{}': {}",
                slot_name, e
            ))
        })?;

    Ok(row.get(0))
}

pub async fn get_current_wal_lsn(client: &Client) -> Result<u64> {
    let row = client
        .query_one("SELECT pg_current_wal_lsn()", &[])
        .await
        .map_err(|e| PgError::ReplicationError(format!("Failed to get current WAL LSN: {e}")))?;
    let lsn: tokio_postgres::types::PgLsn = row.get(0);
    Ok(u64::from(lsn))
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
