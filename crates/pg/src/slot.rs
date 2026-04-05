use tokio_postgres::Client;
use tokio_postgres::error::SqlState;

use crate::{PgError, Result};

const PGOUTPUT: &str = "pgoutput";

async fn get_slot_plugin(client: &Client, slot_name: &str) -> Result<Option<String>> {
    let rows = client
        .query(
            "SELECT plugin FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot_name],
        )
        .await
        .map_err(|e| {
            PgError::from_replication_err(
                format!("Failed to get plugin for slot '{}': {}", slot_name, e),
                &e,
            )
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
        Err(e) => Err(PgError::from_replication_err(
            format!("Failed to create slot '{}': {}", slot_name, e),
            &e,
        )),
    }
}

/// Ensure a logical replication slot exists with the `pgoutput` plugin.
///
/// Uses create-and-catch-duplicate instead of check-then-create to avoid a
/// TOCTOU race where another process creates the slot between our check and
/// our create call.
pub async fn ensure_slot(client: &Client, slot_name: &str) -> Result<()> {
    create_slot(client, slot_name).await
}

/// Kill any stale backend still holding the replication slot from a previous run.
///
/// Note on PID recycling: between reading `active_pid` and calling
/// `pg_terminate_backend`, the PID could theoretically be recycled to a
/// different backend. In practice this is extremely unlikely on short
/// timescales, and the worst case is terminating an unrelated connection
/// (which that client would reconnect from). The PID is parameterized ($1).
pub async fn terminate_active_slot_backend(client: &Client, slot_name: &str) -> Result<()> {
    let row = client
        .query_one(
            "SELECT active_pid FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot_name],
        )
        .await
        .map_err(|e| {
            PgError::from_replication_err(
                format!("Failed to check active PID for slot '{}': {}", slot_name, e),
                &e,
            )
        })?;

    let active_pid: Option<i32> = row.get(0);
    if let Some(pid) = active_pid {
        client
            .execute("SELECT pg_terminate_backend($1)", &[&pid])
            .await
            .map_err(|e| {
                PgError::from_replication_err(
                    format!(
                        "Failed to terminate backend PID {} for slot '{}': {}",
                        pid, slot_name, e
                    ),
                    &e,
                )
            })?;
        tracing::info!(pid = pid, slot = slot_name, "terminated stale backend");
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
            PgError::from_replication_err(
                format!("Failed to check active PID for slot '{}': {}", slot_name, e),
                &e,
            )
        })?;

    Ok(row.get(0))
}

/// Drop a logical replication slot. Returns Ok if the slot was dropped or
/// didn't exist.
pub async fn drop_slot(client: &Client, slot_name: &str) -> Result<()> {
    match client
        .execute("SELECT pg_drop_replication_slot($1)", &[&slot_name])
        .await
    {
        Ok(_) => Ok(()),
        Err(e) if e.code() == Some(&SqlState::UNDEFINED_OBJECT) => Ok(()),
        Err(e) => Err(PgError::from_replication_err(
            format!("Failed to drop slot '{}': {}", slot_name, e),
            &e,
        )),
    }
}

pub async fn get_current_wal_lsn(client: &Client) -> Result<u64> {
    let row = client
        .query_one("SELECT pg_current_wal_lsn()", &[])
        .await
        .map_err(|e| {
            PgError::from_replication_err(format!("Failed to get current WAL LSN: {e}"), &e)
        })?;
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
            PgError::from_replication_err(
                format!(
                    "Failed to get confirmed_flush_lsn for slot '{}': {}",
                    slot_name, e
                ),
                &e,
            )
        })?;

    match rows.first() {
        Some(row) => {
            let lsn: Option<tokio_postgres::types::PgLsn> = row.get(0);
            Ok(lsn.map(u64::from))
        }
        None => Ok(None),
    }
}
