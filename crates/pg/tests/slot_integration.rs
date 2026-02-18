use std::time::Duration;

use pg::connect::connect;
use pg::slot::{
    ensure_slot, get_confirmed_flush_lsn, get_current_wal_lsn, terminate_active_slot_backend,
};
use pg::test_utils::{setup_postgres, setup_postgres_logical};
use replication::{ReplicationStream, ReplicationStreamConfig};

async fn query_slot(client: &tokio_postgres::Client, slot_name: &str) -> Option<(String, String)> {
    let rows = client
        .query(
            "SELECT slot_name, plugin FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot_name],
        )
        .await
        .unwrap();

    rows.first().map(|row| (row.get(0), row.get(1)))
}

#[tokio::test]
async fn ensure_slot_creates_with_pgoutput() {
    let ctx = setup_postgres_logical().await;
    let client = connect(&ctx.connection_string).await.unwrap();

    assert!(query_slot(&client, "test_slot").await.is_none());

    ensure_slot(&client, "test_slot").await.unwrap();

    let (name, plugin) = query_slot(&client, "test_slot")
        .await
        .expect("slot should exist");
    assert_eq!(name, "test_slot");
    assert_eq!(plugin, "pgoutput");
}

#[tokio::test]
async fn ensure_slot_idempotent() {
    let ctx = setup_postgres_logical().await;
    let client = connect(&ctx.connection_string).await.unwrap();

    ensure_slot(&client, "test_slot").await.unwrap();
    ensure_slot(&client, "test_slot").await.unwrap();

    // Still exactly one slot
    let count: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM pg_replication_slots WHERE slot_name = $1",
            &[&"test_slot"],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 1);
}

#[tokio::test]
async fn ensure_slot_rejects_wrong_plugin() {
    let ctx = setup_postgres_logical().await;
    let client = connect(&ctx.connection_string).await.unwrap();

    client
        .execute(
            "SELECT pg_create_logical_replication_slot('wrong_plugin_slot', 'test_decoding')",
            &[],
        )
        .await
        .unwrap();

    let result = ensure_slot(&client, "wrong_plugin_slot").await;
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("test_decoding"));
    assert!(err.contains("pgoutput"));
}

#[tokio::test]
async fn get_confirmed_flush_lsn_returns_value_for_existing_slot() {
    let ctx = setup_postgres_logical().await;
    let client = connect(&ctx.connection_string).await.unwrap();

    ensure_slot(&client, "test_slot").await.unwrap();

    let lsn = get_confirmed_flush_lsn(&client, "test_slot").await.unwrap();
    assert!(lsn.is_some());
    assert!(lsn.unwrap() > 0);
}

#[tokio::test]
async fn ensure_slot_noop_when_slot_precreated() {
    let ctx = setup_postgres_logical().await;
    let client = connect(&ctx.connection_string).await.unwrap();

    // Pre-create the slot outside of ensure_slot (simulates another process creating it)
    client
        .execute(
            "SELECT pg_create_logical_replication_slot('precreated_slot', 'pgoutput')",
            &[],
        )
        .await
        .unwrap();

    // ensure_slot should succeed without trying to create a duplicate
    ensure_slot(&client, "precreated_slot").await.unwrap();

    // Still exactly one slot
    let count: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM pg_replication_slots WHERE slot_name = $1",
            &[&"precreated_slot"],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 1);
}

#[tokio::test]
async fn terminate_active_slot_backend_kills_stale_connection() {
    let ctx = setup_postgres_logical().await;
    let client = connect(&ctx.connection_string).await.unwrap();

    ensure_slot(&client, "active_slot").await.unwrap();

    // Create a publication so the replication stream can connect
    client
        .execute("CREATE PUBLICATION active_slot_pub FOR ALL TABLES", &[])
        .await
        .unwrap();

    // Simulate a stale backend by starting a replication stream
    let _stream = ReplicationStream::connect(ReplicationStreamConfig {
        connection_string: ctx.connection_url.clone(),
        slot_name: "active_slot".to_string(),
        publication_name: "active_slot_pub".to_string(),
        start_lsn: None,
        status_interval: Duration::from_secs(10),
    })
    .await
    .unwrap();

    // Give the replication stream a moment to register
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Slot should now have an active_pid
    let row = client
        .query_one(
            "SELECT active, active_pid FROM pg_replication_slots WHERE slot_name = 'active_slot'",
            &[],
        )
        .await
        .unwrap();
    let active: Option<bool> = row.get(0);
    let active_pid: Option<i32> = row.get(1);
    assert!(
        active == Some(true) && active_pid.is_some(),
        "slot should have an active backend (active={:?}, pid={:?})",
        active,
        active_pid,
    );

    // Terminate the stale backend
    terminate_active_slot_backend(&client, "active_slot")
        .await
        .unwrap();

    // Give pg a moment to clean up
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // active_pid should now be gone
    let row = client
        .query_one(
            "SELECT active_pid FROM pg_replication_slots WHERE slot_name = 'active_slot'",
            &[],
        )
        .await
        .unwrap();
    let active_pid: Option<i32> = row.get(0);
    assert!(active_pid.is_none(), "stale backend should be terminated");
}

#[tokio::test]
async fn terminate_active_slot_backend_noop_when_no_active_backend() {
    let ctx = setup_postgres_logical().await;
    let client = connect(&ctx.connection_string).await.unwrap();

    ensure_slot(&client, "idle_slot").await.unwrap();

    // Should succeed without error when no backend is active
    terminate_active_slot_backend(&client, "idle_slot")
        .await
        .unwrap();
}

#[tokio::test]
async fn get_current_wal_lsn_returns_nonzero() {
    let ctx = setup_postgres().await;
    let client = connect(&ctx.connection_string).await.unwrap();

    let lsn = get_current_wal_lsn(&client).await.unwrap();
    assert!(lsn > 0);
}

#[tokio::test]
async fn get_confirmed_flush_lsn_returns_none_for_missing_slot() {
    let ctx = setup_postgres_logical().await;
    let client = connect(&ctx.connection_string).await.unwrap();

    let lsn = get_confirmed_flush_lsn(&client, "nonexistent")
        .await
        .unwrap();
    assert!(lsn.is_none());
}
