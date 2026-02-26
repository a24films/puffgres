use pg::connect::connect;
use pg::slot::{ensure_slot, get_confirmed_flush_lsn};
use pg::test_utils::setup_postgres_logical;

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
async fn get_confirmed_flush_lsn_returns_none_for_missing_slot() {
    let ctx = setup_postgres_logical().await;
    let client = connect(&ctx.connection_string).await.unwrap();

    let lsn = get_confirmed_flush_lsn(&client, "nonexistent")
        .await
        .unwrap();
    assert!(lsn.is_none());
}
