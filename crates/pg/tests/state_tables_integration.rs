use pg::connect::connect;
use pg::schema_bootstrap::{
    PUFFGRES_SCHEMA, ensure_schema, ensure_state_tables, state_tables_exist,
};
use pg::test_utils::setup_postgres;

const EXPECTED_TABLES: [&str; 5] = [
    "configs",
    "streaming_checkpoints",
    "backfill_progress",
    "dlq",
    "runtime_state",
];

#[tokio::test]
async fn ensure_state_tables_creates_tables_in_puffgres_schema() {
    let ctx = setup_postgres().await;
    let client = connect(&ctx.connection_string).await.unwrap();

    ensure_schema(&client, PUFFGRES_SCHEMA).await.unwrap();

    assert!(!state_tables_exist(&client, PUFFGRES_SCHEMA).await.unwrap());

    ensure_state_tables(&client, PUFFGRES_SCHEMA).await.unwrap();

    assert!(state_tables_exist(&client, PUFFGRES_SCHEMA).await.unwrap());
}

#[tokio::test]
async fn ensure_state_tables_does_not_create_public_tables() {
    let ctx = setup_postgres().await;
    let client = connect(&ctx.connection_string).await.unwrap();

    ensure_schema(&client, PUFFGRES_SCHEMA).await.unwrap();
    ensure_state_tables(&client, PUFFGRES_SCHEMA).await.unwrap();

    let row = client
        .query_one(
            "SELECT COUNT(*)
             FROM information_schema.tables
             WHERE table_schema = 'public'
               AND table_name = ANY($1)",
            &[&EXPECTED_TABLES.as_slice()],
        )
        .await
        .unwrap();
    let public_table_count: i64 = row.get(0);

    assert_eq!(public_table_count, 0);
}
