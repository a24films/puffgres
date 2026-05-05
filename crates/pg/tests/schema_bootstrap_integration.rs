use pg::connect::connect;
use pg::schema_bootstrap::{PUFFGRES_SCHEMA, ensure_schema, schema_exists};
use pg::test_utils::setup_postgres;

#[tokio::test]
async fn ensure_schema_creates_puffgres_schema() {
    let ctx = setup_postgres().await;
    let client = connect(&ctx.connection_string).await.unwrap();

    assert!(!schema_exists(&client, PUFFGRES_SCHEMA).await.unwrap());

    ensure_schema(&client, PUFFGRES_SCHEMA).await.unwrap();

    assert!(schema_exists(&client, PUFFGRES_SCHEMA).await.unwrap());
}

#[tokio::test]
async fn ensure_schema_is_idempotent() {
    let ctx = setup_postgres().await;
    let client = connect(&ctx.connection_string).await.unwrap();

    ensure_schema(&client, PUFFGRES_SCHEMA).await.unwrap();
    ensure_schema(&client, PUFFGRES_SCHEMA).await.unwrap();

    let row = client
        .query_one(
            "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = $1",
            &[&PUFFGRES_SCHEMA],
        )
        .await
        .unwrap();
    let count: i64 = row.get(0);

    assert_eq!(count, 1);
}
