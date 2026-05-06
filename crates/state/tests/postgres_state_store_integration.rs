use pg::schema_bootstrap::{PUFFGRES_SCHEMA, state_tables_exist};
use pg::test_utils::setup_postgres;
use state::PostgresStateStore;

#[tokio::test]
async fn postgres_state_store_bootstraps_schema_and_tables() {
    let ctx = setup_postgres().await;
    let store = PostgresStateStore::connect(&ctx.connection_string)
        .await
        .unwrap();

    assert_eq!(store.schema_name(), PUFFGRES_SCHEMA);
    assert!(
        state_tables_exist(store.client(), PUFFGRES_SCHEMA)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn postgres_state_store_roundtrip_succeeds() {
    let ctx = setup_postgres().await;
    let store = PostgresStateStore::connect(&ctx.connection_string)
        .await
        .unwrap();

    store.verify_startup_roundtrip().await.unwrap();
}
