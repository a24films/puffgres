use chrono::Utc;
use pg::schema_bootstrap::{PUFFGRES_SCHEMA, state_tables_exist};
use pg::test_utils::setup_postgres;
use state::{BackfillProgress, BackfillStatus, PostgresStateStore, StreamingCheckpoint};

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

#[tokio::test]
async fn postgres_state_store_persists_checkpoint_and_backfill_progress() {
    let ctx = setup_postgres().await;
    let store = PostgresStateStore::connect(&ctx.connection_string)
        .await
        .unwrap();

    store
        .client()
        .execute(
            "INSERT INTO puffgres.configs
                (name, namespace, content_hash, transform_hash, applied_at, tombstone_applied_at, namespace_prefix)
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
            &[
                &"films",
                &"films",
                &"hash",
                &Option::<String>::None,
                &Utc::now().timestamp_millis(),
                &Option::<i64>::None,
                &Option::<String>::None,
            ],
        )
        .await
        .unwrap();

    let checkpoint = StreamingCheckpoint {
        config_name: "films".to_string(),
        lsn: 123,
        events_processed: 9,
        updated_at: Utc::now(),
    };
    store.save_streaming_checkpoint(&checkpoint).await.unwrap();

    let backfill = BackfillProgress {
        config_name: "films".to_string(),
        last_id: Some("abc".to_string()),
        total_rows: Some(10),
        processed_rows: 4,
        status: BackfillStatus::InProgress,
        started_at: Some(Utc::now()),
        completed_at: None,
        error_message: None,
        watermark_lsn: Some(456),
    };
    store.save_backfill_progress(&backfill).await.unwrap();

    let saved_checkpoint = store.get_streaming_checkpoint("films").await.unwrap().unwrap();
    assert_eq!(saved_checkpoint.config_name, "films");
    assert_eq!(saved_checkpoint.lsn, 123);
    assert_eq!(saved_checkpoint.events_processed, 9);

    let saved_backfill = store.get_backfill_progress("films").await.unwrap().unwrap();
    assert_eq!(saved_backfill.config_name, "films");
    assert_eq!(saved_backfill.last_id.as_deref(), Some("abc"));
    assert_eq!(saved_backfill.total_rows, Some(10));
    assert_eq!(saved_backfill.processed_rows, 4);
    assert_eq!(saved_backfill.status, BackfillStatus::InProgress);
    assert_eq!(saved_backfill.watermark_lsn, Some(456));

    assert!(store.delete_streaming_checkpoint("films").await.unwrap());
    assert!(store.get_streaming_checkpoint("films").await.unwrap().is_none());
}
