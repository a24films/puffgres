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

#[tokio::test]
async fn postgres_state_store_persists_configs_and_dlq() {
    let ctx = setup_postgres().await;
    let store = PostgresStateStore::connect(&ctx.connection_string)
        .await
        .unwrap();

    let config = state::ConfigRecord {
        name: "buyers".to_string(),
        namespace: "buyers".to_string(),
        content_hash: "hash".to_string(),
        transform_hash: Some("transform".to_string()),
        applied_at: Utc::now(),
        tombstone_applied_at: None,
        namespace_prefix: None,
    };
    store.insert_config(&config).await.unwrap();
    store.set_namespace_prefix("buyers", Some("prod")).await.unwrap();

    let saved_config = store.get_config("buyers").await.unwrap().unwrap();
    assert_eq!(saved_config.namespace_prefix.as_deref(), Some("prod"));

    let dlq_entry = state::DlqEntry::retryable(
        "buyers",
        999,
        state::DlqOperation::Insert,
        Some(r#"{"String":"doc-1"}"#.to_string()),
        "temporary failure",
    );
    let dlq_id = store.insert_dlq_entry(&dlq_entry).await.unwrap();

    let retryable_entries = store.list_retryable_entries(10).await.unwrap();
    assert_eq!(retryable_entries.len(), 1);
    assert_eq!(retryable_entries[0].config_name, "buyers");
    assert_eq!(retryable_entries[0].retry_count, 0);

    store.increment_retry(dlq_id).await.unwrap();
    store.mark_permanent(dlq_id, "exhausted").await.unwrap();

    let cleared = store.clear_old_permanent_entries(0).await.unwrap();
    assert_eq!(cleared, 1);

    store.tombstone_config("buyers").await.unwrap();
    let tombstoned = store.list_tombstoned_configs().await.unwrap();
    assert_eq!(tombstoned.len(), 1);
    assert_eq!(tombstoned[0].name, "buyers");
}

#[tokio::test]
async fn postgres_state_store_manages_spool_lifecycle() {
    let ctx = setup_postgres().await;
    let store = PostgresStateStore::connect(&ctx.connection_string)
        .await
        .unwrap();

    let spool_id = store
        .insert_spool_entry(
            42,
            Some(1234),
            true,
            r#"["cfg-a"]"#,
            r#"{"config_batches":[]}"#,
        )
        .await
        .unwrap();

    assert_eq!(store.count_pending_spool_entries().await.unwrap(), 1);

    let claimed = store.claim_pending_spool_entries(10).await.unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].id, spool_id);
    assert_eq!(claimed[0].transaction_id, 42);
    assert_eq!(claimed[0].ack_lsn, Some(1234));
    assert_eq!(claimed[0].status, state::SpoolStatus::Processing);

    assert_eq!(store.count_pending_spool_entries().await.unwrap(), 0);

    store.release_spool_entry(spool_id).await.unwrap();
    assert_eq!(store.count_pending_spool_entries().await.unwrap(), 1);

    let claimed_again = store.claim_pending_spool_entries(10).await.unwrap();
    assert_eq!(claimed_again.len(), 1);

    store
        .mark_spool_entry_failed(spool_id, "temporary downstream error")
        .await
        .unwrap();

    let second_spool_id = store
        .insert_spool_entry(
            43,
            None,
            false,
            r#"["cfg-b"]"#,
            r#"{"config_batches":[{"config_name":"cfg-b"}]}"#,
        )
        .await
        .unwrap();
    let final_spool_id = store
        .insert_spool_entry(
            43,
            Some(5678),
            true,
            r#"["cfg-b"]"#,
            r#"{"config_batches":[{"config_name":"cfg-b"}]}"#,
        )
        .await
        .unwrap();
    let claimed_third = store.claim_pending_spool_entries(10).await.unwrap();
    assert_eq!(claimed_third.len(), 2);
    assert_eq!(claimed_third[0].id, second_spool_id);
    assert_eq!(claimed_third[1].id, final_spool_id);

    store.mark_spool_entry_done(second_spool_id).await.unwrap();
    store.mark_spool_entry_done(final_spool_id).await.unwrap();
}

#[tokio::test]
async fn postgres_state_store_requeues_processing_and_drops_incomplete_spool_entries() {
    let ctx = setup_postgres().await;
    let store = PostgresStateStore::connect(&ctx.connection_string)
        .await
        .unwrap();

    let incomplete_id = store
        .insert_spool_entry(
            100,
            None,
            false,
            r#"[]"#,
            r#"{"config_batches":[]}"#,
        )
        .await
        .unwrap();
    let pending_complete_id = store
        .insert_spool_entry(
            200,
            None,
            false,
            r#"[]"#,
            r#"{"config_batches":[]}"#,
        )
        .await
        .unwrap();
    let final_complete_id = store
        .insert_spool_entry(
            200,
            Some(9000),
            true,
            r#"["cfg-c"]"#,
            r#"{"config_batches":[]}"#,
        )
        .await
        .unwrap();

    let claimed = store.claim_pending_spool_entries(10).await.unwrap();
    assert_eq!(claimed.len(), 2);
    assert_eq!(claimed[0].id, pending_complete_id);
    assert_eq!(claimed[1].id, final_complete_id);

    assert_eq!(store.requeue_processing_spool_entries().await.unwrap(), 2);

    let claimed_again = store.claim_pending_spool_entries(10).await.unwrap();
    assert_eq!(claimed_again.len(), 2);

    assert_eq!(store.delete_incomplete_spool_entries().await.unwrap(), 1);

    let remaining_incomplete = store
        .client()
        .query_one(
            "SELECT COUNT(*) FROM puffgres.cdc_spool WHERE id = $1",
            &[&incomplete_id],
        )
        .await
        .unwrap()
        .get::<_, i64>(0);
    assert_eq!(remaining_incomplete, 0);
}
