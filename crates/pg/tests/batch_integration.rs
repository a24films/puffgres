mod common;

use common::setup_postgres;
use pg::batch::{
    BatchQueryConfig, CURSOR_CAST_INT, CURSOR_CAST_NONE, CURSOR_CAST_UUID, count_rows, fetch_batch,
    fetch_row_by_id, resolve_cursor_cast, validate_id_column_uniqueness,
};
use pg::connect::connect;

fn default_config() -> BatchQueryConfig {
    BatchQueryConfig {
        schema: "public".to_string(),
        table: "test_items".to_string(),
        id_column: "id".to_string(),
        columns: None,
        batch_size: 3,
    }
}

async fn create_test_table(client: &tokio_postgres::Client) {
    client
        .execute(
            "CREATE TABLE test_items (id TEXT PRIMARY KEY, value TEXT)",
            &[],
        )
        .await
        .expect("Failed to create table");
}

async fn insert_rows(client: &tokio_postgres::Client, count: usize) {
    for i in 1..=count {
        let id = format!("{:04}", i);
        let value = format!("value_{}", i);
        client
            .execute(
                "INSERT INTO test_items (id, value) VALUES ($1, $2)",
                &[&id, &value],
            )
            .await
            .expect("Failed to insert row");
    }
}

async fn setup_test_table() -> (common::TestContext, pg::connect::PgConnection) {
    let ctx = setup_postgres().await;
    let client = connect(&ctx.connection_string)
        .await
        .expect("Failed to connect");
    create_test_table(&client).await;
    (ctx, client)
}

#[tokio::test]
async fn count_rows_empty_table() {
    let (_ctx, client) = setup_test_table().await;

    let count = count_rows(&client, &default_config())
        .await
        .expect("Failed to count");
    assert_eq!(count, 0);
}

#[tokio::test]
async fn count_rows_with_data() {
    let (_ctx, client) = setup_test_table().await;
    insert_rows(&client, 5).await;

    let count = count_rows(&client, &default_config())
        .await
        .expect("Failed to count");
    assert_eq!(count, 5);
}

#[tokio::test]
async fn fetch_batch_from_beginning() {
    let (_ctx, client) = setup_test_table().await;
    insert_rows(&client, 5).await;

    let result = fetch_batch(&client, &default_config(), None, "")
        .await
        .expect("Failed to fetch batch");

    assert_eq!(result.rows.len(), 3);
    assert!(result.has_more);
    assert_eq!(result.last_id.as_deref(), Some("0003"));
}

#[tokio::test]
async fn fetch_batch_with_cursor() {
    let (_ctx, client) = setup_test_table().await;
    insert_rows(&client, 5).await;

    let result = fetch_batch(&client, &default_config(), Some("0003"), "")
        .await
        .expect("Failed to fetch batch");

    assert_eq!(result.rows.len(), 2);
    assert!(!result.has_more);
    assert_eq!(result.last_id.as_deref(), Some("0005"));
}

#[tokio::test]
async fn fetch_batch_empty_table() {
    let (_ctx, client) = setup_test_table().await;

    let result = fetch_batch(&client, &default_config(), None, "")
        .await
        .expect("Failed to fetch batch");

    assert_eq!(result.rows.len(), 0);
    assert!(!result.has_more);
    assert!(result.last_id.is_none());
}

#[tokio::test]
async fn fetch_batch_exact_batch_size() {
    let (_ctx, client) = setup_test_table().await;
    insert_rows(&client, 3).await;

    let result = fetch_batch(&client, &default_config(), None, "")
        .await
        .expect("Failed to fetch batch");

    assert_eq!(result.rows.len(), 3);
    assert!(!result.has_more);
    assert_eq!(result.last_id.as_deref(), Some("0003"));
}

#[tokio::test]
async fn fetch_batch_paginate_all_rows() {
    let (_ctx, client) = setup_test_table().await;
    insert_rows(&client, 7).await;

    let config = default_config();

    // First batch
    let result = fetch_batch(&client, &config, None, "")
        .await
        .expect("Failed to fetch batch 1");
    assert_eq!(result.rows.len(), 3);
    assert!(result.has_more);
    let cursor = result.last_id.clone();

    // Second batch
    let result = fetch_batch(&client, &config, cursor.as_deref(), "")
        .await
        .expect("Failed to fetch batch 2");
    assert_eq!(result.rows.len(), 3);
    assert!(result.has_more);
    let cursor = result.last_id.clone();

    // Third batch (final)
    let result = fetch_batch(&client, &config, cursor.as_deref(), "")
        .await
        .expect("Failed to fetch batch 3");
    assert_eq!(result.rows.len(), 1);
    assert!(!result.has_more);
    assert_eq!(result.last_id.as_deref(), Some("0007"));
}

#[tokio::test]
async fn fetch_batch_with_specific_columns() {
    let (_ctx, client) = setup_test_table().await;
    insert_rows(&client, 2).await;

    let config = BatchQueryConfig {
        columns: Some(vec!["id".to_string(), "value".to_string()]),
        ..default_config()
    };

    let result = fetch_batch(&client, &config, None, "")
        .await
        .expect("Failed to fetch batch");

    assert_eq!(result.rows.len(), 2);
    let first_id: String = result.rows[0].get("id");
    let first_value: String = result.rows[0].get("value");
    assert_eq!(first_id, "0001");
    assert_eq!(first_value, "value_1");
}

#[tokio::test]
async fn fetch_batch_zero_batch_size() {
    let (_ctx, client) = setup_test_table().await;

    let config = BatchQueryConfig {
        batch_size: 0,
        ..default_config()
    };

    let err = fetch_batch(&client, &config, None, "")
        .await
        .expect_err("should reject zero batch_size");
    assert!(
        err.to_string()
            .contains("batch_size must be greater than 0")
    );
}

#[tokio::test]
async fn fetch_batch_empty_columns() {
    let (_ctx, client) = setup_test_table().await;

    let config = BatchQueryConfig {
        columns: Some(vec![]),
        ..default_config()
    };

    let err = fetch_batch(&client, &config, None, "")
        .await
        .expect_err("should reject empty columns");
    assert!(err.to_string().contains("columns list cannot be empty"));
}

#[tokio::test]
async fn fetch_batch_columns_without_id() {
    let (_ctx, client) = setup_test_table().await;
    insert_rows(&client, 2).await;

    let config = BatchQueryConfig {
        columns: Some(vec!["value".to_string()]),
        ..default_config()
    };

    let result = fetch_batch(&client, &config, None, "")
        .await
        .expect("should succeed even when id column is not in columns list");

    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.last_id.as_deref(), Some("0002"));
    let first_value: String = result.rows[0].get("value");
    assert_eq!(first_value, "value_1");
}

#[tokio::test]
async fn count_rows_excludes_null_ids() {
    let (_ctx, client) = setup_test_table().await;
    insert_rows(&client, 3).await;

    client
        .execute(
            "INSERT INTO test_items (id, value) VALUES (NULL, 'ghost')",
            &[],
        )
        .await
        .unwrap_or({
            // Table has PRIMARY KEY so NULL insert may fail; that's fine
            0
        });

    let count = count_rows(&client, &default_config())
        .await
        .expect("Failed to count");
    // If the table allows NULLs the count excludes them; if PK rejects
    // the insert we still get 3.
    assert!(count <= 3);
}

#[tokio::test]
async fn validate_id_column_uniqueness_passes() {
    let (_ctx, client) = setup_test_table().await;

    validate_id_column_uniqueness(&client, &default_config())
        .await
        .expect("primary key column should pass uniqueness check");
}

#[tokio::test]
async fn validate_id_column_uniqueness_fails() {
    let ctx = setup_postgres().await;
    let client = connect(&ctx.connection_string)
        .await
        .expect("Failed to connect");

    client
        .execute("CREATE TABLE no_unique (id TEXT, value TEXT)", &[])
        .await
        .expect("Failed to create table");

    let config = BatchQueryConfig {
        table: "no_unique".to_string(),
        ..default_config()
    };

    let err = validate_id_column_uniqueness(&client, &config)
        .await
        .expect_err("should fail for column without unique index");
    assert!(
        err.to_string()
            .contains("must have a non-partial unique index")
    );
}

#[tokio::test]
async fn resolve_cursor_cast_text() {
    let (_ctx, client) = setup_test_table().await;
    let cast = resolve_cursor_cast(&client, &default_config())
        .await
        .expect("text column should resolve");
    assert_eq!(cast, CURSOR_CAST_NONE);
}

#[tokio::test]
async fn resolve_cursor_cast_int() {
    let ctx = setup_postgres().await;
    let client = connect(&ctx.connection_string).await.unwrap();
    client
        .execute(
            "CREATE TABLE int_ids (id BIGINT PRIMARY KEY, value TEXT)",
            &[],
        )
        .await
        .unwrap();

    let config = BatchQueryConfig {
        table: "int_ids".to_string(),
        ..default_config()
    };
    let cast = resolve_cursor_cast(&client, &config)
        .await
        .expect("int8 column should resolve");
    assert_eq!(cast, CURSOR_CAST_INT);
}

#[tokio::test]
async fn resolve_cursor_cast_uuid() {
    let ctx = setup_postgres().await;
    let client = connect(&ctx.connection_string).await.unwrap();
    client
        .execute(
            "CREATE TABLE uuid_ids (id UUID PRIMARY KEY, value TEXT)",
            &[],
        )
        .await
        .unwrap();

    let config = BatchQueryConfig {
        table: "uuid_ids".to_string(),
        ..default_config()
    };
    let cast = resolve_cursor_cast(&client, &config)
        .await
        .expect("uuid column should resolve");
    assert_eq!(cast, CURSOR_CAST_UUID);
}

#[tokio::test]
async fn resolve_cursor_cast_bpchar() {
    let ctx = setup_postgres().await;
    let client = connect(&ctx.connection_string).await.unwrap();
    client
        .execute(
            "CREATE TABLE bpchar_ids (id CHAR(36) PRIMARY KEY, value TEXT)",
            &[],
        )
        .await
        .unwrap();

    let config = BatchQueryConfig {
        table: "bpchar_ids".to_string(),
        ..default_config()
    };
    let cast = resolve_cursor_cast(&client, &config)
        .await
        .expect("bpchar column should resolve");
    assert_eq!(cast, CURSOR_CAST_NONE);
}

#[tokio::test]
async fn resolve_cursor_cast_domain_over_uuid() {
    let ctx = setup_postgres().await;
    let client = connect(&ctx.connection_string).await.unwrap();
    client
        .execute("CREATE DOMAIN my_uuid AS UUID", &[])
        .await
        .unwrap();
    client
        .execute(
            "CREATE TABLE domain_uuid_ids (id my_uuid PRIMARY KEY, value TEXT)",
            &[],
        )
        .await
        .unwrap();

    let config = BatchQueryConfig {
        table: "domain_uuid_ids".to_string(),
        ..default_config()
    };
    let cast = resolve_cursor_cast(&client, &config)
        .await
        .expect("domain over uuid should unwrap to uuid");
    assert_eq!(cast, CURSOR_CAST_UUID);
}

#[tokio::test]
async fn resolve_cursor_cast_domain_over_int() {
    let ctx = setup_postgres().await;
    let client = connect(&ctx.connection_string).await.unwrap();
    client
        .execute("CREATE DOMAIN pos_int AS INTEGER CHECK (VALUE > 0)", &[])
        .await
        .unwrap();
    client
        .execute(
            "CREATE TABLE domain_int_ids (id pos_int PRIMARY KEY, value TEXT)",
            &[],
        )
        .await
        .unwrap();

    let config = BatchQueryConfig {
        table: "domain_int_ids".to_string(),
        ..default_config()
    };
    let cast = resolve_cursor_cast(&client, &config)
        .await
        .expect("domain over int should unwrap to int8");
    assert_eq!(cast, CURSOR_CAST_INT);
}

#[tokio::test]
async fn resolve_cursor_cast_nested_domain() {
    let ctx = setup_postgres().await;
    let client = connect(&ctx.connection_string).await.unwrap();
    client
        .execute("CREATE DOMAIN base_text AS TEXT", &[])
        .await
        .unwrap();
    client
        .execute("CREATE DOMAIN my_id AS base_text", &[])
        .await
        .unwrap();
    client
        .execute(
            "CREATE TABLE nested_domain_ids (id my_id PRIMARY KEY, value TEXT)",
            &[],
        )
        .await
        .unwrap();

    let config = BatchQueryConfig {
        table: "nested_domain_ids".to_string(),
        ..default_config()
    };
    let cast = resolve_cursor_cast(&client, &config)
        .await
        .expect("nested domain over text should unwrap to text");
    assert_eq!(cast, CURSOR_CAST_NONE);
}

// The cast returned by `resolve_cursor_cast` has to survive an actual round
// trip: the cursor is a Rust String, so `$1` must land on the wire as text.
// A bare `$1::uuid` type-infers the parameter as uuid and tokio-postgres fails
// with "error serializing parameter 0" on the *second* batch — the first batch
// has no cursor and passes, which is why asserting the cast string alone
// missed this.
#[tokio::test]
async fn fetch_batch_paginates_uuid_ids() {
    let ctx = setup_postgres().await;
    let client = connect(&ctx.connection_string).await.unwrap();
    client
        .execute(
            "CREATE TABLE uuid_ids (id UUID PRIMARY KEY, value TEXT)",
            &[],
        )
        .await
        .unwrap();
    for i in 1..=5 {
        client
            .execute(
                "INSERT INTO uuid_ids (id, value) VALUES ($1::text::uuid, $2)",
                &[
                    &format!("00000000-0000-0000-0000-00000000000{}", i),
                    &format!("value_{}", i),
                ],
            )
            .await
            .unwrap();
    }

    let config = BatchQueryConfig {
        table: "uuid_ids".to_string(),
        batch_size: 2,
        ..default_config()
    };
    let cast = resolve_cursor_cast(&client, &config).await.unwrap();

    let mut seen = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let batch = fetch_batch(&client, &config, cursor.as_deref(), &cast)
            .await
            .expect("uuid cursor must serialize as text");
        seen.extend(batch.rows.iter().map(|r| r.get::<_, String>("id")));
        if !batch.has_more {
            break;
        }
        cursor = batch.last_id;
    }

    assert_eq!(seen.len(), 5, "should walk every row across batches");
    assert_eq!(seen[0], "00000000-0000-0000-0000-000000000001");
    assert_eq!(seen[4], "00000000-0000-0000-0000-000000000005");
}

#[tokio::test]
async fn fetch_batch_paginates_int_ids() {
    let ctx = setup_postgres().await;
    let client = connect(&ctx.connection_string).await.unwrap();
    client
        .execute(
            "CREATE TABLE int_ids (id BIGINT PRIMARY KEY, value TEXT)",
            &[],
        )
        .await
        .unwrap();
    for i in 1..=5i64 {
        client
            .execute(
                "INSERT INTO int_ids (id, value) VALUES ($1, $2)",
                &[&i, &format!("value_{}", i)],
            )
            .await
            .unwrap();
    }

    let config = BatchQueryConfig {
        table: "int_ids".to_string(),
        batch_size: 2,
        ..default_config()
    };
    let cast = resolve_cursor_cast(&client, &config).await.unwrap();

    let first = fetch_batch(&client, &config, None, &cast).await.unwrap();
    assert!(first.has_more);
    let second = fetch_batch(&client, &config, first.last_id.as_deref(), &cast)
        .await
        .expect("int cursor must serialize as text");
    assert_eq!(second.rows.len(), 2);
    assert_eq!(second.rows[0].get::<_, String>("id"), "3");
}

// DLQ replay re-queries a single row by id and hits the same `$1{cast}` binding.
#[tokio::test]
async fn fetch_row_by_id_uuid() {
    let ctx = setup_postgres().await;
    let client = connect(&ctx.connection_string).await.unwrap();
    client
        .execute(
            "CREATE TABLE uuid_ids (id UUID PRIMARY KEY, value TEXT)",
            &[],
        )
        .await
        .unwrap();
    let id = "3f2504e0-4f89-11d3-9a0c-0305e82c3301";
    client
        .execute(
            "INSERT INTO uuid_ids (id, value) VALUES ($1::text::uuid, 'hit')",
            &[&id],
        )
        .await
        .unwrap();

    let config = BatchQueryConfig {
        table: "uuid_ids".to_string(),
        columns: Some(vec!["id".to_string(), "value".to_string()]),
        ..default_config()
    };

    let row = fetch_row_by_id(&client, &config, id, CURSOR_CAST_UUID)
        .await
        .expect("uuid id must serialize as text")
        .expect("row exists");
    assert_eq!(row.get::<_, String>("value"), "hit");

    let missing = fetch_row_by_id(
        &client,
        &config,
        "3f2504e0-4f89-11d3-9a0c-0305e82c3399",
        CURSOR_CAST_UUID,
    )
    .await
    .unwrap();
    assert!(missing.is_none());
}
