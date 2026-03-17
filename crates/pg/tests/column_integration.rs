mod common;

use common::setup_postgres;
use pg::column::{resolve_column_info, validate_column};
use pg::connect::connect;

#[tokio::test]
async fn resolve_column_info_plain_types() {
    let ctx = setup_postgres().await;
    let client = connect(&ctx.connection_string).await.unwrap();
    client
        .execute(
            "CREATE TABLE plain_types (id INTEGER PRIMARY KEY, name TEXT, uid UUID)",
            &[],
        )
        .await
        .unwrap();

    let cols = resolve_column_info(&client, "public", "plain_types")
        .await
        .unwrap();
    assert_eq!(cols.len(), 3);
    assert_eq!(cols[0].name, "id");
    assert_eq!(cols[0].udt_name, "int4");
    assert_eq!(cols[1].name, "name");
    assert_eq!(cols[1].udt_name, "text");
    assert_eq!(cols[2].name, "uid");
    assert_eq!(cols[2].udt_name, "uuid");
}

#[tokio::test]
async fn resolve_column_info_domain_over_uuid() {
    let ctx = setup_postgres().await;
    let client = connect(&ctx.connection_string).await.unwrap();
    client
        .execute("CREATE DOMAIN my_uuid AS UUID", &[])
        .await
        .unwrap();
    client
        .execute(
            "CREATE TABLE domain_cols (id my_uuid PRIMARY KEY, value TEXT)",
            &[],
        )
        .await
        .unwrap();

    let cols = resolve_column_info(&client, "public", "domain_cols")
        .await
        .unwrap();
    assert_eq!(cols[0].name, "id");
    assert_eq!(
        cols[0].udt_name, "uuid",
        "domain should resolve to base type"
    );
}

#[tokio::test]
async fn resolve_column_info_domain_over_int() {
    let ctx = setup_postgres().await;
    let client = connect(&ctx.connection_string).await.unwrap();
    client
        .execute("CREATE DOMAIN pos_int AS INTEGER CHECK (VALUE > 0)", &[])
        .await
        .unwrap();
    client
        .execute(
            "CREATE TABLE domain_int_cols (id pos_int PRIMARY KEY, value TEXT)",
            &[],
        )
        .await
        .unwrap();

    let cols = resolve_column_info(&client, "public", "domain_int_cols")
        .await
        .unwrap();
    assert_eq!(cols[0].name, "id");
    assert_eq!(
        cols[0].udt_name, "int4",
        "domain over integer should resolve to int4"
    );
}

#[tokio::test]
async fn validate_column_domain_over_uuid() {
    let ctx = setup_postgres().await;
    let client = connect(&ctx.connection_string).await.unwrap();
    client
        .execute("CREATE DOMAIN my_uuid AS UUID", &[])
        .await
        .unwrap();
    client
        .execute(
            "CREATE TABLE validate_domain (id my_uuid PRIMARY KEY, value TEXT)",
            &[],
        )
        .await
        .unwrap();

    let udt = validate_column(&client, "public", "validate_domain", "id")
        .await
        .unwrap();
    assert_eq!(
        udt, "uuid",
        "validate_column should resolve domain to base type"
    );
}

#[tokio::test]
async fn validate_column_domain_over_int() {
    let ctx = setup_postgres().await;
    let client = connect(&ctx.connection_string).await.unwrap();
    client
        .execute("CREATE DOMAIN pos_int AS INTEGER CHECK (VALUE > 0)", &[])
        .await
        .unwrap();
    client
        .execute(
            "CREATE TABLE validate_domain_int (id pos_int PRIMARY KEY, value TEXT)",
            &[],
        )
        .await
        .unwrap();

    let udt = validate_column(&client, "public", "validate_domain_int", "id")
        .await
        .unwrap();
    assert_eq!(
        udt, "int4",
        "validate_column should resolve domain over integer to int4"
    );
}

#[tokio::test]
async fn validate_column_plain_type() {
    let ctx = setup_postgres().await;
    let client = connect(&ctx.connection_string).await.unwrap();
    client
        .execute(
            "CREATE TABLE validate_plain (id UUID PRIMARY KEY, value TEXT)",
            &[],
        )
        .await
        .unwrap();

    let udt = validate_column(&client, "public", "validate_plain", "id")
        .await
        .unwrap();
    assert_eq!(udt, "uuid");

    let udt = validate_column(&client, "public", "validate_plain", "value")
        .await
        .unwrap();
    assert_eq!(udt, "text");
}

#[tokio::test]
async fn resolve_column_info_nested_domain() {
    let ctx = setup_postgres().await;
    let client = connect(&ctx.connection_string).await.unwrap();
    client
        .execute("CREATE DOMAIN base_uuid AS UUID", &[])
        .await
        .unwrap();
    client
        .execute("CREATE DOMAIN my_id AS base_uuid", &[])
        .await
        .unwrap();
    client
        .execute(
            "CREATE TABLE nested_domain_cols (id my_id PRIMARY KEY, value TEXT)",
            &[],
        )
        .await
        .unwrap();

    let cols = resolve_column_info(&client, "public", "nested_domain_cols")
        .await
        .unwrap();
    assert_eq!(cols[0].name, "id");
    assert_eq!(
        cols[0].udt_name, "uuid",
        "nested domain (my_id -> base_uuid -> uuid) should resolve to uuid"
    );
}

#[tokio::test]
async fn validate_column_nested_domain() {
    let ctx = setup_postgres().await;
    let client = connect(&ctx.connection_string).await.unwrap();
    client
        .execute("CREATE DOMAIN base_text AS TEXT", &[])
        .await
        .unwrap();
    client
        .execute("CREATE DOMAIN my_label AS base_text", &[])
        .await
        .unwrap();
    client
        .execute(
            "CREATE TABLE nested_domain_validate (id my_label PRIMARY KEY, value TEXT)",
            &[],
        )
        .await
        .unwrap();

    let udt = validate_column(&client, "public", "nested_domain_validate", "id")
        .await
        .unwrap();
    assert_eq!(
        udt, "text",
        "nested domain (my_label -> base_text -> text) should resolve to text"
    );
}

#[tokio::test]
async fn resolve_column_info_array_types() {
    let ctx = setup_postgres().await;
    let client = connect(&ctx.connection_string).await.unwrap();
    client
        .execute(
            "CREATE TABLE array_cols (id INTEGER PRIMARY KEY, tags TEXT[], scores FLOAT8[], flags BOOLEAN[])",
            &[],
        )
        .await
        .unwrap();

    let cols = resolve_column_info(&client, "public", "array_cols")
        .await
        .unwrap();
    assert_eq!(cols.len(), 4);
    assert_eq!(cols[0].name, "id");
    assert_eq!(cols[0].udt_name, "int4");
    assert_eq!(cols[1].name, "tags");
    assert_eq!(cols[1].udt_name, "text[]", "text array should resolve to text[]");
    assert_eq!(cols[2].name, "scores");
    assert_eq!(cols[2].udt_name, "float8[]", "float8 array should resolve to float8[]");
    assert_eq!(cols[3].name, "flags");
    assert_eq!(cols[3].udt_name, "bool[]", "boolean array should resolve to bool[]");
}

#[tokio::test]
async fn resolve_column_info_array_of_domain() {
    let ctx = setup_postgres().await;
    let client = connect(&ctx.connection_string).await.unwrap();
    client
        .execute("CREATE DOMAIN my_uuid AS UUID", &[])
        .await
        .unwrap();
    client
        .execute(
            "CREATE TABLE array_domain_cols (id INTEGER PRIMARY KEY, uids my_uuid[])",
            &[],
        )
        .await
        .unwrap();

    let cols = resolve_column_info(&client, "public", "array_domain_cols")
        .await
        .unwrap();
    assert_eq!(cols[1].name, "uids");
    assert_eq!(
        cols[1].udt_name, "uuid[]",
        "array of domain should resolve element type to base type"
    );
}

#[tokio::test]
async fn validate_column_array_type() {
    let ctx = setup_postgres().await;
    let client = connect(&ctx.connection_string).await.unwrap();
    client
        .execute(
            "CREATE TABLE validate_array (id INTEGER PRIMARY KEY, tags TEXT[])",
            &[],
        )
        .await
        .unwrap();

    let udt = validate_column(&client, "public", "validate_array", "tags")
        .await
        .unwrap();
    assert_eq!(udt, "text[]", "validate_column should return text[] for text array");
}

#[tokio::test]
async fn validate_column_array_of_domain() {
    let ctx = setup_postgres().await;
    let client = connect(&ctx.connection_string).await.unwrap();
    client
        .execute("CREATE DOMAIN my_int AS INTEGER", &[])
        .await
        .unwrap();
    client
        .execute(
            "CREATE TABLE validate_array_domain (id INTEGER PRIMARY KEY, vals my_int[])",
            &[],
        )
        .await
        .unwrap();

    let udt = validate_column(&client, "public", "validate_array_domain", "vals")
        .await
        .unwrap();
    assert_eq!(
        udt, "int4[]",
        "validate_column should resolve domain element and return int4[]"
    );
}
