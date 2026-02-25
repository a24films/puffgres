use pg::column::validate_column;
use pg::connect::{connect, validate_tables};
use pg::sample::fetch_sample_row;
use testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};
use testcontainers_modules::postgres::Postgres;

struct TestContext {
    _container: ContainerAsync<Postgres>,
    connection_string: String,
}

async fn setup_postgres() -> TestContext {
    let container = Postgres::default()
        .with_tag("17-alpine")
        .start()
        .await
        .expect("Failed to start postgres container");

    let host = container.get_host().await.expect("Failed to get host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("Failed to get port");

    let connection_string = format!(
        "host={} port={} user=postgres password=postgres dbname=postgres",
        host, port
    );

    TestContext {
        _container: container,
        connection_string,
    }
}

#[tokio::test]
async fn test_connect_success() {
    let ctx = setup_postgres().await;
    let client = connect(&ctx.connection_string).await;
    assert!(client.is_ok());
}

#[tokio::test]
async fn test_connect_invalid_connection_string() {
    let result = connect("host=nonexistent user=invalid").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_validate_tables_existing_table() {
    let ctx = setup_postgres().await;
    let client = connect(&ctx.connection_string)
        .await
        .expect("Failed to connect");

    client
        .execute("CREATE TABLE test_table (id SERIAL PRIMARY KEY)", &[])
        .await
        .expect("Failed to create table");

    let result = validate_tables(&client, &[("public", "test_table")]).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_tables_nonexistent_table() {
    let ctx = setup_postgres().await;
    let client = connect(&ctx.connection_string)
        .await
        .expect("Failed to connect");

    let result = validate_tables(&client, &[("public", "nonexistent_table")]).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_validate_multiple_tables() {
    let ctx = setup_postgres().await;
    let client = connect(&ctx.connection_string)
        .await
        .expect("Failed to connect");

    client
        .execute("CREATE TABLE users (id SERIAL PRIMARY KEY)", &[])
        .await
        .expect("Failed to create users table");

    client
        .execute("CREATE TABLE orders (id SERIAL PRIMARY KEY)", &[])
        .await
        .expect("Failed to create orders table");

    let result = validate_tables(&client, &[("public", "users"), ("public", "orders")]).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_tables_mixed_exist_and_not_exist() {
    let ctx = setup_postgres().await;
    let client = connect(&ctx.connection_string)
        .await
        .expect("Failed to connect");

    client
        .execute("CREATE TABLE existing_table (id SERIAL PRIMARY KEY)", &[])
        .await
        .expect("Failed to create table");

    let result = validate_tables(
        &client,
        &[("public", "existing_table"), ("public", "missing_table")],
    )
    .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_validate_tables_with_data() {
    let ctx = setup_postgres().await;
    let client = connect(&ctx.connection_string)
        .await
        .expect("Failed to connect");

    client
        .execute(
            "CREATE TABLE data_table (id SERIAL PRIMARY KEY, value TEXT)",
            &[],
        )
        .await
        .expect("Failed to create table");

    client
        .execute("INSERT INTO data_table (value) VALUES ('test')", &[])
        .await
        .expect("Failed to insert data");

    let result = validate_tables(&client, &[("public", "data_table")]).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_tables_special_characters() {
    let ctx = setup_postgres().await;
    let client = connect(&ctx.connection_string)
        .await
        .expect("Failed to connect");

    client
        .execute(
            "CREATE TABLE \"TableWithCaps\" (id SERIAL PRIMARY KEY)",
            &[],
        )
        .await
        .expect("Failed to create table");

    let result = validate_tables(&client, &[("public", "TableWithCaps")]).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_existing_column() {
    let ctx = setup_postgres().await;
    let client = connect(&ctx.connection_string)
        .await
        .expect("Failed to connect");

    client
        .execute(
            "CREATE TABLE col_test (id SERIAL PRIMARY KEY, name TEXT, email VARCHAR(255))",
            &[],
        )
        .await
        .expect("Failed to create table");

    let udt = validate_column(&client, "public", "col_test", "id").await;
    assert!(udt.is_ok());
    assert_eq!(udt.unwrap(), "int4");

    let udt = validate_column(&client, "public", "col_test", "name").await;
    assert!(udt.is_ok());
    assert_eq!(udt.unwrap(), "text");
}

#[tokio::test]
async fn test_rejects_nonexistent_column() {
    let ctx = setup_postgres().await;
    let client = connect(&ctx.connection_string)
        .await
        .expect("Failed to connect");

    client
        .execute("CREATE TABLE col_test2 (id SERIAL PRIMARY KEY)", &[])
        .await
        .expect("Failed to create table");

    let result = validate_column(&client, "public", "col_test2", "missing").await;
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("missing"));
}

#[tokio::test]
async fn test_fetch_sample_row() {
    let ctx = setup_postgres().await;
    let client = connect(&ctx.connection_string)
        .await
        .expect("Failed to connect");

    client
        .execute(
            "CREATE TABLE sample_test (id SERIAL PRIMARY KEY, name TEXT, age INT)",
            &[],
        )
        .await
        .expect("Failed to create table");

    client
        .execute(
            "INSERT INTO sample_test (name, age) VALUES ('alice', 30)",
            &[],
        )
        .await
        .expect("Failed to insert data");

    let result = fetch_sample_row(&client, "public", "sample_test")
        .await
        .unwrap();
    assert!(result.is_some());

    let (cols, vals) = result.unwrap();
    assert_eq!(cols, vec!["id", "name", "age"]);
    assert_eq!(vals.len(), 3);
    assert_eq!(vals[1], Some("alice".to_string()));
    assert_eq!(vals[2], Some("30".to_string()));
}

#[tokio::test]
async fn test_sample_row_empty_table() {
    let ctx = setup_postgres().await;
    let client = connect(&ctx.connection_string)
        .await
        .expect("Failed to connect");

    client
        .execute(
            "CREATE TABLE empty_test (id SERIAL PRIMARY KEY, name TEXT)",
            &[],
        )
        .await
        .expect("Failed to create table");

    let result = fetch_sample_row(&client, "public", "empty_test")
        .await
        .unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn test_sample_row_null_values() {
    let ctx = setup_postgres().await;
    let client = connect(&ctx.connection_string)
        .await
        .expect("Failed to connect");

    client
        .execute(
            "CREATE TABLE null_test (id SERIAL PRIMARY KEY, name TEXT)",
            &[],
        )
        .await
        .expect("Failed to create table");

    client
        .execute("INSERT INTO null_test (name) VALUES (NULL)", &[])
        .await
        .expect("Failed to insert data");

    let result = fetch_sample_row(&client, "public", "null_test")
        .await
        .unwrap();
    assert!(result.is_some());

    let (cols, vals) = result.unwrap();
    assert_eq!(cols, vec!["id", "name"]);
    assert_eq!(vals[1], None);
}
