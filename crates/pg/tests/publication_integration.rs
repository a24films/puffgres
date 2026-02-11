use pg::connect::connect;
use pg::publication::{
    add_tables_to_publication, drop_publication, ensure_publication, get_publication_tables,
};
use testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};
use testcontainers_modules::postgres::Postgres;

struct TestContext {
    _container: ContainerAsync<Postgres>,
    connection_string: String,
}

async fn setup_postgres() -> TestContext {
    let container = Postgres::default()
        .with_tag("16-alpine")
        .with_cmd(vec![
            "postgres".to_string(),
            "-c".to_string(),
            "wal_level=logical".to_string(),
        ])
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

async fn create_test_tables(client: &tokio_postgres::Client) {
    client
        .execute(
            "CREATE TABLE public.users (id SERIAL PRIMARY KEY, name TEXT)",
            &[],
        )
        .await
        .expect("Failed to create users table");
    client
        .execute(
            "CREATE TABLE public.orders (id SERIAL PRIMARY KEY, user_id INT)",
            &[],
        )
        .await
        .expect("Failed to create orders table");
}

#[tokio::test]
async fn ensure_publication_creates_and_is_idempotent() {
    let ctx = setup_postgres().await;
    let client = connect(&ctx.connection_string).await.unwrap();
    create_test_tables(&client).await;

    let tables = vec!["public.users".to_string()];
    ensure_publication(&client, "test_pub", &tables)
        .await
        .unwrap();
    ensure_publication(&client, "test_pub", &tables)
        .await
        .unwrap();

    let pub_tables = get_publication_tables(&client, "test_pub").await.unwrap();
    assert_eq!(pub_tables, vec!["public.users".to_string()]);
}

#[tokio::test]
async fn ensure_publication_multiple_tables() {
    let ctx = setup_postgres().await;
    let client = connect(&ctx.connection_string).await.unwrap();
    create_test_tables(&client).await;

    let tables = vec!["public.users".to_string(), "public.orders".to_string()];
    ensure_publication(&client, "multi_pub", &tables)
        .await
        .unwrap();

    let mut pub_tables = get_publication_tables(&client, "multi_pub").await.unwrap();
    pub_tables.sort();
    assert_eq!(
        pub_tables,
        vec!["public.orders".to_string(), "public.users".to_string()]
    );
}

#[tokio::test]
async fn drop_publication_removes_it() {
    let ctx = setup_postgres().await;
    let client = connect(&ctx.connection_string).await.unwrap();
    create_test_tables(&client).await;

    let tables = vec!["public.users".to_string()];
    ensure_publication(&client, "drop_me", &tables)
        .await
        .unwrap();

    drop_publication(&client, "drop_me").await.unwrap();

    let pub_tables = get_publication_tables(&client, "drop_me").await.unwrap();
    assert!(pub_tables.is_empty());
}

#[tokio::test]
async fn get_publication_tables_empty_for_nonexistent() {
    let ctx = setup_postgres().await;
    let client = connect(&ctx.connection_string).await.unwrap();

    let tables = get_publication_tables(&client, "nonexistent")
        .await
        .unwrap();
    assert!(tables.is_empty());
}

#[tokio::test]
async fn publication_for_table_exists_in_pg_catalog() {
    let ctx = setup_postgres().await;
    let client = connect(&ctx.connection_string).await.unwrap();
    create_test_tables(&client).await;

    let tables = vec!["public.users".to_string()];
    ensure_publication(&client, "catalog_pub", &tables)
        .await
        .unwrap();

    // Verify directly against pg_publication_tables that the table is published
    let row = client
        .query_one(
            "SELECT COUNT(*) FROM pg_publication_tables WHERE pubname = $1 AND schemaname = $2 AND tablename = $3",
            &[&"catalog_pub", &"public", &"users"],
        )
        .await
        .unwrap();
    let count: i64 = row.get(0);
    assert_eq!(
        count, 1,
        "expected 'public.users' to exist in publication 'catalog_pub'"
    );

    // Also verify the publication itself exists in pg_publication
    let row = client
        .query_one(
            "SELECT COUNT(*) FROM pg_publication WHERE pubname = $1",
            &[&"catalog_pub"],
        )
        .await
        .unwrap();
    let count: i64 = row.get(0);
    assert_eq!(
        count, 1,
        "expected publication 'catalog_pub' to exist in pg_publication"
    );
}

#[tokio::test]
async fn add_tables_to_publication_appends_new_table() {
    let ctx = setup_postgres().await;
    let client = connect(&ctx.connection_string).await.unwrap();
    create_test_tables(&client).await;

    let tables = vec!["public.users".to_string()];
    ensure_publication(&client, "add_pub", &tables)
        .await
        .unwrap();

    add_tables_to_publication(&client, "add_pub", &["public.orders".to_string()])
        .await
        .unwrap();

    let mut pub_tables = get_publication_tables(&client, "add_pub").await.unwrap();
    pub_tables.sort();
    assert_eq!(
        pub_tables,
        vec!["public.orders".to_string(), "public.users".to_string()]
    );
}

#[tokio::test]
async fn add_tables_to_publication_empty_is_noop() {
    let ctx = setup_postgres().await;
    let client = connect(&ctx.connection_string).await.unwrap();
    create_test_tables(&client).await;

    let tables = vec!["public.users".to_string()];
    ensure_publication(&client, "noop_pub", &tables)
        .await
        .unwrap();

    add_tables_to_publication(&client, "noop_pub", &[])
        .await
        .unwrap();

    let pub_tables = get_publication_tables(&client, "noop_pub").await.unwrap();
    assert_eq!(pub_tables, vec!["public.users".to_string()]);
}

#[tokio::test]
async fn ensure_publication_adds_missing_tables_to_existing() {
    let ctx = setup_postgres().await;
    let client = connect(&ctx.connection_string).await.unwrap();
    create_test_tables(&client).await;

    // Create with only users
    ensure_publication(&client, "grow_pub", &["public.users".to_string()])
        .await
        .unwrap();

    let pub_tables = get_publication_tables(&client, "grow_pub").await.unwrap();
    assert_eq!(pub_tables, vec!["public.users".to_string()]);

    // Call again with both tables — should add orders without error
    ensure_publication(
        &client,
        "grow_pub",
        &["public.users".to_string(), "public.orders".to_string()],
    )
    .await
    .unwrap();

    let mut pub_tables = get_publication_tables(&client, "grow_pub").await.unwrap();
    pub_tables.sort();
    assert_eq!(
        pub_tables,
        vec!["public.orders".to_string(), "public.users".to_string()]
    );
}

#[tokio::test]
async fn ensure_publication_unqualified_table_defaults_to_public() {
    let ctx = setup_postgres().await;
    let client = connect(&ctx.connection_string).await.unwrap();
    create_test_tables(&client).await;

    // Pass unqualified table name (no schema prefix)
    let tables = vec!["users".to_string()];
    ensure_publication(&client, "unqual_pub", &tables)
        .await
        .unwrap();

    let pub_tables = get_publication_tables(&client, "unqual_pub").await.unwrap();
    assert_eq!(pub_tables, vec!["public.users".to_string()]);
}
