use pg::connect::connect;
use pg::publication::{drop_publication, ensure_publication, get_publication_tables};
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
