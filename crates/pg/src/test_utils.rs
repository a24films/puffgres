use testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};
use testcontainers_modules::postgres::Postgres;

pub struct TestContext {
    pub _container: ContainerAsync<Postgres>,
    pub connection_string: String,
    pub connection_url: String,
}

pub async fn setup_postgres() -> TestContext {
    let container = Postgres::default()
        .with_tag("17-alpine")
        .start()
        .await
        .expect("Failed to start postgres container");

    test_context(container).await
}

pub async fn setup_postgres_logical() -> TestContext {
    let container = Postgres::default()
        .with_tag("17-alpine")
        .with_cmd(vec![
            "postgres".to_string(),
            "-c".to_string(),
            "wal_level=logical".to_string(),
            "-c".to_string(),
            "max_replication_slots=4".to_string(),
        ])
        .start()
        .await
        .expect("Failed to start postgres container");

    test_context(container).await
}

async fn test_context(container: ContainerAsync<Postgres>) -> TestContext {
    let host = container.get_host().await.expect("Failed to get host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("Failed to get port");

    let connection_string = format!(
        "host={} port={} user=postgres password=postgres dbname=postgres",
        host, port
    );
    let connection_url = format!("postgresql://postgres:postgres@{}:{}/postgres", host, port);

    TestContext {
        _container: container,
        connection_string,
        connection_url,
    }
}
