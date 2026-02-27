#![allow(dead_code)]

use testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};
use testcontainers_modules::postgres::Postgres;

pub struct TestContext {
    pub _container: ContainerAsync<Postgres>,
    pub connection_string: String,
    pub connection_url: String,
}

pub async fn setup_postgres() -> TestContext {
    setup_postgres_with_cmd(vec![]).await
}

pub async fn setup_postgres_with_cmd(cmd: Vec<String>) -> TestContext {
    let mut request = Postgres::default().with_tag("17-alpine");

    if !cmd.is_empty() {
        request = request.with_cmd(cmd);
    }

    let container = request
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
    let connection_url = format!("postgresql://postgres:postgres@{}:{}/postgres", host, port);

    TestContext {
        _container: container,
        connection_string,
        connection_url,
    }
}
