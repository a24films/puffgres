use std::fs;

use puffgres_cli::dry_run::run_async;
use puffgres_cli::{EnvConfig, ProjectPaths};
use state::StateDb;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;

fn setup_project() -> (tempfile::TempDir, ProjectPaths) {
    let dir = tempfile::tempdir().unwrap();
    let paths = ProjectPaths::new(dir.path().to_path_buf());

    fs::create_dir_all(&paths.configs).unwrap();
    fs::create_dir_all(&paths.transforms).unwrap();

    let db = StateDb::open(&paths.state_db).unwrap();
    db.initialize().unwrap();

    (dir, paths)
}

fn write_config(
    paths: &ProjectPaths,
    name: &str,
    version: i64,
    schema: &str,
    table: &str,
    id_column: &str,
    id_type: &str,
) {
    let config_name = format!("{name}_{version:04}");
    let content = format!(
        r#"name = "{config_name}"
version = {version}
namespace = "{name}"

[source]
schema = "{schema}"
table = "{table}"

[id]
column = "{id_column}"
type = "{id_type}"

[transform]
path = "transforms/{name}.ts"
"#
    );
    fs::write(paths.configs.join(format!("{config_name}.toml")), content).unwrap();
}

fn write_passthrough_transform(paths: &ProjectPaths, name: &str) {
    let script = r#"
import { readFileSync } from "fs";
const input = JSON.parse(readFileSync("/dev/stdin", "utf-8"));
const output = input.map((event: any) => {
  if (event.operation === "delete") {
    return { type: "delete", id: event.id };
  }
  return { type: "upsert", id: event.id, document: { raw: event.columns } };
});
process.stdout.write(JSON.stringify(output));
"#;
    fs::write(paths.transforms.join(format!("{name}.ts")), script).unwrap();
}

fn write_vector_transform(paths: &ProjectPaths, name: &str, include_distance: bool) {
    let distance_field = if include_distance {
        r#", distance_metric: "cosine_distance""#
    } else {
        ""
    };
    let script = format!(
        r#"
import {{ readFileSync }} from "fs";
const input = JSON.parse(readFileSync("/dev/stdin", "utf-8"));
const output = input.map((event: any) => {{
  if (event.operation === "delete") {{
    return {{ type: "delete", id: event.id }};
  }}
  return {{ type: "upsert", id: event.id, vector: [1.0, 2.0, 3.0]{distance_field}, document: {{ raw: event.columns }} }};
}});
process.stdout.write(JSON.stringify(output));
"#
    );
    fs::write(paths.transforms.join(format!("{name}.ts")), script).unwrap();
}

async fn start_postgres() -> (ContainerAsync<Postgres>, EnvConfig) {
    let container = Postgres::default()
        .with_tag("16-alpine")
        .start()
        .await
        .expect("Failed to start postgres container");

    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();

    let database_url = format!(
        "host={} port={} user=postgres password=postgres dbname=postgres",
        host, port
    );

    let env_config = EnvConfig {
        database_url,
        turbopuffer_api_key: "fake-key".to_string(),
        turbopuffer_region: None,
        turbopuffer_namespace_prefix: None,
    };

    (container, env_config)
}

#[tokio::test]
async fn test_rejects_vector_without_distance_metric() {
    let (_container, env_config) = start_postgres().await;

    let pg_client = pg::connect::connect(&env_config.database_url)
        .await
        .unwrap();
    pg_client
        .execute(
            "CREATE TABLE dry_vec_test (id SERIAL PRIMARY KEY, name TEXT)",
            &[],
        )
        .await
        .unwrap();
    pg_client
        .execute("INSERT INTO dry_vec_test (name) VALUES ('test')", &[])
        .await
        .unwrap();
    drop(pg_client);

    let (_dir, paths) = setup_project();
    write_config(&paths, "vec", 1, "public", "dry_vec_test", "id", "uint");
    write_vector_transform(&paths, "vec", false);

    let err = run_async(&paths, &env_config, None).await.unwrap_err();
    assert!(
        err.to_string().contains("error"),
        "expected dry-run error, got: {err}"
    );
}

#[tokio::test]
async fn test_accepts_valid_transform() {
    let (_container, env_config) = start_postgres().await;

    let pg_client = pg::connect::connect(&env_config.database_url)
        .await
        .unwrap();
    pg_client
        .execute(
            "CREATE TABLE dry_valid_test (id SERIAL PRIMARY KEY, name TEXT)",
            &[],
        )
        .await
        .unwrap();
    pg_client
        .execute("INSERT INTO dry_valid_test (name) VALUES ('test')", &[])
        .await
        .unwrap();
    drop(pg_client);

    let (_dir, paths) = setup_project();
    write_config(&paths, "valid", 1, "public", "dry_valid_test", "id", "uint");
    write_passthrough_transform(&paths, "valid");

    let result = run_async(&paths, &env_config, None).await;
    assert!(
        result.is_ok(),
        "expected dry-run to succeed, got: {result:?}"
    );
}

#[tokio::test]
async fn test_accepts_vector_with_distance_metric() {
    let (_container, env_config) = start_postgres().await;

    let pg_client = pg::connect::connect(&env_config.database_url)
        .await
        .unwrap();
    pg_client
        .execute(
            "CREATE TABLE dry_dist_test (id SERIAL PRIMARY KEY, name TEXT)",
            &[],
        )
        .await
        .unwrap();
    pg_client
        .execute("INSERT INTO dry_dist_test (name) VALUES ('test')", &[])
        .await
        .unwrap();
    drop(pg_client);

    let (_dir, paths) = setup_project();
    write_config(&paths, "dist", 1, "public", "dry_dist_test", "id", "uint");
    write_vector_transform(&paths, "dist", true);

    let result = run_async(&paths, &env_config, None).await;
    assert!(
        result.is_ok(),
        "expected dry-run with distance_metric to succeed, got: {result:?}"
    );
}

#[tokio::test]
async fn test_skips_empty_table_gracefully() {
    let (_container, env_config) = start_postgres().await;

    let pg_client = pg::connect::connect(&env_config.database_url)
        .await
        .unwrap();
    pg_client
        .execute(
            "CREATE TABLE dry_empty_test (id SERIAL PRIMARY KEY, name TEXT)",
            &[],
        )
        .await
        .unwrap();
    drop(pg_client);

    let (_dir, paths) = setup_project();
    write_config(&paths, "empty", 1, "public", "dry_empty_test", "id", "uint");
    write_passthrough_transform(&paths, "empty");

    let result = run_async(&paths, &env_config, None).await;
    assert!(
        result.is_ok(),
        "expected dry-run to succeed on empty table, got: {result:?}"
    );
}

#[tokio::test]
async fn test_filters_by_config_name() {
    let (_container, env_config) = start_postgres().await;

    let pg_client = pg::connect::connect(&env_config.database_url)
        .await
        .unwrap();
    pg_client
        .execute(
            "CREATE TABLE dry_filter_test (id SERIAL PRIMARY KEY, name TEXT)",
            &[],
        )
        .await
        .unwrap();
    pg_client
        .execute("INSERT INTO dry_filter_test (name) VALUES ('test')", &[])
        .await
        .unwrap();
    drop(pg_client);

    let (_dir, paths) = setup_project();
    write_config(&paths, "good", 1, "public", "dry_filter_test", "id", "uint");
    write_passthrough_transform(&paths, "good");

    write_config(
        &paths,
        "bad",
        1,
        "public",
        "nonexistent_table",
        "id",
        "uint",
    );
    write_passthrough_transform(&paths, "bad");

    // Dry-running only the good config should pass
    let result = run_async(&paths, &env_config, Some("good_0001")).await;
    assert!(
        result.is_ok(),
        "expected dry-run to succeed for filtered config, got: {result:?}"
    );
}
