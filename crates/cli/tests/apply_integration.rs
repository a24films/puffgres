use std::fs;

use puffgres_cli::apply::run_async;
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

fn write_vector_no_metric_transform(paths: &ProjectPaths, name: &str) {
    let script = r#"
import { readFileSync } from "fs";
const input = JSON.parse(readFileSync("/dev/stdin", "utf-8"));
const output = input.map((event: any) => {
  if (event.operation === "delete") {
    return { type: "delete", id: event.id };
  }
  return {
    type: "upsert",
    id: event.id,
    document: {},
    vector: [0.1, 0.2, 0.3],
  };
});
process.stdout.write(JSON.stringify(output));
"#;
    fs::write(paths.transforms.join(format!("{name}.ts")), script).unwrap();
}

fn write_vector_with_metric_transform(paths: &ProjectPaths, name: &str) {
    let script = r#"
import { readFileSync } from "fs";
const input = JSON.parse(readFileSync("/dev/stdin", "utf-8"));
const output = input.map((event: any) => {
  if (event.operation === "delete") {
    return { type: "delete", id: event.id };
  }
  return {
    type: "upsert",
    id: event.id,
    document: {},
    vector: [0.1, 0.2, 0.3],
    distance_metric: "cosine_distance",
  };
});
process.stdout.write(JSON.stringify(output));
"#;
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
    };

    (container, env_config)
}

async fn setup_pg(tables: &[&str]) -> (ContainerAsync<Postgres>, EnvConfig) {
    let (container, env_config) = start_postgres().await;
    let pg_client = pg::connect::connect(&env_config.database_url)
        .await
        .unwrap();
    for table in tables {
        pg_client
            .execute(
                &format!("CREATE TABLE {table} (id SERIAL PRIMARY KEY)"),
                &[],
            )
            .await
            .unwrap();
    }
    drop(pg_client);
    (container, env_config)
}

#[tokio::test]
async fn test_apply_and_idempotency() {
    let (_container, env_config) = setup_pg(&["users", "films"]).await;
    let (_dir, paths) = setup_project();

    write_config(&paths, "user", 1, "public", "users", "id", "uint");
    write_config(&paths, "film", 2, "public", "films", "id", "uint");
    write_passthrough_transform(&paths, "user");
    write_passthrough_transform(&paths, "film");

    // First apply: both configs written
    run_async(&paths, &env_config).await.unwrap();

    let db = StateDb::open(&paths.state_db).unwrap();
    assert_eq!(db.list_configs().unwrap().len(), 2);

    let user = db.get_config("user_0001").unwrap().unwrap();
    assert_eq!(user.version, 1);
    assert_eq!(user.namespace, "user_v1");
    assert!(user.transform_hash.is_some());
    assert_eq!(user.content_hash.len(), 64);

    let film = db.get_config("film_0002").unwrap().unwrap();
    assert_eq!(film.namespace, "film_v2");

    // Second apply: idempotent, no errors, same count
    run_async(&paths, &env_config).await.unwrap();
    assert_eq!(db.list_configs().unwrap().len(), 2);
}

#[tokio::test]
async fn test_rejects_nonexistent_table() {
    let (_container, env_config) = start_postgres().await;
    let (_dir, paths) = setup_project();

    write_config(
        &paths,
        "ghost",
        1,
        "public",
        "nonexistent_table",
        "id",
        "uint",
    );
    write_passthrough_transform(&paths, "ghost");

    let err = run_async(&paths, &env_config).await.unwrap_err();
    assert!(
        err.to_string().contains("error"),
        "expected apply error, got: {err}"
    );
}

#[tokio::test]
async fn test_rejects_nonexistent_id_column() {
    let (_container, env_config) = start_postgres().await;

    let pg_client = pg::connect::connect(&env_config.database_url)
        .await
        .unwrap();
    pg_client
        .execute(
            "CREATE TABLE col_test (id SERIAL PRIMARY KEY, name TEXT)",
            &[],
        )
        .await
        .unwrap();
    drop(pg_client);

    let (_dir, paths) = setup_project();
    write_config(
        &paths,
        "col",
        1,
        "public",
        "col_test",
        "missing_col",
        "uint",
    );
    write_passthrough_transform(&paths, "col");

    let err = run_async(&paths, &env_config).await.unwrap_err();
    assert!(
        err.to_string().contains("error"),
        "expected apply error for missing column, got: {err}"
    );
}

#[tokio::test]
async fn test_rejects_incompatible_id_type() {
    let (_container, env_config) = start_postgres().await;

    let pg_client = pg::connect::connect(&env_config.database_url)
        .await
        .unwrap();
    pg_client
        .execute(
            "CREATE TABLE type_test (id TEXT PRIMARY KEY, name TEXT)",
            &[],
        )
        .await
        .unwrap();
    drop(pg_client);

    let (_dir, paths) = setup_project();
    write_config(&paths, "typed", 1, "public", "type_test", "id", "uint");
    write_passthrough_transform(&paths, "typed");

    let err = run_async(&paths, &env_config).await.unwrap_err();
    assert!(
        err.to_string().contains("error"),
        "expected apply error for incompatible id type, got: {err}"
    );
}

#[tokio::test]
async fn test_rejects_vector_without_distance_metric() {
    let (_container, env_config) = start_postgres().await;

    let pg_client = pg::connect::connect(&env_config.database_url)
        .await
        .unwrap();
    pg_client
        .execute(
            "CREATE TABLE vec_test (id SERIAL PRIMARY KEY, name TEXT)",
            &[],
        )
        .await
        .unwrap();
    pg_client
        .execute("INSERT INTO vec_test (name) VALUES ('sample')", &[])
        .await
        .unwrap();
    drop(pg_client);

    let (_dir, paths) = setup_project();
    write_config(&paths, "vec", 1, "public", "vec_test", "id", "uint");
    write_vector_no_metric_transform(&paths, "vec");

    let err = run_async(&paths, &env_config).await.unwrap_err();
    assert!(
        err.to_string().contains("error"),
        "expected apply error for vector without distance_metric, got: {err}"
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
            "CREATE TABLE good_vec (id SERIAL PRIMARY KEY, name TEXT)",
            &[],
        )
        .await
        .unwrap();
    pg_client
        .execute("INSERT INTO good_vec (name) VALUES ('sample')", &[])
        .await
        .unwrap();
    drop(pg_client);

    let (_dir, paths) = setup_project();
    write_config(&paths, "goodvec", 1, "public", "good_vec", "id", "uint");
    write_vector_with_metric_transform(&paths, "goodvec");

    let result = run_async(&paths, &env_config).await;
    assert!(result.is_ok(), "expected apply to succeed, got: {result:?}");
}

#[tokio::test]
async fn test_accepts_empty_table_skips_dry_run() {
    let (_container, env_config) = start_postgres().await;

    let pg_client = pg::connect::connect(&env_config.database_url)
        .await
        .unwrap();
    pg_client
        .execute(
            "CREATE TABLE empty_apply (id SERIAL PRIMARY KEY, name TEXT)",
            &[],
        )
        .await
        .unwrap();
    drop(pg_client);

    let (_dir, paths) = setup_project();
    write_config(&paths, "empty", 1, "public", "empty_apply", "id", "uint");
    write_passthrough_transform(&paths, "empty");

    let result = run_async(&paths, &env_config).await;
    assert!(
        result.is_ok(),
        "expected apply to succeed on empty table, got: {result:?}"
    );
}
