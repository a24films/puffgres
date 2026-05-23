use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use puffgres_cli::dry_run::run_async;
use puffgres_cli::{EnvConfig, ProjectPaths};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;

static TEST_TIMESTAMP: AtomicU64 = AtomicU64::new(2000000000000);

fn setup_project() -> (tempfile::TempDir, ProjectPaths) {
    let dir = tempfile::tempdir().unwrap();
    let paths = ProjectPaths::new(dir.path().to_path_buf()).unwrap();

    fs::create_dir_all(&paths.configs).unwrap();

    (dir, paths)
}

fn write_config(
    paths: &ProjectPaths,
    name: &str,
    schema: &str,
    table: &str,
    id_column: &str,
    id_type: &str,
) -> std::path::PathBuf {
    let ts = TEST_TIMESTAMP.fetch_add(1, Ordering::SeqCst);
    let dir_name = format!("{}_{}", ts, name);
    let config_dir = paths.configs.join(&dir_name);
    fs::create_dir_all(&config_dir).unwrap();
    let content = format!(
        r#"name = "{name}"
namespace = "{name}"

[source]
schema = "{schema}"
table = "{table}"

[id]
column = "{id_column}"
type = "{id_type}"
"#
    );
    fs::write(config_dir.join("config.toml"), content).unwrap();
    config_dir
}

fn write_transform(config_dir: &Path, script: &str) {
    fs::write(config_dir.join("transform.ts"), script).unwrap();
}

const PASSTHROUGH_TRANSFORM: &str = r#"
import { createInterface } from "readline";

const rl = createInterface({ input: process.stdin });

void (async () => {
  for await (const line of rl) {
    const input = JSON.parse(line);
    const output = input.map((event: any) => {
      if (event.operation === "delete") {
        return { type: "delete", id: event.id };
      }
      return { type: "upsert", id: event.id, document: { raw: event.columns } };
    });
    process.stdout.write(JSON.stringify(output) + "\n");
  }
})();
"#;

fn write_vector_transform(config_dir: &Path, include_distance: bool) {
    let distance_field = if include_distance {
        r#", distance_metric: "cosine_distance""#
    } else {
        ""
    };
    let script = format!(
        r#"
import {{ createInterface }} from "readline";

const rl = createInterface({{ input: process.stdin }});

void (async () => {{
  for await (const line of rl) {{
    const input = JSON.parse(line);
    const output = input.map((event: any) => {{
      if (event.operation === "delete") {{
        return {{ type: "delete", id: event.id }};
      }}
      return {{ type: "upsert", id: event.id, vector: [1.0, 2.0, 3.0]{distance_field}, document: {{ raw: event.columns }} }};
    }});
    process.stdout.write(JSON.stringify(output) + "\n");
  }}
}})();
"#
    );
    fs::write(config_dir.join("transform.ts"), script).unwrap();
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
        otel_endpoint: None,
        otel_headers: None,
        state_schema: "puffgres".to_string(),
        dlq_max_age_hours: None,
        inspect_port: None,
    };

    (container, env_config)
}

#[tokio::test]
async fn rejects_vector_without_distance_metric() {
    let (_dir, paths) = setup_project();
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
    let vec_dir = write_config(&paths, "vec", "public", "dry_vec_test", "id", "uint");
    write_vector_transform(&vec_dir, false);

    let err = run_async(
        &paths,
        &env_config,
        None,
        &puffgres_cli::ProjectConfig::default(),
    )
    .await
    .unwrap_err();
    assert!(
        err.to_string().contains("error"),
        "expected dry-run error, got: {err}"
    );
}

#[tokio::test]
async fn accepts_valid_transform() {
    let (_dir, paths) = setup_project();
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
    let valid_dir = write_config(&paths, "valid", "public", "dry_valid_test", "id", "uint");
    write_transform(&valid_dir, PASSTHROUGH_TRANSFORM);

    let result = run_async(
        &paths,
        &env_config,
        None,
        &puffgres_cli::ProjectConfig::default(),
    )
    .await;
    assert!(
        result.is_ok(),
        "expected dry-run to succeed, got: {result:?}"
    );
}

#[tokio::test]
async fn accepts_vector_with_distance_metric() {
    let (_dir, paths) = setup_project();
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
    let dist_dir = write_config(&paths, "dist", "public", "dry_dist_test", "id", "uint");
    write_vector_transform(&dist_dir, true);

    let result = run_async(
        &paths,
        &env_config,
        None,
        &puffgres_cli::ProjectConfig::default(),
    )
    .await;
    assert!(
        result.is_ok(),
        "expected dry-run with distance_metric to succeed, got: {result:?}"
    );
}

#[tokio::test]
async fn skips_empty_table_gracefully() {
    let (_dir, paths) = setup_project();
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
    let empty_dir = write_config(&paths, "empty", "public", "dry_empty_test", "id", "uint");
    write_transform(&empty_dir, PASSTHROUGH_TRANSFORM);

    let result = run_async(
        &paths,
        &env_config,
        None,
        &puffgres_cli::ProjectConfig::default(),
    )
    .await;
    assert!(
        result.is_ok(),
        "expected dry-run to succeed on empty table, got: {result:?}"
    );
}

#[tokio::test]
async fn filters_by_config_name() {
    let (_dir, paths) = setup_project();
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
    let good_dir = write_config(&paths, "good", "public", "dry_filter_test", "id", "uint");
    write_transform(&good_dir, PASSTHROUGH_TRANSFORM);

    let bad_dir = write_config(&paths, "bad", "public", "nonexistent_table", "id", "uint");
    write_transform(&bad_dir, PASSTHROUGH_TRANSFORM);

    // Dry-running only the good config should pass
    let result = run_async(
        &paths,
        &env_config,
        Some("good"),
        &puffgres_cli::ProjectConfig::default(),
    )
    .await;
    assert!(
        result.is_ok(),
        "expected dry-run to succeed for filtered config, got: {result:?}"
    );
}
