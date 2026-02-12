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
async fn test_rejects_modified_config() {
    let (_container, env_config) = setup_pg(&["users", "accounts"]).await;
    let (_dir, paths) = setup_project();

    write_config(&paths, "user", 1, "public", "users", "id", "uint");
    write_passthrough_transform(&paths, "user");
    run_async(&paths, &env_config).await.unwrap();

    // Mutate the already-applied config
    write_config(&paths, "user", 1, "public", "accounts", "id", "uint");

    let err = run_async(&paths, &env_config).await.unwrap_err();
    assert!(
        err.to_string().contains("modified"),
        "expected immutability error, got: {err}"
    );
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
