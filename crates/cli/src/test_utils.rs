use crate::paths::ProjectPaths;
use state::StateDb;
use std::fs;

pub fn setup_project() -> (tempfile::TempDir, ProjectPaths) {
    let dir = tempfile::tempdir().unwrap();
    let paths = ProjectPaths::new(dir.path().to_path_buf());

    fs::create_dir_all(&paths.configs).unwrap();
    fs::create_dir_all(&paths.transforms).unwrap();

    let db = StateDb::open(&paths.state_db).unwrap();
    db.initialize().unwrap();

    (dir, paths)
}

pub fn write_config(
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

pub fn write_passthrough_transform(paths: &ProjectPaths, name: &str) {
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

pub async fn start_postgres() -> (
    testcontainers::ContainerAsync<testcontainers_modules::postgres::Postgres>,
    crate::EnvConfig,
) {
    use testcontainers::ImageExt;
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::postgres::Postgres;

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

    let env_config = crate::EnvConfig {
        database_url,
        turbopuffer_api_key: "fake-key".to_string(),
        turbopuffer_region: None,
    };

    (container, env_config)
}
