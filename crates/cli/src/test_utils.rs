use crate::paths::ProjectPaths;
use state::StateDb;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

static TEST_TIMESTAMP: AtomicU64 = AtomicU64::new(1000000000000);

pub fn setup_project() -> (tempfile::TempDir, ProjectPaths) {
    let dir = tempfile::tempdir().unwrap();
    let paths = ProjectPaths::new(dir.path().to_path_buf()).unwrap();

    fs::create_dir_all(&paths.configs).unwrap();
    fs::create_dir_all(&paths.transforms).unwrap();

    StateDb::open(&paths.state_db).unwrap();

    (dir, paths)
}

pub fn write_config(
    paths: &ProjectPaths,
    name: &str,
    schema: &str,
    table: &str,
    id_column: &str,
    id_type: &str,
) -> PathBuf {
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

pub fn write_config_with_columns(
    paths: &ProjectPaths,
    name: &str,
    schema: &str,
    table: &str,
    id_column: &str,
    id_type: &str,
    columns: &[&str],
) -> PathBuf {
    let ts = TEST_TIMESTAMP.fetch_add(1, Ordering::SeqCst);
    let dir_name = format!("{}_{}", ts, name);
    let config_dir = paths.configs.join(&dir_name);
    fs::create_dir_all(&config_dir).unwrap();
    let columns_toml = columns
        .iter()
        .map(|c| format!("\"{c}\""))
        .collect::<Vec<_>>()
        .join(", ");
    let content = format!(
        r#"name = "{name}"
namespace = "{name}"
columns = [{columns_toml}]

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

pub fn write_transform(config_dir: &Path, script: &str) {
    fs::write(config_dir.join("transform.ts"), script).unwrap();
}

use std::path::Path;

pub const PASSTHROUGH_TRANSFORM: &str = r#"
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

pub const VECTOR_NO_METRIC_TRANSFORM: &str = r#"
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

pub const VECTOR_WITH_METRIC_TRANSFORM: &str = r#"
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
