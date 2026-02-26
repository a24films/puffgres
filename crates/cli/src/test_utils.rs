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

pub fn write_config_with_columns(
    paths: &ProjectPaths,
    name: &str,
    version: i64,
    schema: &str,
    table: &str,
    id_column: &str,
    id_type: &str,
    columns: &[&str],
) {
    let config_name = format!("{name}_{version:04}");
    let columns_toml = columns
        .iter()
        .map(|c| format!("\"{c}\""))
        .collect::<Vec<_>>()
        .join(", ");
    let content = format!(
        r#"name = "{config_name}"
version = {version}
namespace = "{name}"
columns = [{columns_toml}]

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
