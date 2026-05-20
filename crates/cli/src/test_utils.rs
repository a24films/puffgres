use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};
use testcontainers_modules::postgres::Postgres;
use tokio::sync::OnceCell;

use crate::paths::ProjectPaths;

static TEST_TIMESTAMP: AtomicU64 = AtomicU64::new(1000000000000);

/// Shared Postgres testcontainer for CLI integration/unit tests.
///
/// Per-test isolation is achieved by allocating a fresh schema (`test_<N>`)
/// for each call to `fresh_schema`.
pub struct SharedTestPg {
    _container: ContainerAsync<Postgres>,
    pub database_url: String,
}

static SHARED_PG: OnceCell<SharedTestPg> = OnceCell::const_new();
static SCHEMA_COUNTER: AtomicU64 = AtomicU64::new(0);

impl SharedTestPg {
    pub async fn get() -> &'static SharedTestPg {
        SHARED_PG
            .get_or_init(|| async {
                let container = Postgres::default()
                    .with_tag("17-alpine")
                    .start()
                    .await
                    .expect("failed to start postgres testcontainer");
                let host = container.get_host().await.unwrap();
                let port = container.get_host_port_ipv4(5432).await.unwrap();
                let database_url = format!("postgresql://postgres:postgres@{host}:{port}/postgres");
                SharedTestPg {
                    _container: container,
                    database_url,
                }
            })
            .await
    }

    /// Allocate a fresh, unique schema name for this test.
    /// Returns (database_url, schema_name).
    pub fn fresh_schema(&self) -> (String, String) {
        let n = SCHEMA_COUNTER.fetch_add(1, Ordering::SeqCst);
        (self.database_url.clone(), format!("cli_test_{n}"))
    }
}

/// Set up a project directory tree (no state DB connection).
///
/// Returns the tempdir handle (must be kept alive for the project to exist
/// on disk) and a `ProjectPaths` pointing at it.
pub fn setup_project() -> (tempfile::TempDir, ProjectPaths) {
    let dir = tempfile::tempdir().unwrap();
    let paths = ProjectPaths::new(dir.path().to_path_buf()).unwrap();

    fs::create_dir_all(&paths.configs).unwrap();
    fs::create_dir_all(&paths.transforms).unwrap();

    (dir, paths)
}

/// Set up a project directory tree plus a fresh Postgres schema in the
/// shared testcontainer. Returns (tempdir, paths, database_url, schema).
pub async fn setup_project_with_state() -> (tempfile::TempDir, ProjectPaths, String, String) {
    let (dir, paths) = setup_project();
    let pg = SharedTestPg::get().await;
    let (url, schema) = pg.fresh_schema();
    (dir, paths, url, schema)
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

pub fn stub_schema(config_dir: &Path) {
    fs::write(
        config_dir.join("schema.ts"),
        "/* stub schema for tests */\nexport {};\n",
    )
    .unwrap();
}

pub const PASSTHROUGH_TRANSFORM: &str = r#"
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

pub const VECTOR_NO_METRIC_TRANSFORM: &str = r#"
import { createInterface } from "readline";

const rl = createInterface({ input: process.stdin });

void (async () => {
  for await (const line of rl) {
    const input = JSON.parse(line);
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
    process.stdout.write(JSON.stringify(output) + "\n");
  }
})();
"#;

pub const VECTOR_WITH_METRIC_TRANSFORM: &str = r#"
import { createInterface } from "readline";

const rl = createInterface({ input: process.stdin });

void (async () => {
  for await (const line of rl) {
    const input = JSON.parse(line);
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
    process.stdout.write(JSON.stringify(output) + "\n");
  }
})();
"#;
