use chrono::Utc;
use std::sync::atomic::{AtomicU64, Ordering};
use testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};
use testcontainers_modules::postgres::Postgres;
use tokio::sync::OnceCell;

use crate::{ConfigRecord, StateDb};

struct SharedContainer {
    _container: ContainerAsync<Postgres>,
    database_url: String,
}

static CONTAINER: OnceCell<SharedContainer> = OnceCell::const_new();
static SCHEMA_COUNTER: AtomicU64 = AtomicU64::new(0);

async fn shared_container() -> &'static SharedContainer {
    CONTAINER
        .get_or_init(|| async {
            let container = Postgres::default()
                .with_tag("17-alpine")
                .start()
                .await
                .expect("failed to start postgres testcontainer");
            let host = container.get_host().await.unwrap();
            let port = container.get_host_port_ipv4(5432).await.unwrap();
            let database_url = format!("postgresql://postgres:postgres@{host}:{port}/postgres");
            SharedContainer {
                _container: container,
                database_url,
            }
        })
        .await
}

/// Open a fresh `StateDb` against a unique schema in the shared test container.
pub async fn setup_test_db() -> StateDb {
    let container = shared_container().await;
    let n = SCHEMA_COUNTER.fetch_add(1, Ordering::SeqCst);
    let schema = format!("test_{n}");
    StateDb::connect(&container.database_url, &schema)
        .await
        .expect("connect StateDb")
}

pub fn sample_config(name: &str) -> ConfigRecord {
    ConfigRecord {
        name: name.to_string(),
        namespace: name.to_string(),
        content_hash: "abc123".to_string(),
        transform_hash: None,
        applied_at: Utc::now(),
        tombstone_applied_at: None,
        namespace_prefix: None,
    }
}
