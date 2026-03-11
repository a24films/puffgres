use std::path::PathBuf;

use pg::test_utils::setup_postgres;
use puffgres_cli::EnvConfig;
use puffgres_cli::apply::run_async;
use puffgres_cli::test_utils::{
    PASSTHROUGH_TRANSFORM, VECTOR_NO_METRIC_TRANSFORM, VECTOR_WITH_METRIC_TRANSFORM, setup_project,
    write_config, write_transform,
};
use state::StateDb;

async fn start_postgres_env(state_db_path: PathBuf) -> (pg::test_utils::TestContext, EnvConfig) {
    let ctx = setup_postgres().await;
    let env_config = EnvConfig {
        database_url: ctx.connection_string.clone(),
        turbopuffer_api_key: "fake-key".to_string(),
        turbopuffer_region: None,
        turbopuffer_namespace_prefix: None,
        otel_endpoint: None,
        otel_headers: None,
        state_db_path,
        dlq_max_age_hours: None,
    };
    (ctx, env_config)
}

async fn setup_pg(
    tables: &[&str],
    state_db_path: PathBuf,
) -> (pg::test_utils::TestContext, EnvConfig) {
    let (ctx, env_config) = start_postgres_env(state_db_path).await;
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
    (ctx, env_config)
}

#[tokio::test]
async fn apply_and_idempotency() {
    let (_dir, paths, state_db_path) = setup_project();
    let (_ctx, env_config) = setup_pg(&["users", "films"], state_db_path.clone()).await;

    let user_dir = write_config(&paths, "user", "public", "users", "id", "uint");
    write_transform(&user_dir, PASSTHROUGH_TRANSFORM);
    let film_dir = write_config(&paths, "film", "public", "films", "id", "uint");
    write_transform(&film_dir, PASSTHROUGH_TRANSFORM);

    // First apply: both configs written
    run_async(&paths, &env_config).await.unwrap();

    let db = StateDb::open(&state_db_path).unwrap();
    assert_eq!(db.list_configs().unwrap().len(), 2);

    let user = db.get_config("user").unwrap().unwrap();
    assert_eq!(user.namespace, "user");
    assert!(user.transform_hash.is_some());
    assert_eq!(user.content_hash.len(), 64);

    let film = db.get_config("film").unwrap().unwrap();
    assert_eq!(film.namespace, "film");

    // Second apply: idempotent, no errors, same count
    run_async(&paths, &env_config).await.unwrap();
    assert_eq!(db.list_configs().unwrap().len(), 2);
}

#[tokio::test]
async fn rejects_modified_config() {
    let (_dir, paths, state_db_path) = setup_project();
    let (_ctx, env_config) = setup_pg(&["users", "accounts"], state_db_path).await;

    let user_dir = write_config(&paths, "user", "public", "users", "id", "uint");
    write_transform(&user_dir, PASSTHROUGH_TRANSFORM);
    run_async(&paths, &env_config).await.unwrap();

    // Mutate the already-applied config
    let config_toml = user_dir.join("config.toml");
    std::fs::write(
        config_toml,
        r#"name = "user"
namespace = "user"

[source]
schema = "public"
table = "accounts"

[id]
column = "id"
type = "uint"
"#,
    )
    .unwrap();

    let err = run_async(&paths, &env_config).await.unwrap_err();
    assert!(
        err.to_string().contains("modified"),
        "expected immutability error, got: {err}"
    );
}

#[tokio::test]
async fn rejects_nonexistent_table() {
    let (_dir, paths, state_db_path) = setup_project();
    let (_ctx, env_config) = start_postgres_env(state_db_path).await;

    let ghost_dir = write_config(&paths, "ghost", "public", "nonexistent_table", "id", "uint");
    write_transform(&ghost_dir, PASSTHROUGH_TRANSFORM);

    let err = run_async(&paths, &env_config).await.unwrap_err();
    assert!(
        err.to_string().contains("error"),
        "expected apply error, got: {err}"
    );
}

#[tokio::test]
async fn rejects_nonexistent_id_column() {
    let (_dir, paths, state_db_path) = setup_project();
    let (_ctx, env_config) = start_postgres_env(state_db_path).await;

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
    let col_dir = write_config(&paths, "col", "public", "col_test", "missing_col", "uint");
    write_transform(&col_dir, PASSTHROUGH_TRANSFORM);

    let err = run_async(&paths, &env_config).await.unwrap_err();
    assert!(
        err.to_string().contains("error"),
        "expected apply error for missing column, got: {err}"
    );
}

#[tokio::test]
async fn rejects_incompatible_id_type() {
    let (_dir, paths, state_db_path) = setup_project();
    let (_ctx, env_config) = start_postgres_env(state_db_path).await;

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
    let typed_dir = write_config(&paths, "typed", "public", "type_test", "id", "uint");
    write_transform(&typed_dir, PASSTHROUGH_TRANSFORM);

    let err = run_async(&paths, &env_config).await.unwrap_err();
    assert!(
        err.to_string().contains("error"),
        "expected apply error for incompatible id type, got: {err}"
    );
}

#[tokio::test]
async fn rejects_vector_without_distance_metric() {
    let (_dir, paths, state_db_path) = setup_project();
    let (_ctx, env_config) = start_postgres_env(state_db_path).await;

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
    let vec_dir = write_config(&paths, "vec", "public", "vec_test", "id", "uint");
    write_transform(&vec_dir, VECTOR_NO_METRIC_TRANSFORM);

    let err = run_async(&paths, &env_config).await.unwrap_err();
    assert!(
        err.to_string().contains("error"),
        "expected apply error for vector without distance_metric, got: {err}"
    );
}

#[tokio::test]
async fn accepts_vector_with_distance_metric() {
    let (_dir, paths, state_db_path) = setup_project();
    let (_ctx, env_config) = start_postgres_env(state_db_path).await;

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
    let goodvec_dir = write_config(&paths, "goodvec", "public", "good_vec", "id", "uint");
    write_transform(&goodvec_dir, VECTOR_WITH_METRIC_TRANSFORM);

    let result = run_async(&paths, &env_config).await;
    assert!(result.is_ok(), "expected apply to succeed, got: {result:?}");
}

#[tokio::test]
async fn accepts_empty_table_skips_dry_run() {
    let (_dir, paths, state_db_path) = setup_project();
    let (_ctx, env_config) = start_postgres_env(state_db_path).await;

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
    let empty_dir = write_config(&paths, "empty", "public", "empty_apply", "id", "uint");
    write_transform(&empty_dir, PASSTHROUGH_TRANSFORM);

    let result = run_async(&paths, &env_config).await;
    assert!(
        result.is_ok(),
        "expected apply to succeed on empty table, got: {result:?}"
    );
}
