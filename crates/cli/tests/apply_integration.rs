use pg::test_utils::setup_postgres;
use puffgres_cli::EnvConfig;
use puffgres_cli::apply::run_async;
use puffgres_cli::test_utils::{
    PASSTHROUGH_TRANSFORM, VECTOR_NO_METRIC_TRANSFORM, VECTOR_WITH_METRIC_TRANSFORM, setup_project,
    write_config, write_transform,
};
use state::StateDb;

async fn start_postgres_env() -> (pg::test_utils::TestContext, EnvConfig) {
    let ctx = setup_postgres().await;
    let env_config = EnvConfig {
        database_url: ctx.connection_string.clone(),
        turbopuffer_api_key: "fake-key".to_string(),
        turbopuffer_region: None,
        turbopuffer_namespace_prefix: None,
        otel_endpoint: None,
        otel_headers: None,
    };
    (ctx, env_config)
}

async fn setup_pg(tables: &[&str]) -> (pg::test_utils::TestContext, EnvConfig) {
    let (ctx, env_config) = start_postgres_env().await;
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
async fn test_apply_and_idempotency() {
    let (_ctx, env_config) = setup_pg(&["users", "films"]).await;
    let (_dir, paths) = setup_project();

    write_config(&paths, "user", 1, "public", "users", "id", "uint");
    write_config(&paths, "film", 2, "public", "films", "id", "uint");
    write_transform(&paths, "user", PASSTHROUGH_TRANSFORM);
    write_transform(&paths, "film", PASSTHROUGH_TRANSFORM);

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
    let (_ctx, env_config) = setup_pg(&["users", "accounts"]).await;
    let (_dir, paths) = setup_project();

    write_config(&paths, "user", 1, "public", "users", "id", "uint");
    write_transform(&paths, "user", PASSTHROUGH_TRANSFORM);
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
    let (_ctx, env_config) = start_postgres_env().await;
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
    write_transform(&paths, "ghost", PASSTHROUGH_TRANSFORM);

    let err = run_async(&paths, &env_config).await.unwrap_err();
    assert!(
        err.to_string().contains("error"),
        "expected apply error, got: {err}"
    );
}

#[tokio::test]
async fn test_rejects_nonexistent_id_column() {
    let (_ctx, env_config) = start_postgres_env().await;

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
    write_transform(&paths, "col", PASSTHROUGH_TRANSFORM);

    let err = run_async(&paths, &env_config).await.unwrap_err();
    assert!(
        err.to_string().contains("error"),
        "expected apply error for missing column, got: {err}"
    );
}

#[tokio::test]
async fn test_rejects_incompatible_id_type() {
    let (_ctx, env_config) = start_postgres_env().await;

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
    write_transform(&paths, "typed", PASSTHROUGH_TRANSFORM);

    let err = run_async(&paths, &env_config).await.unwrap_err();
    assert!(
        err.to_string().contains("error"),
        "expected apply error for incompatible id type, got: {err}"
    );
}

#[tokio::test]
async fn test_rejects_vector_without_distance_metric() {
    let (_ctx, env_config) = start_postgres_env().await;

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
    write_transform(&paths, "vec", VECTOR_NO_METRIC_TRANSFORM);

    let err = run_async(&paths, &env_config).await.unwrap_err();
    assert!(
        err.to_string().contains("error"),
        "expected apply error for vector without distance_metric, got: {err}"
    );
}

#[tokio::test]
async fn test_accepts_vector_with_distance_metric() {
    let (_ctx, env_config) = start_postgres_env().await;

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
    write_transform(&paths, "goodvec", VECTOR_WITH_METRIC_TRANSFORM);

    let result = run_async(&paths, &env_config).await;
    assert!(result.is_ok(), "expected apply to succeed, got: {result:?}");
}

#[tokio::test]
async fn test_accepts_empty_table_skips_dry_run() {
    let (_ctx, env_config) = start_postgres_env().await;

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
    write_transform(&paths, "empty", PASSTHROUGH_TRANSFORM);

    let result = run_async(&paths, &env_config).await;
    assert!(
        result.is_ok(),
        "expected apply to succeed on empty table, got: {result:?}"
    );
}
