use pg::test_utils::setup_postgres;
use puffgres_cli::EnvConfig;
use puffgres_cli::apply::run_async;
use puffgres_cli::test_utils::{setup_project, write_config, write_passthrough_transform};
use state::StateDb;

async fn start_postgres_env() -> (pg::test_utils::TestContext, EnvConfig) {
    let ctx = setup_postgres().await;
    let env_config = EnvConfig {
        database_url: ctx.connection_string.clone(),
        turbopuffer_api_key: "fake-key".to_string(),
        turbopuffer_region: None,
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
    let (_ctx, env_config) = setup_pg(&["users", "accounts"]).await;
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
    write_passthrough_transform(&paths, "ghost");

    let err = run_async(&paths, &env_config).await.unwrap_err();
    assert!(
        err.to_string().contains("error"),
        "expected apply error, got: {err}"
    );
}
