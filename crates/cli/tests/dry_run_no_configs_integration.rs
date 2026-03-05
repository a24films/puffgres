use puffgres_cli::EnvConfig;
use puffgres_cli::dry_run::run_async;
use puffgres_cli::test_utils::setup_project;

#[tokio::test]
async fn named_dry_run_fails_with_no_configs() {
    let (_dir, paths, state_db_path) = setup_project();

    // Use a dummy env_config; the error fires before any Postgres connection.
    let env_config = EnvConfig {
        database_url: String::new(),
        turbopuffer_api_key: String::new(),
        turbopuffer_region: None,
        turbopuffer_namespace_prefix: None,
        otel_endpoint: None,
        otel_headers: None,
        state_db_path,
        dlq_max_age_hours: None,
    };

    let err = run_async(&paths, &env_config, Some("nonexistent_0001"))
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("no config found matching"),
        "expected missing-config error, got: {err}"
    );
}
