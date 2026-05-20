use pg::test_utils::setup_postgres;
use puffgres_cli::EnvConfig;
use puffgres_cli::check::run_async as check_async;
use puffgres_cli::generate::run_async as generate_async;
use puffgres_cli::test_utils::{
    PASSTHROUGH_TRANSFORM, setup_project, write_config, write_config_with_columns, write_transform,
};

async fn start_postgres_env() -> (pg::test_utils::TestContext, EnvConfig) {
    let ctx = setup_postgres().await;
    let env_config = EnvConfig {
        database_url: ctx.connection_url.clone(),
        turbopuffer_api_key: "fake-key".to_string(),
        turbopuffer_region: None,
        turbopuffer_namespace_prefix: None,
        otel_endpoint: None,
        otel_headers: None,
        state_schema: "puffgres".to_string(),
        dlq_max_age_hours: None,
        inspect_port: None,
    };
    (ctx, env_config)
}

async fn setup_pg(tables: &[(&str, &str)]) -> (pg::test_utils::TestContext, EnvConfig) {
    let (ctx, env_config) = start_postgres_env().await;
    let pg_client = pg::connect::connect(&env_config.database_url)
        .await
        .unwrap();
    for (table, ddl) in tables {
        pg_client
            .execute(&format!("CREATE TABLE {table} ({ddl})"), &[])
            .await
            .unwrap();
    }
    drop(pg_client);
    (ctx, env_config)
}

#[tokio::test]
async fn generate_check_lifecycle() {
    let (_dir, paths) = setup_project();
    let (_ctx, env_config) =
        setup_pg(&[("users", "id SERIAL PRIMARY KEY, name TEXT, email VARCHAR")]).await;

    let user_dir = write_config(&paths, "user", "public", "users", "id", "uint");
    write_transform(&user_dir, PASSTHROUGH_TRANSFORM);

    // 1. Generate schema.ts
    generate_async(&paths, &env_config.database_url)
        .await
        .unwrap();

    let schema_path = user_dir.join("schema.ts");
    assert!(schema_path.exists(), "schema.ts should be created");

    let content = std::fs::read_to_string(&schema_path).unwrap();
    assert!(content.contains("// Source: public.users"));
    assert!(content.contains(r#"name: "id""#));
    assert!(content.contains(r#"name: "name""#));
    assert!(content.contains(r#"name: "email""#));
    assert!(content.contains("parseRow"));

    // 2. Check should succeed
    check_async(
        &paths,
        &env_config.database_url,
        &env_config.state_schema,
        &puffgres_cli::ProjectConfig::default(),
    )
    .await
    .unwrap();

    // 3. ALTER TABLE to add a column → check should fail (schema drift)
    let pg_client = pg::connect::connect(&env_config.database_url)
        .await
        .unwrap();
    pg_client
        .execute("ALTER TABLE users ADD COLUMN age INT", &[])
        .await
        .unwrap();
    drop(pg_client);

    let check_result = check_async(
        &paths,
        &env_config.database_url,
        &env_config.state_schema,
        &puffgres_cli::ProjectConfig::default(),
    )
    .await;
    assert!(
        check_result.is_err(),
        "check should fail after ALTER TABLE: {check_result:?}"
    );
    let err = check_result.unwrap_err().to_string();
    assert!(
        err.contains("puffgres generate"),
        "error should suggest running generate: {err}"
    );

    // 4. Re-generate → schema.ts should be updated with new column
    generate_async(&paths, &env_config.database_url)
        .await
        .unwrap();

    let updated = std::fs::read_to_string(&schema_path).unwrap();
    assert!(
        updated.contains(r#""age""#),
        "updated schema should contain 'age' column"
    );

    // 5. Check should succeed again
    check_async(
        &paths,
        &env_config.database_url,
        &env_config.state_schema,
        &puffgres_cli::ProjectConfig::default(),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn tombstoned_config_skipped() {
    let (_dir, paths) = setup_project();
    let (_ctx, env_config) = setup_pg(&[("items", "id SERIAL PRIMARY KEY, title TEXT")]).await;

    let item_dir = write_config(&paths, "item", "public", "items", "id", "uint");
    write_transform(&item_dir, PASSTHROUGH_TRANSFORM);

    // Tombstone the config
    std::fs::write(
        item_dir.join("tombstone.toml"),
        "tombstoned_at = \"2025-01-01T00:00:00Z\"\n",
    )
    .unwrap();

    // Generate should skip it
    generate_async(&paths, &env_config.database_url)
        .await
        .unwrap();

    let schema_path = item_dir.join("schema.ts");
    assert!(
        !schema_path.exists(),
        "schema.ts should NOT be created for tombstoned config"
    );
}

#[tokio::test]
async fn config_columns_filtering() {
    let (_dir, paths) = setup_project();
    let (_ctx, env_config) = setup_pg(&[(
        "products",
        "id SERIAL PRIMARY KEY, name TEXT, price NUMERIC, description TEXT",
    )])
    .await;

    // Config only selects id and name columns
    let prod_dir = write_config_with_columns(
        &paths,
        "product",
        "public",
        "products",
        "id",
        "uint",
        &["name", "id"],
    );
    write_transform(&prod_dir, PASSTHROUGH_TRANSFORM);

    generate_async(&paths, &env_config.database_url)
        .await
        .unwrap();

    let content = std::fs::read_to_string(prod_dir.join("schema.ts")).unwrap();

    // Should have only name and id, in config order (name first, then id)
    assert!(content.contains(r#"{ name: "name", type: "string" },"#));
    assert!(content.contains(r#"{ name: "id", type: "int" },"#));
    // Should NOT contain price or description
    assert!(!content.contains("price"));
    assert!(!content.contains("description"));
}
