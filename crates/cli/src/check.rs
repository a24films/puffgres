use std::path::PathBuf;

use config::Config;

use crate::env::EnvConfig;
use crate::error::CliError;
use crate::paths::ProjectPaths;
use crate::validate::validate_schema;

pub fn run(
    paths: &ProjectPaths,
    env_config: &EnvConfig,
    name: Option<&str>,
) -> Result<(), CliError> {
    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| CliError::Check(format!("failed to create async runtime: {e}")))?;
    rt.block_on(run_async(paths, env_config, name))
}

async fn run_async(
    paths: &ProjectPaths,
    env_config: &EnvConfig,
    name: Option<&str>,
) -> Result<(), CliError> {
    let loader = config::ConfigLoader::new(&paths.configs);
    let configs = loader.load_all()?;

    if configs.is_empty() {
        eprintln!("No config files found in configs/");
        return Ok(());
    }

    // Filter by name if provided
    let configs: Vec<(PathBuf, Config)> = if let Some(name) = name {
        configs
            .into_iter()
            .filter(|(_, c)| c.name == name)
            .collect()
    } else {
        configs
    };

    if configs.is_empty() {
        return Err(CliError::Check(format!(
            "no config found matching '{}'",
            name.unwrap_or("")
        )));
    }

    // Structural validation
    let mut errors: Vec<String> = Vec::new();
    let mut valid_configs: Vec<(PathBuf, Config)> = Vec::new();

    for (path, config) in &configs {
        if let Err(validation_errors) = config.validate() {
            for err in &validation_errors {
                errors.push(format!(
                    "{}: {} - {}",
                    path.display(),
                    err.field,
                    err.message
                ));
            }
            continue;
        }

        // Check transform file exists
        let transform_path = paths.root.join(&config.transform.path);
        if !transform_path.exists() {
            errors.push(format!(
                "{}: transform file '{}' does not exist",
                path.display(),
                config.transform.path,
            ));
            continue;
        }

        valid_configs.push((path.clone(), config.clone()));
    }

    if !errors.is_empty() {
        for err in &errors {
            eprintln!("Error: {}", err);
        }
        return Err(CliError::Check(format!(
            "{} config(s) had errors",
            errors.len()
        )));
    }

    // Live schema validation
    match validate_schema(env_config, &valid_configs).await {
        Ok(passed) => {
            eprintln!("{} config(s) passed", passed.len());
            Ok(())
        }
        Err(errors) => {
            for err in &errors {
                eprintln!("Error: {}", err);
            }
            Err(CliError::Check(format!(
                "{} config(s) had errors",
                errors.len()
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::test_utils::{
        setup_project, start_postgres, write_config, write_passthrough_transform,
    };

    #[tokio::test]
    async fn rejects_nonexistent_table() {
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

        let (_container, env_config) = start_postgres().await;

        let err = run_async(&paths, &env_config, None).await.unwrap_err();
        assert!(
            err.to_string().contains("error"),
            "expected check error, got: {err}"
        );
    }

    #[tokio::test]
    async fn rejects_nonexistent_id_column() {
        let (_container, env_config) = start_postgres().await;

        let pg_client = pg::connect::connect(&env_config.database_url)
            .await
            .unwrap();
        pg_client
            .execute(
                "CREATE TABLE check_col_test (id SERIAL PRIMARY KEY, name TEXT)",
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
            "check_col_test",
            "missing_col",
            "uint",
        );
        write_passthrough_transform(&paths, "col");

        let err = run_async(&paths, &env_config, None).await.unwrap_err();
        assert!(
            err.to_string().contains("error"),
            "expected check error for missing column, got: {err}"
        );
    }

    #[tokio::test]
    async fn rejects_incompatible_id_type() {
        let (_container, env_config) = start_postgres().await;

        let pg_client = pg::connect::connect(&env_config.database_url)
            .await
            .unwrap();
        pg_client
            .execute(
                "CREATE TABLE check_type_test (id TEXT PRIMARY KEY, name TEXT)",
                &[],
            )
            .await
            .unwrap();
        drop(pg_client);

        let (_dir, paths) = setup_project();
        write_config(
            &paths,
            "typed",
            1,
            "public",
            "check_type_test",
            "id",
            "uint",
        );
        write_passthrough_transform(&paths, "typed");

        let err = run_async(&paths, &env_config, None).await.unwrap_err();
        assert!(
            err.to_string().contains("error"),
            "expected check error for incompatible id type, got: {err}"
        );
    }

    #[tokio::test]
    async fn accepts_valid_config() {
        let (_container, env_config) = start_postgres().await;

        let pg_client = pg::connect::connect(&env_config.database_url)
            .await
            .unwrap();
        pg_client
            .execute(
                "CREATE TABLE check_valid (id SERIAL PRIMARY KEY, name TEXT)",
                &[],
            )
            .await
            .unwrap();
        drop(pg_client);

        let (_dir, paths) = setup_project();
        write_config(&paths, "valid", 1, "public", "check_valid", "id", "uint");
        write_passthrough_transform(&paths, "valid");

        let result = run_async(&paths, &env_config, None).await;
        assert!(result.is_ok(), "expected check to succeed, got: {result:?}");
    }

    #[tokio::test]
    async fn filters_by_config_name() {
        let (_container, env_config) = start_postgres().await;

        let pg_client = pg::connect::connect(&env_config.database_url)
            .await
            .unwrap();
        pg_client
            .execute(
                "CREATE TABLE check_filter (id SERIAL PRIMARY KEY, name TEXT)",
                &[],
            )
            .await
            .unwrap();
        drop(pg_client);

        let (_dir, paths) = setup_project();
        write_config(&paths, "good", 1, "public", "check_filter", "id", "uint");
        write_passthrough_transform(&paths, "good");

        write_config(
            &paths,
            "bad",
            1,
            "public",
            "nonexistent_table",
            "id",
            "uint",
        );
        write_passthrough_transform(&paths, "bad");

        // Checking only the good config should pass
        let result = run_async(&paths, &env_config, Some("good_0001")).await;
        assert!(
            result.is_ok(),
            "expected check to succeed for filtered config, got: {result:?}"
        );

        // Checking only the bad config should fail
        let err = run_async(&paths, &env_config, Some("bad_0001"))
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("error"),
            "expected check error for bad config, got: {err}"
        );
    }
}
