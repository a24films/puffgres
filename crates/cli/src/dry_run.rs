use std::path::PathBuf;

use config::Config;

use crate::dry_transform::dry_run_transform;
use crate::env::EnvConfig;
use crate::error::CliError;
use crate::paths::ProjectPaths;

pub fn run(
    paths: &ProjectPaths,
    env_config: &EnvConfig,
    name: Option<&str>,
) -> Result<(), CliError> {
    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| CliError::DryRun(format!("failed to create async runtime: {e}")))?;
    rt.block_on(run_async(paths, env_config, name))
}

pub async fn run_async(
    paths: &ProjectPaths,
    env_config: &EnvConfig,
    name: Option<&str>,
) -> Result<(), CliError> {
    let loader = config::ConfigLoader::new(&paths.configs);
    let configs = loader.load_all()?;

    if configs.is_empty() {
        if let Some(name) = name {
            return Err(CliError::DryRun(format!(
                "no config found matching '{name}'"
            )));
        }
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
        return Err(CliError::DryRun(format!(
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
        return Err(CliError::DryRun(format!(
            "{} config(s) had errors",
            errors.len()
        )));
    }

    // Connect to Postgres and dry-run each config
    let pg_client = pg::connect::connect(&env_config.database_url)
        .await
        .map_err(|e| CliError::DryRun(format!("failed to connect to postgres: {e}")))?;

    let mut dry_run_errors: Vec<String> = Vec::new();
    let mut passed = 0;
    let mut skipped = 0;

    for (path, config) in &valid_configs {
        let display = path.display();

        let sample = match pg::sample::fetch_sample_row(
            &pg_client,
            &config.source.schema,
            &config.source.table,
        )
        .await
        {
            Ok(s) => s,
            Err(e) => {
                dry_run_errors.push(format!("{display}: failed to fetch sample row: {e}"));
                continue;
            }
        };

        match sample {
            Some((column_names, values)) => {
                match dry_run_transform(paths, config, &column_names, &values).await {
                    Ok(actions) => {
                        eprintln!("{display}: transform returned {} action(s):", actions.len());
                        for action in &actions {
                            eprintln!("  {action:?}");
                        }
                        passed += 1;
                    }
                    Err(e) => {
                        dry_run_errors.push(format!("{display}: {e}"));
                        continue;
                    }
                }
            }
            None => {
                eprintln!("{display}: table is empty, skipping dry-run");
                skipped += 1;
            }
        }
    }

    if !dry_run_errors.is_empty() {
        for err in &dry_run_errors {
            eprintln!("Error: {}", err);
        }
        return Err(CliError::DryRun(format!(
            "{} config(s) had errors",
            dry_run_errors.len()
        )));
    }

    eprintln!("{passed} passed, {skipped} skipped (empty tables)");
    Ok(())
}
