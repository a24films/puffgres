use std::time::Duration;

use crate::error::CliError;
use crate::generate;
use crate::paths::ProjectPaths;
use crate::project_config::ProjectConfig;
use crate::validate::preflight_check;

pub async fn run_async(
    paths: &ProjectPaths,
    database_url: &str,
    state_schema: &str,
    project_config: &ProjectConfig,
) -> Result<(), CliError> {
    let transform_timeout = Duration::from_secs(project_config.transform_timeout_secs());
    let loader = config::ConfigLoader::new(&paths.configs);
    let configs = loader.load_all()?;

    if configs.is_empty() {
        println!("No config files found in configs/");
        return Ok(());
    }

    // Verify schema.ts files are current before preflight (which dry-runs transforms)
    let schema_errors = generate::verify_schemas(&configs, database_url).await?;
    if !schema_errors.is_empty() {
        for err in &schema_errors {
            println!("  schema: {err}");
        }
        return Err(CliError::Check(format!(
            "{} config(s) have schema.ts issues. Run `puffgres generate`",
            schema_errors.len()
        )));
    }

    preflight_check(
        database_url,
        state_schema,
        &configs,
        None,
        transform_timeout,
    )
    .await
    .map_err(CliError::Check)?;

    Ok(())
}
