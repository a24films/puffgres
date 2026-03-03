use std::path::PathBuf;

use crate::error::CliError;
use crate::generate;
use crate::paths::ProjectPaths;
use crate::validate::preflight_check;

pub fn run(
    paths: &ProjectPaths,
    database_url: &str,
    state_db_path: &PathBuf,
) -> Result<(), CliError> {
    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| CliError::Check(format!("failed to create async runtime: {e}")))?;
    rt.block_on(run_async(paths, database_url, state_db_path))
}

pub async fn run_async(
    paths: &ProjectPaths,
    database_url: &str,
    state_db_path: &PathBuf,
) -> Result<(), CliError> {
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

    preflight_check(database_url, state_db_path, &configs, None)
        .await
        .map_err(CliError::Check)?;

    Ok(())
}
