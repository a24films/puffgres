use std::time::Duration;

use crate::error::CliError;
use crate::generate;
use crate::paths::ProjectPaths;
use crate::project_config::ProjectConfig;
use crate::validate::preflight_check;

/// Regenerate `schema.ts` from the live database, then validate configs against
/// it. `name` narrows validation to one config. Never writes state, so it's safe
/// before `apply`.
pub async fn run_async(
    paths: &ProjectPaths,
    database_url: &str,
    state_schema: &str,
    project_config: &ProjectConfig,
    name: Option<&str>,
) -> Result<(), CliError> {
    let transform_timeout = Duration::from_secs(project_config.transform_timeout_secs());
    let loader = config::ConfigLoader::new(&paths.configs);
    let mut configs = loader.load_all()?;

    if configs.is_empty() {
        println!("No config files found in configs/");
        return Ok(());
    }

    if let Some(name) = name {
        configs.retain(|(_, c)| c.name == name);
        if configs.is_empty() {
            return Err(CliError::Check(format!(
                "no config found matching '{name}'"
            )));
        }
    }

    // Regenerate schema.ts from the live database so the typed schema is current
    // before preflight dry-runs the transforms.
    generate::run_async(paths, database_url).await?;

    preflight_check(
        database_url,
        state_schema,
        &configs,
        None,
        transform_timeout,
        true,
    )
    .await
    .map_err(CliError::Check)?;

    Ok(())
}
