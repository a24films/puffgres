use std::path::PathBuf;

use config::Config;

use crate::env::EnvConfig;

/// Validate configs against Postgres schema: check that source tables exist.
/// Returns the indices of configs that passed, or a list of error messages.
pub async fn validate_tables(
    env_config: &EnvConfig,
    configs: &[(PathBuf, Config)],
) -> Result<Vec<usize>, Vec<String>> {
    let mut passed: Vec<usize> = Vec::new();
    let mut errors: Vec<String> = Vec::new();

    let pg_client = pg::connect::connect(&env_config.database_url)
        .await
        .map_err(|e| vec![format!("failed to connect to postgres: {e}")])?;

    for (i, (path, config)) in configs.iter().enumerate() {
        let display = path.display();

        let table_refs = vec![(config.source.schema.as_str(), config.source.table.as_str())];
        if let Err(e) = pg::connect::validate_tables(&pg_client, &table_refs).await {
            errors.push(format!("{display}: {e}"));
            continue;
        }

        passed.push(i);
    }

    if !errors.is_empty() {
        return Err(errors);
    }

    Ok(passed)
}
