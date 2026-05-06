use std::path::Path;

use config::ConfigLoader;
use state::StateStore;

use crate::error::CliError;
use crate::paths::ProjectPaths;

pub fn has_on_disk_tombstone(config_path: &Path) -> bool {
    config_path
        .parent()
        .unwrap()
        .join("tombstone.toml")
        .exists()
}

pub fn reconcile_on_disk_tombstones(
    paths: &ProjectPaths,
    db: &impl StateStore,
) -> Result<Vec<String>, CliError> {
    let loader = ConfigLoader::new(&paths.configs);
    let configs = loader.load_all()?;
    let mut tombstoned = Vec::new();

    for (config_path, config) in configs {
        if !has_on_disk_tombstone(&config_path) {
            continue;
        }
        let tombstone_path = config_path.parent().unwrap().join("tombstone.toml");

        if let Some(existing) = db.get_config(&config.name)?
            && existing.tombstone_applied_at.is_none()
        {
            tracing::info!(
                config = %config.name,
                tombstone_path = %tombstone_path.display(),
                "found tombstone marker on disk, applying tombstone to state db",
            );
            db.tombstone_config(&config.name)?;
            println!("Tombstoned: {}", config.name);
            tombstoned.push(config.name);
        }
    }

    Ok(tombstoned)
}
