use pg::PgLsn;
use state::StateDb;

use crate::error::CliError;
use crate::paths::ProjectPaths;

pub fn run(paths: &ProjectPaths) -> Result<(), CliError> {
    let db = StateDb::open(&paths.state_db)?;
    let configs = db.list_configs()?;

    if configs.is_empty() {
        eprintln!("No configs applied yet. Run `puffgres apply` to apply configs.");
        return Ok(());
    }

    let checkpoints = db.list_streaming_checkpoints()?;
    let checkpoint_map: std::collections::HashMap<&str, &state::StreamingCheckpoint> = checkpoints
        .iter()
        .map(|c| (c.config_name.as_str(), c))
        .collect();

    eprintln!(
        "{:<20} {:<20} {:<8} {:<16} {}",
        "CONFIG", "NAMESPACE", "VERSION", "LSN", "EVENTS"
    );
    eprintln!("{}", "-".repeat(76));

    for config in &configs {
        match checkpoint_map.get(config.name.as_str()) {
            Some(cp) => {
                let lsn = PgLsn::from(cp.lsn);
                eprintln!(
                    "{:<20} {:<20} {:<8} {:<16} {}",
                    config.name, config.namespace, config.version, lsn, cp.events_processed,
                );
            }
            None => {
                eprintln!(
                    "{:<20} {:<20} {:<8} {:<16} {}",
                    config.name, config.namespace, config.version, "-", "-",
                );
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use state::{ConfigRecord, StreamingCheckpoint};

    use crate::test_utils::setup_project;

    fn sample_config(name: &str, version: i64) -> ConfigRecord {
        ConfigRecord {
            name: name.to_string(),
            version,
            namespace: format!("{}_v{}", name, version),
            content_hash: "abc123".to_string(),
            transform_hash: None,
            applied_at: Utc::now(),
        }
    }

    fn sample_checkpoint(config_name: &str, lsn: u64, events: u64) -> StreamingCheckpoint {
        StreamingCheckpoint {
            config_name: config_name.to_string(),
            lsn,
            events_processed: events,
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn no_configs_succeeds() {
        let (_dir, paths) = setup_project();
        run(&paths).unwrap();
    }

    #[test]
    fn configs_without_checkpoints() {
        let (_dir, paths) = setup_project();
        let db = StateDb::open(&paths.state_db).unwrap();

        db.insert_config(&sample_config("film", 1)).unwrap();
        db.insert_config(&sample_config("actor", 2)).unwrap();

        run(&paths).unwrap();
    }

    #[test]
    fn configs_with_checkpoints() {
        let (_dir, paths) = setup_project();
        let db = StateDb::open(&paths.state_db).unwrap();

        db.insert_config(&sample_config("film", 1)).unwrap();
        db.insert_config(&sample_config("actor", 2)).unwrap();
        db.save_streaming_checkpoint(&sample_checkpoint("film", 0x016B_3740, 500))
            .unwrap();

        run(&paths).unwrap();
    }

    #[test]
    fn mixed_configs_some_with_checkpoints() {
        let (_dir, paths) = setup_project();
        let db = StateDb::open(&paths.state_db).unwrap();

        db.insert_config(&sample_config("film", 1)).unwrap();
        db.insert_config(&sample_config("actor", 2)).unwrap();
        db.insert_config(&sample_config("genre", 1)).unwrap();

        db.save_streaming_checkpoint(&sample_checkpoint("film", 0x016B_3740, 500))
            .unwrap();
        db.save_streaming_checkpoint(&sample_checkpoint("genre", 0x0000_0002_ABCD_EF01, 1200))
            .unwrap();

        run(&paths).unwrap();
    }
}
