use pg::PgLsn;
use state::StateDb;

use crate::error::CliError;
use crate::paths::ProjectPaths;

pub fn run(paths: &ProjectPaths) -> Result<(), CliError> {
    let mut db = StateDb::open(&paths.state_db)?;
    let configs = db.list_configs()?;

    if configs.is_empty() {
        println!("No configs applied yet. Run `puffgres apply` to apply configs.");
        return Ok(());
    }

    let checkpoints = db.list_streaming_checkpoints()?;
    let checkpoint_map: std::collections::HashMap<&str, &state::StreamingCheckpoint> = checkpoints
        .iter()
        .map(|c| (c.config_name.as_str(), c))
        .collect();

    println!(
        "{:<20} {:<20} {:<16} {}",
        "CONFIG", "NAMESPACE", "LSN", "EVENTS"
    );
    println!("{}", "-".repeat(67));

    for config in &configs {
        match checkpoint_map.get(config.name.as_str()) {
            Some(cp) => {
                let lsn = PgLsn::from(cp.lsn);
                println!(
                    "{:<20} {:<20} {:<16} {}",
                    config.name, config.namespace, lsn, cp.events_processed,
                );
            }
            None => {
                println!(
                    "{:<20} {:<20} {:<16} {}",
                    config.name, config.namespace, "-", "-",
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

    fn sample_config(name: &str) -> ConfigRecord {
        ConfigRecord {
            name: name.to_string(),
            namespace: name.to_string(),
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
        let mut db = StateDb::open(&paths.state_db).unwrap();

        db.insert_config(&sample_config("film")).unwrap();
        db.insert_config(&sample_config("actor")).unwrap();

        run(&paths).unwrap();
    }

    #[test]
    fn configs_with_checkpoints() {
        let (_dir, paths) = setup_project();
        let mut db = StateDb::open(&paths.state_db).unwrap();

        db.insert_config(&sample_config("film")).unwrap();
        db.insert_config(&sample_config("actor")).unwrap();
        db.save_streaming_checkpoint(&sample_checkpoint("film", 0x016B_3740, 500))
            .unwrap();

        run(&paths).unwrap();
    }

    #[test]
    fn mixed_configs_some_with_checkpoints() {
        let (_dir, paths) = setup_project();
        let mut db = StateDb::open(&paths.state_db).unwrap();

        db.insert_config(&sample_config("film")).unwrap();
        db.insert_config(&sample_config("actor")).unwrap();
        db.insert_config(&sample_config("genre")).unwrap();

        db.save_streaming_checkpoint(&sample_checkpoint("film", 0x016B_3740, 500))
            .unwrap();
        db.save_streaming_checkpoint(&sample_checkpoint("genre", 0x0000_0002_ABCD_EF01, 1200))
            .unwrap();

        run(&paths).unwrap();
    }
}
