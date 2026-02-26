use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::time::Duration;

use chrono::Utc;
use config::ConfigLoader;
use puff::TurbopufferClient;
use puffgres_core::{JsTransformer, Mapping, Router, Transformer};
use replication::{ReplicationStream, ReplicationStreamConfig};
use sha2::{Digest, Sha256};
use state::{StateDb, StreamingCheckpoint};

use crate::env::EnvConfig;
use crate::error::CliError;
use crate::paths::ProjectPaths;

const SLOT_NAME: &str = "puffgres";
const PUBLICATION_NAME: &str = "puffgres";
const STATUS_INTERVAL: Duration = Duration::from_secs(10);

pub fn run(paths: &ProjectPaths, env_config: &EnvConfig) -> Result<(), CliError> {
    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| CliError::Run(format!("failed to create async runtime: {e}")))?;
    rt.block_on(run_async(paths, env_config))
}

fn prefixed_namespace(prefix: &Option<String>, namespace: &str) -> String {
    match prefix {
        Some(p) if !p.is_empty() => format!("{}_{}", p, namespace),
        _ => namespace.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefixed_namespace_with_prefix() {
        let prefix = Some("production".to_string());
        assert_eq!(prefixed_namespace(&prefix, "user_v1"), "production_user_v1");
    }

    #[test]
    fn prefixed_namespace_without_prefix() {
        assert_eq!(prefixed_namespace(&None, "user_v1"), "user_v1");
    }

    #[test]
    fn prefixed_namespace_empty_prefix_treated_as_none() {
        let prefix = Some("".to_string());
        assert_eq!(prefixed_namespace(&prefix, "user_v1"), "user_v1");
    }

    use crate::test_utils::{PASSTHROUGH_TRANSFORM, setup_project, write_config, write_transform};
    use state::ConfigRecord;

    fn dummy_env() -> EnvConfig {
        EnvConfig {
            database_url: "host=invalid".to_string(),
            turbopuffer_api_key: "fake".to_string(),
            turbopuffer_region: None,
            turbopuffer_namespace_prefix: None,
        }
    }

    #[test]
    fn test_errors_on_unreadable_transform_for_applied_config() {
        let (_dir, paths) = setup_project();
        write_config(&paths, "user", 1, "public", "users", "id", "uint");
        write_transform(&paths, "user", PASSTHROUGH_TRANSFORM);

        let loader = ConfigLoader::new(&paths.configs);
        let cfg = &loader.load_all().unwrap()[0].1;
        let transform_bytes = fs::read(paths.root.join(&cfg.transform.path)).unwrap();
        let transform_hash = format!("{:x}", Sha256::digest(&transform_bytes));
        let db = StateDb::open(&paths.state_db).unwrap();
        db.insert_config(&ConfigRecord {
            name: cfg.name.clone(),
            version: cfg.version,
            namespace: cfg.full_namespace(),
            content_hash: cfg.content_hash().unwrap(),
            transform_hash: Some(transform_hash),
            applied_at: Utc::now(),
        })
        .unwrap();

        // Delete the transform file so it can't be read
        fs::remove_file(paths.root.join(&cfg.transform.path)).unwrap();

        let err = run(&paths, &dummy_env()).unwrap_err();
        assert!(
            err.to_string().contains("cannot read transform"),
            "expected unreadable transform error, got: {err}"
        );
    }

    #[test]
    fn test_errors_on_modified_transform_for_applied_config() {
        let (_dir, paths) = setup_project();
        write_config(&paths, "user", 1, "public", "users", "id", "uint");
        write_transform(&paths, "user", PASSTHROUGH_TRANSFORM);

        let loader = ConfigLoader::new(&paths.configs);
        let cfg = &loader.load_all().unwrap()[0].1;
        let db = StateDb::open(&paths.state_db).unwrap();
        db.insert_config(&ConfigRecord {
            name: cfg.name.clone(),
            version: cfg.version,
            namespace: cfg.full_namespace(),
            content_hash: cfg.content_hash().unwrap(),
            transform_hash: Some("stale_hash".to_string()),
            applied_at: Utc::now(),
        })
        .unwrap();

        let err = run(&paths, &dummy_env()).unwrap_err();
        assert!(
            err.to_string().contains("was modified"),
            "expected modified transform error, got: {err}"
        );
    }
}

async fn run_async(paths: &ProjectPaths, env_config: &EnvConfig) -> Result<(), CliError> {
    let db = StateDb::open(&paths.state_db)?;

    let loader = ConfigLoader::new(&paths.configs);
    let all_configs = loader.load_all()?;

    let applied_configs: Vec<_> = all_configs
        .into_iter()
        .filter(|(_, config)| db.get_config(&config.name).ok().flatten().is_some())
        .map(|(_, config)| config)
        .collect();

    if applied_configs.is_empty() {
        eprintln!("No applied configs. Run `puffgres apply` first.");
        return Ok(());
    }

    let mut mappings = Vec::new();
    let mut transformers: HashMap<String, Box<dyn Transformer>> = HashMap::new();
    let mut namespaces: HashMap<String, String> = HashMap::new();
    let mut tables: BTreeSet<String> = BTreeSet::new();

    for config in &applied_configs {
        mappings.push(Mapping::from_config(config));
        namespaces.insert(
            config.name.clone(),
            prefixed_namespace(
                &env_config.turbopuffer_namespace_prefix,
                &config.full_namespace(),
            ),
        );
        tables.insert(format!("{}.{}", config.source.schema, config.source.table));

        let transform_path = paths.root.join(&config.transform.path);

        // Verify transform file can be read and hasn't drifted from the applied hash
        let transform_content = fs::read(&transform_path).map_err(|e| {
            CliError::Run(format!(
                "cannot read transform file '{}' for config '{}': {e}",
                config.transform.path, config.name,
            ))
        })?;

        if let Some(record) = db.get_config(&config.name)? {
            if let Some(ref stored_hash) = record.transform_hash {
                let current_hash = format!("{:x}", Sha256::digest(&transform_content));
                if *stored_hash != current_hash {
                    return Err(CliError::Run(format!(
                        "transform file '{}' was modified after config '{}' was applied",
                        config.transform.path, config.name,
                    )));
                }
            }
        }

        transformers.insert(
            config.name.clone(),
            Box::new(JsTransformer::new(
                transform_path,
                config.id.id_type.clone(),
            )),
        );
    }

    let router = Router::new(mappings);
    let tables: Vec<String> = tables.into_iter().collect();

    let pg_client = pg::connect::connect(&env_config.database_url)
        .await
        .map_err(|e| CliError::Run(format!("failed to connect to postgres: {e}")))?;

    let mut table_refs: Vec<(&str, &str)> = applied_configs
        .iter()
        .map(|c| (c.source.schema.as_str(), c.source.table.as_str()))
        .collect();
    table_refs.sort();
    table_refs.dedup();

    pg::connect::validate_tables(&pg_client, &table_refs)
        .await
        .map_err(|e| CliError::Run(format!("table validation failed: {e}")))?;

    pg::slot::ensure_slot(&pg_client, SLOT_NAME)
        .await
        .map_err(|e| CliError::Run(format!("failed to ensure replication slot: {e}")))?;

    pg::publication::ensure_publication(&pg_client, PUBLICATION_NAME, &tables)
        .await
        .map_err(|e| CliError::Run(format!("failed to ensure publication: {e}")))?;

    eprintln!("Replication slot '{}' ready", SLOT_NAME);
    eprintln!("Publication '{}' ready", PUBLICATION_NAME);

    // TODO: When a mix of configs have/lack checkpoints, falling back to
    // confirmed_flush_lsn may skip events for configs whose checkpoint is
    // behind the flush LSN. Backfill will need to handle this gap.
    let start_lsn = {
        let mut min_lsn: Option<u64> = None;
        let mut all_have_checkpoints = true;

        for config in &applied_configs {
            match db.get_streaming_checkpoint(&config.name)? {
                Some(cp) => {
                    min_lsn = Some(match min_lsn {
                        Some(current) => current.min(cp.lsn),
                        None => cp.lsn,
                    });
                }
                None => {
                    all_have_checkpoints = false;
                    break;
                }
            }
        }

        if all_have_checkpoints {
            min_lsn
        } else {
            pg::slot::get_confirmed_flush_lsn(&pg_client, SLOT_NAME)
                .await
                .map_err(|e| CliError::Run(format!("failed to get flush LSN: {e}")))?
        }
    };

    drop(pg_client);

    let stream_config = ReplicationStreamConfig {
        connection_string: env_config.database_url.clone(),
        slot_name: SLOT_NAME.to_string(),
        publication_name: PUBLICATION_NAME.to_string(),
        start_lsn,
        status_interval: STATUS_INTERVAL,
    };

    let mut stream = ReplicationStream::connect(stream_config)
        .await
        .map_err(|e| CliError::Run(format!("failed to connect replication stream: {e}")))?;

    let lsn_display = start_lsn
        .map(|l| pg::PgLsn::from(l).to_string())
        .unwrap_or_else(|| "-".to_string());
    eprintln!("Streaming from LSN {}", lsn_display);

    let puff_client = TurbopufferClient::new(
        env_config.turbopuffer_api_key.clone(),
        env_config.turbopuffer_region.clone(),
    )
    .map_err(|e| CliError::Run(format!("failed to create turbopuffer client: {e}")))?;

    let mut events_processed: HashMap<String, u64> = HashMap::new();
    for config in &applied_configs {
        let count = db
            .get_streaming_checkpoint(&config.name)?
            .map(|c| c.events_processed)
            .unwrap_or(0);
        events_processed.insert(config.name.clone(), count);
    }

    eprintln!("Listening for changes...");

    // Note: delivery to Turbopuffer is at-least-once. If we crash between
    // send_batch and save_streaming_checkpoint, we'll re-send on restart.
    // This is fine because Turbopuffer upserts are idempotent.
    loop {
        let batch = match stream.recv_batch().await {
            Ok(Some(batch)) => batch,
            Ok(None) => {
                eprintln!("Replication stream ended");
                break;
            }
            Err(e) => {
                return Err(CliError::Run(format!("replication stream error: {e}")));
            }
        };

        if batch.events.is_empty() {
            stream.ack();
            continue;
        }

        let config_events = router.route_batch(&batch.events, stream.relation_cache());

        for (config_name, events) in &config_events {
            let transformer = transformers
                .get(*config_name)
                .expect("transformer missing for applied config");
            let namespace = namespaces
                .get(*config_name)
                .expect("namespace missing for applied config");

            let actions = transformer
                .transform_batch(events.as_slice())
                .await
                .map_err(|e| CliError::Run(format!("transform error for {config_name}: {e}")))?;

            puff_client
                .send_batch(namespace, &actions)
                .await
                .map_err(|e| CliError::Run(format!("turbopuffer error for {config_name}: {e}")))?;

            let count = events_processed.entry(config_name.to_string()).or_insert(0);
            *count += events.len() as u64;

            eprintln!(
                "  {} -> {} ({} events, {} total)",
                config_name,
                namespace,
                events.len(),
                count,
            );
        }

        for (config_name, _) in &config_events {
            let checkpoint = StreamingCheckpoint {
                config_name: config_name.to_string(),
                lsn: batch.end_lsn,
                events_processed: *events_processed.get(*config_name).unwrap_or(&0),
                updated_at: Utc::now(),
            };
            db.save_streaming_checkpoint(&checkpoint)?;
        }

        stream.ack();
    }

    Ok(())
}
