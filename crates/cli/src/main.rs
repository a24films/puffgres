use clap::{Parser, Subcommand};

use puffgres_cli::{CliError, EnvConfig, ProjectConfig, ProjectPaths};

#[derive(Parser)]
#[command(name = "puffgres")]
#[command(about = "Replicate Postgres to Turbopuffer")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Initialize a puffgres project
    Init,
    /// Create a new table config
    New {
        /// Name for the config (e.g. "user", "film")
        name: String,
    },
    /// Validate all configs against the live database without applying
    Check,
    /// Run transforms on sample data without writing state
    DryRun {
        /// Optional config name to dry-run
        name: Option<String>,
    },
    /// Apply pending config changes
    Apply,
    /// Start the replication pipeline
    Run,
    /// Clear all state (configs and checkpoints)
    Reset,
    /// Tombstone a config (exclude from CDC, backfill, and DLQ replay)
    Tombstone {
        /// Name of the config to tombstone
        #[arg(long)]
        name: String,
    },
    /// Generate typed schema.ts files for each config
    Generate,
    /// Launch a light UI to see the contents of turbopuffer namespaces
    Debug {
        /// Port to serve on
        #[arg(long, default_value = "3333")]
        port: u16,
        /// Replication slot name (defaults to puffgres_debug)
        #[arg(long, default_value = "puffgres_debug")]
        slot: String,
        /// Publication name (defaults to puffgres)
        #[arg(long, default_value = "puffgres")]
        publication: String,
    },
}

fn main() {
    let (result, telemetry) = run();
    if let Some(t) = telemetry {
        t.shutdown();
    }
    if let Err(e) = result {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}

fn run() -> (
    Result<(), CliError>,
    Option<puffgres_cli::observability::Telemetry>,
) {
    let cli = Cli::parse();

    // Tier 1: no env needed
    if let Command::Init = cli.command {
        return (puffgres_cli::init::run(), None);
    }

    // Tier 2: ProjectPaths only
    if let Command::New { ref name } = cli.command {
        let paths = match ProjectPaths::from_current_dir() {
            Ok(p) => p,
            Err(e) => return (Err(e), None),
        };
        return (puffgres_cli::new::run(&paths, name), None);
    }

    // All remaining commands need at least ProjectPaths
    let paths = match ProjectPaths::from_current_dir() {
        Ok(p) => p,
        Err(e) => return (Err(e), None),
    };

    // Tier 3: Generate only needs DATABASE_URL (no state DB).
    if let Command::Generate = cli.command {
        let project_config = match ProjectConfig::load_unvalidated(&paths.project_config) {
            Ok(c) => c,
            Err(e) => return (Err(e), None),
        };
        let env_paths = project_config.resolve_env_paths(&paths.root);
        let database_url = match puffgres_cli::env::resolve_database_url(&env_paths) {
            Ok(u) => u,
            Err(e) => return (Err(e), None),
        };
        return (puffgres_cli::generate::run(&paths, &database_url), None);
    }

    if let Command::Debug {
        port,
        ref slot,
        ref publication,
    } = cli.command
    {
        let project_config = match ProjectConfig::load_unvalidated(&paths.project_config) {
            Ok(c) => c,
            Err(e) => return (Err(e), None),
        };
        let env_paths = project_config.resolve_env_paths(&paths.root);
        let file_vars = match puffgres_cli::env::load_env_files(&env_paths) {
            Ok(v) => v,
            Err(e) => return (Err(e), None),
        };
        let api_key = match puffgres_cli::env::resolve_env_var("TURBOPUFFER_API_KEY", &file_vars) {
            Some(v) => v,
            None => {
                return (
                    Err(puffgres_cli::CliError::MissingEnvVar(
                        "TURBOPUFFER_API_KEY".into(),
                    )),
                    None,
                );
            }
        };
        let region = puffgres_cli::env::resolve_env_var("TURBOPUFFER_REGION", &file_vars);
        let client = match puff::TurbopufferClient::new(api_key, region) {
            Ok(c) => c,
            Err(e) => return (Err(CliError::Debug(e.to_string())), None),
        };

        let database_url = puffgres_cli::env::resolve_env_var("DATABASE_URL", &file_vars);
        let replication_config = if let Some(url) = database_url {
            let rt_temp = tokio::runtime::Runtime::new().unwrap();
            match rt_temp.block_on(async {
                let pg_client = pg::connect::connect(&url)
                    .await
                    .map_err(|e| CliError::Debug(format!("Failed to connect to Postgres: {e}")))?;
                pg::slot::ensure_slot(&pg_client, slot).await.map_err(|e| {
                    CliError::Debug(format!("Failed to ensure replication slot: {e}"))
                })?;
                Ok::<_, CliError>(())
            }) {
                Ok(()) => {
                    eprintln!(
                        "Replication enabled (slot={}, publication={})",
                        slot, publication
                    );
                    Some(replication::ReplicationStreamConfig {
                        connection_string: url,
                        slot_name: slot.clone(),
                        publication_name: publication.clone(),
                        start_lsn: None,
                        status_interval: std::time::Duration::from_secs(10),
                    })
                }
                Err(e) => {
                    eprintln!("Replication disabled: {e}");
                    None
                }
            }
        } else {
            eprintln!("Replication disabled (DATABASE_URL not set)");
            None
        };

        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(puffgres_debug::run(client, port, replication_config));
        return (result.map_err(|e| CliError::Debug(e.to_string())), None);
    }

    // Tier 4: ProjectPaths + state_db_path (no full ProjectConfig validation needed).
    // These recovery/status commands only read environment_files from puffgres.toml
    // so they still work when runtime config fields (e.g. batch_size) are invalid.
    match cli.command {
        Command::Reset | Command::Tombstone { .. } => {
            let project_config = match ProjectConfig::load_unvalidated(&paths.project_config) {
                Ok(c) => c,
                Err(e) => return (Err(e), None),
            };
            let env_paths = project_config.resolve_env_paths(&paths.root);
            let state_db_path =
                match puffgres_cli::env::resolve_state_db_path(&env_paths, &paths.root) {
                    Ok(p) => p,
                    Err(e) => return (Err(e), None),
                };

            let result = match cli.command {
                Command::Reset => puffgres_cli::reset::run(&state_db_path),
                Command::Tombstone { ref name } => {
                    puffgres_cli::tombstone::run(&paths, &state_db_path, name)
                }
                _ => unreachable!(),
            };

            return (result, None);
        }
        _ => {}
    }

    // Tier 5: Check only needs DATABASE_URL + state_db_path (no TURBOPUFFER_API_KEY)
    if let Command::Check = cli.command {
        let project_config = match ProjectConfig::load_unvalidated(&paths.project_config) {
            Ok(c) => c,
            Err(e) => return (Err(e), None),
        };
        let env_paths = project_config.resolve_env_paths(&paths.root);
        let file_vars = match puffgres_cli::env::load_env_files(&env_paths) {
            Ok(v) => v,
            Err(e) => return (Err(e), None),
        };
        let database_url = match puffgres_cli::env::resolve_env_var("DATABASE_URL", &file_vars) {
            Some(v) => v,
            None => {
                return (
                    Err(puffgres_cli::CliError::MissingEnvVar("DATABASE_URL".into())),
                    None,
                );
            }
        };
        let state_db_path = match puffgres_cli::env::resolve_state_db_path(&env_paths, &paths.root)
        {
            Ok(p) => p,
            Err(e) => return (Err(e), None),
        };

        return (
            puffgres_cli::check::run(&paths, &database_url, &state_db_path),
            None,
        );
    }

    // Tier 6: full ProjectConfig + EnvConfig (DATABASE_URL, TURBOPUFFER_API_KEY, etc.)
    let project_config = match ProjectConfig::load(&paths.project_config) {
        Ok(c) => c,
        Err(e) => return (Err(e), None),
    };
    let env_paths = project_config.resolve_env_paths(&paths.root);
    let env_config = match EnvConfig::load(&env_paths, &paths.root) {
        Ok(c) => c,
        Err(e) => return (Err(e), None),
    };

    let (telemetry, metrics) = if let Some(endpoint) = &env_config.otel_endpoint {
        match puffgres_cli::observability::init(endpoint, env_config.otel_headers.as_deref()) {
            Ok((telemetry, metrics)) => (Some(telemetry), Some(metrics)),
            Err(e) => return (Err(e), None),
        }
    } else {
        puffgres_cli::observability::init_fmt_only();
        (None, None)
    };

    let result = match cli.command {
        Command::Init
        | Command::New { .. }
        | Command::Reset
        | Command::Tombstone { .. }
        | Command::Check
        | Command::Generate
        | Command::Debug { .. } => unreachable!(),
        Command::DryRun { name } => {
            puffgres_cli::dry_run::run(&paths, &env_config, name.as_deref())
        }
        Command::Apply => puffgres_cli::apply::run(&paths, &env_config),
        Command::Run => {
            puffgres_cli::run::run(&paths, &env_config, &project_config, metrics.as_ref())
        }
    };

    (result, telemetry)
}
