use std::collections::HashMap;

use clap::{Parser, Subcommand};

use puffgres_cli::{CliError, EnvConfig, ProjectConfig, ProjectPaths};

#[derive(Parser)]
#[command(name = "puffgres", version)]
#[command(about = "Replicate Postgres to Turbopuffer")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Initialize a puffgres project
    Init,
    /// Create a new table config (interactive, or non-interactive with flags)
    New {
        /// Config name. Also the default for the table and namespace.
        #[arg(long)]
        name: Option<String>,
        /// Postgres table to replicate (defaults to the config name)
        #[arg(long)]
        table: Option<String>,
        /// Destination turbopuffer namespace (defaults to the config name)
        #[arg(long)]
        namespace: Option<String>,
        /// Column to use as the document id (defaults to "id")
        #[arg(long)]
        id_column: Option<String>,
        /// Embedding provider: none, zeroentropy, baseten, cloudflare
        #[arg(long)]
        provider: Option<String>,
        /// Column to embed in the generated transform
        #[arg(long)]
        embed_column: Option<String>,
        /// Build the config from flags without prompting (for scripts and agents)
        #[arg(long)]
        non_interactive: bool,
    },
    /// Validate configs against the live database (regenerates schema.ts)
    Check {
        /// Optional config name to check (defaults to all configs)
        #[arg(long)]
        name: Option<String>,
    },
    /// Apply pending config changes
    Apply,
    /// Start the replication pipeline
    Run,
    /// Tombstone a config (exclude from CDC, backfill, and DLQ replay).
    /// Prompts you to pick one when --name is omitted.
    Tombstone {
        /// Name of the config to tombstone (defaults to an interactive picker)
        #[arg(long)]
        name: Option<String>,
    },
    /// Remove config(s): deletes turbopuffer namespace(s), state, and files
    Remove {
        /// Name of the config to remove
        #[arg(long)]
        name: Option<String>,
        /// Remove the most recently applied config
        #[arg(long)]
        last: bool,
        /// Remove every applied config (full reset)
        #[arg(long)]
        all: bool,
        /// Skip the confirmation prompt (only meaningful with --all)
        #[arg(long)]
        force: bool,
    },
    /// Reset everything: drop the replication slot(s), publication, and state
    /// schema, then delete the project directory (leaves turbopuffer untouched)
    Reset {
        /// Skip the confirmation prompt
        #[arg(long)]
        force: bool,
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

#[tokio::main]
async fn main() {
    let (result, telemetry) = run().await;
    if let Some(t) = telemetry {
        t.shutdown();
    }
    if let Err(e) = result {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}

async fn run() -> (
    Result<(), CliError>,
    Option<puffgres_cli::observability::Telemetry>,
) {
    let cli = Cli::parse();

    // Tier 1: no env needed
    if let Command::Init = cli.command {
        return (puffgres_cli::init::run(), None);
    }

    // Tier 2: ProjectPaths only. We additionally try (best-effort) to resolve
    // DATABASE_URL so `new` can auto-detect the id type; failure is fine.
    if let Command::New {
        ref name,
        ref table,
        ref namespace,
        ref id_column,
        ref provider,
        ref embed_column,
        non_interactive,
    } = cli.command
    {
        let paths = match ProjectPaths::from_current_dir() {
            Ok(p) => p,
            Err(e) => return (Err(e), None),
        };
        let database_url = ProjectConfig::load_unvalidated(&paths.project_config)
            .ok()
            .and_then(|pc| {
                let env_paths = pc.resolve_env_paths(&paths.root);
                puffgres_cli::env::resolve_database_url(&env_paths).ok()
            });
        let args = puffgres_cli::new::NewArgs {
            name: name.clone(),
            table: table.clone(),
            namespace: namespace.clone(),
            id_column: id_column.clone(),
            provider: provider.clone(),
            embed_column: embed_column.clone(),
            non_interactive,
        };
        return (
            puffgres_cli::new::run(&paths, args, database_url.as_deref()).await,
            None,
        );
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
        return (
            puffgres_cli::generate::run_async(&paths, &database_url).await,
            None,
        );
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
            match async {
                let pg_client = pg::connect::connect(&url)
                    .await
                    .map_err(|e| CliError::Debug(format!("Failed to connect to Postgres: {e}")))?;
                pg::slot::ensure_slot(&pg_client, slot).await.map_err(|e| {
                    CliError::Debug(format!("Failed to ensure replication slot: {e}"))
                })?;
                Ok::<_, CliError>(())
            }
            .await
            {
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
                        max_transaction_events: None,
                        sub_batch_size: None,
                        watched_columns: HashMap::new(),
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

        let result = puffgres_debug::run(client, port, replication_config).await;
        return (result.map_err(|e| CliError::Debug(e.to_string())), None);
    }

    // Tier 4: ProjectPaths + database_url + state_schema (no full ProjectConfig validation needed).
    // This recovery command only reads environment_files from puffgres.toml so it
    // still works when runtime config fields (e.g. batch_size) are invalid.
    if let Command::Tombstone { ref name } = cli.command {
        let project_config = match ProjectConfig::load_unvalidated(&paths.project_config) {
            Ok(c) => c,
            Err(e) => return (Err(e), None),
        };
        let env_paths = project_config.resolve_env_paths(&paths.root);
        let database_url = match puffgres_cli::env::resolve_database_url(&env_paths) {
            Ok(u) => u,
            Err(e) => return (Err(e), None),
        };
        let state_schema = match puffgres_cli::env::resolve_state_schema(&env_paths) {
            Ok(s) => s,
            Err(e) => return (Err(e), None),
        };

        return (
            puffgres_cli::tombstone::run(&paths, &database_url, &state_schema, name.as_deref())
                .await,
            None,
        );
    }

    // Reset is a recovery command (like Tombstone): loads env unvalidated so it
    // works even when the runtime config or state DB is broken.
    if let Command::Reset { force } = cli.command {
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
        let state_schema = match puffgres_cli::env::resolve_state_schema(&env_paths) {
            Ok(s) => s,
            Err(e) => return (Err(e), None),
        };

        return (
            puffgres_cli::reset::run(&paths, &database_url, &state_schema, force).await,
            None,
        );
    }

    // Tier 5: Check only needs DATABASE_URL + state_schema (no TURBOPUFFER_API_KEY)
    if let Command::Check { ref name } = cli.command {
        let project_config = match ProjectConfig::load(&paths.project_config) {
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
        let state_schema = match puffgres_cli::env::resolve_state_schema(&env_paths) {
            Ok(s) => s,
            Err(e) => return (Err(e), None),
        };

        return (
            puffgres_cli::check::run_async(
                &paths,
                &database_url,
                &state_schema,
                &project_config,
                name.as_deref(),
            )
            .await,
            None,
        );
    }

    // Tier 6: full ProjectConfig + EnvConfig (DATABASE_URL, TURBOPUFFER_API_KEY, etc.)
    let project_config = match ProjectConfig::load(&paths.project_config) {
        Ok(c) => c,
        Err(e) => return (Err(e), None),
    };
    let env_paths = project_config.resolve_env_paths(&paths.root);
    let env_config = match EnvConfig::load(&env_paths) {
        Ok(c) => c,
        Err(e) => return (Err(e), None),
    };
    tracing::info!(
        state_schema = %env_config.state_schema,
        "state schema resolved"
    );

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
        | Command::Tombstone { .. }
        | Command::Reset { .. }
        | Command::Check { .. }
        | Command::Generate
        | Command::Debug { .. } => unreachable!(),
        Command::Remove {
            ref name,
            last,
            all,
            force,
        } => {
            puffgres_cli::remove::run_async(&paths, &env_config, name.as_deref(), last, all, force)
                .await
        }
        Command::Apply => {
            puffgres_cli::apply::run_async(&paths, &env_config, &project_config).await
        }
        Command::Run => {
            puffgres_cli::pipeline::run_async(
                &paths,
                &env_config,
                &project_config,
                metrics.as_ref(),
            )
            .await
        }
    };

    (result, telemetry)
}
