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
    /// Initialize the state database
    Setup,
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

    // Tier 3: ProjectPaths + state_db_path (no full ProjectConfig validation needed).
    // These recovery/status commands only read environment_files from puffgres.toml
    // so they still work when runtime config fields (e.g. batch_size) are invalid.
    match cli.command {
        Command::Setup | Command::Reset | Command::Tombstone { .. } => {
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
                Command::Setup => puffgres_cli::setup::run(&state_db_path),
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

    // Tier 4: Check only needs DATABASE_URL + state_db_path (no TURBOPUFFER_API_KEY)
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

    // Tier 5: full ProjectConfig + EnvConfig (DATABASE_URL, TURBOPUFFER_API_KEY, etc.)
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
        | Command::Setup
        | Command::Reset
        | Command::Tombstone { .. }
        | Command::Check => unreachable!(),
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
