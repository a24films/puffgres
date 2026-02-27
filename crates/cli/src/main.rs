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
    /// Run transforms on sample data without writing state
    DryRun {
        /// Optional config name to dry-run
        name: Option<String>,
    },
    /// Apply pending config changes
    Apply,
    /// Start the replication pipeline
    Run,
    /// Show replication status
    Status,
    /// Clear all state (configs and checkpoints)
    Reset,
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

    match cli.command {
        Command::Init => return (puffgres_cli::init::run(), None),
        Command::Reset => {
            let paths = match ProjectPaths::from_current_dir() {
                Ok(p) => p,
                Err(e) => return (Err(e), None),
            };
            return (puffgres_cli::reset::run(&paths), None);
        }
        _ => {}
    }

    let paths = match ProjectPaths::from_current_dir() {
        Ok(p) => p,
        Err(e) => return (Err(e), None),
    };
    let project_config = match ProjectConfig::load(&paths.project_config) {
        Ok(c) => c,
        Err(e) => return (Err(e), None),
    };
    let env_paths = project_config.resolve_env_paths(&paths.root);
    let env_config = match EnvConfig::load(&env_paths) {
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
        Command::Init | Command::Reset => unreachable!(),
        Command::New { name } => puffgres_cli::new::run(&paths, &name),
        Command::DryRun { name } => {
            puffgres_cli::dry_run::run(&paths, &env_config, name.as_deref())
        }
        Command::Apply => puffgres_cli::apply::run(&paths, &env_config),
        Command::Run => {
            puffgres_cli::run::run(&paths, &env_config, &project_config, metrics.as_ref())
        }
        Command::Status => puffgres_cli::status::run(&paths),
    };

    (result, telemetry)
}
