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

fn main() -> Result<(), CliError> {
    let cli = Cli::parse();

    match cli.command {
        Command::Init => return puffgres_cli::init::run(),
        Command::Reset => {
            let paths = ProjectPaths::from_current_dir()?;
            return puffgres_cli::reset::run(&paths);
        }
        _ => {}
    }

    let paths = ProjectPaths::from_current_dir()?;
    let project_config = ProjectConfig::load(&paths.project_config)?;
    let env_paths = project_config.resolve_env_paths(&paths.root);
    let env_config = EnvConfig::load(&env_paths)?;

    match cli.command {
        Command::Init | Command::Reset => unreachable!(),
        Command::New { name } => puffgres_cli::new::run(&paths, &name),
        Command::DryRun { name } => {
            puffgres_cli::dry_run::run(&paths, &env_config, name.as_deref())
        }
        Command::Apply => puffgres_cli::apply::run(&paths, &env_config),
        Command::Run => puffgres_cli::run::run(&paths, &env_config, &project_config),
        Command::Status => puffgres_cli::status::run(&paths),
    }
}
