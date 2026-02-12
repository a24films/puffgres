mod apply;
mod env;
mod error;
mod init;
mod new;
mod paths;
mod project_config;
mod run;
mod status;
#[cfg(test)]
mod test_utils;
mod validate;

use clap::{Parser, Subcommand};

pub use env::EnvConfig;
pub use error::CliError;
pub use paths::ProjectPaths;
pub use project_config::ProjectConfig;

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
    /// Apply pending config changes
    Apply,
    /// Start the replication pipeline
    Run,
    /// Show replication status
    Status,
}

fn main() -> Result<(), CliError> {
    let cli = Cli::parse();
    let paths = ProjectPaths::from_current_dir()?;

    match cli.command {
        Command::Init => return init::run(&paths),
        _ => {}
    }

    let project_config = ProjectConfig::load(&paths.project_config)?;
    let env_paths = project_config.resolve_env_paths(&paths.root);
    let env_config = EnvConfig::load(&env_paths)?;

    match cli.command {
        Command::Init => unreachable!(),
        Command::New { name } => new::run(&paths, &name),
        Command::Apply => apply::run(&paths, &env_config),
        Command::Run => run::run(&paths, &env_config),
        Command::Status => status::run(&paths),
    }
}
