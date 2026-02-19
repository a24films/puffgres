mod env;
mod error;
mod init;
mod paths;
mod project_config;
mod status;

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
    NewConfig,
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
    let _env_config = EnvConfig::load(&env_paths)?;

    match cli.command {
        Command::Init => unreachable!(),
        Command::NewConfig => todo!("new-config"),
        Command::Apply => todo!("apply"),
        Command::Run => todo!("run"),
        Command::Status => status::run(&paths),
    }
}
