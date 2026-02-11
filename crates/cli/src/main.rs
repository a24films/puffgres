use std::path::PathBuf;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "puffgres")]
#[command(about = "Replicate Postgres to Turbopuffer")]
struct Cli {
    /// Path to .env file
    #[arg(long, global = true)]
    env: Option<PathBuf>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Initialize puffgres state database
    Init,
    /// Set up Postgres publication and replication slot
    Setup,
    /// Create a new table config
    NewConfig,
    /// Apply pending config changes
    Apply,
    /// Start the replication pipeline
    Run,
    /// Show replication status
    Status,
}

fn main() {
    let cli = Cli::parse();

    if let Some(env_path) = &cli.env {
        eprintln!("Using env file: {}", env_path.display());
    }

    match cli.command {
        Command::Init => todo!("init"),
        Command::Setup => todo!("setup"),
        Command::NewConfig => todo!("new-config"),
        Command::Apply => todo!("apply"),
        Command::Run => todo!("run"),
        Command::Status => todo!("status"),
    }
}
