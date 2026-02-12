use std::io;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum CliError {
    #[error("Missing required environment variable: {0}")]
    MissingEnvVar(String),

    #[error("Failed to read env file {path}: {source}")]
    EnvFile { path: String, source: io::Error },

    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error("Failed to parse {path}: {source}")]
    ProjectConfig {
        path: String,
        source: toml::de::Error,
    },

    #[error("State database error: {0}")]
    State(#[from] state::StateError),

    #[error("Config error: {0}")]
    Config(#[from] config::ConfigError),

    #[error("{0}")]
    Apply(String),

    #[error("{0}")]
    Check(String),

    #[error("{0}")]
    Run(String),

    #[error("{0} already exists")]
    AlreadyExists(String),

    #[error("{0} directory not found. Run `puffgres init` first.")]
    NotInitialized(String),
}
