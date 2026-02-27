use std::io;
use std::time::SystemTimeError;

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
    DryRun(String),

    #[error("{0}")]
    Run(String),

    #[error("{0}")]
    Reset(String),

    #[error("A config with {field} \"{name}\" already exists")]
    DuplicateConfig { name: String, field: String },

    #[error("{0} already exists")]
    AlreadyExists(String),

    #[error("{0} not found. Run `puffgres init` first.")]
    NotInitialized(String),

    #[error("Invalid PUFFGRES_STATE_PATH: {0}")]
    InvalidStatePath(String),

    #[error("OTLP exporter error: {0}")]
    Otel(String),

    #[error("System time error: {0}")]
    SystemTime(#[from] SystemTimeError),
}
