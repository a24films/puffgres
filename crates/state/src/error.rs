use thiserror::Error;

#[derive(Debug, Error)]
pub enum StateError {
    #[error("database error: {0}")]
    Database(#[from] diesel::result::Error),
    #[error("connection error: {0}")]
    Connection(#[from] diesel::ConnectionError),
    #[error("migration error: {0}")]
    Migration(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("invalid state: {0}")]
    InvalidState(String),
}
