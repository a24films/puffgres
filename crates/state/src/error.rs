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

impl StateError {
    /// Whether this error is likely transient (e.g. SQLite busy/locked) and
    /// worth retrying after a backoff.
    pub fn is_retryable(&self) -> bool {
        match self {
            StateError::Database(diesel::result::Error::DatabaseError(_, info)) => {
                let msg = info.message().to_lowercase();
                msg.contains("database is locked") || msg.contains("database is busy")
            }
            _ => false,
        }
    }
}
