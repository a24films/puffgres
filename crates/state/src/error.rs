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
    pub fn is_transient(&self) -> bool {
        match self {
            StateError::Connection(_) => true,
            StateError::Database(diesel::result::Error::DatabaseError(kind, _)) => {
                use diesel::result::DatabaseErrorKind::*;
                matches!(kind, SerializationFailure | ClosedConnection)
            }
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn not_found_is_permanent() {
        assert!(!StateError::NotFound("missing".into()).is_transient());
    }

    #[test]
    fn invalid_state_is_permanent() {
        assert!(!StateError::InvalidState("corrupt".into()).is_transient());
    }

    #[test]
    fn migration_is_permanent() {
        assert!(!StateError::Migration("failed".into()).is_transient());
    }
}
