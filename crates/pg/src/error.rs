use thiserror::Error;

#[derive(Debug, Error)]
pub enum PgError {
    #[error("Connection error: {0}")]
    ConnectionError(String),

    #[error("Query error: {0}")]
    QueryError(String),

    #[error("Table {schema}.{table} does not exist")]
    TableNotFound { schema: String, table: String },

    #[error("Replication error: {0}")]
    ReplicationError(String),

    #[error("Postgres error: {0}")]
    PostgresError(#[from] tokio_postgres::Error),
}

impl PgError {
    pub fn is_transient(&self) -> bool {
        match self {
            PgError::ConnectionError(_) => true,
            PgError::PostgresError(_) => true,
            PgError::ReplicationError(_) => true,
            PgError::QueryError(_) | PgError::TableNotFound { .. } => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_error_is_transient() {
        assert!(PgError::ConnectionError("timeout".into()).is_transient());
    }

    #[test]
    fn table_not_found_is_permanent() {
        assert!(
            !PgError::TableNotFound {
                schema: "public".into(),
                table: "foo".into()
            }
            .is_transient()
        );
    }

    #[test]
    fn query_error_is_permanent() {
        assert!(!PgError::QueryError("syntax error".into()).is_transient());
    }
}
