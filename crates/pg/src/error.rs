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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_error_creation() {
        let err = PgError::ConnectionError("failed to connect".to_string());
        assert_eq!(err.to_string(), "Connection error: failed to connect");
    }

    #[test]
    fn query_error_creation() {
        let err = PgError::QueryError("invalid query".to_string());
        assert_eq!(err.to_string(), "Query error: invalid query");
    }

    #[test]
    fn replication_error_creation() {
        let err = PgError::ReplicationError("replication slot error".to_string());
        assert_eq!(err.to_string(), "Replication error: replication slot error");
    }
}
