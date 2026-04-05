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
            PgError::QueryError(_) => false,
            PgError::TableNotFound { .. } => false,
        }
    }

    pub fn from_query_err(msg: String, source: &tokio_postgres::Error) -> Self {
        if is_connection_error(source) {
            PgError::ConnectionError(msg)
        } else {
            PgError::QueryError(msg)
        }
    }

    pub fn from_replication_err(msg: String, source: &tokio_postgres::Error) -> Self {
        if is_connection_error(source) {
            PgError::ConnectionError(msg)
        } else {
            PgError::ReplicationError(msg)
        }
    }
}

fn is_connection_error(e: &tokio_postgres::Error) -> bool {
    if e.is_closed() {
        return true;
    }
    if let Some(code) = e.code() {
        // SQL state class 08 = Connection Exception
        return code.code().starts_with("08");
    }
    false
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
        assert!(!PgError::QueryError("permission denied".into()).is_transient());
    }

    #[test]
    fn replication_error_is_transient() {
        assert!(PgError::ReplicationError("stream ended".into()).is_transient());
    }
}
