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

    // Non-transient: retrying can't fix it (wrong wal_level, privilege, slots full).
    #[error("Prerequisite not met: {0}")]
    Prerequisite(String),

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
            PgError::Prerequisite(_) => false,
        }
    }

    pub fn from_query_err(msg: String, source: &tokio_postgres::Error) -> Self {
        let msg = with_detail(msg, source);
        if is_connection_error(source) {
            PgError::ConnectionError(msg)
        } else {
            PgError::QueryError(msg)
        }
    }

    pub fn from_replication_err(msg: String, source: &tokio_postgres::Error) -> Self {
        let msg = with_detail(msg, source);
        if is_connection_error(source) {
            PgError::ConnectionError(msg)
        } else if is_permanent_error(source) {
            PgError::Prerequisite(msg)
        } else {
            PgError::ReplicationError(msg)
        }
    }
}

// Error Display is just "db error"; splice in the DbError cause's real message.
fn with_detail(msg: String, e: &tokio_postgres::Error) -> String {
    match e.as_db_error() {
        Some(db) if !msg.contains(db.message()) => {
            let mut out = format!("{msg}: {}", db.message());
            if let Some(hint) = db.hint() {
                out.push_str(&format!(" (hint: {hint})"));
            }
            out
        }
        _ => msg,
    }
}

// SqlStates a retry can't fix: 55000 wrong-state, 0A000 unsupported, 53400
// slots-full, 42* access, 28* auth, 3D*/3F* bad catalog/schema.
fn is_permanent_error(e: &tokio_postgres::Error) -> bool {
    let Some(code) = e.code() else { return false };
    let c = code.code();
    matches!(c, "55000" | "0A000" | "53400")
        || c.starts_with("42")
        || c.starts_with("28")
        || c.starts_with("3D")
        || c.starts_with("3F")
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

    #[test]
    fn prerequisite_error_is_permanent() {
        assert!(!PgError::Prerequisite("wal_level is 'replica'".into()).is_transient());
    }
}
