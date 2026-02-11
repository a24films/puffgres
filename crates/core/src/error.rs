use thiserror::Error;

#[derive(Debug, Error)]
pub enum CoreError {
    #[error("config error: {0}")]
    Config(#[from] config::ConfigError),

    #[error("pg error: {0}")]
    Pg(#[from] pg::PgError),

    #[error("replication error: {0}")]
    Replication(#[from] replication::ReplicationError),

    #[error("pipeline error: {0}")]
    Pipeline(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pipeline_error_creation() {
        let err = CoreError::Pipeline("stage failed".to_string());
        assert_eq!(err.to_string(), "pipeline error: stage failed");
    }

    #[test]
    fn config_error_conversion() {
        let config_err = config::ConfigError::NotFound("missing.toml".to_string());
        let err = CoreError::from(config_err);
        assert!(err.to_string().contains("config error"));
    }

    #[test]
    fn pg_error_conversion() {
        let pg_err = pg::PgError::ConnectionError("refused".to_string());
        let err = CoreError::from(pg_err);
        assert!(err.to_string().contains("pg error"));
    }

    #[test]
    fn replication_error_conversion() {
        let repl_err = replication::ReplicationError::Stream("disconnected".to_string());
        let err = CoreError::from(repl_err);
        assert!(err.to_string().contains("replication error"));
    }
}
