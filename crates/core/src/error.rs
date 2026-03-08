use thiserror::Error;

#[derive(Debug, Error)]
pub enum CoreError {
    #[error("config error: {0}")]
    Config(#[from] config::ConfigError),

    #[error("pg error: {0}")]
    Pg(#[from] pg::PgError),

    #[error("replication error: {0}")]
    Replication(#[from] replication::ReplicationError),

    #[error("state error: {0}")]
    State(#[from] state::StateError),

    #[error("pipeline error: {0}")]
    Pipeline(String),
}

impl CoreError {
    pub fn is_transient(&self) -> bool {
        match self {
            CoreError::Config(e) => e.is_transient(),
            CoreError::Pg(e) => e.is_transient(),
            CoreError::Replication(e) => e.is_transient(),
            CoreError::State(e) => e.is_transient(),
            CoreError::Pipeline(_) => false,
        }
    }
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

    #[test]
    fn transient_pg_error_propagates() {
        let pg_err = pg::PgError::ConnectionError("timeout".into());
        let err = CoreError::from(pg_err);
        assert!(err.is_transient());
    }

    #[test]
    fn permanent_config_error_propagates() {
        let config_err = config::ConfigError::NotFound("missing".into());
        let err = CoreError::from(config_err);
        assert!(!err.is_transient());
    }

    #[test]
    fn pipeline_error_is_permanent() {
        assert!(!CoreError::Pipeline("failed".into()).is_transient());
    }
}
