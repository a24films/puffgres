use std::io;
use std::path::PathBuf;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("Parse error: {0}")]
    ParseError(String),

    #[error("Validation error: {0}")]
    ValidationError(String),

    #[error("IO error: {0}")]
    IoError(#[from] io::Error),

    #[error("TOML parse error: {0}")]
    TomlError(#[from] toml::de::Error),

    #[error("TOML serialization error: {0}")]
    TomlSerError(#[from] toml::ser::Error),

    #[error("Config not found: {0}")]
    NotFound(String),

    #[error("{path}: {source}")]
    FileError {
        path: PathBuf,
        source: Box<ConfigError>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_error_creation() {
        let err = ConfigError::ParseError("invalid syntax".to_string());
        assert_eq!(err.to_string(), "Parse error: invalid syntax");
    }

    #[test]
    fn validation_error_creation() {
        let err = ConfigError::ValidationError("missing required field".to_string());
        assert_eq!(err.to_string(), "Validation error: missing required field");
    }

    #[test]
    fn not_found_error_creation() {
        let err = ConfigError::NotFound("config.toml".to_string());
        assert_eq!(err.to_string(), "Config not found: config.toml");
    }

    #[test]
    fn io_error_conversion() {
        let io_err = io::Error::new(io::ErrorKind::NotFound, "file not found");
        let err = ConfigError::from(io_err);
        assert!(err.to_string().contains("IO error"));
    }

    #[test]
    fn toml_error_conversion() {
        let toml_str = "invalid = toml = syntax";
        let toml_err = toml::from_str::<toml::Value>(toml_str).unwrap_err();
        let err = ConfigError::from(toml_err);
        assert!(err.to_string().contains("TOML parse error"));
    }
}
