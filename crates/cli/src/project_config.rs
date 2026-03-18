use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::CliError;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectConfig {
    pub environment_files: Vec<String>,
    #[serde(default)]
    pub batch_size: Option<u32>,
    #[serde(default)]
    pub max_retries: Option<u32>,
    #[serde(default)]
    pub dlq_replay_interval: Option<u64>,
    #[serde(default)]
    pub dlq_replay_batch_size: Option<usize>,
    #[serde(default)]
    pub dlq_max_retries: Option<u32>,
    #[serde(default)]
    pub dlq_permanent_max_age_hours: Option<u64>,
    #[serde(default)]
    pub max_transaction_events: Option<usize>,
    /// When set, yield sub-batches of this size during large transactions instead
    /// of buffering the entire transaction in memory. The pipeline processes chunks
    /// as they arrive, giving natural backpressure. The commit finalizes the group.
    #[serde(default)]
    pub sub_batch_size: Option<usize>,
    /// Logging level for unclean TLS shutdowns (missing close_notify).
    /// Supported values: "error", "warn", "silent".
    #[serde(default)]
    pub tls_unclean_close_level: Option<String>,
    #[serde(default)]
    pub transform_timeout_secs: Option<u64>,
}

impl ProjectConfig {
    pub fn load(path: &Path) -> Result<Self, CliError> {
        let config = Self::load_unvalidated(path)?;
        config.validate()?;
        Ok(config)
    }

    /// Load config without running runtime validation. Use this for commands
    /// (reset, tombstone, status) that only need `environment_files` and should
    /// work even when runtime fields like `batch_size` are invalid.
    pub fn load_unvalidated(path: &Path) -> Result<Self, CliError> {
        let contents = fs::read_to_string(path)?;
        let config: Self = toml::from_str(&contents).map_err(|e| CliError::ProjectConfig {
            path: path.display().to_string(),
            source: e,
        })?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), CliError> {
        if self.batch_size == Some(0) {
            return Err(CliError::RunValidation(
                "batch_size must be at least 1 in puffgres.toml".to_string(),
            ));
        }
        if self.dlq_replay_interval == Some(0) {
            return Err(CliError::RunValidation(
                "dlq_replay_interval must be at least 1 in puffgres.toml".to_string(),
            ));
        }
        if self.dlq_replay_batch_size == Some(0) {
            return Err(CliError::RunValidation(
                "dlq_replay_batch_size must be at least 1 in puffgres.toml".to_string(),
            ));
        }
        if self.max_transaction_events == Some(0) {
            return Err(CliError::RunValidation(
                "max_transaction_events must be at least 1 in puffgres.toml".to_string(),
            ));
        }
        if self.sub_batch_size == Some(0) {
            return Err(CliError::RunValidation(
                "sub_batch_size must be at least 1 in puffgres.toml".to_string(),
            ));
        }
        if self.transform_timeout_secs == Some(0) {
            return Err(CliError::RunValidation(
                "transform_timeout_secs must be at least 1 in puffgres.toml".to_string(),
            ));
        }
        Ok(())
    }

    pub fn batch_size(&self) -> u32 {
        self.batch_size.unwrap_or(1000)
    }

    pub fn max_retries(&self) -> u32 {
        self.max_retries.unwrap_or(5)
    }

    pub fn dlq_replay_interval(&self) -> u64 {
        self.dlq_replay_interval.unwrap_or(10)
    }

    pub fn dlq_replay_batch_size(&self) -> usize {
        self.dlq_replay_batch_size.unwrap_or(50)
    }

    pub fn dlq_max_retries(&self) -> u32 {
        self.dlq_max_retries.unwrap_or(5)
    }

    pub fn dlq_permanent_max_age_hours(&self) -> u64 {
        self.dlq_permanent_max_age_hours.unwrap_or(72)
    }

    pub fn max_transaction_events(&self) -> Option<usize> {
        self.max_transaction_events
    }

    pub fn sub_batch_size(&self) -> Option<usize> {
        self.sub_batch_size
    }

    pub fn tls_unclean_close_level(&self) -> &str {
        match self.tls_unclean_close_level.as_deref() {
            Some("warn") => "warn",
            Some("silent") => "silent",
            _ => "error",
        }
    }

    pub fn transform_timeout_secs(&self) -> u64 {
        self.transform_timeout_secs.unwrap_or(30)
    }

    pub fn resolve_env_paths(&self, root: &Path) -> Vec<PathBuf> {
        self.environment_files
            .iter()
            .map(|p| root.join(p))
            .collect()
    }
}

impl Default for ProjectConfig {
    fn default() -> Self {
        Self {
            environment_files: vec![".env".to_string()],
            batch_size: Some(1000),
            max_retries: Some(5),
            dlq_replay_interval: Some(10),
            dlq_replay_batch_size: Some(50),
            dlq_max_retries: Some(5),
            dlq_permanent_max_age_hours: Some(72),
            max_transaction_events: None,
            sub_batch_size: None,
            tls_unclean_close_level: None,
            transform_timeout_secs: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserialize_from_toml() {
        let toml = r#"environment_files = [".env", ".env.local"]"#;
        let config: ProjectConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.environment_files, vec![".env", ".env.local"]);
    }

    #[test]
    fn resolve_env_paths_relative_to_root() {
        let config = ProjectConfig {
            environment_files: vec![".env".into(), ".env.local".into()],
            ..Default::default()
        };
        let root = Path::new("/home/user/project");
        let resolved = config.resolve_env_paths(root);
        assert_eq!(
            resolved,
            vec![
                PathBuf::from("/home/user/project/.env"),
                PathBuf::from("/home/user/project/.env.local"),
            ]
        );
    }

    #[test]
    fn default_has_dotenv() {
        let config = ProjectConfig::default();
        assert_eq!(config.environment_files, vec![".env"]);
    }

    #[test]
    fn load_from_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("puffgres.toml");
        std::fs::write(&path, r#"environment_files = [".env"]"#).unwrap();

        let config = ProjectConfig::load(&path).unwrap();
        assert_eq!(config.environment_files, vec![".env"]);
    }

    #[test]
    fn batch_size_default() {
        let config = ProjectConfig::default();
        assert_eq!(config.batch_size(), 1000);
    }

    #[test]
    fn batch_size_custom() {
        let toml = r#"
environment_files = [".env"]
batch_size = 1000
"#;
        let config: ProjectConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.batch_size(), 1000);
    }

    #[test]
    fn max_retries_default() {
        let config = ProjectConfig::default();
        assert_eq!(config.max_retries(), 5);
    }

    #[test]
    fn deserialize_all_fields() {
        let toml = r#"
environment_files = [".env"]
batch_size = 250
max_retries = 10
dlq_replay_interval = 20
dlq_replay_batch_size = 100
dlq_max_retries = 3
dlq_permanent_max_age_hours = 48
tls_unclean_close_level = "warn"
transform_timeout_secs = 45
"#;
        let config: ProjectConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.environment_files, vec![".env"]);
        assert_eq!(config.batch_size(), 250);
        assert_eq!(config.max_retries(), 10);
        assert_eq!(config.dlq_replay_interval(), 20);
        assert_eq!(config.dlq_replay_batch_size(), 100);
        assert_eq!(config.dlq_max_retries(), 3);
        assert_eq!(config.dlq_permanent_max_age_hours(), 48);
        assert_eq!(config.tls_unclean_close_level(), "warn");
        assert_eq!(config.transform_timeout_secs(), 45);
    }

    #[test]
    fn dlq_defaults() {
        let config = ProjectConfig::default();
        assert_eq!(config.dlq_replay_interval(), 10);
        assert_eq!(config.dlq_replay_batch_size(), 50);
        assert_eq!(config.dlq_max_retries(), 5);
        assert_eq!(config.dlq_permanent_max_age_hours(), 72);
    }

    #[test]
    fn load_missing_file_errors() {
        let result = ProjectConfig::load(Path::new("/nonexistent/puffgres.toml"));
        assert!(result.is_err());
    }

    #[test]
    fn load_invalid_toml_errors() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("puffgres.toml");
        std::fs::write(&path, "not valid { toml").unwrap();

        let result = ProjectConfig::load(&path);
        assert!(result.is_err());
    }

    #[test]
    fn zero_batch_size_errors() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("puffgres.toml");
        std::fs::write(&path, "environment_files = [\".env\"]\nbatch_size = 0\n").unwrap();

        let result = ProjectConfig::load(&path);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("batch_size"),
            "error should mention batch_size: {err}"
        );
    }

    #[test]
    fn zero_dlq_replay_batch_size_errors() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("puffgres.toml");
        std::fs::write(
            &path,
            "environment_files = [\".env\"]\ndlq_replay_batch_size = 0\n",
        )
        .unwrap();

        let result = ProjectConfig::load(&path);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("dlq_replay_batch_size"),
            "error should mention dlq_replay_batch_size: {err}"
        );
    }

    #[test]
    fn zero_max_transaction_events_errors() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("puffgres.toml");
        std::fs::write(
            &path,
            "environment_files = [\".env\"]\nmax_transaction_events = 0\n",
        )
        .unwrap();

        let result = ProjectConfig::load(&path);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("max_transaction_events"),
            "error should mention max_transaction_events: {err}"
        );
    }

    #[test]
    fn zero_dlq_replay_interval_errors() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("puffgres.toml");
        std::fs::write(
            &path,
            "environment_files = [\".env\"]\ndlq_replay_interval = 0\n",
        )
        .unwrap();

        let result = ProjectConfig::load(&path);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("dlq_replay_interval"),
            "error should mention dlq_replay_interval: {err}"
        );
    }

    #[test]
    fn zero_transform_timeout_errors() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("puffgres.toml");
        std::fs::write(
            &path,
            "environment_files = [\".env\"]\ntransform_timeout_secs = 0\n",
        )
        .unwrap();

        let result = ProjectConfig::load(&path);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("transform_timeout_secs"),
            "error should mention transform_timeout_secs: {err}"
        );
    }
}
