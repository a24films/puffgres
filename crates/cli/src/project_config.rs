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
}

impl ProjectConfig {
    pub fn load(path: &Path) -> Result<Self, CliError> {
        let contents = fs::read_to_string(path)?;
        toml::from_str(&contents).map_err(|e| CliError::ProjectConfig {
            path: path.display().to_string(),
            source: e,
        })
    }

    pub fn batch_size(&self) -> u32 {
        self.batch_size.unwrap_or(500)
    }

    pub fn max_retries(&self) -> u32 {
        self.max_retries.unwrap_or(5)
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
            batch_size: None,
            max_retries: None,
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
        assert_eq!(config.batch_size(), 500);
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
"#;
        let config: ProjectConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.environment_files, vec![".env"]);
        assert_eq!(config.batch_size(), 250);
        assert_eq!(config.max_retries(), 10);
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
}
