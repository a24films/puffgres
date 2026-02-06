use crate::{Config, ConfigError};
use sha2::{Digest, Sha256};
use std::fs;
use std::path::{Path, PathBuf};

impl Config {
    pub fn from_toml(content: &str) -> Result<Self, ConfigError> {
        Ok(toml::from_str(content)?)
    }

    pub fn from_file(path: &Path) -> Result<Self, ConfigError> {
        let content = fs::read_to_string(path)?;
        Self::from_toml(&content)
    }
}

pub struct ConfigLoader {
    config_dir: PathBuf,
}

impl ConfigLoader {
    pub fn new(config_dir: &Path) -> Self {
        Self {
            config_dir: config_dir.to_path_buf(),
        }
    }

    pub fn load_all(&self) -> Result<Vec<(PathBuf, Config)>, ConfigError> {
        if !self.config_dir.exists() {
            return Ok(Vec::new());
        }

        let mut configs = Vec::new();
        let mut entries: Vec<_> = fs::read_dir(&self.config_dir)?
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry
                    .path()
                    .extension()
                    .and_then(|ext| ext.to_str())
                    .map(|ext| ext == "toml")
                    .unwrap_or(false)
            })
            .collect();

        entries.sort_by_key(|entry| entry.file_name());

        for entry in entries {
            let path = entry.path();
            match Config::from_file(&path) {
                Ok(config) => configs.push((path, config)),
                Err(e) => {
                    // Continue loading other configs even if one fails
                    eprintln!("Warning: Failed to load config from {:?}: {}", path, e);
                }
            }
        }

        Ok(configs)
    }

    pub fn compute_transform_hash(&self, config: &Config) -> Result<Option<String>, ConfigError> {
        let transform_path = self.config_dir.join(&config.transform.path);

        if !transform_path.exists() {
            return Ok(None);
        }

        let content = fs::read(&transform_path)?;
        let hash = Sha256::digest(&content);
        Ok(Some(format!("{:x}", hash)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn create_test_config_dir() -> tempfile::TempDir {
        tempfile::tempdir().unwrap()
    }

    fn write_config_file(dir: &Path, filename: &str, content: &str) {
        fs::write(dir.join(filename), content).unwrap();
    }

    #[test]
    fn test_from_toml() {
        let toml_str = std::fs::read_to_string("tests/fixtures/valid.toml").unwrap();
        let config = Config::from_toml(&toml_str).unwrap();
        assert_eq!(config.name, "user_0001");
        assert_eq!(config.version, 1);
        assert_eq!(config.namespace, "user");
    }

    #[test]
    fn test_from_toml_invalid() {
        let invalid_toml = "invalid toml content {[}";
        let result = Config::from_toml(invalid_toml);
        assert!(result.is_err());
    }

    #[test]
    fn test_from_file() {
        let dir = create_test_config_dir();
        let config_content = r#"
name = "film_0001"
version = 1
namespace = "film"

[source]
schema = "public"
table = "films"

[id]
column = "id"
type = "uuid"

[transform]
path = "transforms/film.ts"
"#;
        write_config_file(dir.path(), "film.toml", config_content);

        let config = Config::from_file(&dir.path().join("film.toml")).unwrap();
        assert_eq!(config.name, "film_0001");
        assert_eq!(config.namespace, "film");
    }

    #[test]
    fn test_from_file_not_found() {
        let result = Config::from_file(Path::new("/nonexistent/config.toml"));
        assert!(result.is_err());
    }

    #[test]
    fn test_loader_new() {
        let dir = create_test_config_dir();
        let loader = ConfigLoader::new(dir.path());
        assert_eq!(loader.config_dir, dir.path());
    }

    #[test]
    fn test_load_all_empty_directory() {
        let dir = create_test_config_dir();
        let loader = ConfigLoader::new(dir.path());
        let configs = loader.load_all().unwrap();
        assert_eq!(configs.len(), 0);
    }

    #[test]
    fn test_load_all_nonexistent_directory() {
        let loader = ConfigLoader::new(Path::new("/nonexistent/dir"));
        let configs = loader.load_all().unwrap();
        assert_eq!(configs.len(), 0);
    }

    #[test]
    fn test_load_all_multiple_configs() {
        let dir = create_test_config_dir();

        let config1 = r#"
name = "user_0001"
version = 1
namespace = "user"

[source]
schema = "public"
table = "users"

[id]
column = "id"
type = "uint"

[transform]
path = "transforms/user.ts"
"#;

        let config2 = r#"
name = "film_0001"
version = 1
namespace = "film"

[source]
schema = "public"
table = "films"

[id]
column = "id"
type = "uuid"

[transform]
path = "transforms/film.ts"
"#;

        write_config_file(dir.path(), "user.toml", config1);
        write_config_file(dir.path(), "film.toml", config2);
        // Non-TOML file should be ignored
        write_config_file(dir.path(), "readme.txt", "This is not a config");

        let loader = ConfigLoader::new(dir.path());
        let configs = loader.load_all().unwrap();

        assert_eq!(configs.len(), 2);
        // Sorted by filename
        assert_eq!(configs[0].1.name, "film_0001");
        assert_eq!(configs[1].1.name, "user_0001");
    }

    #[test]
    fn test_compute_transform_hash_file_not_exists() {
        let dir = create_test_config_dir();

        let config = r#"
name = "user_0001"
version = 1
namespace = "user"

[source]
schema = "public"
table = "users"

[id]
column = "id"
type = "uint"

[transform]
path = "transforms/user.ts"
"#;

        write_config_file(dir.path(), "user.toml", config);

        let loader = ConfigLoader::new(dir.path());
        let all_configs = loader.load_all().unwrap();
        let (_, cfg) = all_configs
            .iter()
            .find(|(_, c)| c.name == "user_0001")
            .unwrap();

        let hash = loader.compute_transform_hash(cfg).unwrap();
        assert!(hash.is_none());
    }

    #[test]
    fn test_compute_transform_hash_file_exists() {
        let dir = create_test_config_dir();

        let config = r#"
name = "user_0001"
version = 1
namespace = "user"

[source]
schema = "public"
table = "users"

[id]
column = "id"
type = "uint"

[transform]
path = "transform.ts"
"#;

        write_config_file(dir.path(), "user.toml", config);

        let transform_content = "export function transform(data) { return data; }";
        write_config_file(dir.path(), "transform.ts", transform_content);

        let loader = ConfigLoader::new(dir.path());
        let all_configs = loader.load_all().unwrap();
        let (_, cfg) = all_configs
            .iter()
            .find(|(_, c)| c.name == "user_0001")
            .unwrap();

        let hash = loader.compute_transform_hash(cfg).unwrap();
        assert!(hash.is_some());
        let hash_str = hash.unwrap();
        assert_eq!(hash_str.len(), 64); // SHA-256 produces 64 hex characters

        // Verify hash is deterministic
        let hash2 = loader.compute_transform_hash(cfg).unwrap().unwrap();
        assert_eq!(hash_str, hash2);
    }

    #[test]
    fn test_compute_transform_hash_different_content() {
        let dir = create_test_config_dir();

        let config = r#"
name = "user_0001"
version = 1
namespace = "user"

[source]
schema = "public"
table = "users"

[id]
column = "id"
type = "uint"

[transform]
path = "transform.ts"
"#;

        write_config_file(dir.path(), "user.toml", config);

        let loader = ConfigLoader::new(dir.path());
        let all_configs = loader.load_all().unwrap();
        let (_, cfg) = all_configs
            .iter()
            .find(|(_, c)| c.name == "user_0001")
            .unwrap();

        write_config_file(dir.path(), "transform.ts", "content1");
        let hash1 = loader.compute_transform_hash(cfg).unwrap().unwrap();

        write_config_file(dir.path(), "transform.ts", "content2");
        let hash2 = loader.compute_transform_hash(cfg).unwrap().unwrap();

        assert_ne!(hash1, hash2);
    }
}
