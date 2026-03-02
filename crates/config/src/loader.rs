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
            .filter(|entry| entry.path().is_dir())
            .collect();

        entries.sort_by_key(|entry| entry.file_name());

        for entry in entries {
            let config_path = entry.path().join("config.toml");
            let config = Config::from_file(&config_path).map_err(|e| ConfigError::FileError {
                path: config_path.clone(),
                source: Box::new(e),
            })?;
            configs.push((config_path, config));
        }

        Ok(configs)
    }

    pub fn compute_transform_hash(config_path: &Path) -> Result<Option<String>, ConfigError> {
        let transform_path = config_path.parent().unwrap().join("transform.ts");

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

    fn fixture(name: &str) -> String {
        fs::read_to_string(format!("tests/fixtures/{}.toml", name)).unwrap()
    }

    fn create_test_config_dir() -> tempfile::TempDir {
        tempfile::tempdir().unwrap()
    }

    fn write_config_dir(dir: &Path, dir_name: &str, content: &str) -> PathBuf {
        let config_dir = dir.join(dir_name);
        fs::create_dir_all(&config_dir).unwrap();
        fs::write(config_dir.join("config.toml"), content).unwrap();
        config_dir
    }

    mod parse {
        use super::*;

        #[test]
        fn from_toml_valid() {
            let config = Config::from_toml(&fixture("valid")).unwrap();
            assert_eq!(config.name, "users");
            assert_eq!(config.namespace, "users");
        }

        #[test]
        fn from_toml_invalid() {
            let result = Config::from_toml("invalid toml content {[}");
            assert!(result.is_err());
        }

        #[test]
        fn from_file() {
            let dir = create_test_config_dir();
            let config_dir = write_config_dir(dir.path(), "1000_film", &fixture("film"));

            let config = Config::from_file(&config_dir.join("config.toml")).unwrap();
            assert_eq!(config.name, "film");
            assert_eq!(config.namespace, "film");
        }

        #[test]
        fn from_file_not_found() {
            let result = Config::from_file(Path::new("/nonexistent/config.toml"));
            assert!(result.is_err());
        }

        #[test]
        fn invalid_id_type_errors() {
            let result = Config::from_toml(&fixture("invalid_id_type"));
            assert!(
                result.is_err(),
                "invalid id.type = \"text\" must error, not silently pass"
            );
            let err = result.unwrap_err().to_string();
            assert!(
                err.contains("unknown variant `text`"),
                "error should mention the invalid variant, got: {err}"
            );
        }

        #[test]
        fn missing_id_type_errors() {
            let result = Config::from_toml(&fixture("missing_id_type"));
            assert!(result.is_err(), "missing id.type must error");
        }

        #[test]
        fn missing_id_section_errors() {
            let result = Config::from_toml(&fixture("missing_id_section"));
            assert!(result.is_err(), "missing [id] section must error");
        }
    }

    mod load_all {
        use super::*;

        #[test]
        fn empty_directory() {
            let dir = create_test_config_dir();
            let loader = ConfigLoader::new(dir.path());
            assert_eq!(loader.load_all().unwrap().len(), 0);
        }

        #[test]
        fn nonexistent_directory() {
            let loader = ConfigLoader::new(Path::new("/nonexistent/dir"));
            assert_eq!(loader.load_all().unwrap().len(), 0);
        }

        #[test]
        fn multiple_configs_sorted_by_dirname() {
            let dir = create_test_config_dir();
            write_config_dir(dir.path(), "1000_user", &fixture("valid"));
            write_config_dir(dir.path(), "0999_film", &fixture("film"));
            // Non-directory file should be ignored
            fs::write(dir.path().join("readme.txt"), "not a config").unwrap();

            let loader = ConfigLoader::new(dir.path());
            let configs = loader.load_all().unwrap();

            assert_eq!(configs.len(), 2);
            assert_eq!(configs[0].1.name, "film");
            assert_eq!(configs[1].1.name, "users");
        }

        #[test]
        fn invalid_config_in_dir_errors_not_skipped() {
            let dir = create_test_config_dir();
            write_config_dir(dir.path(), "1000_valid", &fixture("valid"));
            write_config_dir(dir.path(), "1001_invalid", &fixture("invalid_id_type"));

            let loader = ConfigLoader::new(dir.path());
            let result = loader.load_all();

            assert!(
                result.is_err(),
                "load_all must error when a config has an invalid id.type, not silently skip it"
            );
        }

        #[test]
        fn malformed_toml_in_dir_errors_not_skipped() {
            let dir = create_test_config_dir();
            write_config_dir(dir.path(), "1000_valid", &fixture("valid"));
            write_config_dir(dir.path(), "1001_broken", &fixture("malformed"));

            let loader = ConfigLoader::new(dir.path());
            let result = loader.load_all();

            assert!(
                result.is_err(),
                "load_all must error on malformed TOML, not silently skip it"
            );
        }

        #[test]
        fn preserves_io_error_variant() {
            let dir = create_test_config_dir();
            // A directory without config.toml triggers an IoError from read_to_string
            fs::create_dir(dir.path().join("1000_bad")).unwrap();

            let loader = ConfigLoader::new(dir.path());
            let err = loader.load_all().unwrap_err();
            match &err {
                crate::ConfigError::FileError { path, source } => {
                    assert!(
                        path.ends_with("config.toml"),
                        "expected path ending in config.toml, got: {path:?}"
                    );
                    assert!(
                        matches!(**source, crate::ConfigError::IoError(_)),
                        "expected inner IoError variant, got: {source:?}"
                    );
                }
                _ => panic!("expected FileError variant, got: {err:?}"),
            }
        }

        #[test]
        fn preserves_toml_error_variant() {
            let dir = create_test_config_dir();
            write_config_dir(dir.path(), "1000_broken", "not valid toml {[}");

            let loader = ConfigLoader::new(dir.path());
            let err = loader.load_all().unwrap_err();
            match &err {
                crate::ConfigError::FileError { path, source } => {
                    assert!(
                        path.ends_with("config.toml"),
                        "expected path ending in config.toml, got: {path:?}"
                    );
                    assert!(
                        matches!(**source, crate::ConfigError::TomlError(_)),
                        "expected inner TomlError variant, got: {source:?}"
                    );
                }
                _ => panic!("expected FileError variant, got: {err:?}"),
            }
        }
    }

    mod compute_hashes {
        use super::*;

        #[test]
        fn returns_none_when_transform_missing() {
            let dir = create_test_config_dir();
            let config_dir = write_config_dir(dir.path(), "1000_user", &fixture("valid"));

            assert!(
                ConfigLoader::compute_transform_hash(&config_dir.join("config.toml"))
                    .unwrap()
                    .is_none()
            );
        }

        #[test]
        fn returns_deterministic_hash() {
            let dir = create_test_config_dir();
            let config_dir = write_config_dir(dir.path(), "1000_user", &fixture("valid"));
            fs::write(
                config_dir.join("transform.ts"),
                "export function transform(data) { return data; }",
            )
            .unwrap();

            let hash1 = ConfigLoader::compute_transform_hash(&config_dir.join("config.toml"))
                .unwrap()
                .unwrap();
            let hash2 = ConfigLoader::compute_transform_hash(&config_dir.join("config.toml"))
                .unwrap()
                .unwrap();

            assert_eq!(hash1.len(), 64);
            assert_eq!(hash1, hash2);
        }

        #[test]
        fn different_content_produces_different_hash() {
            let dir = create_test_config_dir();
            let config_dir = write_config_dir(dir.path(), "1000_user", &fixture("valid"));

            fs::write(config_dir.join("transform.ts"), "content1").unwrap();
            let hash1 = ConfigLoader::compute_transform_hash(&config_dir.join("config.toml"))
                .unwrap()
                .unwrap();

            fs::write(config_dir.join("transform.ts"), "content2").unwrap();
            let hash2 = ConfigLoader::compute_transform_hash(&config_dir.join("config.toml"))
                .unwrap()
                .unwrap();

            assert_ne!(hash1, hash2);
        }
    }
}
