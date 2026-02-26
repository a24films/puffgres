use std::collections::HashMap;
use std::path::Path;

use crate::error::CliError;

#[derive(Debug, Clone)]
pub struct EnvConfig {
    pub database_url: String,
    pub turbopuffer_api_key: String,
    pub turbopuffer_region: Option<String>,
    pub turbopuffer_namespace_prefix: Option<String>,
}

impl EnvConfig {
    /// Load environment config from multiple `.env` file paths.
    ///
    /// Files are loaded in order — later files override earlier ones.
    /// Actual environment variables take highest precedence over all files.
    pub fn load(paths: &[impl AsRef<Path>]) -> Result<Self, CliError> {
        let mut vars: HashMap<String, String> = HashMap::new();

        for path in paths {
            let path = path.as_ref();
            match dotenvy::from_path_iter(path) {
                Ok(iter) => {
                    for item in iter.flatten() {
                        vars.insert(item.0, item.1);
                    }
                }
                Err(e) if e.not_found() => {
                    // Skip missing files silently
                }
                Err(e) => {
                    return Err(CliError::EnvFile {
                        path: path.display().to_string(),
                        source: std::io::Error::new(std::io::ErrorKind::Other, e.to_string()),
                    });
                }
            }
        }

        let mut resolve = |key: &str| -> Option<String> {
            std::env::var(key)
                .ok()
                .or_else(|| vars.remove(&key.to_string()))
        };

        let database_url = resolve("DATABASE_URL")
            .ok_or_else(|| CliError::MissingEnvVar("DATABASE_URL".into()))?;
        let turbopuffer_api_key = resolve("TURBOPUFFER_API_KEY")
            .ok_or_else(|| CliError::MissingEnvVar("TURBOPUFFER_API_KEY".into()))?;
        let turbopuffer_region = resolve("TURBOPUFFER_REGION");
        let turbopuffer_namespace_prefix = resolve("TURBOPUFFER_NAMESPACE_PREFIX");

        Ok(Self {
            database_url,
            turbopuffer_api_key,
            turbopuffer_region,
            turbopuffer_namespace_prefix,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    /// All env vars that EnvConfig::load reads. Each test clears these via
    /// temp_env so that real env vars don't leak between tests.
    const ENV_KEYS: [&str; 4] = [
        "DATABASE_URL",
        "TURBOPUFFER_API_KEY",
        "TURBOPUFFER_REGION",
        "TURBOPUFFER_NAMESPACE_PREFIX",
    ];

    /// Returns (key, None) pairs for every env var EnvConfig reads,
    /// suitable for passing to `temp_env::with_vars`.
    fn cleared() -> Vec<(&'static str, Option<&'static str>)> {
        ENV_KEYS.iter().map(|k| (*k, None)).collect()
    }

    fn write_env(dir: &Path, name: &str, content: &str) -> std::path::PathBuf {
        let path = dir.join(name);
        fs::write(&path, content).unwrap();
        path
    }

    #[test]
    fn load_single_env_file() {
        let dir = TempDir::new().unwrap();
        let p = write_env(
            dir.path(),
            ".env",
            "DATABASE_URL=postgres://localhost/test\nTURBOPUFFER_API_KEY=tp-key-123\n",
        );

        temp_env::with_vars(cleared(), || {
            let cfg = EnvConfig::load(&[&p]).unwrap();
            assert_eq!(cfg.database_url, "postgres://localhost/test");
            assert_eq!(cfg.turbopuffer_api_key, "tp-key-123");
            assert!(cfg.turbopuffer_region.is_none());
        });
    }

    #[test]
    fn later_file_overrides_earlier() {
        let dir = TempDir::new().unwrap();
        let base = write_env(
            dir.path(),
            ".env",
            "DATABASE_URL=postgres://base\nTURBOPUFFER_API_KEY=base-key\nTURBOPUFFER_REGION=us-east-1\n",
        );
        let local = write_env(
            dir.path(),
            ".env.local",
            "DATABASE_URL=postgres://local\nTURBOPUFFER_API_KEY=local-key\n",
        );

        temp_env::with_vars(cleared(), || {
            let cfg = EnvConfig::load(&[&base, &local]).unwrap();
            assert_eq!(cfg.database_url, "postgres://local");
            assert_eq!(cfg.turbopuffer_api_key, "local-key");
            assert_eq!(cfg.turbopuffer_region.as_deref(), Some("us-east-1"));
        });
    }

    #[test]
    fn env_var_overrides_file() {
        let dir = TempDir::new().unwrap();
        let p = write_env(
            dir.path(),
            ".env",
            "DATABASE_URL=postgres://file\nTURBOPUFFER_API_KEY=file-key\n",
        );

        temp_env::with_vars(
            [
                ("DATABASE_URL", Some("postgres://env")),
                ("TURBOPUFFER_API_KEY", Some("env-key")),
                ("TURBOPUFFER_REGION", None),
                ("TURBOPUFFER_NAMESPACE_PREFIX", None),
            ],
            || {
                let cfg = EnvConfig::load(&[&p]).unwrap();
                assert_eq!(cfg.database_url, "postgres://env");
                assert_eq!(cfg.turbopuffer_api_key, "env-key");
            },
        );
    }

    #[test]
    fn missing_required_var_errors() {
        let dir = TempDir::new().unwrap();
        let p = write_env(dir.path(), ".env", "TURBOPUFFER_API_KEY=key\n");

        temp_env::with_vars(cleared(), || {
            let err = EnvConfig::load(&[&p]).unwrap_err();
            assert!(err.to_string().contains("DATABASE_URL"));
        });
    }

    #[test]
    fn missing_file_is_skipped() {
        let dir = TempDir::new().unwrap();
        let missing = dir.path().join(".env.missing");
        let present = write_env(
            dir.path(),
            ".env",
            "DATABASE_URL=postgres://ok\nTURBOPUFFER_API_KEY=key\n",
        );

        temp_env::with_vars(cleared(), || {
            let cfg = EnvConfig::load(&[&missing, &present]).unwrap();
            assert_eq!(cfg.database_url, "postgres://ok");
        });
    }

    #[test]
    fn turbopuffer_namespace_prefix_loaded() {
        let dir = TempDir::new().unwrap();
        let p = write_env(
            dir.path(),
            ".env",
            "DATABASE_URL=postgres://localhost/test\nTURBOPUFFER_API_KEY=key\nTURBOPUFFER_NAMESPACE_PREFIX=PRODUCTION\n",
        );

        temp_env::with_vars(cleared(), || {
            let cfg = EnvConfig::load(&[&p]).unwrap();
            assert_eq!(
                cfg.turbopuffer_namespace_prefix.as_deref(),
                Some("PRODUCTION")
            );
        });
    }

    #[test]
    fn turbopuffer_namespace_prefix_optional() {
        let dir = TempDir::new().unwrap();
        let p = write_env(
            dir.path(),
            ".env",
            "DATABASE_URL=postgres://localhost/test\nTURBOPUFFER_API_KEY=key\n",
        );

        temp_env::with_vars(cleared(), || {
            let cfg = EnvConfig::load(&[&p]).unwrap();
            assert!(cfg.turbopuffer_namespace_prefix.is_none());
        });
    }

    #[test]
    fn no_files_falls_back_to_env_vars() {
        temp_env::with_vars(
            [
                ("DATABASE_URL", Some("postgres://env-only")),
                ("TURBOPUFFER_API_KEY", Some("env-only-key")),
                ("TURBOPUFFER_REGION", None),
                ("TURBOPUFFER_NAMESPACE_PREFIX", None),
            ],
            || {
                let cfg = EnvConfig::load(&[] as &[&Path]).unwrap();
                assert_eq!(cfg.database_url, "postgres://env-only");
                assert_eq!(cfg.turbopuffer_api_key, "env-only-key");
            },
        );
    }
}
