use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::error::CliError;

#[derive(Debug, Clone)]
pub struct EnvConfig {
    pub database_url: String,
    pub turbopuffer_api_key: String,
    pub turbopuffer_region: Option<String>,
    pub turbopuffer_namespace_prefix: Option<String>,
    pub otel_endpoint: Option<String>,
    pub otel_headers: Option<String>,
    pub state_db_path: PathBuf,
}

/// Load all key-value pairs from a list of `.env` file paths.
///
/// Files are loaded in order — later files override earlier ones.
/// Missing files are skipped silently.
pub fn load_env_files(paths: &[impl AsRef<Path>]) -> Result<HashMap<String, String>, CliError> {
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

    Ok(vars)
}

/// Resolve a single environment variable: process env takes precedence, then file vars.
pub fn resolve_env_var(key: &str, file_vars: &HashMap<String, String>) -> Option<String> {
    std::env::var(key)
        .ok()
        .or_else(|| file_vars.get(key).cloned())
}

/// Resolve the state database path from env files and process env.
///
/// Use this for commands that need only the DB path (setup, reset, tombstone, status)
/// without requiring the full EnvConfig (DATABASE_URL, TURBOPUFFER_API_KEY, etc.).
///
/// Falls back to `<project_root>/state.db` when `PUFFGRES_STATE_DB` is not set.
/// Relative paths are resolved against `project_root`.
pub fn resolve_state_db_path(
    env_file_paths: &[impl AsRef<Path>],
    project_root: &Path,
) -> Result<PathBuf, CliError> {
    let file_vars = load_env_files(env_file_paths)?;
    let raw = resolve_env_var("PUFFGRES_STATE_DB", &file_vars);
    resolve_state_db(raw, project_root)
}

/// Shared logic for resolving the state DB path from a raw env var value.
///
/// - If the value is empty or whitespace-only, returns an error.
/// - Falls back to `<project_root>/state.db` when the value is `None`.
/// - Relative paths are resolved against `project_root`.
fn resolve_state_db(raw: Option<String>, project_root: &Path) -> Result<PathBuf, CliError> {
    let path = match raw {
        Some(val) if val.trim().is_empty() => {
            return Err(CliError::MissingEnvVar(
                "PUFFGRES_STATE_DB is set but empty. Set it to a valid path or remove it to use the default."
                    .into(),
            ));
        }
        Some(val) => PathBuf::from(val),
        None => PathBuf::from("state.db"),
    };

    if path.is_relative() {
        Ok(project_root.join(path))
    } else {
        Ok(path)
    }
}

impl EnvConfig {
    /// Load environment config from multiple `.env` file paths.
    ///
    /// Files are loaded in order — later files override earlier ones.
    /// Actual environment variables take highest precedence over all files.
    ///
    /// `project_root` is used to resolve relative `PUFFGRES_STATE_DB` paths
    /// and as the default location for `state.db` when the var is unset.
    pub fn load(paths: &[impl AsRef<Path>], project_root: &Path) -> Result<Self, CliError> {
        let mut vars = load_env_files(paths)?;

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
        let otel_endpoint = resolve("OTEL_EXPORTER_OTLP_ENDPOINT");
        let otel_headers = resolve("OTEL_EXPORTER_OTLP_HEADERS");
        let state_db_path = resolve_state_db(resolve("PUFFGRES_STATE_DB"), project_root)?;

        Ok(Self {
            database_url,
            turbopuffer_api_key,
            turbopuffer_region,
            turbopuffer_namespace_prefix,
            otel_endpoint,
            otel_headers,
            state_db_path,
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
    const ENV_KEYS: [&str; 7] = [
        "DATABASE_URL",
        "TURBOPUFFER_API_KEY",
        "TURBOPUFFER_REGION",
        "TURBOPUFFER_NAMESPACE_PREFIX",
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "OTEL_EXPORTER_OTLP_HEADERS",
        "PUFFGRES_STATE_DB",
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
            "DATABASE_URL=postgres://localhost/test\nTURBOPUFFER_API_KEY=tp-key-123\nPUFFGRES_STATE_DB=/tmp/state.db\n",
        );

        temp_env::with_vars(cleared(), || {
            let cfg = EnvConfig::load(&[&p], dir.path()).unwrap();
            assert_eq!(cfg.database_url, "postgres://localhost/test");
            assert_eq!(cfg.turbopuffer_api_key, "tp-key-123");
            assert!(cfg.turbopuffer_region.is_none());
            assert_eq!(cfg.state_db_path, PathBuf::from("/tmp/state.db"));
        });
    }

    #[test]
    fn later_file_overrides_earlier() {
        let dir = TempDir::new().unwrap();
        let base = write_env(
            dir.path(),
            ".env",
            "DATABASE_URL=postgres://base\nTURBOPUFFER_API_KEY=base-key\nTURBOPUFFER_REGION=us-east-1\nPUFFGRES_STATE_DB=/tmp/state.db\n",
        );
        let local = write_env(
            dir.path(),
            ".env.local",
            "DATABASE_URL=postgres://local\nTURBOPUFFER_API_KEY=local-key\n",
        );

        temp_env::with_vars(cleared(), || {
            let cfg = EnvConfig::load(&[&base, &local], dir.path()).unwrap();
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
            "DATABASE_URL=postgres://file\nTURBOPUFFER_API_KEY=file-key\nPUFFGRES_STATE_DB=/tmp/state.db\n",
        );

        temp_env::with_vars(
            [
                ("DATABASE_URL", Some("postgres://env")),
                ("TURBOPUFFER_API_KEY", Some("env-key")),
                ("TURBOPUFFER_REGION", None),
                ("TURBOPUFFER_NAMESPACE_PREFIX", None),
                ("PUFFGRES_STATE_DB", None),
            ],
            || {
                let cfg = EnvConfig::load(&[&p], dir.path()).unwrap();
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
            let err = EnvConfig::load(&[&p], dir.path()).unwrap_err();
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
            "DATABASE_URL=postgres://ok\nTURBOPUFFER_API_KEY=key\nPUFFGRES_STATE_DB=/tmp/state.db\n",
        );

        temp_env::with_vars(cleared(), || {
            let cfg = EnvConfig::load(&[&missing, &present], dir.path()).unwrap();
            assert_eq!(cfg.database_url, "postgres://ok");
        });
    }

    #[test]
    fn turbopuffer_namespace_prefix_loaded() {
        let dir = TempDir::new().unwrap();
        let p = write_env(
            dir.path(),
            ".env",
            "DATABASE_URL=postgres://localhost/test\nTURBOPUFFER_API_KEY=key\nTURBOPUFFER_NAMESPACE_PREFIX=PRODUCTION\nPUFFGRES_STATE_DB=/tmp/state.db\n",
        );

        temp_env::with_vars(cleared(), || {
            let cfg = EnvConfig::load(&[&p], dir.path()).unwrap();
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
            "DATABASE_URL=postgres://localhost/test\nTURBOPUFFER_API_KEY=key\nPUFFGRES_STATE_DB=/tmp/state.db\n",
        );

        temp_env::with_vars(cleared(), || {
            let cfg = EnvConfig::load(&[&p], dir.path()).unwrap();
            assert!(cfg.turbopuffer_namespace_prefix.is_none());
        });
    }

    #[test]
    fn no_files_falls_back_to_env_vars() {
        let dir = TempDir::new().unwrap();
        temp_env::with_vars(
            [
                ("DATABASE_URL", Some("postgres://env-only")),
                ("TURBOPUFFER_API_KEY", Some("env-only-key")),
                ("TURBOPUFFER_REGION", None),
                ("TURBOPUFFER_NAMESPACE_PREFIX", None),
                ("PUFFGRES_STATE_DB", Some("/tmp/state.db")),
            ],
            || {
                let cfg = EnvConfig::load(&[] as &[&Path], dir.path()).unwrap();
                assert_eq!(cfg.database_url, "postgres://env-only");
                assert_eq!(cfg.turbopuffer_api_key, "env-only-key");
            },
        );
    }

    #[test]
    fn resolve_state_db_path_from_env_file() {
        let dir = TempDir::new().unwrap();
        let p = write_env(dir.path(), ".env", "PUFFGRES_STATE_DB=/mnt/data/state.db\n");

        temp_env::with_vars([("PUFFGRES_STATE_DB", None::<&str>)], || {
            let path = resolve_state_db_path(&[&p], dir.path()).unwrap();
            assert_eq!(path, PathBuf::from("/mnt/data/state.db"));
        });
    }

    #[test]
    fn resolve_state_db_path_falls_back_to_default() {
        let dir = TempDir::new().unwrap();
        let p = write_env(dir.path(), ".env", "OTHER_VAR=foo\n");

        temp_env::with_vars([("PUFFGRES_STATE_DB", None::<&str>)], || {
            let path = resolve_state_db_path(&[&p], dir.path()).unwrap();
            assert_eq!(path, dir.path().join("state.db"));
        });
    }

    #[test]
    fn resolve_state_db_path_relative_resolved_against_root() {
        let dir = TempDir::new().unwrap();
        let p = write_env(dir.path(), ".env", "PUFFGRES_STATE_DB=data/my.db\n");

        temp_env::with_vars([("PUFFGRES_STATE_DB", None::<&str>)], || {
            let path = resolve_state_db_path(&[&p], dir.path()).unwrap();
            assert_eq!(path, dir.path().join("data/my.db"));
        });
    }

    #[test]
    fn resolve_state_db_path_empty_errors() {
        let dir = TempDir::new().unwrap();
        let p = write_env(dir.path(), ".env", "PUFFGRES_STATE_DB=\n");

        temp_env::with_vars([("PUFFGRES_STATE_DB", None::<&str>)], || {
            let err = resolve_state_db_path(&[&p], dir.path()).unwrap_err();
            assert!(err.to_string().contains("PUFFGRES_STATE_DB"));
        });
    }

    #[test]
    fn state_db_defaults_in_env_config_load() {
        let dir = TempDir::new().unwrap();
        let p = write_env(
            dir.path(),
            ".env",
            "DATABASE_URL=postgres://localhost/test\nTURBOPUFFER_API_KEY=key\n",
        );

        temp_env::with_vars(cleared(), || {
            let cfg = EnvConfig::load(&[&p], dir.path()).unwrap();
            assert_eq!(cfg.state_db_path, dir.path().join("state.db"));
        });
    }
}
