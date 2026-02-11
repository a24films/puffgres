use std::collections::HashMap;
use std::path::Path;

use crate::error::CliError;

#[derive(Debug, Clone)]
pub struct EnvConfig {
    pub database_url: String,
    pub turbopuffer_api_key: String,
    pub turbopuffer_region: Option<String>,
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

        Ok(Self {
            database_url,
            turbopuffer_api_key,
            turbopuffer_region,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::Mutex;
    use tempfile::TempDir;

    // Env var mutations are process-wide, so these tests must not run in parallel.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn write_env(dir: &Path, name: &str, content: &str) -> std::path::PathBuf {
        let path = dir.join(name);
        fs::write(&path, content).unwrap();
        path
    }

    unsafe fn clear_env() {
        unsafe {
            std::env::remove_var("DATABASE_URL");
            std::env::remove_var("TURBOPUFFER_API_KEY");
            std::env::remove_var("TURBOPUFFER_REGION");
        }
    }

    #[test]
    fn load_single_env_file() {
        let _lock = ENV_LOCK.lock().unwrap();
        let dir = TempDir::new().unwrap();
        let p = write_env(
            dir.path(),
            ".env",
            "DATABASE_URL=postgres://localhost/test\nTURBOPUFFER_API_KEY=tp-key-123\n",
        );

        unsafe { clear_env() };

        let cfg = EnvConfig::load(&[&p]).unwrap();
        assert_eq!(cfg.database_url, "postgres://localhost/test");
        assert_eq!(cfg.turbopuffer_api_key, "tp-key-123");
        assert!(cfg.turbopuffer_region.is_none());
    }

    #[test]
    fn later_file_overrides_earlier() {
        let _lock = ENV_LOCK.lock().unwrap();
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

        unsafe { clear_env() };

        let cfg = EnvConfig::load(&[&base, &local]).unwrap();
        assert_eq!(cfg.database_url, "postgres://local");
        assert_eq!(cfg.turbopuffer_api_key, "local-key");
        assert_eq!(cfg.turbopuffer_region.as_deref(), Some("us-east-1"));
    }

    #[test]
    fn env_var_overrides_file() {
        let _lock = ENV_LOCK.lock().unwrap();
        let dir = TempDir::new().unwrap();
        let p = write_env(
            dir.path(),
            ".env",
            "DATABASE_URL=postgres://file\nTURBOPUFFER_API_KEY=file-key\n",
        );

        unsafe {
            std::env::set_var("DATABASE_URL", "postgres://env");
            std::env::set_var("TURBOPUFFER_API_KEY", "env-key");
            std::env::remove_var("TURBOPUFFER_REGION");
        }

        let cfg = EnvConfig::load(&[&p]).unwrap();
        assert_eq!(cfg.database_url, "postgres://env");
        assert_eq!(cfg.turbopuffer_api_key, "env-key");

        unsafe { clear_env() };
    }

    #[test]
    fn missing_required_var_errors() {
        let _lock = ENV_LOCK.lock().unwrap();
        let dir = TempDir::new().unwrap();
        let p = write_env(dir.path(), ".env", "TURBOPUFFER_API_KEY=key\n");

        unsafe { clear_env() };

        let err = EnvConfig::load(&[&p]).unwrap_err();
        assert!(err.to_string().contains("DATABASE_URL"));
    }

    #[test]
    fn missing_file_is_skipped() {
        let _lock = ENV_LOCK.lock().unwrap();
        let dir = TempDir::new().unwrap();
        let missing = dir.path().join(".env.missing");
        let present = write_env(
            dir.path(),
            ".env",
            "DATABASE_URL=postgres://ok\nTURBOPUFFER_API_KEY=key\n",
        );

        unsafe { clear_env() };

        let cfg = EnvConfig::load(&[&missing, &present]).unwrap();
        assert_eq!(cfg.database_url, "postgres://ok");
    }

    #[test]
    fn no_files_falls_back_to_env_vars() {
        let _lock = ENV_LOCK.lock().unwrap();
        unsafe {
            std::env::set_var("DATABASE_URL", "postgres://env-only");
            std::env::set_var("TURBOPUFFER_API_KEY", "env-only-key");
            std::env::remove_var("TURBOPUFFER_REGION");
        }

        let cfg = EnvConfig::load(&[] as &[&Path]).unwrap();
        assert_eq!(cfg.database_url, "postgres://env-only");
        assert_eq!(cfg.turbopuffer_api_key, "env-only-key");

        unsafe { clear_env() };
    }
}
