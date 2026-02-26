use std::path::{Path, PathBuf};

use crate::env::{load_env_files, resolve_env_var};
use crate::error::CliError;
use crate::project_config::ProjectConfig;

#[derive(Debug, Clone)]
pub struct ProjectPaths {
    pub root: PathBuf,
    pub configs: PathBuf,
    pub transforms: PathBuf,
    pub state_db: PathBuf,
    pub project_config: PathBuf,
    pub dockerfile: PathBuf,
    pub dockerignore: PathBuf,
}

impl ProjectPaths {
    pub fn new(root: PathBuf) -> Result<Self, CliError> {
        let state_override = Self::resolve_state_override(&root);
        Self::new_with_state_override(root, state_override)
    }

    /// Resolve PUFFGRES_STATE_PATH by checking process env first,
    /// then env files referenced in puffgres.toml.
    fn resolve_state_override(root: &Path) -> Option<String> {
        let config_path = root.join("puffgres.toml");
        let config = ProjectConfig::load(&config_path).ok()?;
        let env_paths = config.resolve_env_paths(root);
        let file_vars = load_env_files(&env_paths).ok()?;
        resolve_env_var("PUFFGRES_STATE_PATH", &file_vars)
    }

    fn new_with_state_override(
        root: PathBuf,
        state_override: Option<String>,
    ) -> Result<Self, CliError> {
        let state_db = match state_override {
            Some(s) if s.trim().is_empty() => {
                return Err(CliError::InvalidStatePath(
                    "PUFFGRES_STATE_PATH is set but empty".into(),
                ));
            }
            Some(s) => {
                let p = PathBuf::from(&s);
                if p.is_relative() { root.join(p) } else { p }
            }
            None => root.join("state.db"),
        };

        let configs = root.join("configs");
        let transforms = root.join("transforms");
        let project_config = root.join("puffgres.toml");
        let dockerfile = root.join("Dockerfile");
        let dockerignore = root.join(".dockerignore");

        Ok(Self {
            root,
            configs,
            transforms,
            state_db,
            project_config,
            dockerfile,
            dockerignore,
        })
    }

    pub fn from_current_dir() -> Result<Self, CliError> {
        let cwd = std::env::current_dir()?;
        Self::new(Self::detect_root(cwd))
    }

    /// Detect the project root from a given directory.
    ///
    /// - If `dir/puffgres.toml` exists, root = dir (running from inside the project)
    /// - If `dir/puffgres/puffgres.toml` exists, root = dir/puffgres/ (running from parent)
    /// - Otherwise, root = dir (will fail later when loading config)
    pub fn detect_root(dir: PathBuf) -> PathBuf {
        if dir.join("puffgres.toml").exists() {
            dir
        } else if dir.join("puffgres").join("puffgres.toml").exists() {
            dir.join("puffgres")
        } else {
            dir
        }
    }

    pub fn from_path(path: &Path) -> Result<Self, CliError> {
        Self::new(path.to_path_buf())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths_derived_from_root() {
        let root = PathBuf::from("/tmp/myproject");
        let paths = ProjectPaths::new_with_state_override(root.clone(), None).unwrap();

        assert_eq!(paths.root, root);
        assert_eq!(paths.configs, root.join("configs"));
        assert_eq!(paths.transforms, root.join("transforms"));
        assert_eq!(paths.state_db, root.join("state.db"));
        assert_eq!(paths.project_config, root.join("puffgres.toml"));
        assert_eq!(paths.dockerfile, root.join("Dockerfile"));
        assert_eq!(paths.dockerignore, root.join(".dockerignore"));
    }

    #[test]
    fn from_current_dir_succeeds() {
        let paths = ProjectPaths::from_current_dir().unwrap();
        assert!(paths.root.is_absolute());
    }

    #[test]
    fn from_path_works() {
        let dir = tempfile::tempdir().unwrap();
        let paths = ProjectPaths::from_path(dir.path()).unwrap();
        assert_eq!(paths.root, dir.path());
        assert_eq!(paths.configs, dir.path().join("configs"));
    }

    #[test]
    fn detect_root_with_config_in_cwd() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("puffgres.toml"), "").unwrap();

        let root = ProjectPaths::detect_root(dir.path().to_path_buf());
        assert_eq!(root, dir.path());
    }

    #[test]
    fn detect_root_with_puffgres_subfolder() {
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("puffgres");
        std::fs::create_dir_all(&sub).unwrap();
        std::fs::write(sub.join("puffgres.toml"), "").unwrap();

        let root = ProjectPaths::detect_root(dir.path().to_path_buf());
        assert_eq!(root, sub);
    }

    #[test]
    fn detect_root_fallback_to_cwd() {
        let dir = tempfile::tempdir().unwrap();

        let root = ProjectPaths::detect_root(dir.path().to_path_buf());
        assert_eq!(root, dir.path());
    }

    #[test]
    fn state_db_respects_env_override() {
        let root = PathBuf::from("/tmp/myproject");

        let paths =
            ProjectPaths::new_with_state_override(root, Some("/mnt/data/state.db".to_string()))
                .unwrap();

        assert_eq!(paths.state_db, PathBuf::from("/mnt/data/state.db"));
    }

    #[test]
    fn state_db_defaults_without_override() {
        let root = PathBuf::from("/tmp/myproject");

        let paths = ProjectPaths::new_with_state_override(root.clone(), None).unwrap();

        assert_eq!(paths.state_db, root.join("state.db"));
    }

    #[test]
    fn state_db_rejects_empty_override() {
        let root = PathBuf::from("/tmp/myproject");

        let err = ProjectPaths::new_with_state_override(root, Some("".to_string())).unwrap_err();
        assert!(err.to_string().contains("empty"));

        // Whitespace-only is also rejected
        let root = PathBuf::from("/tmp/myproject");
        let err = ProjectPaths::new_with_state_override(root, Some("  ".to_string())).unwrap_err();
        assert!(err.to_string().contains("empty"));
    }

    #[test]
    fn state_db_resolves_relative_override_against_root() {
        let root = PathBuf::from("/tmp/myproject");

        let paths = ProjectPaths::new_with_state_override(
            root.clone(),
            Some("custom/state.db".to_string()),
        )
        .unwrap();

        assert_eq!(paths.state_db, root.join("custom/state.db"));
    }

    #[test]
    fn resolve_state_override_reads_env_files() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();

        std::fs::write(root.join("puffgres.toml"), "environment_files = [\".env\"]").unwrap();
        std::fs::write(
            root.join(".env"),
            "PUFFGRES_STATE_PATH=/mnt/data/state.db\n",
        )
        .unwrap();

        temp_env::with_vars([("PUFFGRES_STATE_PATH", None::<&str>)], || {
            let val = ProjectPaths::resolve_state_override(root);
            assert_eq!(val.as_deref(), Some("/mnt/data/state.db"));
        });
    }
}
