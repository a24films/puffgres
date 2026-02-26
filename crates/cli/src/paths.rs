use std::path::{Path, PathBuf};

use crate::error::CliError;

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
    pub fn new(root: PathBuf) -> Self {
        let configs = root.join("configs");
        let transforms = root.join("transforms");
        let state_db = root.join("state.db");
        let project_config = root.join("puffgres.toml");
        let dockerfile = root.join("Dockerfile");
        let dockerignore = root.join(".dockerignore");
        Self {
            root,
            configs,
            transforms,
            state_db,
            project_config,
            dockerfile,
            dockerignore,
        }
    }

    pub fn from_current_dir() -> Result<Self, CliError> {
        let root = std::env::current_dir()?;
        Ok(Self::new(root))
    }

    pub fn from_path(path: &Path) -> Self {
        Self::new(path.to_path_buf())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths_derived_from_root() {
        let root = PathBuf::from("/tmp/myproject");
        let paths = ProjectPaths::new(root.clone());

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
        let paths = ProjectPaths::from_path(Path::new("/some/dir"));
        assert_eq!(paths.root, PathBuf::from("/some/dir"));
        assert_eq!(paths.configs, PathBuf::from("/some/dir/configs"));
    }
}
