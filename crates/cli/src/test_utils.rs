use crate::paths::ProjectPaths;
use state::StateDb;
use std::fs;

pub fn setup_project() -> (tempfile::TempDir, ProjectPaths) {
    let dir = tempfile::tempdir().unwrap();
    let paths = ProjectPaths::new(dir.path().to_path_buf());

    fs::create_dir_all(&paths.configs).unwrap();
    fs::create_dir_all(&paths.transforms).unwrap();

    let db = StateDb::open(&paths.state_db).unwrap();
    db.initialize().unwrap();

    (dir, paths)
}
