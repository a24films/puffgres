use std::fs;

use state::StateDb;

use crate::error::CliError;
use crate::paths::ProjectPaths;
use crate::project_config::ProjectConfig;

pub fn run(paths: &ProjectPaths) -> Result<(), CliError> {
    fs::create_dir_all(&paths.configs)?;
    fs::create_dir_all(&paths.transforms)?;
    ensure_project_config(paths)?;

    let db = StateDb::open(&paths.state_db)?;
    db.initialize()?;

    eprintln!("Initialized puffgres project at {}", paths.root.display());
    eprintln!();
    eprintln!("Make sure the following environment variables are set:");
    eprintln!("  DATABASE_URL          (required)");
    eprintln!("  TURBOPUFFER_API_KEY   (required)");
    eprintln!("  TURBOPUFFER_REGION    (optional)");

    Ok(())
}

fn ensure_project_config(paths: &ProjectPaths) -> Result<(), CliError> {
    if paths.project_config.exists() {
        return Ok(());
    }

    let config = ProjectConfig::default();
    let contents = toml::to_string_pretty(&config).expect("default ProjectConfig should serialize");
    fs::write(&paths.project_config, contents)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_directory_structure() {
        let dir = tempfile::tempdir().unwrap();
        let paths = ProjectPaths::new(dir.path().to_path_buf());

        run(&paths).unwrap();

        assert!(paths.configs.is_dir());
        assert!(paths.transforms.is_dir());
    }

    #[test]
    fn creates_state_db() {
        let dir = tempfile::tempdir().unwrap();
        let paths = ProjectPaths::new(dir.path().to_path_buf());

        run(&paths).unwrap();

        assert!(paths.state_db.exists());
    }

    #[test]
    fn creates_project_config() {
        let dir = tempfile::tempdir().unwrap();
        let paths = ProjectPaths::new(dir.path().to_path_buf());

        run(&paths).unwrap();

        let config = ProjectConfig::load(&paths.project_config).unwrap();
        assert_eq!(config.environment_files, vec![".env"]);
    }

    #[test]
    fn does_not_overwrite_existing_project_config() {
        let dir = tempfile::tempdir().unwrap();
        let paths = ProjectPaths::new(dir.path().to_path_buf());
        fs::write(
            &paths.project_config,
            r#"environment_files = [".env", ".env.prod"]"#,
        )
        .unwrap();

        run(&paths).unwrap();

        let config = ProjectConfig::load(&paths.project_config).unwrap();
        assert_eq!(config.environment_files, vec![".env", ".env.prod"]);
    }

    #[test]
    fn idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let paths = ProjectPaths::new(dir.path().to_path_buf());

        run(&paths).unwrap();
        run(&paths).unwrap();

        assert!(paths.configs.is_dir());
        assert!(paths.transforms.is_dir());
        assert!(paths.state_db.exists());
    }
}
