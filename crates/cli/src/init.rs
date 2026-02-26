use std::fs;

use state::StateDb;

use crate::error::CliError;
use crate::paths::ProjectPaths;
use crate::project_config::ProjectConfig;

pub fn run(paths: &ProjectPaths) -> Result<(), CliError> {
    fs::create_dir_all(&paths.configs)?;
    fs::create_dir_all(&paths.transforms)?;
    ensure_project_config(paths)?;
    ensure_dockerfile(paths)?;
    ensure_dockerignore(paths)?;

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

fn ensure_dockerfile(paths: &ProjectPaths) -> Result<(), CliError> {
    if paths.dockerfile.exists() {
        return Ok(());
    }

    let template = include_str!("../templates/Dockerfile");
    fs::write(&paths.dockerfile, template)?;

    Ok(())
}

fn ensure_dockerignore(paths: &ProjectPaths) -> Result<(), CliError> {
    if paths.dockerignore.exists() {
        return Ok(());
    }

    let template = include_str!("../templates/dockerignore");
    fs::write(&paths.dockerignore, template)?;

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
    fn creates_dockerfile() {
        let dir = tempfile::tempdir().unwrap();
        let paths = ProjectPaths::new(dir.path().to_path_buf());

        run(&paths).unwrap();

        let dockerfile = fs::read_to_string(&paths.dockerfile).unwrap();
        assert!(dockerfile.contains("mount=type=secret,id=github_token"));
        assert!(dockerfile.contains("PUFFGRES_BRANCH_NAME"));
        assert!(dockerfile.contains("cargo install --path crates/cli"));
    }

    #[test]
    fn does_not_overwrite_existing_dockerfile() {
        let dir = tempfile::tempdir().unwrap();
        let paths = ProjectPaths::new(dir.path().to_path_buf());
        fs::write(&paths.dockerfile, "custom").unwrap();

        run(&paths).unwrap();

        let dockerfile = fs::read_to_string(&paths.dockerfile).unwrap();
        assert_eq!(dockerfile, "custom");
    }

    #[test]
    fn creates_dockerignore() {
        let dir = tempfile::tempdir().unwrap();
        let paths = ProjectPaths::new(dir.path().to_path_buf());

        run(&paths).unwrap();

        let dockerignore = fs::read_to_string(&paths.dockerignore).unwrap();
        assert!(dockerignore.contains("state.db"));
        assert!(dockerignore.contains(".env"));
    }

    #[test]
    fn does_not_overwrite_existing_dockerignore() {
        let dir = tempfile::tempdir().unwrap();
        let paths = ProjectPaths::new(dir.path().to_path_buf());
        fs::write(&paths.dockerignore, "custom").unwrap();

        run(&paths).unwrap();

        let dockerignore = fs::read_to_string(&paths.dockerignore).unwrap();
        assert_eq!(dockerignore, "custom");
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
