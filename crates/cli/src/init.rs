use std::fs;

use state::StateDb;

use crate::error::CliError;
use crate::paths::ProjectPaths;
use crate::project_config::ProjectConfig;

pub fn run(paths: &ProjectPaths) -> Result<(), CliError> {
    fs::create_dir_all(&paths.configs)?;
    fs::create_dir_all(&paths.transforms)?;
    ensure_gitignore(paths)?;
    ensure_project_config(paths)?;
    ensure_dockerfile(paths)?;

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

fn ensure_gitignore(paths: &ProjectPaths) -> Result<(), CliError> {
    let gitignore_path = paths.root.join(".gitignore");
    let entry = "state.db";

    let existing = fs::read_to_string(&gitignore_path).unwrap_or_default();
    if existing.lines().any(|line| line.trim() == entry) {
        return Ok(());
    }

    let needs_leading_newline = !existing.is_empty() && !existing.ends_with('\n');

    use std::io::Write;
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&gitignore_path)?;

    if needs_leading_newline {
        file.write_all(b"\n")?;
    }
    file.write_all(format!("{entry}\n").as_bytes())?;

    Ok(())
}

fn ensure_dockerfile(paths: &ProjectPaths) -> Result<(), CliError> {
    let dockerfile_path = paths.root.join("Dockerfile");
    if dockerfile_path.exists() {
        return Ok(());
    }

    let template = include_str!("../templates/Dockerfile");
    fs::write(&dockerfile_path, template)?;

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
    fn creates_gitignore_with_state_db() {
        let dir = tempfile::tempdir().unwrap();
        let paths = ProjectPaths::new(dir.path().to_path_buf());

        run(&paths).unwrap();

        let gitignore = fs::read_to_string(paths.root.join(".gitignore")).unwrap();
        assert!(gitignore.lines().any(|l| l.trim() == "state.db"));
    }

    #[test]
    fn appends_to_existing_gitignore() {
        let dir = tempfile::tempdir().unwrap();
        let paths = ProjectPaths::new(dir.path().to_path_buf());
        fs::write(paths.root.join(".gitignore"), "node_modules\n").unwrap();

        run(&paths).unwrap();

        let gitignore = fs::read_to_string(paths.root.join(".gitignore")).unwrap();
        assert!(gitignore.contains("node_modules"));
        assert!(gitignore.lines().any(|l| l.trim() == "state.db"));
    }

    #[test]
    fn appends_newline_if_missing() {
        let dir = tempfile::tempdir().unwrap();
        let paths = ProjectPaths::new(dir.path().to_path_buf());
        fs::write(paths.root.join(".gitignore"), "node_modules").unwrap();

        run(&paths).unwrap();

        let gitignore = fs::read_to_string(paths.root.join(".gitignore")).unwrap();
        assert_eq!(gitignore, "node_modules\nstate.db\n");
    }

    #[test]
    fn creates_dockerfile() {
        let dir = tempfile::tempdir().unwrap();
        let paths = ProjectPaths::new(dir.path().to_path_buf());

        run(&paths).unwrap();

        let dockerfile = fs::read_to_string(paths.root.join("Dockerfile")).unwrap();
        assert!(dockerfile.contains("PUFFGRES_GITHUB_ACCESS_TOKEN"));
        assert!(dockerfile.contains("PUFFGRES_BRANCH_NAME"));
        assert!(dockerfile.contains("cargo install --path crates/cli"));
    }

    #[test]
    fn does_not_overwrite_existing_dockerfile() {
        let dir = tempfile::tempdir().unwrap();
        let paths = ProjectPaths::new(dir.path().to_path_buf());
        fs::write(paths.root.join("Dockerfile"), "custom").unwrap();

        run(&paths).unwrap();

        let dockerfile = fs::read_to_string(paths.root.join("Dockerfile")).unwrap();
        assert_eq!(dockerfile, "custom");
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

        let gitignore = fs::read_to_string(paths.root.join(".gitignore")).unwrap();
        assert_eq!(gitignore.matches("state.db").count(), 1);
    }
}
