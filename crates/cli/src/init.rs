use std::fs;

use state::StateDb;

use crate::error::CliError;
use crate::paths::ProjectPaths;
use crate::project_config::ProjectConfig;

pub fn run() -> Result<(), CliError> {
    let cwd = std::env::current_dir()?;
    run_in(&cwd)
}

pub fn run_in(cwd: &std::path::Path) -> Result<(), CliError> {
    let root = if cwd.join("puffgres.toml").exists() {
        // Re-init / Docker: puffgres.toml already in cwd, operate in-place
        cwd.to_path_buf()
    } else {
        // Fresh init: create puffgres/ subdirectory
        let sub = cwd.join("puffgres");
        fs::create_dir_all(&sub)?;
        sub
    };

    let paths = ProjectPaths::new(root);

    fs::create_dir_all(&paths.configs)?;
    fs::create_dir_all(&paths.transforms)?;
    ensure_gitignore(cwd, &paths)?;
    ensure_project_config(&paths)?;
    ensure_dockerfile(&paths)?;

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

fn ensure_gitignore(cwd: &std::path::Path, paths: &ProjectPaths) -> Result<(), CliError> {
    // Write to the parent directory's .gitignore, referencing state.db from
    // within the puffgres subdirectory. In Docker / re-init mode (root == cwd)
    // there is no parent to write to, so write directly into root.
    let (gitignore_path, entry) = if paths.root != cwd {
        (cwd.join(".gitignore"), "puffgres/state.db")
    } else {
        (paths.root.join(".gitignore"), "state.db")
    };

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
    fn creates_puffgres_subdirectory() {
        let dir = tempfile::tempdir().unwrap();

        run_in(dir.path()).unwrap();

        let sub = dir.path().join("puffgres");
        assert!(sub.is_dir());
        assert!(sub.join("configs").is_dir());
        assert!(sub.join("transforms").is_dir());
    }

    #[test]
    fn creates_gitignore_in_parent_with_puffgres_state_db() {
        let dir = tempfile::tempdir().unwrap();

        run_in(dir.path()).unwrap();

        let gitignore = fs::read_to_string(dir.path().join(".gitignore")).unwrap();
        assert!(gitignore.lines().any(|l| l.trim() == "puffgres/state.db"));
    }

    #[test]
    fn appends_to_existing_parent_gitignore() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join(".gitignore"), "node_modules\n").unwrap();

        run_in(dir.path()).unwrap();

        let gitignore = fs::read_to_string(dir.path().join(".gitignore")).unwrap();
        assert!(gitignore.contains("node_modules"));
        assert!(gitignore.lines().any(|l| l.trim() == "puffgres/state.db"));
    }

    #[test]
    fn appends_newline_if_missing() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join(".gitignore"), "node_modules").unwrap();

        run_in(dir.path()).unwrap();

        let gitignore = fs::read_to_string(dir.path().join(".gitignore")).unwrap();
        assert_eq!(gitignore, "node_modules\npuffgres/state.db\n");
    }

    #[test]
    fn creates_dockerfile() {
        let dir = tempfile::tempdir().unwrap();

        run_in(dir.path()).unwrap();

        let dockerfile =
            fs::read_to_string(dir.path().join("puffgres").join("Dockerfile")).unwrap();
        assert!(dockerfile.contains("PUFFGRES_GITHUB_ACCESS_TOKEN"));
        assert!(dockerfile.contains("PUFFGRES_BRANCH_NAME"));
        assert!(dockerfile.contains("cargo install --path crates/cli"));
    }

    #[test]
    fn does_not_overwrite_existing_dockerfile() {
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("puffgres");
        fs::create_dir_all(&sub).unwrap();
        fs::write(sub.join("Dockerfile"), "custom").unwrap();

        run_in(dir.path()).unwrap();

        let dockerfile = fs::read_to_string(sub.join("Dockerfile")).unwrap();
        assert_eq!(dockerfile, "custom");
    }

    #[test]
    fn creates_state_db() {
        let dir = tempfile::tempdir().unwrap();

        run_in(dir.path()).unwrap();

        assert!(dir.path().join("puffgres").join("state.db").exists());
    }

    #[test]
    fn creates_project_config() {
        let dir = tempfile::tempdir().unwrap();

        run_in(dir.path()).unwrap();

        let config_path = dir.path().join("puffgres").join("puffgres.toml");
        let config = ProjectConfig::load(&config_path).unwrap();
        assert_eq!(config.environment_files, vec![".env"]);
    }

    #[test]
    fn does_not_overwrite_existing_project_config() {
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("puffgres");
        fs::create_dir_all(&sub).unwrap();
        fs::write(
            sub.join("puffgres.toml"),
            r#"environment_files = [".env", ".env.prod"]"#,
        )
        .unwrap();

        // puffgres.toml is NOT in cwd, so run_in still targets the subfolder
        run_in(dir.path()).unwrap();

        let config = ProjectConfig::load(&sub.join("puffgres.toml")).unwrap();
        assert_eq!(config.environment_files, vec![".env", ".env.prod"]);
    }

    #[test]
    fn reinit_in_place_when_config_exists_in_cwd() {
        let dir = tempfile::tempdir().unwrap();
        // Simulate Docker: puffgres.toml already in cwd
        fs::write(
            dir.path().join("puffgres.toml"),
            "environment_files = [\".env\"]",
        )
        .unwrap();

        run_in(dir.path()).unwrap();

        // Should NOT create a puffgres/ subfolder
        assert!(!dir.path().join("puffgres").exists());
        // Should create files directly in cwd
        assert!(dir.path().join("configs").is_dir());
        assert!(dir.path().join("transforms").is_dir());
        assert!(dir.path().join("state.db").exists());
    }

    #[test]
    fn idempotent_with_subdirectory() {
        let dir = tempfile::tempdir().unwrap();

        run_in(dir.path()).unwrap();
        run_in(dir.path()).unwrap();

        let gitignore = fs::read_to_string(dir.path().join(".gitignore")).unwrap();
        assert_eq!(gitignore.matches("puffgres/state.db").count(), 1);
    }
}
