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

    let paths = ProjectPaths::new(root)?;

    fs::create_dir_all(&paths.configs)?;
    fs::create_dir_all(&paths.transforms)?;
    ensure_gitignore(cwd, &paths)?;
    ensure_project_config(cwd, &paths)?;
    ensure_dockerfile(&paths)?;
    ensure_dockerignore(&paths)?;

    let db = StateDb::open(&paths.state_db)?;
    db.initialize()?;

    println!("Initialized puffgres project at {}", paths.root.display());
    println!();

    // -- environment_files hint ----------------------------------------
    println!(
        "Configure env file paths in {}:",
        paths.project_config.display()
    );
    println!();
    println!("  environment_files = [\".env\", \".env.local\"]");
    println!();
    println!("  Files are loaded in order — later files override earlier ones.");
    println!("  Shell environment variables take highest precedence over all files.");
    println!();

    // -- per-variable status -------------------------------------------
    println!("Environment variables:");
    println!();

    let env_vars: &[(&str, bool)] = &[
        ("DATABASE_URL", true),
        ("TURBOPUFFER_API_KEY", true),
        ("TURBOPUFFER_REGION", false),
        ("TURBOPUFFER_NAMESPACE_PREFIX", false),
        ("PUFFGRES_STATE_PATH", false),
        ("OTEL_EXPORTER_OTLP_ENDPOINT", false),
    ];

    for &(name, required) in env_vars {
        let req_label = if required { "required" } else { "optional" };
        let status = if std::env::var(name).is_ok() {
            "set"
        } else {
            "not set"
        };
        println!("  {name:<32} ({req_label}, {status})");
    }

    Ok(())
}

fn ensure_gitignore(cwd: &std::path::Path, paths: &ProjectPaths) -> Result<(), CliError> {
    // Place .gitignore in the parent directory for fresh-init (root != cwd),
    // or in the project root for Docker / re-init mode (root == cwd).
    let gitignore_path = if paths.root != cwd {
        cwd.join(".gitignore")
    } else {
        paths.root.join(".gitignore")
    };

    let gitignore_dir = gitignore_path.parent().unwrap_or(cwd);

    // Derive the entry from the resolved state_db path. If state_db is
    // outside the gitignore directory (e.g. an absolute external path),
    // there's nothing to ignore.
    let entry = match paths.state_db.strip_prefix(gitignore_dir) {
        Ok(relative) => relative.display().to_string(),
        Err(_) => return Ok(()),
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

    let mut content = String::new();

    // Generate state DB ignore patterns from the resolved path.
    if let Ok(relative) = paths.state_db.strip_prefix(&paths.root) {
        let base = relative.display().to_string();
        content.push_str(&format!("{base}\n{base}-journal\n{base}-wal\n{base}-shm\n"));
    }

    content.push_str(".env\n.env.*\nnode_modules\nDockerfile\n.dockerignore\n.git\n");
    fs::write(&paths.dockerignore, content)?;

    Ok(())
}

fn ensure_project_config(cwd: &std::path::Path, paths: &ProjectPaths) -> Result<(), CliError> {
    if paths.project_config.exists() {
        return Ok(());
    }

    let mut config = ProjectConfig::default();

    // In fresh-init mode (subdir), point .env to the parent directory so
    // runtime resolution finds the repo-root .env instead of looking inside
    // the puffgres subdirectory.
    if paths.root != cwd {
        config.environment_files = vec!["../.env".to_string()];
    }

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
        assert!(dockerfile.contains("mount=type=secret,id=github_token"));
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
    fn creates_dockerignore() {
        let dir = tempfile::tempdir().unwrap();

        run_in(dir.path()).unwrap();

        let dockerignore =
            fs::read_to_string(dir.path().join("puffgres").join(".dockerignore")).unwrap();
        assert!(dockerignore.contains("state.db"));
        assert!(dockerignore.contains(".env"));
    }

    #[test]
    fn does_not_overwrite_existing_dockerignore() {
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("puffgres");
        fs::create_dir_all(&sub).unwrap();
        fs::write(sub.join(".dockerignore"), "custom").unwrap();

        run_in(dir.path()).unwrap();

        let dockerignore = fs::read_to_string(sub.join(".dockerignore")).unwrap();
        assert_eq!(dockerignore, "custom");
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
        assert_eq!(config.environment_files, vec!["../.env"]);
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

    #[test]
    fn reinit_preserves_dotenv_default() {
        let dir = tempfile::tempdir().unwrap();
        // Simulate Docker: puffgres.toml already in cwd
        fs::write(
            dir.path().join("puffgres.toml"),
            "environment_files = [\".env\"]",
        )
        .unwrap();

        run_in(dir.path()).unwrap();

        let config = ProjectConfig::load(&dir.path().join("puffgres.toml")).unwrap();
        assert_eq!(config.environment_files, vec![".env"]);
    }
}
