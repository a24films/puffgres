use std::fs;

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
    ensure_package_json(&paths)?;
    ensure_vitest_config(&paths)?;
    ensure_utils(&paths)?;

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
        ("PUFFGRES_STATE_DB", false),
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

    println!();
    println!("Next, run `puffgres setup` to create the state database.");

    Ok(())
}

fn ensure_gitignore(_cwd: &std::path::Path, paths: &ProjectPaths) -> Result<(), CliError> {
    let gitignore_path = paths.root.join(".gitignore");

    let entries = [
        "state.db",
        "state.db-journal",
        "state.db-wal",
        "state.db-shm",
    ];

    if gitignore_path.exists() {
        let existing = fs::read_to_string(&gitignore_path)?;
        let mut to_add = Vec::new();
        for entry in &entries {
            if !existing.lines().any(|l| l.trim() == *entry) {
                to_add.push(*entry);
            }
        }
        if !to_add.is_empty() {
            let mut content = existing;
            if !content.ends_with('\n') && !content.is_empty() {
                content.push('\n');
            }
            for entry in to_add {
                content.push_str(entry);
                content.push('\n');
            }
            fs::write(&gitignore_path, content)?;
        }
    } else {
        let content: String = entries.iter().map(|e| format!("{e}\n")).collect();
        fs::write(&gitignore_path, content)?;
    }

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

    let content = "state.db\nstate.db-journal\nstate.db-wal\nstate.db-shm\n.env\n.env.*\nnode_modules\nDockerfile\n.dockerignore\n.git\n";
    fs::write(&paths.dockerignore, content)?;

    Ok(())
}

fn ensure_package_json(paths: &ProjectPaths) -> Result<(), CliError> {
    let path = paths.root.join("package.json");
    if path.exists() {
        return Ok(());
    }

    let template = include_str!("../templates/package.json");
    fs::write(&path, template)?;

    Ok(())
}

fn ensure_vitest_config(paths: &ProjectPaths) -> Result<(), CliError> {
    let path = paths.root.join("vitest.config.ts");
    if path.exists() {
        return Ok(());
    }

    let template = include_str!("../templates/vitest.config.ts");
    fs::write(&path, template)?;

    Ok(())
}

fn ensure_utils(paths: &ProjectPaths) -> Result<(), CliError> {
    let utils_dir = paths.root.join("utils");
    fs::create_dir_all(&utils_dir)?;

    let files = &[
        (
            "load-env.ts",
            include_str!("../templates/utils/load-env.ts"),
        ),
        ("embed.ts", include_str!("../templates/utils/embed.ts")),
        (
            "tokenize.ts",
            include_str!("../templates/utils/tokenize.ts"),
        ),
    ];

    for (name, content) in files {
        let path = utils_dir.join(name);
        if !path.exists() {
            fs::write(&path, content)?;
        }
    }

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
    fn creates_gitignore_with_state_db_entries() {
        let dir = tempfile::tempdir().unwrap();

        run_in(dir.path()).unwrap();

        let gitignore = fs::read_to_string(dir.path().join("puffgres").join(".gitignore")).unwrap();
        assert!(gitignore.contains("state.db\n"));
        assert!(gitignore.contains("state.db-journal"));
        assert!(gitignore.contains("state.db-wal"));
        assert!(gitignore.contains("state.db-shm"));
    }

    #[test]
    fn appends_state_db_to_existing_gitignore() {
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("puffgres");
        fs::create_dir_all(&sub).unwrap();
        fs::write(sub.join(".gitignore"), "node_modules\n").unwrap();

        run_in(dir.path()).unwrap();

        let gitignore = fs::read_to_string(sub.join(".gitignore")).unwrap();
        assert!(gitignore.contains("node_modules"));
        assert!(gitignore.contains("state.db\n"));
        assert!(gitignore.contains("state.db-wal"));
    }

    #[test]
    fn does_not_duplicate_gitignore_entries() {
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("puffgres");
        fs::create_dir_all(&sub).unwrap();
        fs::write(sub.join(".gitignore"), "state.db\nstate.db-wal\n").unwrap();

        run_in(dir.path()).unwrap();

        let gitignore = fs::read_to_string(sub.join(".gitignore")).unwrap();
        // Should not have duplicates
        assert_eq!(gitignore.matches("state.db\n").count(), 1);
        assert!(gitignore.contains("state.db-journal"));
        assert!(gitignore.contains("state.db-shm"));
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
        assert!(dockerignore.contains("state.db-wal"));
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
    fn does_not_create_state_db() {
        let dir = tempfile::tempdir().unwrap();

        run_in(dir.path()).unwrap();

        assert!(!dir.path().join("puffgres").join("state.db").exists());
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
        // Should NOT create state.db (handled by `puffgres setup`)
        assert!(!dir.path().join("state.db").exists());
    }

    #[test]
    fn idempotent_with_subdirectory() {
        let dir = tempfile::tempdir().unwrap();

        run_in(dir.path()).unwrap();
        run_in(dir.path()).unwrap();

        // Should still have all directories
        let sub = dir.path().join("puffgres");
        assert!(sub.join("configs").is_dir());
        assert!(sub.join("transforms").is_dir());
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

    #[test]
    fn creates_package_json() {
        let dir = tempfile::tempdir().unwrap();

        run_in(dir.path()).unwrap();

        let package_json =
            fs::read_to_string(dir.path().join("puffgres").join("package.json")).unwrap();
        assert!(package_json.contains("together-ai"));
        assert!(package_json.contains("@huggingface/transformers"));
        assert!(package_json.contains("vitest"));
    }

    #[test]
    fn does_not_overwrite_existing_package_json() {
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("puffgres");
        fs::create_dir_all(&sub).unwrap();
        fs::write(sub.join("package.json"), "custom").unwrap();

        run_in(dir.path()).unwrap();

        let package_json = fs::read_to_string(sub.join("package.json")).unwrap();
        assert_eq!(package_json, "custom");
    }

    #[test]
    fn creates_vitest_config() {
        let dir = tempfile::tempdir().unwrap();

        run_in(dir.path()).unwrap();

        let vitest =
            fs::read_to_string(dir.path().join("puffgres").join("vitest.config.ts")).unwrap();
        assert!(vitest.contains("vitest"));
        assert!(vitest.contains("tests/**/*.test.ts"));
    }

    #[test]
    fn does_not_overwrite_existing_vitest_config() {
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("puffgres");
        fs::create_dir_all(&sub).unwrap();
        fs::write(sub.join("vitest.config.ts"), "custom").unwrap();

        run_in(dir.path()).unwrap();

        let vitest = fs::read_to_string(sub.join("vitest.config.ts")).unwrap();
        assert_eq!(vitest, "custom");
    }

    #[test]
    fn creates_utils_directory() {
        let dir = tempfile::tempdir().unwrap();

        run_in(dir.path()).unwrap();

        let sub = dir.path().join("puffgres");
        assert!(sub.join("utils").is_dir());
        assert!(sub.join("utils/load-env.ts").exists());
        assert!(sub.join("utils/embed.ts").exists());
        assert!(sub.join("utils/tokenize.ts").exists());
    }

    #[test]
    fn utils_contain_expected_content() {
        let dir = tempfile::tempdir().unwrap();

        run_in(dir.path()).unwrap();

        let sub = dir.path().join("puffgres");
        let load_env = fs::read_to_string(sub.join("utils/load-env.ts")).unwrap();
        assert!(load_env.contains("dotenv"));
        assert!(load_env.contains("smol-toml"));

        let embed = fs::read_to_string(sub.join("utils/embed.ts")).unwrap();
        assert!(embed.contains("together-ai"));
        assert!(embed.contains("embedBatch"));

        let tokenize = fs::read_to_string(sub.join("utils/tokenize.ts")).unwrap();
        assert!(tokenize.contains("@huggingface/transformers"));
        assert!(tokenize.contains("tokenizeBatch"));
    }

    #[test]
    fn does_not_overwrite_existing_utils() {
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("puffgres");
        let utils = sub.join("utils");
        fs::create_dir_all(&utils).unwrap();
        fs::write(utils.join("tokenize.ts"), "custom").unwrap();

        run_in(dir.path()).unwrap();

        let content = fs::read_to_string(utils.join("tokenize.ts")).unwrap();
        assert_eq!(content, "custom");
        // But the other files should still be created
        assert!(utils.join("embed.ts").exists());
        assert!(utils.join("load-env.ts").exists());
    }
}
