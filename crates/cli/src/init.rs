use std::fs;
use std::io::IsTerminal;
use std::path::{Path, PathBuf};

use dialoguer::{Confirm, theme::ColorfulTheme};

use crate::env_discovery;
use crate::error::CliError;
use crate::paths::ProjectPaths;
use crate::project_config::ProjectConfig;

pub fn run() -> Result<(), CliError> {
    let cwd = std::env::current_dir()?;
    run_in(&cwd)
}

/// Whether `init` should prompt interactively. Requires both stdin and stdout
/// to be a TTY so non-interactive runs (Docker, CI, tests, piped input) skip the
/// picker and fall back to the default `environment_files`.
fn is_interactive() -> bool {
    std::io::stdin().is_terminal() && std::io::stdout().is_terminal()
}

pub fn run_in(cwd: &std::path::Path) -> Result<(), CliError> {
    let root = if cwd.join("puffgres.toml").exists() {
        // Re-init / Docker: puffgres.toml already in cwd, operate in-place
        cwd.to_path_buf()
    } else {
        // Fresh init: scaffold into a puffgres/ subdirectory
        cwd.join("puffgres")
    };

    let paths = ProjectPaths::new(root)?;
    let config_exists = paths.project_config.exists();

    let interactive = is_interactive();

    if interactive && !config_exists && !confirm_scaffold(&paths)? {
        println!("Aborted — nothing was created.");
        return Ok(());
    }

    fs::create_dir_all(&paths.root)?;
    fs::create_dir_all(&paths.configs)?;
    fs::create_dir_all(&paths.transforms)?;
    ensure_gitignore(cwd, &paths)?;
    ensure_dockerfile(&paths)?;
    ensure_dockerignore(&paths)?;
    ensure_package_json(&paths)?;
    ensure_vitest_config(&paths)?;
    ensure_utils(&paths)?;
    ensure_node_modules(&paths, interactive)?;

    // Interactive .env discovery — only on a fresh init at a TTY. Reinit (config
    // already present) and non-interactive runs keep the default env files.
    let chosen_env_files = if interactive && !config_exists {
        let candidates = env_discovery::discover_candidates(cwd);
        let picked = env_discovery::pick_env_files(&candidates, &paths.root)?;
        (!picked.is_empty()).then_some(picked)
    } else {
        None
    };
    ensure_project_config(cwd, &paths, chosen_env_files.as_deref())?;

    println!("Initialized puffgres project at {}", paths.root.display());
    println!();

    // Echo the env files puffgres will load, read back from the config just
    // written so the example line reflects reality rather than a placeholder.
    let env_files = ProjectConfig::load_unvalidated(&paths.project_config)
        .map(|pc| pc.environment_files)
        .unwrap_or_default();
    let env_files_toml = env_files
        .iter()
        .map(|f| format!("{f:?}"))
        .collect::<Vec<_>>()
        .join(", ");

    println!(
        "Env files (edit environment_files in {}):",
        paths.project_config.display()
    );
    println!("  environment_files = [{env_files_toml}]");
    println!("  Loaded in order — earlier files take priority; shell vars override all files.");
    println!();

    Ok(())
}

fn planned_structure(root_label: &str) -> String {
    format!(
        "  {root_label}/\n\
         \x20 ├── puffgres.toml       project config: env files to load, settings\n\
         \x20 ├── .gitignore\n\
         \x20 ├── configs/            table sync configs (added by `puffgres new`)\n\
         \x20 ├── transforms/         TypeScript row transforms\n\
         \x20 ├── utils/              transform helpers (env loading, embeddings, tokenizing)\n\
         \x20 ├── package.json        Node deps for transforms (`pnpm install` runs during init)\n\
         \x20 ├── vitest.config.ts    vitest setup for transform tests\n\
         \x20 ├── Dockerfile          used to deploy this project\n\
         \x20 └── .dockerignore\n"
    )
}

fn confirm_scaffold(paths: &ProjectPaths) -> Result<bool, CliError> {
    let root_label = paths
        .root
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| paths.root.display().to_string());

    println!(
        "puffgres init will create this structure at {}:",
        paths.root.display()
    );
    println!();
    println!("{}", planned_structure(&root_label));
    println!(
        "  puffgres needs a DATABASE_URL and TURBOPUFFER_API_KEY — the next step picks\n\
         \x20 which .env files to load them from. puffgres stores its own state (applied\n\
         \x20 configs, checkpoints) in that database, under a `puffgres` schema.\n\
         \n\
         \x20 Deploys reference this directory: the Dockerfile builds and ships everything\n\
         \x20 in it.\n\
         \n\
         \x20 Existing files are left untouched."
    );
    Confirm::with_theme(&ColorfulTheme::default())
        .with_prompt("Create these files?")
        .default(true)
        .interact()
        .map_err(|e| CliError::Generate(format!("prompt failed: {e}")))
}

fn ensure_gitignore(_cwd: &std::path::Path, paths: &ProjectPaths) -> Result<(), CliError> {
    let gitignore_path = paths.root.join(".gitignore");

    let entries = [".env", ".env.*", "node_modules"];

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

    let content = ".env\n.env.*\nnode_modules\nDockerfile\n.dockerignore\n.git\n";
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

/// Install the scaffolded Node dependencies so the first transform doesn't fail
/// with `ERR_MODULE_NOT_FOUND`. Failing here aborts `init`.
///
/// A puffgres project scaffolded inside an existing pnpm workspace is not a
/// member of it, so a plain `pnpm install` installs the *workspace* and skips
/// puffgres's own dependencies. On detecting an enclosing workspace we either add
/// puffgres to it (interactive opt-in) or install standalone with
/// `--ignore-workspace`.
fn ensure_node_modules(paths: &ProjectPaths, interactive: bool) -> Result<(), CliError> {
    if paths.root.join("node_modules").exists() {
        return Ok(());
    }

    let workspace = find_enclosing_pnpm_workspace(&paths.root);

    let join = match &workspace {
        Some(ws_root) if interactive => prompt_join_workspace(ws_root)?,
        Some(ws_root) => {
            // Non-interactive (Docker/CI): don't rewrite the user's workspace file.
            println!(
                "Detected a pnpm workspace at {} — installing puffgres standalone \
                 (pnpm install --ignore-workspace).",
                ws_root.display()
            );
            false
        }
        None => false,
    };

    if join {
        let ws_root = workspace.as_deref().expect("join implies a workspace");
        if add_project_to_workspace(ws_root, &paths.root)? {
            println!(
                "Added puffgres to the workspace at {}. Installing (pnpm install)...",
                ws_root.display()
            );
            return run_pnpm_install(&paths.root, false);
        }
        eprintln!(
            "  Couldn't edit {} automatically — installing puffgres standalone instead.",
            ws_root.join("pnpm-workspace.yaml").display()
        );
        return run_pnpm_install(&paths.root, true);
    }

    println!(
        "Installing Node dependencies (pnpm install{})...",
        if workspace.is_some() {
            " --ignore-workspace"
        } else {
            ""
        }
    );
    run_pnpm_install(&paths.root, workspace.is_some())
}

/// Run `pnpm install` in `dir`, optionally with `--ignore-workspace` so an
/// enclosing `pnpm-workspace.yaml` doesn't hijack the install.
fn run_pnpm_install(dir: &Path, ignore_workspace: bool) -> Result<(), CliError> {
    let mut cmd = std::process::Command::new("pnpm");
    cmd.arg("install");
    if ignore_workspace {
        cmd.arg("--ignore-workspace");
    }
    let status = cmd.current_dir(dir).status();

    let flag = if ignore_workspace {
        " --ignore-workspace"
    } else {
        ""
    };
    match status {
        Ok(status) if status.success() => Ok(()),
        Ok(status) => Err(CliError::Setup(format!(
            "`pnpm install{flag}` exited with {status} in {}. \
             Fix the errors above and re-run `puffgres init`.",
            dir.display()
        ))),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Err(CliError::Setup(format!(
            "puffgres uses pnpm for the node modules needed on JS transforms, \
             but `pnpm` was not found on your PATH. \
             Install pnpm at https://pnpm.io/installation, then re-run `puffgres init` in {}.",
            dir.display()
        ))),
        Err(e) => Err(CliError::Setup(format!(
            "could not run `pnpm install` ({e}) in {}. \
             Install pnpm at https://pnpm.io/installation, then re-run `puffgres init`.",
            dir.display()
        ))),
    }
}

/// Walk up from `root` looking for a `pnpm-workspace.yaml` in an *ancestor*
/// directory. Its presence means any `pnpm install` run inside `root` targets
/// that workspace rather than the puffgres project itself.
fn find_enclosing_pnpm_workspace(root: &Path) -> Option<PathBuf> {
    root.ancestors()
        .skip(1)
        .find(|dir| dir.join("pnpm-workspace.yaml").exists())
        .map(Path::to_path_buf)
}

/// Ask whether to add the puffgres project to the enclosing pnpm workspace.
/// Declining (the default) installs it standalone with `--ignore-workspace`.
fn prompt_join_workspace(ws_root: &Path) -> Result<bool, CliError> {
    println!();
    println!("Detected a pnpm workspace at {}.", ws_root.display());
    println!(
        "  This puffgres project isn't a member, so `pnpm install` here would install the \
         workspace and skip puffgres's own transform dependencies."
    );
    Confirm::with_theme(&ColorfulTheme::default())
        .with_prompt(
            "Add puffgres to that workspace? (No installs it standalone with --ignore-workspace)",
        )
        .default(false)
        .interact()
        .map_err(|e| CliError::Generate(format!("prompt failed: {e}")))
}

/// Add `project_root` to the `packages:` list of `ws_root/pnpm-workspace.yaml`.
/// Returns `true` when the project is now a member (edited or already listed),
/// `false` when the manifest uses a form we won't rewrite (so the caller falls
/// back to a standalone install).
fn add_project_to_workspace(ws_root: &Path, project_root: &Path) -> Result<bool, CliError> {
    let manifest = ws_root.join("pnpm-workspace.yaml");
    let content = fs::read_to_string(&manifest)?;
    let rel = env_discovery::relativize(ws_root, project_root);

    match insert_workspace_package(&content, &rel) {
        WorkspaceEdit::AlreadyPresent => Ok(true),
        WorkspaceEdit::Updated(updated) => {
            fs::write(&manifest, updated)?;
            Ok(true)
        }
        WorkspaceEdit::Unsupported => Ok(false),
    }
}

/// Outcome of trying to add a package entry to a pnpm workspace manifest.
enum WorkspaceEdit {
    /// The package is already listed — no change needed.
    AlreadyPresent,
    /// The manifest text with the new entry inserted.
    Updated(String),
    /// The manifest uses a form we won't edit automatically (e.g. inline flow).
    Unsupported,
}

/// Insert `rel` into the block-style `packages:` list of a pnpm workspace
/// manifest, matching the indentation of any existing entry. Inline
/// `packages: [...]` is left untouched (returns `Unsupported`).
fn insert_workspace_package(content: &str, rel: &str) -> WorkspaceEdit {
    let target = rel.trim_matches('/');
    let lines: Vec<String> = content.lines().map(str::to_string).collect();

    let mut pkg_idx = None;
    for (i, line) in lines.iter().enumerate() {
        let trimmed = line.trim();
        if trimmed == "packages:" {
            pkg_idx = Some(i);
            break;
        }
        if trimmed.starts_with("packages:") {
            return WorkspaceEdit::Unsupported;
        }
    }

    let Some(pkg_idx) = pkg_idx else {
        // No packages key at all — append a fresh block.
        let mut out = content.trim_end().to_string();
        out.push_str(&format!("\npackages:\n  - '{target}'\n"));
        return WorkspaceEdit::Updated(out);
    };

    // Scan list items belonging to this key: they are indented and start with
    // `-`. The block ends at the next unindented (top-level) key.
    let mut indent = String::from("  ");
    let mut found_item = false;
    let mut insert_after = pkg_idx;
    for (i, line) in lines.iter().enumerate().skip(pkg_idx + 1) {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let indent_len = line.len() - line.trim_start().len();
        if indent_len == 0 {
            break; // next top-level key — packages block ended
        }
        if let Some(value) = trimmed.strip_prefix('-') {
            if !found_item {
                indent = line[..indent_len].to_string();
                found_item = true;
            }
            let value = value.trim().trim_matches(['\'', '"']).trim_matches('/');
            if value == target {
                return WorkspaceEdit::AlreadyPresent;
            }
            insert_after = i;
        }
    }

    let mut out_lines = lines;
    out_lines.insert(insert_after + 1, format!("{indent}- '{target}'"));
    let mut out = out_lines.join("\n");
    if content.ends_with('\n') {
        out.push('\n');
    }
    WorkspaceEdit::Updated(out)
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
        (
            "embed-zeroentropy.ts",
            include_str!("../templates/utils/embed-zeroentropy.ts"),
        ),
        (
            "embed-baseten.ts",
            include_str!("../templates/utils/embed-baseten.ts"),
        ),
        (
            "embed-cloudflare.ts",
            include_str!("../templates/utils/embed-cloudflare.ts"),
        ),
        (
            "tokenize.ts",
            include_str!("../templates/utils/tokenize.ts"),
        ),
        (
            "puffgres.ts",
            include_str!("../templates/utils/puffgres.ts"),
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

fn ensure_project_config(
    cwd: &std::path::Path,
    paths: &ProjectPaths,
    chosen_env_files: Option<&[String]>,
) -> Result<(), CliError> {
    if paths.project_config.exists() {
        return Ok(());
    }

    let mut config = ProjectConfig::default();

    if let Some(files) = chosen_env_files {
        // The user picked env files interactively — use them verbatim.
        config.environment_files = files.to_vec();
    } else if paths.root != cwd {
        // In fresh-init mode (subdir), point .env to the parent directory so
        // runtime resolution finds the repo-root .env instead of looking inside
        // the puffgres subdirectory.
        config.environment_files = vec!["../.env".to_string()];
    }

    let contents = toml::to_string_pretty(&config).map_err(|e| {
        CliError::Io(std::io::Error::other(format!(
            "failed to serialize config: {e}"
        )))
    })?;
    fs::write(&paths.project_config, contents)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Run `init` with `node_modules` pre-created so `ensure_node_modules`
    /// early-returns instead of shelling out to `pnpm` (absent in CI).
    fn run_in_no_install(cwd: &Path) -> Result<(), CliError> {
        let root = if cwd.join("puffgres.toml").exists() {
            cwd.to_path_buf()
        } else {
            cwd.join("puffgres")
        };
        fs::create_dir_all(root.join("node_modules")).unwrap();
        run_in(cwd)
    }

    #[test]
    fn planned_structure_lists_scaffold() {
        let tree = planned_structure("puffgres");
        for entry in [
            "puffgres/",
            "puffgres.toml",
            ".gitignore",
            "configs/",
            "transforms/",
            "utils/",
            "package.json",
            "vitest.config.ts",
            "Dockerfile",
            ".dockerignore",
        ] {
            assert!(tree.contains(entry), "missing {entry} in:\n{tree}");
        }
    }

    #[test]
    fn creates_puffgres_subdirectory() {
        let dir = tempfile::tempdir().unwrap();

        run_in_no_install(dir.path()).unwrap();

        let sub = dir.path().join("puffgres");
        assert!(sub.is_dir());
        assert!(sub.join("configs").is_dir());
        assert!(sub.join("transforms").is_dir());
    }

    #[test]
    fn creates_gitignore_with_env_entries() {
        let dir = tempfile::tempdir().unwrap();

        run_in_no_install(dir.path()).unwrap();

        let gitignore = fs::read_to_string(dir.path().join("puffgres").join(".gitignore")).unwrap();
        assert!(gitignore.contains(".env\n"));
        assert!(gitignore.contains(".env.*"));
        assert!(gitignore.contains("node_modules"));
    }

    #[test]
    fn appends_env_entries_to_existing_gitignore() {
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("puffgres");
        fs::create_dir_all(&sub).unwrap();
        fs::write(sub.join(".gitignore"), "node_modules\n").unwrap();

        run_in_no_install(dir.path()).unwrap();

        let gitignore = fs::read_to_string(sub.join(".gitignore")).unwrap();
        assert!(gitignore.contains("node_modules"));
        assert!(gitignore.contains(".env\n"));
        assert!(gitignore.contains(".env.*"));
    }

    #[test]
    fn does_not_duplicate_gitignore_entries() {
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("puffgres");
        fs::create_dir_all(&sub).unwrap();
        fs::write(sub.join(".gitignore"), ".env\nnode_modules\n").unwrap();

        run_in_no_install(dir.path()).unwrap();

        let gitignore = fs::read_to_string(sub.join(".gitignore")).unwrap();
        // Should not have duplicates
        assert_eq!(gitignore.matches(".env\n").count(), 1);
        assert!(gitignore.contains(".env.*"));
        assert!(gitignore.contains("node_modules"));
    }

    #[test]
    fn creates_dockerfile() {
        let dir = tempfile::tempdir().unwrap();

        run_in_no_install(dir.path()).unwrap();

        let dockerfile =
            fs::read_to_string(dir.path().join("puffgres").join("Dockerfile")).unwrap();
        assert!(dockerfile.contains("FROM ghcr.io/a24films/puffgres"));
        assert!(dockerfile.contains("pnpm install"));
        assert!(dockerfile.contains("puffgres run"));
    }

    #[test]
    fn does_not_overwrite_existing_dockerfile() {
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("puffgres");
        fs::create_dir_all(&sub).unwrap();
        fs::write(sub.join("Dockerfile"), "custom").unwrap();

        run_in_no_install(dir.path()).unwrap();

        let dockerfile = fs::read_to_string(sub.join("Dockerfile")).unwrap();
        assert_eq!(dockerfile, "custom");
    }

    #[test]
    fn creates_dockerignore() {
        let dir = tempfile::tempdir().unwrap();

        run_in_no_install(dir.path()).unwrap();

        let dockerignore =
            fs::read_to_string(dir.path().join("puffgres").join(".dockerignore")).unwrap();
        assert!(dockerignore.contains(".env"));
        assert!(dockerignore.contains("node_modules"));
    }

    #[test]
    fn does_not_overwrite_existing_dockerignore() {
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("puffgres");
        fs::create_dir_all(&sub).unwrap();
        fs::write(sub.join(".dockerignore"), "custom").unwrap();

        run_in_no_install(dir.path()).unwrap();

        let dockerignore = fs::read_to_string(sub.join(".dockerignore")).unwrap();
        assert_eq!(dockerignore, "custom");
    }

    #[test]
    fn creates_project_config() {
        let dir = tempfile::tempdir().unwrap();

        run_in_no_install(dir.path()).unwrap();

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
        run_in_no_install(dir.path()).unwrap();

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

        run_in_no_install(dir.path()).unwrap();

        // Should NOT create a puffgres/ subfolder
        assert!(!dir.path().join("puffgres").exists());
        // Should create files directly in cwd
        assert!(dir.path().join("configs").is_dir());
        assert!(dir.path().join("transforms").is_dir());
    }

    #[test]
    fn idempotent_with_subdirectory() {
        let dir = tempfile::tempdir().unwrap();

        run_in_no_install(dir.path()).unwrap();
        run_in_no_install(dir.path()).unwrap();

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

        run_in_no_install(dir.path()).unwrap();

        let config = ProjectConfig::load(&dir.path().join("puffgres.toml")).unwrap();
        assert_eq!(config.environment_files, vec![".env"]);
    }

    #[test]
    fn creates_package_json() {
        let dir = tempfile::tempdir().unwrap();

        run_in_no_install(dir.path()).unwrap();

        let package_json =
            fs::read_to_string(dir.path().join("puffgres").join("package.json")).unwrap();
        assert!(package_json.contains("openai"));
        assert!(package_json.contains("@huggingface/transformers"));
        assert!(package_json.contains("vitest"));
    }

    #[test]
    fn does_not_overwrite_existing_package_json() {
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("puffgres");
        fs::create_dir_all(&sub).unwrap();
        fs::write(sub.join("package.json"), "custom").unwrap();

        run_in_no_install(dir.path()).unwrap();

        let package_json = fs::read_to_string(sub.join("package.json")).unwrap();
        assert_eq!(package_json, "custom");
    }

    #[test]
    fn creates_vitest_config() {
        let dir = tempfile::tempdir().unwrap();

        run_in_no_install(dir.path()).unwrap();

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

        run_in_no_install(dir.path()).unwrap();

        let vitest = fs::read_to_string(sub.join("vitest.config.ts")).unwrap();
        assert_eq!(vitest, "custom");
    }

    #[test]
    fn creates_utils_directory() {
        let dir = tempfile::tempdir().unwrap();

        run_in_no_install(dir.path()).unwrap();

        let sub = dir.path().join("puffgres");
        assert!(sub.join("utils").is_dir());
        assert!(sub.join("utils/load-env.ts").exists());
        assert!(sub.join("utils/embed-zeroentropy.ts").exists());
        assert!(sub.join("utils/embed-baseten.ts").exists());
        assert!(sub.join("utils/embed-cloudflare.ts").exists());
        assert!(sub.join("utils/tokenize.ts").exists());
        assert!(sub.join("utils/puffgres.ts").exists());
    }

    #[test]
    fn utils_contain_expected_content() {
        let dir = tempfile::tempdir().unwrap();

        run_in_no_install(dir.path()).unwrap();

        let sub = dir.path().join("puffgres");
        let load_env = fs::read_to_string(sub.join("utils/load-env.ts")).unwrap();
        assert!(load_env.contains("dotenv"));
        assert!(load_env.contains("smol-toml"));

        let embed = fs::read_to_string(sub.join("utils/embed-cloudflare.ts")).unwrap();
        assert!(embed.contains("embedBatchCloudflare"));
        assert!(embed.contains("tokenizeBatch"));

        let tokenize = fs::read_to_string(sub.join("utils/tokenize.ts")).unwrap();
        assert!(tokenize.contains("@huggingface/transformers"));
        assert!(tokenize.contains("tokenizeBatch"));

        let puffgres = fs::read_to_string(sub.join("utils/puffgres.ts")).unwrap();
        assert!(puffgres.contains("PrimitiveType"));
    }

    fn updated(edit: WorkspaceEdit) -> String {
        match edit {
            WorkspaceEdit::Updated(s) => s,
            WorkspaceEdit::AlreadyPresent => panic!("expected Updated, got AlreadyPresent"),
            WorkspaceEdit::Unsupported => panic!("expected Updated, got Unsupported"),
        }
    }

    #[test]
    fn inserts_into_block_packages_list() {
        let content = "packages:\n  - 'packages/*'\n";
        let out = updated(insert_workspace_package(content, "puffgres"));
        assert_eq!(out, "packages:\n  - 'packages/*'\n  - 'puffgres'\n");
    }

    #[test]
    fn inserts_matching_existing_indentation() {
        let content = "packages:\n    - 'apps/*'\n";
        let out = updated(insert_workspace_package(content, "puffgres"));
        assert!(out.contains("\n    - 'puffgres'"), "got: {out}");
    }

    #[test]
    fn insert_is_idempotent_across_quote_styles() {
        for existing in [
            "packages:\n  - 'puffgres'\n",
            "packages:\n  - \"puffgres\"\n",
            "packages:\n  - puffgres\n",
        ] {
            assert!(
                matches!(
                    insert_workspace_package(existing, "puffgres"),
                    WorkspaceEdit::AlreadyPresent
                ),
                "expected AlreadyPresent for {existing:?}"
            );
        }
    }

    #[test]
    fn insert_stops_at_next_top_level_key() {
        let content = "packages:\n  - 'packages/*'\nonlyBuiltDependencies:\n  - esbuild\n";
        let out = updated(insert_workspace_package(content, "puffgres"));
        // New entry lands under packages, before the next top-level key.
        let pkg_line = out.lines().position(|l| l.contains("'puffgres'")).unwrap();
        let key_line = out
            .lines()
            .position(|l| l.starts_with("onlyBuiltDependencies:"))
            .unwrap();
        assert!(pkg_line < key_line, "got: {out}");
    }

    #[test]
    fn insert_appends_block_when_no_packages_key() {
        let content = "onlyBuiltDependencies:\n  - esbuild\n";
        let out = updated(insert_workspace_package(content, "puffgres"));
        assert!(out.contains("packages:\n  - 'puffgres'\n"), "got: {out}");
    }

    #[test]
    fn insert_leaves_inline_flow_untouched() {
        let content = "packages: ['packages/*']\n";
        assert!(matches!(
            insert_workspace_package(content, "puffgres"),
            WorkspaceEdit::Unsupported
        ));
    }

    #[test]
    fn finds_enclosing_workspace() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(
            dir.path().join("pnpm-workspace.yaml"),
            "packages:\n  - 'x'\n",
        )
        .unwrap();
        let nested = dir.path().join("a/puffgres");
        fs::create_dir_all(&nested).unwrap();

        let found = find_enclosing_pnpm_workspace(&nested).unwrap();
        assert_eq!(
            fs::canonicalize(&found).unwrap(),
            fs::canonicalize(dir.path()).unwrap()
        );
    }

    #[test]
    fn no_enclosing_workspace_when_absent() {
        let dir = tempfile::tempdir().unwrap();
        let nested = dir.path().join("a/puffgres");
        fs::create_dir_all(&nested).unwrap();
        assert!(find_enclosing_pnpm_workspace(&nested).is_none());
    }

    #[test]
    fn does_not_overwrite_existing_utils() {
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("puffgres");
        let utils = sub.join("utils");
        fs::create_dir_all(&utils).unwrap();
        fs::write(utils.join("tokenize.ts"), "custom").unwrap();

        run_in_no_install(dir.path()).unwrap();

        let content = fs::read_to_string(utils.join("tokenize.ts")).unwrap();
        assert_eq!(content, "custom");
        // But the other files should still be created
        assert!(utils.join("embed-cloudflare.ts").exists());
        assert!(utils.join("load-env.ts").exists());
    }
}
