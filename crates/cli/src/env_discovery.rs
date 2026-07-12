//! Discover `.env`-style files near a project and let the user pick which ones
//! to load, for `puffgres init`.
//!
//! Discovery scans two directions from the directory the user ran `init` in:
//! **down** into the project (bounded recursive walk) and **up** the directory
//! tree to the filesystem root. Only file *keys* are ever read — values are
//! never retained. Unreadable directories (permission denied, etc.) are skipped
//! silently; nothing is ever run with elevated privileges.

use std::fs;
use std::path::{Path, PathBuf};

use dialoguer::{MultiSelect, Sort, theme::ColorfulTheme};

use crate::error::CliError;

/// Maximum depth for the downward walk (0 = files directly in the scan root).
const MAX_DEPTH: usize = 4;

/// Cap on how many `.env` files the downward walk collects before it stops.
const MAX_RESULTS: usize = 200;

/// Directories the downward walk never descends into — large or irrelevant
/// trees that would only add noise (and cost) to discovery.
const SKIP_DIRS: &[&str] = &[
    "node_modules",
    ".git",
    "target",
    "dist",
    "build",
    ".next",
    "vendor",
    ".venv",
    "__pycache__",
];

/// A discovered `.env` file and the variable names (not values) it defines.
#[derive(Debug, Clone)]
pub struct EnvFileCandidate {
    pub path: PathBuf,
    pub keys: Vec<String>,
}

/// Whether a file name looks like a `.env` file. Deliberately permissive so
/// `.env`, `.env.local`, `.env.development`, `.env.prod`, `.env.example`, etc.
/// all match.
pub fn is_env_file_name(name: &str) -> bool {
    name.contains(".env")
}

/// List `.env`-style files directly inside `dir` (non-recursive). Returns an
/// empty vec when the directory can't be read (permission denied, etc.).
fn env_files_in_dir(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let Ok(entries) = fs::read_dir(dir) else {
        return out;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        if let Some(name) = path.file_name().and_then(|n| n.to_str())
            && is_env_file_name(name)
        {
            out.push(path);
        }
    }
    out
}

/// Walk up from `start`, collecting `.env` files in each ancestor directory.
///
/// Iterates `start.ancestors()`, so it naturally terminates at the filesystem
/// root. Directories that can't be read are skipped and the walk continues.
pub fn find_ancestor_env_files(start: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    for dir in start.ancestors() {
        out.extend(env_files_in_dir(dir));
    }
    out
}

/// Walk down from `root`, collecting `.env` files up to `MAX_DEPTH` levels deep.
///
/// Skips heavy/irrelevant directories (`SKIP_DIRS`), backs off from directories
/// it can't read, and stops after `MAX_RESULTS` hits (logging a note so the cap
/// isn't silent).
pub fn find_descendant_env_files(root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut truncated = false;
    walk_descendants(root, 0, &mut out, &mut truncated);
    if truncated {
        eprintln!(
            "  note: stopped after {MAX_RESULTS} .env files while scanning {} — add any others by editing puffgres.toml",
            root.display()
        );
    }
    out
}

fn walk_descendants(dir: &Path, depth: usize, out: &mut Vec<PathBuf>, truncated: &mut bool) {
    if *truncated {
        return;
    }

    let Ok(entries) = fs::read_dir(dir) else {
        return; // unreadable directory — back off
    };

    let mut subdirs = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        let Ok(file_type) = entry.file_type() else {
            continue;
        };

        if file_type.is_dir() {
            let skip = path
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| SKIP_DIRS.contains(&n));
            if !skip && depth < MAX_DEPTH {
                subdirs.push(path);
            }
        } else if file_type.is_file()
            && let Some(name) = path.file_name().and_then(|n| n.to_str())
            && is_env_file_name(name)
        {
            if out.len() >= MAX_RESULTS {
                *truncated = true;
                return;
            }
            out.push(path);
        }
    }

    for sub in subdirs {
        walk_descendants(&sub, depth + 1, out, truncated);
        if *truncated {
            return;
        }
    }
}

/// Read only the variable *names* from a `.env` file, in file order, deduped.
///
/// Values are parsed by `dotenvy` but immediately dropped. A file that can't be
/// read or parsed yields an empty list.
pub fn read_env_keys(path: &Path) -> Vec<String> {
    let mut keys = Vec::new();
    let Ok(iter) = dotenvy::from_path_iter(path) else {
        return keys;
    };
    for (key, _value) in iter.flatten() {
        if !keys.contains(&key) {
            keys.push(key);
        }
    }
    keys
}

/// Discover `.env` candidates near `scan_root` (the directory `init` was run
/// in): files inside it (down to `MAX_DEPTH`) and in every ancestor directory.
///
/// Duplicates (a file reachable both ways) are collapsed by canonical path.
/// Results are ordered nearest-first: descendants (shallowest first) then
/// ancestors (near to far).
pub fn discover_candidates(scan_root: &Path) -> Vec<EnvFileCandidate> {
    let mut ordered: Vec<PathBuf> = Vec::new();

    // Downward first (the strongest candidates), then upward. Skip the scan
    // root itself in the ancestor pass — its direct files are already covered
    // by the descendant walk at depth 0.
    ordered.extend(find_descendant_env_files(scan_root));
    for dir in scan_root.ancestors().skip(1) {
        ordered.extend(env_files_in_dir(dir));
    }

    let mut seen: Vec<PathBuf> = Vec::new();
    let mut candidates = Vec::new();
    for path in ordered {
        let canon = fs::canonicalize(&path).unwrap_or_else(|_| path.clone());
        if seen.contains(&canon) {
            continue;
        }
        seen.push(canon);
        let keys = read_env_keys(&path);
        candidates.push(EnvFileCandidate { path, keys });
    }
    candidates
}

/// Compute a path to `target` relative to `base`, using `..` segments. Falls
/// back to the absolute target path when the two share no common root (e.g. a
/// different Windows drive), which is still valid since `base.join(abs) == abs`.
pub fn relativize(base: &Path, target: &Path) -> String {
    let base = fs::canonicalize(base).unwrap_or_else(|_| base.to_path_buf());
    let target = fs::canonicalize(target).unwrap_or_else(|_| target.to_path_buf());

    let base_comps: Vec<_> = base.components().collect();
    let target_comps: Vec<_> = target.components().collect();

    let mut common = 0;
    while common < base_comps.len()
        && common < target_comps.len()
        && base_comps[common] == target_comps[common]
    {
        common += 1;
    }

    // No shared root — not expressible as a relative path.
    if common == 0 {
        return target.to_string_lossy().into_owned();
    }

    let mut parts: Vec<String> = Vec::new();
    for _ in common..base_comps.len() {
        parts.push("..".to_string());
    }
    for comp in &target_comps[common..] {
        parts.push(comp.as_os_str().to_string_lossy().into_owned());
    }

    if parts.is_empty() {
        ".".to_string()
    } else {
        parts.join("/")
    }
}

/// Summarize a file's variable names for an inline preview: `"3 vars: A, B, C"`,
/// truncating the name list so a single MultiSelect row never grows long enough
/// to wrap (wrapping corrupts dialoguer's cursor math).
fn format_var_summary(keys: &[String]) -> String {
    if keys.is_empty() {
        return "no vars".to_string();
    }
    // Budget for the comma-joined name list before we switch to a "+N more" tail.
    const MAX_NAMES_LEN: usize = 48;
    let plural = if keys.len() == 1 { "" } else { "s" };

    let mut names = String::new();
    let mut shown = 0;
    for key in keys {
        let sep_len = if names.is_empty() { 0 } else { 2 };
        if names.len() + sep_len + key.len() > MAX_NAMES_LEN {
            break;
        }
        if !names.is_empty() {
            names.push_str(", ");
        }
        names.push_str(key);
        shown += 1;
    }

    if shown == 0 {
        // Even the first name blows the budget — fall back to the count alone.
        format!("{} var{plural}", keys.len())
    } else if shown < keys.len() {
        format!(
            "{} var{plural}: {names}, +{} more",
            keys.len(),
            keys.len() - shown
        )
    } else {
        format!("{} var{plural}: {names}", keys.len())
    }
}

/// Interactively pick which discovered `.env` files to load, returning the
/// chosen paths as strings relative to `root` (where `puffgres.toml` lives), in
/// load-priority order, suitable for `environment_files`.
///
/// Flow: a [`MultiSelect`] chooses the files (each row previews its variable
/// names inline), and when more than one file is chosen a [`Sort`] step sets the
/// priority order. Order matters — files load first-to-last and earlier files
/// take priority over later ones — so the chosen order is preserved verbatim into
/// the config. An empty selection returns an empty vec (the caller falls back to
/// a default).
///
/// Discovery can't reach everything (paths outside the scan, files not yet
/// created), so instead of an inline manual-entry prompt we point the user at
/// `puffgres.toml`, where `environment_files` can be edited by hand afterward.
pub fn pick_env_files(
    candidates: &[EnvFileCandidate],
    root: &Path,
) -> Result<Vec<String>, CliError> {
    let theme = ColorfulTheme::default();

    let config_path = root.join("puffgres.toml");

    if candidates.is_empty() {
        eprintln!("  No .env files found nearby.");
        eprintln!(
            "  Add env file paths by hand in {} (environment_files).",
            config_path.display()
        );
        return Ok(Vec::new());
    }

    // A single candidate leaves nothing to choose — pick it and skip the
    // selection screen entirely.
    if let [only] = candidates {
        let rel = relativize(root, &only.path);
        println!("Using {rel} (the only .env file found nearby).");
        return Ok(vec![rel]);
    }

    // Precompute the relative label for each candidate and pad so the inline
    // variable previews line up in a column.
    let rels: Vec<String> = candidates
        .iter()
        .map(|c| relativize(root, &c.path))
        .collect();
    let width = rels.iter().map(String::len).max().unwrap_or(0);

    // (relative path, variable names) for every file the user wants, built up in
    // the order they were chosen and then reordered by the priority step below.
    let mut chosen: Vec<(String, Vec<String>)> = Vec::new();

    let items: Vec<String> = candidates
        .iter()
        .zip(&rels)
        .map(|(c, rel)| format!("{rel:<width$}  ({})", format_var_summary(&c.keys)))
        .collect();

    // Discovery misses paths outside the scan; the picker can't add those, so
    // tell the user where to add them by hand. The priority order is set on the
    // next (Sort) screen when more than one file is chosen.
    println!(
        "Don't see a file? Add it later in {} (environment_files).",
        config_path.display()
    );
    println!("If you pick more than one, the next screen sets their priority order.");
    println!();

    let checked = MultiSelect::with_theme(&theme)
        .with_prompt("Select .env files to load (space toggles, enter confirms)")
        .items(&items)
        .interact()
        .map_err(|e| CliError::Generate(format!("prompt failed: {e}")))?;

    for idx in checked {
        chosen.push((rels[idx].clone(), candidates[idx].keys.clone()));
    }

    // With more than one file, let the user set the override priority.
    // MultiSelect always hands back its selections in list order, so this Sort
    // step is the only way the user can actually express override order.
    if chosen.len() > 1 {
        order_by_priority(&theme, &mut chosen)?;
    }

    // Echo the final priority order, numbered so the override chain is obvious.
    if !chosen.is_empty() {
        println!();
        println!("Env files in priority order (earlier takes priority, later fills gaps):");
        for (i, (rel, keys)) in chosen.iter().enumerate() {
            let n = i + 1;
            if keys.is_empty() {
                println!("  [{n}] {rel}  (no variables)");
            } else {
                println!("  [{n}] {rel}");
                for key in keys {
                    println!("        {key}");
                }
            }
        }
    }

    Ok(chosen.into_iter().map(|(rel, _)| rel).collect())
}

/// Reorder `chosen` in place by load priority via a [`Sort`] prompt. The list
/// enters seeded in its current (discovery) order; pressing enter without moving
/// anything keeps that order.
fn order_by_priority(
    theme: &ColorfulTheme,
    chosen: &mut Vec<(String, Vec<String>)>,
) -> Result<(), CliError> {
    let labels: Vec<String> = chosen
        .iter()
        .map(|(rel, keys)| format!("{rel}  ({})", format_var_summary(keys)))
        .collect();

    // `order[new_position] == original_index`.
    let order = Sort::with_theme(theme)
        .with_prompt("Order by priority — top wins, bottom fills gaps (space grabs, ↑↓ move, enter confirms)")
        .items(&labels)
        .interact()
        .map_err(|e| CliError::Generate(format!("prompt failed: {e}")))?;

    *chosen = order.iter().map(|&i| chosen[i].clone()).collect();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn write(path: &Path, content: &str) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(path, content).unwrap();
    }

    #[test]
    fn matches_env_file_names() {
        assert!(is_env_file_name(".env"));
        assert!(is_env_file_name(".env.local"));
        assert!(is_env_file_name(".env.development"));
        assert!(is_env_file_name(".env.prod"));
        assert!(is_env_file_name(".env.example"));
        assert!(!is_env_file_name("envrc"));
        assert!(!is_env_file_name("config.toml"));
        assert!(!is_env_file_name("readme.md"));
    }

    #[test]
    fn reads_keys_not_values() {
        let dir = TempDir::new().unwrap();
        let p = dir.path().join(".env");
        write(&p, "DATABASE_URL=postgres://secret\nAPI_KEY=super-secret\n");

        let keys = read_env_keys(&p);
        assert_eq!(keys, vec!["DATABASE_URL", "API_KEY"]);
        assert!(!keys.iter().any(|k| k.contains("secret")));
    }

    #[test]
    fn read_keys_dedupes_preserving_order() {
        let dir = TempDir::new().unwrap();
        let p = dir.path().join(".env");
        write(&p, "A=1\nB=2\nA=3\n");
        assert_eq!(read_env_keys(&p), vec!["A", "B"]);
    }

    #[test]
    fn read_keys_of_missing_file_is_empty() {
        assert!(read_env_keys(Path::new("/nonexistent/.env")).is_empty());
    }

    #[test]
    fn descendant_scan_finds_nested_and_skips_heavy_dirs() {
        let dir = TempDir::new().unwrap();
        let root = dir.path();
        write(&root.join(".env"), "A=1\n");
        write(&root.join("services/api/.env.local"), "B=2\n");
        write(&root.join("node_modules/pkg/.env"), "C=3\n");
        write(&root.join(".git/.env"), "D=4\n");

        let found = find_descendant_env_files(root);
        let names: Vec<String> = found
            .iter()
            .map(|p| p.strip_prefix(root).unwrap().to_string_lossy().into_owned())
            .collect();

        assert!(names.iter().any(|n| n == ".env"));
        assert!(
            names
                .iter()
                .any(|n| n.ends_with("api/.env.local") || n.contains("api"))
        );
        assert!(!names.iter().any(|n| n.contains("node_modules")));
        assert!(!names.iter().any(|n| n.contains(".git")));
    }

    #[test]
    fn descendant_scan_respects_max_depth() {
        let dir = TempDir::new().unwrap();
        let root = dir.path();
        // Depth MAX_DEPTH+1 — one level too deep to be found.
        let mut deep = root.to_path_buf();
        for _ in 0..(MAX_DEPTH + 1) {
            deep = deep.join("d");
        }
        write(&deep.join(".env"), "A=1\n");

        let found = find_descendant_env_files(root);
        assert!(found.is_empty(), "file below MAX_DEPTH should not be found");
    }

    #[test]
    fn ancestor_scan_finds_parent_env_files() {
        let dir = TempDir::new().unwrap();
        let root = dir.path();
        write(&root.join(".env"), "A=1\n");
        let child = root.join("a/b");
        fs::create_dir_all(&child).unwrap();

        let found = find_ancestor_env_files(&child);
        // The root's .env is an ancestor of a/b.
        assert!(found.iter().any(|p| p.ends_with(".env")));
    }

    #[test]
    fn discover_dedupes_files_reachable_both_ways() {
        // scan_root's own directory .env is reached by the descendant walk at
        // depth 0; the ancestor pass skips scan_root, so no duplicate arises.
        let dir = TempDir::new().unwrap();
        let root = dir.path();
        write(&root.join(".env"), "A=1\nB=2\n");

        let candidates = discover_candidates(root);
        let env_hits: Vec<_> = candidates
            .iter()
            .filter(|c| c.path.file_name().unwrap() == ".env")
            .collect();
        assert_eq!(env_hits.len(), 1);
        assert_eq!(env_hits[0].keys, vec!["A", "B"]);
    }

    #[test]
    fn var_summary_empty_and_small() {
        assert_eq!(format_var_summary(&[]), "no vars");
        assert_eq!(format_var_summary(&["A".into()]), "1 var: A");
        assert_eq!(
            format_var_summary(&["A".into(), "B".into()]),
            "2 vars: A, B"
        );
    }

    #[test]
    fn var_summary_truncates_long_lists() {
        let keys: Vec<String> = (0..20).map(|i| format!("VAR_NAME_{i}")).collect();
        let s = format_var_summary(&keys);
        assert!(s.starts_with("20 vars: VAR_NAME_0"));
        assert!(s.contains("more"), "expected a +N more tail: {s}");
    }

    #[test]
    fn var_summary_handles_single_huge_name() {
        let huge = "X".repeat(100);
        // Even one over-budget name must not panic and should report the count.
        assert_eq!(format_var_summary(&[huge]), "1 var");
    }

    #[test]
    fn relativize_child_has_no_dotdot() {
        let dir = TempDir::new().unwrap();
        let base = dir.path();
        let target = base.join(".env");
        write(&target, "A=1\n");
        assert_eq!(relativize(base, &target), ".env");
    }

    #[test]
    fn relativize_parent_uses_dotdot() {
        let dir = TempDir::new().unwrap();
        let root = dir.path();
        let sub = root.join("puffgres");
        fs::create_dir_all(&sub).unwrap();
        let target = root.join(".env");
        write(&target, "A=1\n");
        assert_eq!(relativize(&sub, &target), "../.env");
    }

    #[test]
    fn relativize_two_levels_up() {
        let dir = TempDir::new().unwrap();
        let root = dir.path();
        let sub = root.join("a/puffgres");
        fs::create_dir_all(&sub).unwrap();
        let target = root.join(".env");
        write(&target, "A=1\n");
        assert_eq!(relativize(&sub, &target), "../../.env");
    }
}
