use std::io::{self, Write};
use std::path::Path;

use state::StateDb;

use crate::error::CliError;

pub async fn run(state_db_path: &Path, force: bool) -> Result<(), CliError> {
    if !state_db_path.exists() {
        return Err(CliError::NotInitialized("state.db".to_string()));
    }
    let db = StateDb::open(state_db_path).await?;

    let configs = db.list_configs().await?;

    if !configs.is_empty() && !force {
        println!(
            "This will delete all state for {} config(s):",
            configs.len()
        );
        for c in &configs {
            println!("  - {}", c.name);
        }
        print!("Continue? [y/N] ");
        io::stdout().flush()?;

        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        let input = input.trim().to_lowercase();
        if input != "y" && input != "yes" {
            return Err(CliError::Reset("aborted".to_string()));
        }
    }

    db.reset().await?;
    println!("Reset: cleared all state");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::setup_project;
    use chrono::Utc;
    use state::ConfigRecord;

    #[tokio::test]
    async fn reset_clears_configs() {
        let (_dir, _paths, state_db_path) = setup_project().await;

        let db = StateDb::open(&state_db_path).await.unwrap();
        db.insert_config(&ConfigRecord {
            name: "user".to_string(),

            namespace: "user".to_string(),
            content_hash: "abc".to_string(),
            transform_hash: None,
            applied_at: Utc::now(),
            tombstone_applied_at: None,
            namespace_prefix: None,
        })
        .await
        .unwrap();
        assert_eq!(db.list_configs().await.unwrap().len(), 1);
        drop(db);

        run(&state_db_path, true).await.unwrap();

        let db = StateDb::open(&state_db_path).await.unwrap();
        assert_eq!(db.list_configs().await.unwrap().len(), 0);
    }

    #[tokio::test]
    async fn reset_on_empty_db() {
        let (_dir, _paths, state_db_path) = setup_project().await;
        run(&state_db_path, true).await.unwrap();
    }

    #[tokio::test]
    async fn reset_rejects_uninitialized_project() {
        let dir = tempfile::tempdir().unwrap();
        let missing_db = dir.path().join("nonexistent.db");

        let err = run(&missing_db, true).await.unwrap_err();
        assert!(
            err.to_string().contains("not found"),
            "expected NotInitialized error, got: {err}"
        );
    }
}
