use std::io::{self, Write};

use state::StateDb;

use crate::error::CliError;

pub async fn run(database_url: &str, state_schema: &str, force: bool) -> Result<(), CliError> {
    let db = StateDb::connect(database_url, state_schema).await?;

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
    use crate::test_utils::setup_project_with_state;
    use chrono::Utc;
    use state::ConfigRecord;

    #[tokio::test]
    async fn reset_clears_configs() {
        let (_dir, _paths, url, schema) = setup_project_with_state().await;

        let db = StateDb::connect(&url, &schema).await.unwrap();
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

        run(&url, &schema, true).await.unwrap();

        let db = StateDb::connect(&url, &schema).await.unwrap();
        assert_eq!(db.list_configs().await.unwrap().len(), 0);
    }

    #[tokio::test]
    async fn reset_on_empty_db() {
        let (_dir, _paths, url, schema) = setup_project_with_state().await;
        run(&url, &schema, true).await.unwrap();
    }
}
