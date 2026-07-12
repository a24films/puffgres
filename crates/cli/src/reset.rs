use std::io::{self, Write};

use pg::connect::quote_identifier;

use crate::error::CliError;
use crate::paths::ProjectPaths;
use crate::pipeline::{PUBLICATION_NAME, SLOT_NAME};

/// Slot the `debug` command creates by default; dropped too so no slot is left pinning WAL.
const DEBUG_SLOT_NAME: &str = "puffgres_debug";

/// Tear the project down to a clean slate: drop the replication slot(s),
/// publication, and state schema, then delete the project directory. Leaves
/// turbopuffer namespaces alone. Tolerates a broken state DB — that's what it
/// recovers from.
pub async fn run(
    paths: &ProjectPaths,
    database_url: &str,
    state_schema: &str,
    force: bool,
) -> Result<(), CliError> {
    // Dropping `public` would take non-puffgres tables with it.
    if state_schema == "public" {
        return Err(CliError::Reset(
            "refusing to reset: state schema is 'public'; dropping it would delete non-puffgres tables"
                .to_string(),
        ));
    }

    if !force {
        println!("This will permanently and irreversibly:");
        println!("  - drop the '{SLOT_NAME}' replication slot and publication");
        println!("  - drop the '{state_schema}' state schema (all puffgres state)");
        println!("  - delete the project directory {}", paths.root.display());
        print!("Continue? [y/N] ");
        io::stdout().flush()?;

        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        let input = input.trim().to_lowercase();
        if input != "y" && input != "yes" {
            return Err(CliError::Reset("aborted".to_string()));
        }
    }

    let pg = pg::connect::connect(database_url).await?;

    for slot in [SLOT_NAME, DEBUG_SLOT_NAME] {
        // Kick any pipeline holding the slot; an error just means it's absent.
        let _ = pg::slot::terminate_active_slot_backend(&pg, slot).await;
        pg::slot::drop_slot(&pg, slot).await?;
    }
    println!("Dropped replication slot(s)");

    let drop_publication = format!(
        "DROP PUBLICATION IF EXISTS {}",
        quote_identifier(PUBLICATION_NAME)
    );
    pg.execute(&drop_publication, &[])
        .await
        .map_err(|e| CliError::Reset(format!("failed to drop publication: {e}")))?;
    println!("Dropped publication '{PUBLICATION_NAME}'");

    // CASCADE drops Diesel's migration-tracking table too, so the next run
    // re-migrates cleanly instead of hitting "relation already exists".
    let drop_schema = format!(
        "DROP SCHEMA IF EXISTS {} CASCADE",
        quote_identifier(state_schema)
    );
    pg.execute(&drop_schema, &[]).await.map_err(|e| {
        CliError::Reset(format!("failed to drop state schema '{state_schema}': {e}"))
    })?;
    println!("Dropped state schema '{state_schema}'");

    match std::fs::remove_dir_all(&paths.root) {
        Ok(()) => println!("Deleted project directory {}", paths.root.display()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => {
            return Err(CliError::Reset(format!(
                "failed to delete project directory {}: {e}",
                paths.root.display()
            )));
        }
    }

    println!("Reset complete. Run `puffgres init` to start fresh.");
    Ok(())
}
