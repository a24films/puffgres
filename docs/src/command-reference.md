# Command Reference

## `puffgres init`

Initialize a puffgres project. Creates a `puffgres/` folder with Dockerfile, `puffgres.toml`, and initial setup files.

## `puffgres new <name>`

Create a new table config. Generates a timestamped directory containing `config.toml` and `transform.ts`.

## `puffgres generate`

Generate typed `schema.ts` files for each config using your Postgres table schema.

## `puffgres check`

Validate all configs against the live database without applying. Verifies `schema.ts` files are up-to-date.

## `puffgres dry-run [name]`

Run transforms on sample data without writing state. Optionally filter to a single config by name.

## `puffgres apply`

Apply pending config changes. Registers configs in SQLite so they are picked up for replication.

## `puffgres backfill`

Run the backfill for applied configs.

## `puffgres run`

Start the replication pipeline. Runs the backfill first, then begins the Change Data Capture streaming loop.

## `puffgres debug`

Launch a lightweight web UI to inspect turbopuffer namespaces and view the live replication stream. Defaults to port `3333`. Use `--port` to change it, `--slot` to set the replication slot name (default `puffgres_debug`), and `--publication` to set the publication name (default `puffgres`).

## `puffgres tombstone --name <name>`

Mark a config as inactive. Creates a `tombstone.toml` in the config directory so the CDC loop ignores it.

## `puffgres remove <name>`

Fully remove a config — deletes the turbopuffer namespace, clears all state from SQLite, and removes the config directory. Use `--last` to remove the most recently applied config.

## `puffgres reset`

Clear all state (configs and checkpoints) from the SQLite database. Use `--force` to skip the confirmation prompt.
