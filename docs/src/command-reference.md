# Command Reference

## `puffgres init`

Create directory structure (in `puffgres/` folder) with everything needed for initial setup.  

## `puffgres new`

Create a new config / transform with interactive wizard. 

## `puffgres dry-run [name]`

Run transforms on sample data without writing state, to see results. 

## `puffgres apply`

Apply configs on the file system into state, so that replication will begin for a new config/transform pair. Once you do this, configs / transforms are set (+ their hashes are stored in state) and will throw an error if you try to change them. 

## `puffgres remove`

Remove a config from the state database to try again. Use with either `--name` to remove a specific one or `--last` to remove the last. 


## `puffgres reset`

Clear all state (configs and checkpoints) from the `puffgres` state schema. Use `--force` to skip the confirmation prompt.


## `puffgres tombstone --name <name>`

Creates a `tombstone.toml` file in a config directory so the CDC loop ignores it. 

## `puffgres generate`

(Re)generate typed `schema.ts` files. If you have a Postgres migration on a table you are watching, you need to run this so that transforms access the correct columns. 

## `puffgres check`

Validates that schemas we use in transforms match the live database schema. If this fails, you likely need to re-generate the schema. 

## `puffgres run`

Start the replication pipeline. First run backfill, then run CDC loop. 

## `puffgres debug`

Launches lightweight web UI (on port 3333 by default) to inspect namespaces / view replication stream. 

If you want to remove a config, you should cascade delete the config from the `puffgres` schema in Postgres, or reset the database to a previous state.