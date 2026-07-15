# Command reference

NOTE: All commands must be run from within the user's generated `puffgres` directory (the folder created after init), or one level up, in order to work.

## `puffgres init`

Create directory structure (in `puffgres/` folder) with everything needed for initial setup.

## `puffgres new --name <name>`

Create a new config / transform. Runs an interactive wizard by default.

Pass `--non-interactive` to build the config from flags instead — the codepath scripts and agents should use:

```sh
puffgres new --name film \
  --table internal_film \
  --namespace internal_film_title \
  --id-column id \
  --id-type string \
  --provider zeroentropy \
  --embed-column title \
  --non-interactive
```

Flags:

- `--name` — what to call the config and name the folder
- `--table` — which Postgres table we reference (defaults to name)
- `--namespace` — which turbopuffer namespace this will land in (defaults to name)
- `--id-column` — which column represents our id (defaults to `id`)
- `--provider` — the embedding provider we'd use (`none`, `zeroentropy`, `baseten`, `cloudflare`)
- `--embed-column` — which column to embed

The id type is always auto-detected from the table.

## `puffgres apply`

Apply configs on the file system into state, so that replication will begin for a new config/transform pair. Once you do this, configs / transforms are set (+ their hashes are stored in state) and will throw an error if you try to change them.

## `puffgres check [--name <name>]`

Regenerate `schema.ts` from the live database and validate configs against it. Makes sure referenced tables exist, the id column has a unique index, id types are compatible, each transform runs successfully on a sample row, and schemas are correct. Safe to run before `apply` to check, and we run this in CI.  

## `puffgres remove`

Permanently remove config(s): deletes the turbopuffer namespace, the on-disk config directory, and all state (checkpoints, backfill progress, DLQ). Use `--name <name>` to remove a specific config, `--last` to remove the most recently applied one, or `--all` to remove every applied config. `--force` skips the confirmation prompt (use with `--all`). This should only be used in local dev / when testing, not in production. 

## `puffgres reset`

Nuke the whole project back to a clean slate. Drops the `puffgres` replication slot (and the `puffgres_debug` debug slot), drops the `puffgres` publication, drops the state schema, and deletes the local project directory. turbopuffer namespaces are left untouched — use `puffgres remove --all` first if you also want those gone. We also use this for debugging / puffgres dev. This will also work if the state database is in a weird state, functioning as a total reset. Run `puffgres init` to start over afterwards.

## `puffgres tombstone [--name <name>]`

Creates a `tombstone.toml` file in a config directory so the CDC loop ignores it. Effectively a "soft delete" of a namespace. Rather than changing an active config, you should make a new one and tombstone the old one. 

Without `--name`, puffgres interactively prompts you to pick from the applied configs that aren't tombstoned yet. Pass `--name` to skip the prompt (for scripts and agents).

## `puffgres generate`

(Re)generate typed `schema.ts` files. If you have a Postgres migration on a table you are watching, you need to run this so that transforms access the correct columns. 

## `puffgres run`

Start the replication pipeline. Runs a preflight validation of the applied configs first (failing fast on misconfiguration), then backfill, then the CDC loop.

## `puffgres debug`

Launches lightweight web UI (on port 3333 by default) to inspect namespaces / view replication stream.
