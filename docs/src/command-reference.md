# Command Reference

## `puffgres init`

Create directory structure (in `puffgres/` folder) with everything needed for initial setup.

## `puffgres new [name]`

Create a new config / transform. Runs an interactive wizard by default.

Pass `--non-interactive` to build the config from flags instead — the codepath scripts and agents should use:

```sh
puffgres new film \
  --table internal_film \
  --namespace internal_film_title \
  --id-column id \
  --id-type string \
  --provider zeroentropy \
  --embed-column title \
  --non-interactive
```

Flags: `--table` and `--namespace` default to the config name; `--id-column` defaults to `id`; `--id-type` (`uint`, `int`, `uuid`, `string`) is auto-detected from the table when omitted; `--provider` is one of `none`, `together`, `zeroentropy`, `baseten`, `cloudflare`; `--embed-column` sets the column the generated transform embeds.

## `puffgres apply`

Apply configs on the file system into state, so that replication will begin for a new config/transform pair. Once you do this, configs / transforms are set (+ their hashes are stored in state) and will throw an error if you try to change them.

## `puffgres check [name]`

Regenerate `schema.ts` from the live database and validate configs against it — referenced tables exist, the id column has a unique index, id types are compatible, and each transform runs successfully on a sample row. Pass a config `name` to validate just one. Never writes to the state database, so it's safe to run before `apply` and good to run in CI.

## `puffgres remove`

Permanently remove config(s): deletes the turbopuffer namespace, the on-disk config directory, and all state (checkpoints, backfill progress, DLQ). Use a positional `name` to remove a specific config, `--last` to remove the most recently applied one, or `--all` to remove every applied config. `--force` skips the confirmation prompt (use with `--all`).

## `puffgres tombstone --name <name>`

Creates a `tombstone.toml` file in a config directory so the CDC loop ignores it (a soft delete that leaves the namespace and its data in place).

## `puffgres generate`

(Re)generate typed `schema.ts` files. If you have a Postgres migration on a table you are watching, you need to run this so that transforms access the correct columns. (`check` also regenerates, so you usually don't need to call this directly.)

## `puffgres run`

Start the replication pipeline. Runs a preflight validation of the applied configs first (failing fast on misconfiguration), then backfill, then the CDC loop.

## `puffgres debug`

Launches lightweight web UI (on port 3333 by default) to inspect namespaces / view replication stream.
