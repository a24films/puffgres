# Deploying

puffgres configs should live in your repo, in a `puffgres/` folder. The puffgres service image runs based on this directory structure of configs and transforms. puffgres' own state — applied configs, replication checkpoints, backfill cursors, the DLQ — lives in a dedicated Postgres schema (default `puffgres`) inside your source database. Nothing persists on local disk, so the container is stateless.

## Railway

We deploy on Railway. The setup looks like:

1. Create a new service pointed at your repo, with the root directory set to `puffgres/`.
2. Set your environment variables (see [Environment](./environment.md)).
3. Optionally set `PUFFGRES_STATE_SCHEMA` to override the default schema name (`puffgres`). No persistent volume needed.

The `DATABASE_URL` role needs `CREATE SCHEMA` privilege on first run, plus DML on the `puffgres` schema thereafter. If your DBA prefers a tighter grant, pre-create the schema and grant DML only.

## CI

We run `puffgres check` in CI. It won't catch immutability issues (since CI doesn't have access to the production state schema), but it will catch schema generation errors and invalid configs before they reach production.

## Source restores and rollback

Because state lives in the source DB, a PITR restore on your source rolls puffgres' state back with it — backfill cursors and applied-config records stay consistent with what's actually in the source. The replication slot's confirmed LSN also rolls back, so the next `puffgres run` resumes from the right place. The trade-off is that `puffgres status`, `reset`, and `tombstone` now require source-DB connectivity.

## Observability

puffgres supports OpenTelemetry for tracing and metrics. Set `OTEL_EXPORTER_OTLP_ENDPOINT` and `OTEL_EXPORTER_OTLP_HEADERS` to export telemetry to your provider of choice. We use Sentry and it works great, see the [Environment](./environment.md) section for example values.
