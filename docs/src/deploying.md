# Deploying

puffgres configs should live in your repo, in a `puffgres/` folder. The puffgres service image runs based on this directory structure of configs and transforms. It also relies on a SQLite database for state (tracking applied configs, replication checkpoints, dead letter queue entries, etc.) that needs to persist across runs — if you lose the SQLite DB, puffgres will re-backfill everything from scratch.

## Railway

We deploy on Railway. The setup looks like:

1. Create a new service pointed at your repo, with the root directory set to `puffgres/`.
2. Set your environment variables (see [Environment](./environment.md)).
3. Create a persistent volume called `puffgres-volume` and set `PUFFGRES_STATE_DB` to `/puffgres-volume/data/puffgres-state.db`. This keeps your SQLite state across deploys.

## CI

We run `puffgres check` in CI. It won't catch immutability issues (since CI doesn't have access to the SQLite state DB), but it will catch schema generation errors and invalid configs before they reach production.

## Observability

puffgres supports OpenTelemetry for tracing and metrics. Set `OTEL_EXPORTER_OTLP_ENDPOINT` and `OTEL_EXPORTER_OTLP_HEADERS` to export telemetry to your provider of choice. We use Sentry and it works great, see the [Environment](./environment.md) section for example values.
