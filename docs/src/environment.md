# Environment

See the [Configuration](./configuration.md) section for pointing puffgres toward your environment variables.

Here's the relevant environment variables to set:

### `DATABASE_URL`

Non-pooled URL for your Postgres database. Pooled connections cannot handle logical replication!

```sh
DATABASE_URL="postgresql://user:pass@host:5432/db"
```

### `TURBOPUFFER_API_KEY`

```sh
TURBOPUFFER_API_KEY="tpuf_abc123..."
```

### `TURBOPUFFER_NAMESPACE_PREFIX`

Prefix for all turbopuffer namespaces. If set to `PUFFGRES_PRODUCTION` and you create a namespace called `internal_film`, it saves as `PUFFGRES_PRODUCTION_internal_film`.

```sh
TURBOPUFFER_NAMESPACE_PREFIX="PUFFGRES_PRODUCTION"
```

### `PUFFGRES_STATE_SCHEMA`

Postgres schema (in the same database as `DATABASE_URL`) where puffgres keeps its own state — applied configs, replication checkpoints, backfill cursors, and the DLQ. Defaults to `puffgres`. The schema is created on first run; the `DATABASE_URL` role needs `CREATE SCHEMA` and DML privileges on it (or you can pre-create the schema and grant DML only).

```sh
PUFFGRES_STATE_SCHEMA="puffgres"
```

Because state lives in the source database, source rollbacks (e.g. PITR restores) naturally roll puffgres' state back with them — backfill cursors and config registrations stay consistent. The trade-off is that `puffgres status`, `reset`, and `tombstone` now require the source DB to be reachable.

### `OTEL_EXPORTER_OTLP_ENDPOINT`

OpenTelemetry endpoint, if you want observability.

```sh
OTEL_EXPORTER_OTLP_ENDPOINT="https://a123.ingest.us.sentry.io/api/1234/integration/otlp"
```

### `OTEL_EXPORTER_OTLP_HEADERS`

Headers for the OTLP exporter.

```sh
OTEL_EXPORTER_OTLP_HEADERS="x-sentry-auth=sentry sentry_key=a123"
```

### Sentry Alerting

If you export OTLP to Sentry, keep connection-failure events at warning level in puffgres and set the alert threshold in Sentry. A practical starting point is an issue alert for `connection failed, reconnecting` when it happens more than once in one hour.

### Other Environment Variables

You may also want to set environment variables you use in transformations, i.e. `ZEROENTROPY_API_KEY` or `BASETEN_API_KEY` for embeddings.
