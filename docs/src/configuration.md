# Configuration

puffgres is configured in two places:

- **`puffgres.toml`** — runtime behavior (batch sizes, retries, timeouts) and which `.env` files to load.
- **Environment variables** — secrets and connection details (database URL, turbopuffer key), loaded from the `.env` files that `puffgres.toml` points at.

## `puffgres.toml`

All runtime configuration lives in `puffgres.toml` at the root of your puffgres project. Every field except `environment_files` is optional and has a sensible default.

```toml
environment_files = ["./.env", "../.env", "../.env.development"]
batch_size = 1000
max_retries = 5

# Dead letter queue
dlq_replay_interval = 10
dlq_replay_batch_size = 50
dlq_max_retries = 5
dlq_permanent_max_age_hours = 72

# Advanced (uncomment to override defaults)
# max_transaction_events = 1000000
# sub_batch_size = 1000
# transform_timeout_secs = 30
# maintenance_interval_secs = 600
# tls_unclean_close_level = "error"
```

### `environment_files`

**Required.** List of `.env` file paths to load, relative to the `puffgres.toml` location. Later files override earlier ones. Shell environment variables take highest precedence over all files.

### `batch_size`

Number of replication events to collect before flushing a batch to turbopuffer. Default: **1000**.

### `max_retries`

Number of times to retry a failed batch before sending it to the dead letter queue. Default: **5**.

### Dead letter queue

When a batch fails after `max_retries`, it goes to the dead letter queue (DLQ) so the stream isn't blocked. These knobs control how the DLQ is drained and pruned; the defaults are sensible and most projects never touch them.

#### `dlq_replay_interval`

How often (in seconds) to replay retryable entries from the dead letter queue. Default: **10**.

#### `dlq_replay_batch_size`

Maximum number of dead letter queue entries to replay per interval. Default: **50**.

#### `dlq_max_retries`

Number of times to retry a dead letter queue entry before marking it as permanent. Default: **5**.

#### `dlq_permanent_max_age_hours`

How long (in hours) to keep permanently-failed dead letter queue entries before discarding them. Default: **72**.

### Large transactions

#### `max_transaction_events`

Maximum number of events allowed in a single Postgres transaction. Transactions exceeding this limit are skipped and logged. Has no effect when `sub_batch_size` is set. Default: **1,000,000**.

#### `sub_batch_size`

When set, large transactions are streamed in sub-batches of this size instead of buffering the entire transaction in memory. The pipeline processes chunks as they arrive, giving natural backpressure; the commit finalizes the group. Unset by default (entire transaction is buffered).

### `transform_timeout_secs`

How long puffgres waits for a single `transform.ts` batch response before killing and respawning the worker process. Default: **30** seconds.

### `maintenance_interval_secs`

How often (in seconds) puffgres runs background maintenance — currently pruning stale permanent DLQ entries past `dlq_permanent_max_age_hours`. Default: **600**.

### `tls_unclean_close_level`

Logging level for unclean TLS shutdowns (missing `close_notify`). Supported values: `error`, `warn`, `silent`. Default: **error**.

## Environment variables

These are loaded from the `.env` files listed in `environment_files` (shell environment always wins). Keep secrets here, not in `puffgres.toml`.

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

Because state lives in the source database, source rollbacks (e.g. PITR restores) naturally roll puffgres' state back with them — backfill cursors and config registrations stay consistent. The trade-off is that `puffgres remove` and `tombstone` require the source DB to be reachable.

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

#### Sentry alerting

If you export OTLP to Sentry, keep connection-failure events at warning level in puffgres and set the alert threshold in Sentry. A practical starting point is an issue alert for `connection failed, reconnecting` when it happens more than once in one hour.

### Other environment variables

You may also want to set environment variables you use in transformations, i.e. `ZEROENTROPY_API_KEY` or `BASETEN_API_KEY` for embeddings.
