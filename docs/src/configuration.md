# Configuration

All configuration lives in `puffgres.toml` at the root of your puffgres project. Every field except `environment_files` is optional and has a sensible default.

```toml
environment_files = ["./.env", "../.env", "../.env.development"]
batch_size = 1000
max_retries = 5
dlq_replay_interval = 10
dlq_replay_batch_size = 50
dlq_max_retries = 5
dlq_permanent_max_age_hours = 72
# max_transaction_events = 1000000
# sub_batch_size = 1000000
```

## Reference

### `environment_files`

**Required.** List of `.env` file paths to load, relative to the `puffgres.toml` location. Later files override earlier ones. Shell environment variables take highest precedence over all files.

### `batch_size`

Number of replication events to collect before flushing a batch to turbopuffer. Default: **1000**.

### `max_retries`

Number of times to retry a failed batch before sending it to the dead letter queue. Default: **5**.

### `dlq_replay_interval`

How often (in seconds) to replay retryable entries from the dead letter queue. Default: **10**.

### `dlq_replay_batch_size`

Maximum number of dead letter queue entries to replay per interval. Default: **50**.

### `dlq_max_retries`

Number of times to retry a dead letter queue entry before marking it as permanent. Default: **5**.

### `dlq_permanent_max_age_hours`

How long (in hours) to keep permanently-failed dead letter queue entries before discarding them. Default: **72**.

### `max_transaction_events`

Maximum number of events allowed in a single Postgres transaction. Transactions exceeding this limit are skipped and logged. Default: **1,000,000**.

### `sub_batch_size`

When set, large transactions are streamed in sub-batches of this size instead of buffering the entire transaction in memory. The pipeline processes chunks as they arrive, giving natural backpressure. The commit finalizes the group. Unset by default (entire transaction is buffered).
