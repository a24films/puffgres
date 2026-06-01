# Delivery Guarantees

puffgres aims for **at-least-once delivery with idempotent writes**, which gives the destination namespace an effectively exactly-once *final state*. The pieces that make this work:

## Checkpoints and replay

Streaming replication advances a checkpoint LSN only **after** a batch has been applied to turbopuffer. If puffgres crashes or the connection drops, it resumes from the last acknowledged LSN — so some events may be re-delivered, but none are skipped.

Backfill is cursor-based: it records the last id it processed (a watermark), so an interrupted backfill resumes where it left off rather than starting over.

## Idempotency

Every write to turbopuffer is keyed by the document id (`upsert` and `delete` both target an id). Re-applying the same event is therefore a no-op against the final state — the redelivery implied by at-least-once doesn't produce duplicates or drift. This is why your config must point at a column with a unique index (`check` enforces this).

## Dead letter queue

A batch that fails after `max_retries` is moved to the dead letter queue rather than blocking the stream. Retryable entries are replayed on an interval; entries that exhaust `dlq_max_retries` are marked permanent and retained (for inspection) until `dlq_permanent_max_age_hours` passes. See [Configuration](./configuration.md) for the knobs.

## Large transactions

By default a transaction is buffered and applied atomically. With `sub_batch_size` set, large transactions stream in chunks — the chunks are applied as they arrive and the commit finalizes the group. A crash mid-transaction is safe because re-streaming re-applies the same idempotent events.

## Schema changes

When a tracked table's schema changes (e.g. `ALTER TABLE ADD COLUMN`), puffgres detects it from the replication stream, tears down, and reconnects with fresh schema metadata instead of misinterpreting rows. Your `schema.ts` must still be regenerated to match — run `puffgres check` (which regenerates and validates) after a migration.

## State and rollbacks

puffgres state (checkpoints, backfill cursors, applied configs, DLQ) lives in the **source Postgres database** under `PUFFGRES_STATE_SCHEMA`. A source rollback (e.g. a PITR restore) rolls puffgres' state back in lockstep, so cursors and registrations stay consistent with the data they describe.

## What is *not* guaranteed

- **No cross-config ordering.** Each config is an independent stream.
- **No exactly-once side effects inside transforms.** If your transform calls an external API (e.g. an embedding provider), redelivery can call it more than once. Keep transforms idempotent or tolerant of repeats.
