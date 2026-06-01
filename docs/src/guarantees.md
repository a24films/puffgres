# Delivery Guarantees

puffgres delivers **at least once, and writes are idempotent** — so a namespace ends up in the same final state it would have with exactly-once delivery. Here's what backs that up.

## Nothing is skipped

A replication checkpoint only advances *after* a batch lands in turbopuffer. If puffgres crashes, it resumes from the last saved point — some events may be re-sent, but none are lost. Backfill works the same way: it remembers the last id it processed and picks up from there.

## Re-sends don't create duplicates

Every write is keyed by document id, so applying the same event twice has no effect. That's why your config must point at a column with a unique index — `check` enforces it.

## Failures don't block the stream

A batch that keeps failing is moved to the dead letter queue instead of stalling everything. Retryable entries are replayed automatically; the rest are kept for inspection. See [Configuration](./configuration.md) for the retry and retention knobs.

## Large transactions

Big transactions are applied atomically by default. Set `sub_batch_size` to stream them in chunks instead — still safe on a crash, since re-streaming just re-applies the same idempotent events.

## Schema changes

When a tracked table's schema changes (e.g. `ALTER TABLE ADD COLUMN`), puffgres notices, reconnects with fresh metadata, and keeps going. Regenerate your `schema.ts` afterward by running `puffgres check`.

## Rollbacks stay consistent

puffgres keeps its state (checkpoints, cursors, applied configs, DLQ) in the source Postgres database under `PUFFGRES_STATE_SCHEMA`. If the source is rolled back (e.g. a PITR restore), puffgres' state rolls back with it — cursors and the data they track never drift apart.

## Not guaranteed

- **Ordering across configs.** Each config is its own independent stream.
- **Exactly-once side effects in transforms.** If a transform calls an external API (say, an embedding provider), a re-send can call it again. Keep transforms idempotent.
