# Delivery Guarantees

Puffgres replicates data from Postgres to Turbopuffer. This page documents the delivery semantics for each mode of operation.

## Streaming (default)

**At-least-once delivery.** Every committed Postgres transaction is delivered to turbopuffer at least once; some events may be re-delivered on crash or re-start. Because turbopuffer upserts are idempotent, this shouldn't be a problem; the values will be eventually consistent. Events from rolled-back PG transactions are never delivered. 

**Partial transaction delivery.** You can optionally enable "sub-batching", when throughput for large transactions matters. By default, it is disabled; if you set `sub_batch_size` it will begin. This setting means that Postgres will chunk  large (think >100k rows) transactions up into smaller bits, but leaves the risk of partially committed state in turbopuffer. 

**At-least-once per row.** Backfill uses cursor-based pagination with checkpointing. We save a cursor position in the puffgres state, and if there is a crash or restart, we can resume from the last saved cursor, guaranteeing at-least-once delivery.
 

## DLQ (Dead Letter Queue)

Events that fail to transform or write are sent to the DLQ rather than blocking the pipeline. Retryable errors are replayed periodically (controlled by `dlq_replay_interval`). After `dlq_max_retries` attempts, entries are marked permanent. Transient errors are those like connections or timeouts, where unretryable errors might look like incorrect database permissioning or incompatible schemas. In any case,  the DLQ ensures that a single bad event does not stall the entire replication pipeline.

## Ordering

Within a single Postgres transaction, events are delivered to Turbopuffer in WAL order. Across transactions, events are delivered in commit order. turbopuffer does not guarantee read-after-write consistency, so there may be a brief delay before written documents are queryable.
