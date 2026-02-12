Puffgres is divided into several crates:
- `state`, which manages Puffgres internal state in a SQLite database 
- `config`, which handles all of the mappings from Postgres tables to Turbopuffer namespaces
- `pg`, which sets up Postgres for logical replication and sets up the publication slot
- `replication`, which handles the actual change stream
- `core`, which routes new changes to their respective configs and deals with processing / retry logic
- `cli`, which provides the interface  


A few design choices throughout:
- **State lives in separate SQLite db**. Puffgres is designed around separating replication from the operations of the primary db. Keeping shared state in Postgres (which we did in an early working version) meant that rollbacks would also wipe Puffgres state, making it much harder to recover cleanly. There's just a few tables so we skipped an ORM / rusqlite was more than enough.

## Backfill

During backfill we bulk-copy an entire table into Turbopuffer, but writes to Postgres don't stop while that's happening. If we just backfilled and then started CDC from "now", we'd miss every insert, update, and delete that occurred during the backfill window.

To avoid this, before starting a backfill we record a `watermark_lsn` — the current WAL position (`pg_current_wal_lsn()`). This marks exactly where the change stream was at the moment we began copying. Once the backfill finishes, we start CDC from that watermark so every write that landed during the backfill gets reprocessed. Because Turbopuffer upserts are idempotent, any overlap between the backfill and the replayed CDC events is harmless.