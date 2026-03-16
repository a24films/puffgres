puffgres is a logical replication service that keeps Postgres entities mirrored in turbopuffer. Rather than duplicating application code every time you modify a vector (and risking partial successes that keep data out of sync), your Postgres changes automatically update.

## Design principles

A bit of puffgres' design philosophy:

- **You should not need extra database calls to keep vectors up to date**. Upserting rows in your primary database and a secondary vector database is bound to produce drift (forgetting to add parallel / compensating calls) and hard-to-detect failures (i.e. just one of the two calls succeeds). puffgres lets us "derive" state, making Postgres the source of truth and keeping Turbopuffer in sync. 
- **We guarantee "at least once" delivery**. Developers should not need to consider batching, retry logic, backfills, or change data capture in any of the code that they write. The service maintains its own state in a separate SQLite database, and can stop/start/ resume at any time without losing changes (even if they are slightly out of date)
- **Sync is maintained through "configs" which link Postgres tables to turbopuffer namespaces.** Each defines a mapping, and a TypeScript-based "transform," which lets us easily do operations like tokenization, embedding, and other manipulation. 
- **Configs and transforms are immutable**. We avoid an abundance of thorny cases that come from letting us change a mapping (i.e. rows produced with two different set of transforms.). If we want to make a change, we should "tombstone" the old one and create a new one. 
