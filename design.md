Puffgres is divided into several crates:
- `state`, which manages Puffgres internal state in a SQLite database 
- `config`, which handles all of the mappings from Postgres tables to Turbopuffer namespaces
- `pg`, which sets up Postgres for logical replication and sets up the publication slot
- `replication`, which handles the actual change stream
- `core`, which routes new changes to their respective configs and deals with processing / retry logic
- `cli`, which provides the interface  


A few design choices throughout:
- **State lives in separate SQLite db**. Puffgres is designed around separating replication from the operations of the primary db. Keeping shared state in Postgres (which we did in an early working version) meant that rollbacks would also wipe Puffgres state, making it much harder to recover cleanly. There's just a few tables so we skipped an ORM / rusqlite was more than enough. 