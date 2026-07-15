# Why puffgres?

Postgres is one of the most common relational databases in the world. turbopuffer is a company building a fast database optimized for search. In particular, its strength is in dealing with vectors. Vector embeddings are a technique we use to search semantically, rather than just what is in the text; for example, searching "dog" on regular search wouldn't surface results for "puppy" or "canine," where semantic search would. 

Storing vectors, and finding results that are "near" (i.e. doing distance calculations) are very different load patterns than those generally found in relational databases. You can store vectors directly in Postgres using an extension called [pgvector]() but it has considerable [negative performance tradeoffs](https://alex-jacobs.com/posts/the-case-against-pgvector/): indexes and filtering aren't very performant, and the extra memory use degrades regular reads and writes.

As such, many people use a relational database for most transactional workloads, and also keep their vectors in a separate database specialized for vector workloads, like Pinecone, Weaviate, or turbopuffer. The challenge in these cases becomes keeping the two databases in sync. There's a few approaches, all of which we thought were inadequate: 

- **Two database calls at each place data is changed**. For example, if you add a new row in Postgres, add a new row in the vector database. The same could happen for updates and deletes. The problem here is that we have no guarantees that both calls will succeed. If the Postgres call succeeds and vector call fails, or vice versa, the two will be permanently out of sync. 
- **Durable job queue for vector calls**. Another way we could do this would be to handle all calls in Postgres, make row changes, and, in a transaction, add the parallel modification to vectors into a durable job queue. If that queue lived in Postgres, the row change and the job to modify the vector could live in a transaction, meaning we'd have good guarantees they'd both succeed or both failed, and we could retry the vector updates if they had temporarily failed. The challenge here is one of later developer hygeine; developers would need to know, everytime they were modifying a given table, that they needed to have the corresponding call. It would be easy for a new developer, or developer months down the line to forget about an equivalent vector, and then the two databases would again be out of sync.
- **Updates in data pipeline** We could have a separate table that tracks when a vector was updated, and, everytime our data pipeline runs, upsert any rows where the vector was updated before the row was updated. This was our initial solution, and it is poor becuase (a) it lower bounds the time to update vectors to the frequency of the data pipeline, in our case several minutes (b) requires janky logic to handle deletes and (c) is super inefficient and requires a full table scan even for minor updates each time. 

Instead of this, we designed puffgres to be a logical replication service. This is based on Martin Kleppman's talk [Turning the database inside out](https://martin.kleppmann.com/2015/03/04/turning-the-database-inside-out.html) about how all data is either source of truth or derived, and we generally should have derived sources replicate changes on source of truth. In essence, as a developer you only need to modify rows in Postgres, define mappings between Postgres tables and turbopuffer namespaces once, and then puffgres will listen to changes (on the Postgres write-ahead-log) and propagate them to turbopuffer.





```
                             ╔═ puffgres ═════════════════════╗         ╔═ turbopuffer ══════╗
╔═ Postgres ══╗              ║                                ║░        ║                    ║░
║             ║░             ║ ┏━ configs ━━━━━━━━━━━━━━━━━━┓ ║░        ║ ┏━ namespaces ━━━┓ ║░
║ ┏━ WAL ━━━┓ ║░ row changes ║ ┃ page     ──▶ page_chunks   ┃ ║░        ║ ┃ page_chunks    ┃ ║░
║ ┃▞▞▞▞▞▞▞▞▞┃ ║░  ░ ░ ░ ░ ──▶║ ┃ person   ──▶ person_names  ┃ ║░──API──▶║ ┃ person_names   ┃ ║░
║ ┗━━━━━━━━━┛ ║░             ║ ┃ document ──▶ document_text ┃ ║░        ║ ┃ document_text  ┃ ║░
║             ║░             ║ ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛ ║░        ║ ┗━━━━━━━━━━━━━━━━┛ ║░
╚═════════════╝░             ║                                ║░        ║                    ║░
 ░░░░░░░░░░░░░░░             ╚════════════════════════════════╝░        ╚════════════════════╝░
                              ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░         ░░░░░░░░░░░░░░░░░░░░░░
```
