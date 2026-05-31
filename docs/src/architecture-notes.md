# Architecture and Meta-Notes

## Package Organization

puffgres is a Rust workspace divided into several crates under `crates/`:

- **`pg`** — Postgres setup, creates / manages publication and slot, generates schema files from table definitions, and runs backfill queries to bring in data
- **`replication`** — the change data capture stream. Decodes Postgres logical replication protocol, manages caching relations, schema changes + in-transaction batching.
- **`core`** — routes change events to their respective configs, runs transforms via a TypeScript subprocess, manages retry logic / dead letter queue, and calls to the puff client
- **`puff`** — turbopuffer API client, light wrapper around `rs-puff`
- **`state`** — stores persistent information (i.e. streaming replication checkpoints, backfill progress, failed entry queue) in a dedicated Postgres schema (by default: `puffgres`).
- **`cli`** — the `puffgres` binary, handles subcommands, environment setup, orchestration, and default / template files. 
- **`config`** — definitions, parsing, validation, and hashing of config files. 
- **`debug`** — light server / web UI to inspect turbopuffer / WAL contents, just easier than the turbopuffer dashboard / making a bunch of cURLs

Documentation lives in `docs/` and is built with [mdbook](https://rust-lang.github.io/mdBook/).

## Meta-notes

We built this because we needed vector embeddings internally and had read compelling evidence [pgvector was a bad solution](TK) because of performance hits to maintain indexes, poor filtered queries, etc. Our naive / base solution was very hacky; we kept a separate table everytime we kept something in turbopuffer that kept an `id`, `turbopuffer_updated_at`, and `updated_at` and would simply embed / upsert whenever `updated_at` was more recent. This meant a full table scan whenever our pipeline ran (very inefficient) and effectively polling for changes in turbopuffer. It didn't handle deletes, required tons of duplicative code, and meant all updates only happened when the pipeline ran.

This was inspired by two tech talks: Martin Kleppman's [Turning the database inside outTK with Apache Samza](TK), that argues strongly for making changes in one place and having derived data act like a materializedd view, and Bryan Cantrill's [The primacy of toolmaking: sharpening the axe TK](TK), which suggests companies are well-suietd investing in (and releasing) generic tools when they find themselves doing repeated work. 

I built a [very hacky](https://github.com/lucasgelfond/puffgres) version of this over a weekend, starting with a detailed spec and working through it with Claude. When we decided to use it in-house, I broke it up into PRs, and, especially at the beginning, ran each through code review + traditional testing, CI, etc. We're been running puffgres internally for a bit now without issue, and after talking with the turbopuffer team figured others might find use in it as well. 