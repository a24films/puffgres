# Architecture and Meta-Notes

## Package Organization

puffgres is a Rust workspace divided into several crates under `crates/`:

- **`cli`** — the `puffgres` binary. Handles all subcommands (`init`, `new`, `apply`, `run`, etc.), config loading, environment setup, and orchestration.
- **`config`** — parsing and validation of `config.toml` files. Defines the `Config`, `SourceConfig`, and `IdConfig` types, and computes content hashes for immutability checking.
- **`core`** — the replication pipeline. Routes change events to their respective configs, runs transforms via a TypeScript subprocess, upserts/deletes in turbopuffer, and manages retry logic and the dead letter queue.
- **`debug`** — the `puffgres debug` web UI. Serves a local web server for inspecting turbopuffer namespace contents and viewing the live Postgres replication stream.
- **`pg`** — Postgres setup. Creates and manages the logical replication publication and slot, runs backfill queries, and generates `schema.ts` files from table definitions.
- **`puff`** — turbopuffer API client. Handles upserts, deletes, and namespace operations.
- **`replication`** — the change data capture stream. Decodes the Postgres logical replication protocol (pgoutput), manages relation caching, schema change detection, transaction batching, and sub-batch streaming for large transactions.
- **`state`** — SQLite state management via Diesel. Tracks applied configs, replication checkpoints, backfill progress, and dead letter queue entries. Runs in WAL mode.

Documentation lives in `docs/` and is built with [mdbook](https://rust-lang.github.io/mdBook/).

## Meta-notes

This originally came about to replace a hacky system we built internally, that kept a `turbopuffer_updated_at` column in Snowflake for some of our vector based tables. This meant a full table scan everytime our data pipeline ran (inefficient!), that it missed deletes, and that additions were quite slow. It also meant lots of duplicate code whenever we wrote a new transform, and easy regressions if we changed old code.

I built a [very hacky](https://github.com/lucasgelfond/puffgres) version of this over a weekend, starting with a detailed spec and working through it with Claude. I then properly broke it up into PRs, we took it through code review (you can see the PRs / merge history on this repo!) and added testing, traditional CI, etc, plus lots of testing before we deployed and felt it was ready. We've been running puffgres in production internally for a little while now, without issue; I figured this was a generic enough problem it would be useful to release, in the Bryan Cantrill [primacy of toolmaking](https://www.youtube.com/watch?v=_GpBkplsGus) tradition.
