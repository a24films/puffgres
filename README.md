If you have feedback about puffgres, or are interested in building tools for the future of film and TV production, A24 Labs is hiring software engineers — email [lgelfond@a24films.com](mailto:lgelfond@a24films.com) for more. 

puffgres (beta) is a logical replication service that keeps Postgres entities mirrored in turbopuffer. Rather than duplicating application code every time you modify a vector (and risking partial successes that keep data out of sync), your Postgres changes automatically update.


A bit of puffgres' design philosophy:

- **You should not need extra database calls to keep vectors up to date**. Upserting rows in your primary database and a secondary vector database is bound to produce drift (forgetting to add parallel / compensating calls) and hard-to-detect failures (i.e. just one of the two calls succeeds). puffgres lets us "derive" state, making Postgres the source of truth and keeping Turbopuffer in sync. 
- **The service handles at-least once delivery**. Developers should not need to consider batching, retry logic, backfills, or change data capture in any of the code that they write. The service maintains its own state in a dedicated `puffgres` schema inside your source Postgres, and can stop/start/resume at any time without losing changes (even if they are slightly out of date). Co-locating state with the source means PITR restores naturally roll the two together.
- **Sync is maintained through "configs" which link Postgres tables to turbopuffer namespaces.** Each defines a mapping, and a TypeScript-based "transform," which lets us easily do operations like tokenization, embedding, and other manipulation. 
- **Configs and transforms are immutable**. We avoid an abundance of thorny cases that come from letting us change a mapping (i.e. rows produced with two different set of transforms.). If we want to make a change, we should "tombstone" the old one and create a new one. 

Read our [docs](https://a24films.github.io/puffgres/) to get started.

## Install

Pull the prebuilt image — it ships the `puffgres` binary plus the Node runtime its transforms need:

```bash
docker pull ghcr.io/a24films/puffgres:latest
```

## Use with coding agents

The docs are published as one file at <https://a24films.github.io/puffgres/AGENTS.md>. Install it as a skill — paste one of these:

```bash
# Claude Code
mkdir -p ~/.claude/skills/puffgres && curl -fsSL https://a24films.github.io/puffgres/AGENTS.md -o ~/.claude/skills/puffgres/SKILL.md
```

```bash
# Codex
mkdir -p ~/.codex/prompts && curl -fsSL https://a24films.github.io/puffgres/AGENTS.md -o ~/.codex/prompts/puffgres.md
```

## Why puffgres?

We built this because we needed vector embeddings internally and had read compelling evidence [pgvector was a bad solution](https://alex-jacobs.com/posts/the-case-against-pgvector/) because of performance hits to maintain indexes, poor filtered queries, etc. Our naive / base solution was very hacky; we kept a separate table everytime we kept something in turbopuffer that kept an `id`, `turbopuffer_updated_at`, and `updated_at` and would simply embed / upsert whenever `updated_at` was more recent. This meant a full table scan whenever our pipeline ran (very inefficient) and effectively polling for changes in turbopuffer. It didn't handle deletes, required tons of duplicative code, and meant all updates only happened when the pipeline ran.

This was inspired by two tech talks: Martin Kleppmann's [Turning the database inside out with Apache Samza](https://martin.kleppmann.com/2015/03/04/turning-the-database-inside-out.html), that argues strongly for making changes in one place and having derived data act like a materialized view, and Bryan Cantrill's [Sharpening the Axe: The Primacy of Toolmaking](https://www.youtube.com/watch?v=_GpBkplsGus), which suggests companies are well-suited investing in (and releasing) generic tools when they find themselves doing repeated work.

I built a [very hacky](https://github.com/lucasgelfond/puffgres) version of this over a weekend, starting with a detailed spec and working through it with Claude. When we decided to use it in-house, I broke it up into PRs, and, especially at the beginning, ran each through code review + traditional testing, CI, etc. We've been running puffgres internally for a bit now without issue, and after talking with the turbopuffer team figured others might find use in it as well.

## Performance

These benchmarks are a bit artificial, in isolation on a GitHub action (`ubuntu-latest`, the 4-core x86 / 16GB RAM runner). In practice, we've used `puffgres` on tables of a few hundred thousand rows, although it should scale much beyond this. If you implement this at large scale or hit bumps, feel free to shoot me an [email](mailto:lgelfond@a24films.com). Initial benchmarking on GitHub Actions runners shows:

- **Throughput**: >600K events/sec sustained over 100M events
- **Batch latency**: p50 <10&micro;s, p99 <100&micro;s across 100K transactions
- **Recovery**: <60ms to resume from checkpoint after crash
- **Memory**: <160 bytes/event at scale, and total memory usage grows sub-linearly with event volume
- **Router fanout**: >1M source events/sec across 1000 configs

## Development

### Package Organization

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

### Install

Build the binary from source:

1. Install [Just](https://github.com/casey/just#installation).
2. Install the [Rust toolchain](https://rust-lang.org/tools/install/).
3. Build and install `puffgres` into `~/.cargo/bin`:

```bash
just install

# To overwrite an existing install
just reinstall
```

### Testing

```bash
# Unit tests
cargo test --workspace --lib

# Integration tests (requires Postgres)
cargo test --workspace
```

### Benchmarks

```bash
cargo bench --package replication --bench decoder_bench
```

### Fuzzing

Fuzz targets live in `fuzz/` and use [cargo-fuzz](https://github.com/rust-fuzz/cargo-fuzz).

```bash
cargo install cargo-fuzz
cargo +nightly fuzz run fuzz_decoder

# Regenerate seed corpus
cd fuzz && cargo run --bin generate_seeds
```
