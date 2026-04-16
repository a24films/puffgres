**NOTE:** Puffgres is in **alpha release** for a select set of design partners. We will announce a more public general release in the future. For now: **use at your own risk**. 

If you have feedback about Puffgres, or are interested in building tools for the future of film and TV production, A24 Labs is hiring software engineers — email [lgelfond@a24films.com](mailto:lgelfond@a24films.com) for more. 



Puffgres is a logical replication service that keeps Postgres entities mirrored in turbopuffer. Rather than duplicating application code every time you modify a vector (and risking partial successes that keep data out of sync), your Postgres changes automatically update.


A bit of Puffgres' design philosophy:

- **You should not need extra database calls to keep vectors up to date**. Upserting rows in your primary database and a secondary vector database is bound to produce drift (forgetting to add parallel / compensating calls) and hard-to-detect failures (i.e. just one of the two calls succeeds). Puffgres lets us "derive" state, making Postgres the source of truth and keeping Turbopuffer in sync. 
- **We guarantee "at least once" delivery**. Developers should not need to consider batching, retry logic, backfills, or change data capture in any of the code that they write. The service maintains its own state in a separate SQLite database, and can stop/start/ resume at any time without losing changes (even if they are slightly out of date)
- **Sync is maintained through "configs" which link Postgres tables to turbopuffer namespaces.** Each defines a mapping, and a TypeScript-based "transform," which lets us easily do operations like tokenization, embedding, and other manipulation. 
- **Configs and transforms are immutable**. We avoid an abundance of thorny cases that come from letting us change a mapping (i.e. rows produced with two different set of transforms.). If we want to make a change, we should "tombstone" the old one and create a new one. 

Read our [docs](TK) to get started.

## Performance

Measured on GitHub Actions `ubuntu-latest` (4-core x86, 16 GB RAM) with `--release` builds (LTO, single codegen unit).

We’ve tested puffgres in production on tables with a few million rows, and it should scale well beyond that. If you implement this at large scale or hit bumps, feel free to shoot me an [email](mailto:lgelfond@a24films.com). Initial benchmarking on GitHub Actions runners shows:

- **Throughput**: >600K events/sec sustained over 100M events
- **Batch latency**: p50 <10&micro;s, p99 <100&micro;s across 100K transactions
- **Recovery**: <60ms to resume from checkpoint after crash
- **Memory**: <160 bytes/event at scale, and total memory usage grows sub-linearly with event volume
- **End-to-end throughput with fanout**: TK source events/sec across 1000 configs
- **Router fanout**: >1M source events/sec across 1000 configs

## Development

### Install

```bash
cargo install just
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
