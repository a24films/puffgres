# Getting Started

## Install

Pull the prebuilt image — it ships the `puffgres` binary plus the Node runtime its transforms need:

```bash
docker pull ghcr.io/a24films/puffgres:latest
```

If you'd rather build the binary from source:

1. Install [Just](https://github.com/casey/just#installation).
2. Install the [Rust toolchain](https://rust-lang.org/tools/install/).
3. Build and install `puffgres` into `~/.cargo/bin`:

```bash
just install

# To overwrite an existing install
just reinstall
```

## Use with coding agents

The docs are published as one file at <https://a24films.github.io/puffgres/AGENTS.md>. Install it as a skill — paste one of these:

```bash
# Claude Code
mkdir -p ~/.claude/skills/puffgres && curl -fsSL https://a24films.github.io/puffgres/AGENTS.md -o ~/.claude/skills/puffgres/SKILL.md
```

```bash
# Codex
mkdir -p ~/.codex/skills/puffgres && curl -fsSL https://a24films.github.io/puffgres/AGENTS.md -o ~/.codex/skills/puffgres/SKILL.md
```

## Setting Up a Project

Navigate to the root level of your repo and run `puffgres init`. This will generate a `puffgres/` folder, complete with Dockerfile, and initial setup files.

The generated `puffgres.toml` is the main configuration file for your project. It controls both runtime behavior and environment variable loading. See the [Configuration](./configuration.md) section for a full reference.

Your environment variable paths are set in `puffgres.toml` — later paths override earlier ones. Our config looks like this, which works both in production and in dev:

```toml
environment_files = ["./.env", "../.env", "../.env.development"]
```
