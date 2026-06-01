# Getting Started

## Install

Install the CLI as a native binary — under the hood verifies you have the Rust toolchain and builds from source + adds `puffgres` to your PATH.

```bash
curl -fsSL https://raw.githubusercontent.com/a24films/puffgres/main/install.sh | sh
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
