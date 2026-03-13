# Getting Started

We will eventually create a persistent install script / Docker image.

In the interim, or if you want to contribute to the Source, you should:
- insall [Rust](https://rust-lang.org/tools/install/)
- install [Just](https://github.com/casey/just)
- clone the repo
- run `just install` on the repo.

## Setting Up a Project

Navigate to the root level of your repo and run `puffgres init`. This will generate a `puffgres/` folder, complete with Dockerfile, and initial setup files.

The generated `puffgres.toml` is the main configuration file for your project. It controls both runtime behavior and environment variable loading. See the [Configuration](./configuration.md) section for a full reference.

Your environment variable paths are set in `puffgres.toml` — later paths override earlier ones. Our config looks like this, which works both in production and in dev:

```toml
environment_files = ["./.env", "../.env", "../.env.development"]
```
