# syntax=docker/dockerfile:1

# Build stage: compile puffgres from source
FROM rust:1-bookworm AS builder

ARG PUFFGRES_BRANCH_NAME=main

ARG CACHEBUST=1
RUN --mount=type=secret,id=github_token \
    test -f /run/secrets/github_token || (echo "Error: github_token secret is required. Pass --secret id=github_token,src=<path>" && exit 1) \
    && git clone https://$(cat /run/secrets/github_token)@github.com/a24films/puffgres.git /puffgres-src \
    && cd /puffgres-src \
    && git checkout ${PUFFGRES_BRANCH_NAME} \
    && cargo install --path crates/cli

# Runtime stage
FROM node:22-bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libssl3 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN corepack enable && corepack prepare pnpm@latest --activate
RUN npm install -g tsx

COPY --from=builder /usr/local/cargo/bin/puffgres /usr/local/bin/puffgres

WORKDIR /app

COPY puffgres/package.json .
RUN pnpm install

COPY puffgres/ .

CMD ["sh", "-c", "puffgres init && puffgres apply && puffgres run"]
