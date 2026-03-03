# syntax=docker/dockerfile:1

# Build stage: compile puffgres from source
FROM rust:1-bookworm AS builder

ARG PUFFGRES_BRANCH_NAME=main

ARG GITHUB_TOKEN
ARG CACHEBUST=1
RUN test -n "$GITHUB_TOKEN" || (echo "Error: GITHUB_TOKEN build arg is required" && exit 1) \
    && git config --global url."https://${GITHUB_TOKEN}@github.com/".insteadOf "https://github.com/" \
    && git clone https://github.com/a24films/puffgres.git /puffgres-src \
    && git config --global --remove-section url."https://${GITHUB_TOKEN}@github.com/" \
    && cd /puffgres-src \
    && git checkout ${PUFFGRES_BRANCH_NAME} \
    && cargo install --path crates/cli \
    && rm -rf /puffgres-src

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

COPY package.json .
RUN pnpm install

COPY . .

CMD ["sh", "-c", "puffgres init && puffgres setup && puffgres apply && puffgres run"]