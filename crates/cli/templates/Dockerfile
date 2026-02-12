# Build stage: compile puffgres from source
FROM rust:1-bookworm AS builder

ARG PUFFGRES_GITHUB_ACCESS_TOKEN
ARG PUFFGRES_BRANCH_NAME=main

RUN git clone https://${PUFFGRES_GITHUB_ACCESS_TOKEN}@github.com/a24films/puffgres.git /puffgres-src \
    && cd /puffgres-src \
    && git checkout ${PUFFGRES_BRANCH_NAME} \
    && cargo install --path crates/cli

# Runtime stage
FROM node:22-bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libssl3 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN npm install -g tsx

COPY --from=builder /usr/local/cargo/bin/puffgres /usr/local/bin/puffgres

WORKDIR /app

COPY . .

CMD ["sh", "-c", "puffgres init && puffgres apply && puffgres run"]
