# syntax=docker/dockerfile:1

# The published puffgres runtime image (binary + Node/tsx/pnpm runtime), pushed
# to ghcr.io/a24films/puffgres by .github/workflows/image.yml. Downstream
# projects FROM this image and add their own configs/transforms.

FROM rust:1-bookworm AS builder
WORKDIR /src
COPY . .
RUN cargo install --path crates/cli --root /usr/local

FROM node:22-bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libssl3 \
    libpq5 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN corepack enable && corepack prepare pnpm@latest --activate
RUN npm install -g tsx

COPY --from=builder /usr/local/bin/puffgres /usr/local/bin/puffgres

WORKDIR /app

CMD ["puffgres", "--help"]
