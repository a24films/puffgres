# syntax=docker/dockerfile:1

# Pulls the prebuilt puffgres image instead of compiling from source.
# Build from this directory: docker build -t my-puffgres .
FROM ghcr.io/a24films/puffgres:latest

WORKDIR /app

COPY package.json .
RUN pnpm install

COPY . .

CMD ["sh", "-c", "puffgres init && puffgres check && puffgres apply && puffgres run"]
