#!/usr/bin/env sh
#
# Flatten the docs (in SUMMARY.md order) into a single AGENTS.md for coding agents.

set -eu

DOCS_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
SRC_DIR="$DOCS_DIR/src"
SUMMARY="$SRC_DIR/SUMMARY.md"
OUT="${1:-$DOCS_DIR/book/AGENTS.md}"

mkdir -p "$(dirname -- "$OUT")"

{
  cat <<'HEADER'
---
name: puffgres
description: Puffgres docs — Postgres→turbopuffer logical replication. Use when writing puffgres configs/transforms or running the puffgres CLI.
---

# Puffgres — Agent Guide

The complete Puffgres documentation flattened into one file for coding agents
(generated from the docs site — do not edit by hand). Puffgres is a
logical-replication service that keeps Postgres entities mirrored in turbopuffer.
Configs link a Postgres table to a turbopuffer namespace via an immutable
TypeScript transform. The CLI is `puffgres` (`init`, `new`, `check`, `apply`,
`remove`, `run`). The full handbook follows.
HEADER

  # Walk SUMMARY.md and inline each linked page in order.
  grep -oE '\]\(\./[^)]+\.md\)' "$SUMMARY" | sed -E 's/^\]\(\.\///; s/\)$//' | while IFS= read -r f; do
    [ -f "$SRC_DIR/$f" ] || continue
    printf '\n\n---\n\n'
    cat "$SRC_DIR/$f"
  done
} > "$OUT"

echo "Wrote $OUT"
