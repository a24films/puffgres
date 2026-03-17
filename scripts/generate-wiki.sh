#!/usr/bin/env bash
set -euo pipefail

# Generate GitHub wiki pages from docs/src/ markdown files.
# Reads SUMMARY.md to discover pages dynamically — no hardcoded page list.
# The Command Reference page is auto-generated from `puffgres --help` output.
#
# Usage: PUFFGRES_BIN=./target/release/puffgres WIKI_DIR=./wiki scripts/generate-wiki.sh

PUFFGRES_BIN="${PUFFGRES_BIN:?Set PUFFGRES_BIN to the puffgres binary path}"
WIKI_DIR="${WIKI_DIR:?Set WIKI_DIR to the wiki checkout path}"
DOCS_DIR="${DOCS_DIR:-docs/src}"
SUMMARY="$DOCS_DIR/SUMMARY.md"

mkdir -p "$WIKI_DIR"

# --- Titlecase a hyphenated slug: "getting-started" -> "Getting-Started" ---
titlecase() {
  local IFS='-'
  local parts=($1)
  local result=""
  for part in "${parts[@]}"; do
    local first="${part:0:1}"
    local rest="${part:1}"
    first=$(printf '%s' "$first" | tr '[:lower:]' '[:upper:]')
    result="${result:+$result-}${first}${rest}"
  done
  echo "$result"
}

# --- Parse SUMMARY.md into parallel arrays ---
labels=()
src_files=()
wiki_names=()

re='\[([^]]+)\]\(\./([^)]+)\)'
while IFS= read -r line; do
  if [[ "$line" =~ $re ]]; then
    labels+=("${BASH_REMATCH[1]}")
    src_files+=("${BASH_REMATCH[2]}")
    wiki_names+=("$(titlecase "${BASH_REMATCH[2]%.md}")")
  fi
done < "$SUMMARY"

# First page becomes Home
if (( ${#wiki_names[@]} > 0 )); then
  wiki_names[0]="Home"
fi

# --- Fix cross-links: replace ./file.md with wiki page name ---
fix_links() {
  local content="$1"
  for i in "${!src_files[@]}"; do
    # Use | as sed delimiter since filenames won't contain it
    content=$(printf '%s' "$content" | sed "s|(\\./${src_files[$i]})|(${wiki_names[$i]})|g")
  done
  printf '%s' "$content"
}

# --- Copy and transform pages ---
for i in "${!src_files[@]}"; do
  src="${src_files[$i]}"
  dest="${wiki_names[$i]}.md"

  # Command Reference is generated from source, skip the static copy
  if [[ "$src" == "command-reference.md" ]]; then
    continue
  fi

  if [[ -f "$DOCS_DIR/$src" ]]; then
    content=$(cat "$DOCS_DIR/$src")
    fix_links "$content" > "$WIKI_DIR/$dest"
    echo "  $src -> $dest"
  else
    echo "  warning: $DOCS_DIR/$src not found, skipping"
  fi
done

# --- Generate Command Reference from puffgres --help ---
echo "  generating Command Reference from $PUFFGRES_BIN --help"

{
  echo "# Command Reference"
  echo ""
  echo "> Auto-generated from \`puffgres --help\`. Do not edit manually."
  echo ""

  echo '```'
  "$PUFFGRES_BIN" --help 2>&1 || true
  echo '```'
  echo ""

  subcommands=$("$PUFFGRES_BIN" --help 2>&1 | awk '/^  [a-z]/ { print $1 }' || true)

  for cmd in $subcommands; do
    [[ "$cmd" == "help" ]] && continue
    echo "## \`puffgres $cmd\`"
    echo ""
    echo '```'
    "$PUFFGRES_BIN" "$cmd" --help 2>&1 || true
    echo '```'
    echo ""
  done
} > "$WIKI_DIR/Command-Reference.md"

echo "  generated Command-Reference.md"

# --- Generate sidebar from parsed SUMMARY.md ---
{
  echo "### puffgres"
  echo ""
  for i in "${!wiki_names[@]}"; do
    echo "- [${labels[$i]}](${wiki_names[$i]})"
  done
} > "$WIKI_DIR/_Sidebar.md"

echo "  generated _Sidebar.md"
echo "Wiki generation complete -> $WIKI_DIR"
