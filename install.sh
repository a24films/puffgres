#!/usr/bin/env sh
#
# Puffgres installer.
#
#   curl -fsSL https://raw.githubusercontent.com/a24films/puffgres/main/install.sh | sh

set -eu

if ! command -v cargo >/dev/null 2>&1; then
  echo "Installing Rust toolchain..."
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
  . "${CARGO_HOME:-$HOME/.cargo}/env"
fi

SRC="$(mktemp -d)"
trap 'rm -rf "$SRC"' EXIT

git clone --depth 1 https://github.com/a24films/puffgres.git "$SRC"
cargo install --locked --path "$SRC/crates/cli"

echo "Installed. If \`puffgres\` isn't found, add \$HOME/.cargo/bin to your PATH."
