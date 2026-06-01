#!/usr/bin/env sh
#
# Install the puffgres CLI as a native binary on your PATH.
#
#   curl -fsSL https://raw.githubusercontent.com/a24films/puffgres/main/install.sh | sh
#
# This builds from source with cargo and drops the `puffgres` binary into
# ~/.cargo/bin (already on your PATH once Rust is installed). The prebuilt
# Docker image (ghcr.io/a24films/puffgres) is the deployment artifact, not a
# local CLI — its binary is Linux-only and never lands on your host PATH.

set -eu

REPO="https://github.com/a24films/puffgres"
PACKAGE="puffgres-cli"

if ! command -v cargo >/dev/null 2>&1; then
  echo "puffgres builds with cargo, but the Rust toolchain isn't installed."
  echo "You can install the Rust toolchain by running:"
  echo
  echo "  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh"
  echo
  echo "Then re-run this script."
  exit 1
fi

if ! command -v node >/dev/null 2>&1; then
  echo "puffgres transforms run on Node, which isn't installed."
  echo "Node version manager (nvm) nicely manages this, which you can install if you"
  echo "run the following commands (will install nvm, then download/install Node 22):"
  echo
  echo "  curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.4/install.sh | bash"
  echo "  nvm install 22 && nvm use 22"
  echo
  echo "Then re-run this script."
  exit 1
fi

if ! command -v tsx >/dev/null 2>&1; then
  echo "Installing tsx (used to run transforms) ..."
  npm install -g tsx
fi

echo "Building puffgres from $REPO ..."
cargo install --git "$REPO" "$PACKAGE" --locked

echo
echo "Installed puffgres to $(command -v puffgres 2>/dev/null || echo "$HOME/.cargo/bin/puffgres")"
echo "Run 'puffgres init' in your repo to get started."
