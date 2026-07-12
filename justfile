# Verify build/runtime toolchains; auto-install tsx since Node is already present
_check-deps:
    @command -v cargo >/dev/null 2>&1 || { echo "Rust not installed. Install it by running: curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh"; exit 1; }
    @command -v node >/dev/null 2>&1 || { echo "Node not installed. Install nvm + Node 22: curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.4/install.sh | bash && nvm install 22"; exit 1; }
    @command -v tsx >/dev/null 2>&1 || npm install -g tsx
    @just _check-libpq

# Ensure libpq (the PostgreSQL client library that diesel/pq-sys links against) is present.
# On macOS it is keg-only, so it must be installed and its lib dir passed to the linker (see _pq-lib-dir).
# Note: zerobrew (zb) can't install libpq on macOS (its store prefix is too long for Mach-O), so we use Homebrew.
_check-libpq:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ "$(uname -s)" != "Darwin" ]; then
        # On Linux libpq lives on the default linker path; just make sure it's installed.
        if ! pkg-config --exists libpq 2>/dev/null && ! ldconfig -p 2>/dev/null | grep -q 'libpq\.'; then
            echo "libpq not found. Install it, e.g. Debian/Ubuntu: sudo apt-get install libpq-dev"
            exit 1
        fi
        exit 0
    fi
    # macOS: already installed via Homebrew?
    if [ -n "$(just _pq-lib-dir)" ]; then
        exit 0
    fi
    echo "libpq not found — installing via Homebrew..."
    if command -v brew >/dev/null 2>&1; then
        brew install libpq
    else
        echo "Homebrew not found. Install libpq manually: brew install libpq"
        exit 1
    fi

# Print the directory containing libpq for the linker (empty if not found). macOS/keg-only only.
_pq-lib-dir:
    #!/usr/bin/env bash
    set -euo pipefail
    for prefix in "$(brew --prefix libpq 2>/dev/null)" /opt/homebrew/opt/libpq /usr/local/opt/libpq; do
        if [ -n "$prefix" ] && [ -f "$prefix/lib/libpq.dylib" ]; then
            echo "$prefix/lib"
            exit 0
        fi
    done

# Install puffgres to ~/.cargo/bin
install: _check-deps
    #!/usr/bin/env bash
    set -euo pipefail
    [ "$(uname -s)" = "Darwin" ] && export PQ_LIB_DIR="$(just _pq-lib-dir)"
    cargo install --path crates/cli

# Reinstall puffgres (force overwrite)
reinstall: _check-deps
    #!/usr/bin/env bash
    set -euo pipefail
    [ "$(uname -s)" = "Darwin" ] && export PQ_LIB_DIR="$(just _pq-lib-dir)"
    cargo install --path crates/cli --force
