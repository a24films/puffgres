# Verify build/runtime toolchains; auto-install tsx since Node is already present
_check-deps:
    @command -v cargo >/dev/null 2>&1 || { echo "Rust not installed. Install it by running: curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh"; exit 1; }
    @command -v node >/dev/null 2>&1 || { echo "Node not installed. Install nvm + Node 22: curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.4/install.sh | bash && nvm install 22"; exit 1; }
    @command -v tsx >/dev/null 2>&1 || npm install -g tsx

# Install puffgres to ~/.cargo/bin
install: _check-deps
    cargo install --path crates/cli

# Reinstall puffgres (force overwrite)
reinstall: _check-deps
    cargo install --path crates/cli --force
