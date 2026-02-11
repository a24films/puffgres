# Install puffgres to ~/.cargo/bin
install:
    cargo install --path crates/cli

# Reinstall puffgres (force overwrite)
reinstall:
    cargo install --path crates/cli --force
