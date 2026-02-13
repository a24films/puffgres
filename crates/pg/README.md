# pg

Postgres connection utilities. This is used to start replication, add a publication, and spin up integration tests.

## Testing

### Unit Tests

```bash
cargo test --lib
```

### Integration Tests

Integration tests require Docker to be running. 

```bash
cargo test --test connect_integration
```
