# Deterministic Parquet Fixtures

These parquet fixtures are generated deterministically by:

- `crates/client/tests/support/mod.rs`
- function: `ensure_integration_parquet_fixtures()`

Files:

- `lineitem.parquet`
- `orders.parquet`
- `docs.parquet`

Contract:

1. The same rows and schema are written on every call.
2. Integration tests can rely on these fixed datasets for embedded/distributed parity checks.
3. Query result validation is done through normalized snapshots (`snapshot_text`) to keep assertions stable.
