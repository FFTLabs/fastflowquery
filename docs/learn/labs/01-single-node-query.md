# Lab 01: Single-Node Query (Embedded)

Goal: run a real parquet query in embedded mode, inspect plan output, and validate deterministic test coverage.

## Prerequisites

1. `cargo build` succeeds.
2. You are in repository root.

## Steps

1. Run a simple SQL query in CLI mode:

```bash
cargo run -p ffq-client -- query --sql "SELECT 1"
```

2. Run a parquet query via catalog profile:

```bash
cargo run -p ffq-client -- query \
  --catalog tests/fixtures/catalog/tpch_dbgen_sf1_parquet.tables.json \
  --sql "SELECT l_orderkey, l_quantity FROM lineitem LIMIT 5"
```

3. Inspect plan-only output for the same query:

```bash
cargo run -p ffq-client -- query \
  --catalog tests/fixtures/catalog/tpch_dbgen_sf1_parquet.tables.json \
  --sql "SELECT l_orderkey, l_quantity FROM lineitem LIMIT 5" \
  --plan
```

4. Run embedded integration suite:

```bash
make test-13.2-embedded
```

## Expected Output

1. Step 1 prints a single-row result for `SELECT 1`.
2. Step 2 prints non-empty rows from `lineitem`.
3. Step 3 prints logical/physical plan text (not row output).
4. Step 4 runs:
   - `integration_parquet_fixtures`
   - `integration_embedded`
   and both pass.

## Troubleshooting

1. `catalog load`/table-not-found errors:
   - verify path exists: `tests/fixtures/catalog/tpch_dbgen_sf1_parquet.tables.json`.
2. Empty query output unexpectedly:
   - confirm fixture parquet files exist under `tests/bench/fixtures/tpch_dbgen_sf1_parquet/`.
3. Rust build/test failures:
   - run `cargo build` first and fix compile errors before proceeding.
