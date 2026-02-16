# Writes and Commit Semantics

This chapter explains how FFQ v1 performs data writes safely: SQL DML planning, sink execution, durable commit patterns, mode semantics (`overwrite`/`append`), and failure cleanup behavior.

## 1) Write entry points

FFQ has two user-facing write entry points:

1. SQL DML: `INSERT INTO <table> SELECT ...`
2. DataFrame APIs:
   - `write_parquet(path)` / `write_parquet_with_mode(path, mode)`
   - `save_as_table(name)` / `save_as_table_with_mode(name, mode)`

Both paths eventually rely on parquet writes with explicit staging and commit behavior.

Relevant code:

1. `crates/planner/src/sql_frontend.rs`
2. `crates/client/src/dataframe.rs`

## 2) SQL `INSERT INTO ... SELECT` planning flow

For SQL DML, planner flow is:

1. SQL frontend parses into logical `InsertInto { table, columns, input }`.
2. Analyzer validates target table existence and schema/type compatibility.
3. Physical planner lowers to `ParquetWriteExec { table, input }`.
4. Runtime executes child query and writes result batches to sink path.

Important v1 behavior:

1. Sink queries are write-oriented; `collect()` returns empty result batches for the sink node itself.
2. Correctness is validated by reading output files (or querying the saved table), not by sink-row return values.

Relevant code:

1. `crates/planner/src/logical_plan.rs`
2. `crates/planner/src/analyzer.rs`
3. `crates/planner/src/physical_planner.rs`
4. `crates/client/src/runtime.rs`

## 3) Sink execution internals

`ParquetWriteExec` runtime behavior:

1. Execute input physical subtree and materialize output batches.
2. Resolve sink target path from table `uri`/`paths`.
3. Write to staged temp location first.
4. Commit staged output into final location atomically (rename/swap pattern where supported).

If target is missing path metadata, runtime fails early with a clear planning/execution error.

Relevant code:

1. `crates/planner/src/physical_plan.rs`
2. `crates/client/src/runtime.rs`

## 4) Temp-then-commit pattern

The central safety mechanism is stage-then-commit:

1. Never write directly to final target first.
2. Write complete parquet payload to sibling staged path (`.ffq_staged_*`).
3. Commit by renaming/moving staged artifact to final path.
4. For overwrite swaps, preserve backup and restore on commit failure.

Why this matters:

1. Prevents partially-written final files from being observed as committed state.
2. Keeps retries cleaner by separating transient staging artifacts from committed artifacts.

Relevant code:

1. `crates/client/src/dataframe.rs`
2. `crates/client/src/runtime.rs`
3. `crates/storage/src/catalog.rs` (same durability pattern for catalog file commits)

## 5) Overwrite vs append semantics

`WriteMode::Overwrite`:

1. Produces deterministic final layout for directory writes (`part-00000.parquet`, etc.).
2. Replaces prior dataset atomically through staged dir/file swap.
3. Intended to be retry-friendly and deterministic.

`WriteMode::Append`:

1. Preserves existing data and adds new part files (monotonic part numbering).
2. Not idempotent by default: repeating append repeats data.
3. For single-file path, append is rejected (must use directory path).

Relevant code:

1. `crates/client/src/dataframe.rs`

## 6) Idempotency model

FFQ v1 idempotency is mode-specific:

1. Overwrite path: effectively idempotent for same input (final shape stays stable).
2. Append path: intentionally non-idempotent (each successful run contributes additional data).
3. Catalog append merge deduplicates identical path strings, but does not deduplicate row data.

Practical implication:

1. Use overwrite for retry-safe replacement jobs.
2. Use append only when additive semantics are intended.

## 7) Failure cleanup and rollback

On failure during write/commit:

1. Staged temp files/dirs are cleaned up best-effort.
2. Overwrite swap failures attempt backup restoration.
3. `save_as_table` updates catalog after durable write success, avoiding broken table registration on failed writes.

Failure guarantee in v1:

1. Failed write should not leave a partially-committed, queryable table in catalog.

Relevant code:

1. `crates/client/src/dataframe.rs`
2. `crates/storage/src/catalog.rs`

## 8) End-to-end success flow

Example: `INSERT INTO dst SELECT a, b FROM src`

1. Parse/analyze to `InsertInto` logical plan.
2. Lower to `ParquetWriteExec`.
3. Run input query and produce result batches.
4. Write staged parquet output.
5. Commit output to final destination.
6. Query completes with sink success status.

Validation pattern:

1. Read destination parquet and verify expected rows/schema.

## 9) End-to-end failure/retry flow

Example failure case:

1. `save_as_table` target path collides with invalid filesystem shape.
2. Staged write or commit step fails.
3. Cleanup removes staged artifacts.
4. Catalog entry is not advanced to partial/broken state.

Retry case:

1. Retry with corrected target and `overwrite` mode.
2. Commit succeeds; final part layout remains deterministic.

## 10) What to inspect when debugging write issues

1. Confirm table has valid write destination (`uri` or `paths`).
2. Check mode mismatch (`append` on single-file path is unsupported).
3. Inspect staged/backup files in target parent directory.
4. Verify catalog persistence path and commit success.
5. Re-run with overwrite for deterministic retry behavior.

## References

1. `crates/planner/src/sql_frontend.rs`
2. `crates/planner/src/analyzer.rs`
3. `crates/planner/src/logical_plan.rs`
4. `crates/planner/src/physical_plan.rs`
5. `crates/planner/src/physical_planner.rs`
6. `crates/client/src/runtime.rs`
7. `crates/client/src/dataframe.rs`
8. `crates/storage/src/catalog.rs`
9. `docs/v1/writes-dml.md`
