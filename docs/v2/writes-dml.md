# Writes, DML, and Commit Semantics (v2 Bootstrap)

- Status: draft
- Owner: @ffq-docs
- Last Verified Commit: TBD
- Last Verified Date: TBD
- Source: inherited/adapted from prior version docs; v2 verification pending


This document describes the bootstrapped v2 write path docs, including SQL DML (`INSERT INTO ... SELECT`), sink operators, DataFrame write APIs, commit behavior, cleanup, and retry/idempotency semantics.

## Scope

Covered:
1. SQL DML parse/analyze/lower path.
2. Logical/physical sink operators.
3. DataFrame write APIs:
- `write_parquet`
- `save_as_table`
4. Write modes:
- `Overwrite`
- `Append`
5. Temp-then-commit behavior.
6. Failure cleanup and retry/idempotency notes.

Core files:
1. `crates/planner/src/sql_frontend.rs`
2. `crates/planner/src/analyzer.rs`
3. `crates/planner/src/logical_plan.rs`
4. `crates/planner/src/physical_plan.rs`
5. `crates/planner/src/physical_planner.rs`
6. `crates/client/src/dataframe.rs`
7. `crates/client/src/runtime.rs`
8. `crates/distributed/src/worker.rs`

## SQL DML: `INSERT INTO ... SELECT ...`

### Parser and logical plan

Implemented in `crates/planner/src/sql_frontend.rs`.

Behavior:
1. Supports `INSERT INTO <table> SELECT ...`.
2. Produces logical node:
- `LogicalPlan::InsertInto { table, columns, input }`

Constraints:
1. Source must be `SELECT`.
2. Non-SELECT insert sources are rejected in v1.

### Analyzer checks

Implemented in `crates/planner/src/analyzer.rs`.

Checks:
1. Target table existence/schema resolution.
2. Column count compatibility.
3. Type compatibility (with limited numeric compatibility rules).

Failure examples:
1. Insert type mismatch -> analyzer error (`INSERT type mismatch ...`).

### Physical lowering

Implemented in `crates/planner/src/physical_planner.rs`.

Lowering:
1. `LogicalPlan::InsertInto` -> `PhysicalPlan::ParquetWrite(ParquetWriteExec)`.

## Sink Operators

### Logical sink

1. `LogicalPlan::InsertInto` (`crates/planner/src/logical_plan.rs`).

### Physical sink

1. `PhysicalPlan::ParquetWrite` (`crates/planner/src/physical_plan.rs`).

### Runtime sink execution

Embedded runtime (`crates/client/src/runtime.rs`):
1. Executes child plan to batches.
2. Calls `write_parquet_sink(table, child_output)`.
3. Returns empty output (`Schema::empty`, `batches = []`).

Distributed worker runtime (`crates/distributed/src/worker.rs`):
1. Uses same physical sink operator during stage execution.
2. Writes parquet sink output and reports task completion.

Implication:
1. DML/sink query `collect()` is write-oriented and not row-returning in v1 (result batches are empty on sink node path).

## DataFrame Write APIs

Implemented in `crates/client/src/dataframe.rs`.

### `write_parquet(path)` / `write_parquet_with_mode(path, mode)`

Behavior:
1. Executes DataFrame and materializes `(schema, batches)`.
2. If path has `.parquet` extension:
- only `Overwrite` supported,
- `Append` is rejected for single-file path.
3. Otherwise treats path as directory write target and supports both modes.

### `save_as_table(name)` / `save_as_table_with_mode(name, mode)`

Behavior:
1. Executes DataFrame to parquet parts under managed table path.
2. Updates in-memory catalog entry:
- `Overwrite`: replace `paths`.
- `Append`: extend and deduplicate `paths`.
3. Persists catalog via `Session::persist_catalog()`.

Constraints:
1. Table name must be non-empty.
2. Catalog persistence uses configured `FFQ_CATALOG_PATH` file.

## Write Modes

`WriteMode` (`crates/client/src/dataframe.rs`):
1. `Overwrite`
2. `Append`

Mode semantics:
1. `Overwrite`
- Uses staged output and atomic replacement.
- Final layout for directory overwrite is deterministic (`part-00000.parquet`).

2. `Append`
- Preserves existing files and adds next numbered part (`part-00001.parquet`, ...).
- Uses temporary staged file then rename into final part path.

## Temp-Then-Commit Semantics

### Single-file overwrite

Functions:
1. `write_single_parquet_file_durable`
2. `replace_file_atomically`

Behavior:
1. Write to sibling staged temp file (`.ffq_staged_*`).
2. Commit via rename to target.
3. If target exists, move target to backup, rename staged -> target.
4. On commit failure, restore backup target.

### Directory overwrite

Functions:
1. `write_parquet_parts_durable` (`Overwrite` branch)
2. `replace_dir_atomically`

Behavior:
1. Write staged directory with `part-00000.parquet`.
2. Commit by renaming staged dir into target dir.
3. If target exists, move target to backup then swap.
4. On commit failure, restore backup dir.

### Append commit

Function:
1. `write_parquet_parts_durable` (`Append` branch)

Behavior:
1. Compute next part index.
2. Write staged temp file for final part.
3. Rename staged temp file -> final `part-xxxxx.parquet`.
4. On failure, remove staged file.

## Failure Cleanup Semantics

Implemented behavior:
1. Staged file/dir cleanup is attempted on write/commit failure.
2. Backup rollback is attempted for overwrite swap failures.
3. `save_as_table` updates catalog **after** successful durable write, preventing failed writes from registering broken tables.

Observed by tests:
1. Failed `save_as_table` leaves no committed table data path and no queryable catalog entry.

## Idempotency and Retry Semantics

v1 semantics:
1. Overwrite retries are deterministic at file layout level:
- repeated overwrite keeps `part-00000.parquet` as final shape.
2. Append is not idempotent by design:
- each successful retry adds a new part file.
3. Catalog append path deduplicates exact path strings after merge.

Practical rule:
1. Use `Overwrite` for deterministic retry behavior.
2. Use `Append` when additive writes are intended.

## Success Flow Example

Scenario: `INSERT INTO dst SELECT a, b FROM src`

1. SQL parser builds `LogicalPlan::InsertInto`.
2. Analyzer validates target existence/schema compatibility.
3. Physical planner lowers to `ParquetWriteExec`.
4. Runtime executes source subtree -> batches.
5. Sink writes durable parquet output for `dst` path.
6. Query completes successfully (sink node returns empty result batches).

Reference test:
1. `crates/client/tests/embedded_parquet_sink.rs` (`insert_into_select_writes_parquet_sink`).

## Failure/Retry Flow Example

Scenario A (failure cleanup):
1. `save_as_table("blocked/table")` where parent path is blocked by a file.
2. Durable write fails during staging/commit.
3. Staged artifacts are cleaned up best-effort.
4. Catalog entry is not registered/persisted.
5. Subsequent query of `blocked/table` fails as expected.

Reference test:
1. `failed_save_as_table_leaves_no_catalog_entry_or_partial_data` in `crates/client/tests/dataframe_write_api.rs`.

Scenario B (retry determinism):
1. Run `save_as_table_with_mode(..., Overwrite)`.
2. Retry the same call.
3. Final output remains deterministic (single `part-00000.parquet` with expected rows).

Reference test:
1. `overwrite_retries_are_deterministic` in `crates/client/tests/dataframe_write_api.rs`.

## Additional Test References

1. `crates/planner/src/sql_frontend.rs` (`parses_insert_into_select`).
2. `crates/planner/src/analyzer.rs` (`analyze_insert_valid`, `analyze_insert_type_mismatch`).
3. `crates/client/tests/dataframe_write_api.rs` (API write, append/overwrite, restart persistence).
4. `crates/client/tests/embedded_parquet_sink.rs` (sink execution via SQL DML).
