# Core SQL Execution Operators (v1)

This page describes the implemented core execution operators in v1 and their behavior contracts.

Primary execution implementations:
1. Embedded: `crates/client/src/runtime.rs`
2. Distributed worker task execution: `crates/distributed/src/worker.rs`

Planner/physical mapping:
1. Logical -> physical lowering: `crates/planner/src/physical_planner.rs`
2. Physical node definitions: `crates/planner/src/physical_plan.rs`

## Operator Catalog

Covered operators:
1. Scan (`ParquetScan`)
2. Filter (`Filter`)
3. Project (`Project`)
4. Aggregate (`PartialHashAggregate`, `FinalHashAggregate`)
5. Join (`HashJoin`)
6. Limit (`Limit`)
7. Top-k (`TopKByScore`)

## 1) Scan (`ParquetScan`)

Inputs:
1. `TableDef` from catalog.
2. Optional projection column list from plan.
3. Filter expressions (serialized as debug strings in v1 scan call path).

Outputs:
1. Stream of Arrow `RecordBatch` with table schema.

Constraints:
1. Table format must be `parquet` for `ParquetProvider`.
2. Table must provide data location via `paths` or `uri`.
3. Runtime currently uses local parquet file read path.

Failure modes:
1. Unknown table -> planning/runtime error.
2. Missing `uri` and `paths` -> invalid config.
3. Non-parquet table passed to parquet provider -> unsupported.
4. File/reader decode failures -> execution error.

## 2) Filter (`Filter`)

Inputs:
1. Child `RecordBatch` stream.
2. Predicate expression compiled against child schema.

Outputs:
1. Filtered `RecordBatch` stream preserving child schema.

Constraints:
1. Predicate must evaluate to Arrow boolean array.

Failure modes:
1. Predicate evaluates to non-boolean -> execution error (`filter predicate must evaluate to boolean`).
2. Expression compilation/evaluation failure -> execution error.
3. Arrow batch filter kernel failure -> execution error.

## 3) Project (`Project`)

Inputs:
1. Child `RecordBatch` stream.
2. Projection expression list `(Expr, output_name)`.

Outputs:
1. New `RecordBatch` stream with projected schema and projected arrays.

Constraints:
1. Each expression must compile against child schema.
2. Output schema is fully derived from projected expressions.

Failure modes:
1. Expression compilation/evaluation failure -> execution error.
2. RecordBatch construction mismatch -> execution error (`project build batch failed`).

## 4) Aggregate (`PartialHashAggregate` and `FinalHashAggregate`)

Inputs:
1. Child `RecordBatch` stream.
2. `group_exprs`.
3. Aggregate expressions (`COUNT`, `SUM`, `MIN`, `MAX`, `AVG`).
4. Aggregate mode: `Partial` or `Final`.

Outputs:
1. Aggregated `RecordBatch`.
2. Deterministic key ordering in output (keys sorted during output build).

Constraints:
1. Physical planner requires grouping keys to be plain columns (`Expr::Column`/`Expr::ColumnRef`).
2. Final aggregation expects partial-shape input from upstream stage.
3. For `AVG`, partial/final path relies on hidden count propagation semantics.

Failure modes:
1. Unsupported grouping expression shape in physical planning -> unsupported.
2. Unknown group column -> execution error.
3. Spill merge state shape/type mismatch -> execution error.
4. Batch/array conversion failures during output materialization -> execution error.

### Partial/Final semantics

1. Partial phase:
- Builds per-task hash map keyed by group values.
- Computes intermediate aggregate states.

2. Final phase:
- Reads grouped/intermediate values (typically after exchange/shuffle boundary).
- Merges intermediate states into final values.

## 5) Join (`HashJoin`)

Inputs:
1. Left and right child `RecordBatch` streams.
2. Join key pairs `on: Vec<(left_col, right_col)>`.
3. Build side hint (`Left` or `Right`).

Outputs:
1. Joined `RecordBatch` with schema = left fields + right fields.

Constraints:
1. v1 physical planner supports `INNER` join only.
2. Join condition must be equi-join columns.
3. Join key columns must resolve in child schemas.

Failure modes:
1. Unsupported join type at planning -> unsupported.
2. Join key missing in schema -> execution error (`join key '...' not found in schema`).
3. Row->scalar or scalar->array conversion failures -> execution error.
4. Spill read/write/serde errors in grace join path -> execution error.

## 6) Limit (`Limit`)

Inputs:
1. Child `RecordBatch` stream.
2. Limit `n`.

Outputs:
1. Prefix of rows up to `n`.
2. Output schema equals child schema.

Constraints:
1. Applies row slicing in stream order.

Failure modes:
1. Child execution failure propagates.
2. No special operator-specific failure expected beyond upstream errors.

## 7) Top-k (`TopKByScore`)

Inputs:
1. Child `RecordBatch` stream.
2. Score expression.
3. `k` value.

Outputs:
1. Top-k rows by score (descending), materialized as one concatenated output batch.
2. If `k == 0` or no non-null scores, returns empty batch with child schema.

Constraints:
1. Score expression must evaluate to `Float32` or `Float64`.
2. Uses min-heap top-k selection (does not require global sort operator).

Failure modes:
1. Score expression evaluates to unsupported type -> execution error.
2. Expression evaluation failure -> execution error.
3. Final concat batch failure -> execution error (`top-k concat failed`).

## Spill Semantics (v1)

Spill is minimal and operator-local; triggered by memory budget thresholds.

### Aggregate spill

Where:
1. `maybe_spill(...)` in embedded and worker runtimes.

Behavior:
1. If estimated group-state bytes exceed `mem_budget_bytes`, current hash map state is spilled to JSONL in spill directory.
2. Runtime later merges spill files and in-memory state.
3. Spill files are best-effort cleaned up after merge.

Failure modes:
1. Spill directory/file create/write failures.
2. Spill JSON serialize/deserialize failures.
3. Spill state merge shape/type mismatches.

### Join spill (grace-style)

Where:
1. `grace_hash_join(...)` in embedded and worker runtimes.

Behavior:
1. If estimated build-side bytes exceed budget, both sides are partitioned to spill files.
2. Runtime joins corresponding partitions one by one.
3. Spill files are removed after partition processing.

Failure modes:
1. Spill file I/O failures.
2. Spill row encode/decode failures.
3. Partition processing errors while rebuilding hash tables.

## Cross-Cutting Notes

1. Operator metrics:
- rows/batches/bytes/time are recorded per operator in `crates/common/src/metrics.rs`.

2. Tracing:
- runtime spans include `query_id`, `stage_id`, `task_id`, and `operator` labels.

3. Unsupported nodes:
- If runtime receives an unimplemented physical node, it fails with explicit `Unsupported` error.

## Related References

1. `crates/planner/src/physical_plan.rs`
2. `crates/planner/src/physical_planner.rs`
3. `crates/client/src/runtime.rs`
4. `crates/distributed/src/worker.rs`
5. `crates/storage/src/parquet_provider.rs`
6. `crates/common/src/metrics.rs`
