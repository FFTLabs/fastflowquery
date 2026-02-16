# Vector/RAG Internals

This chapter explains how FFQ executes vector retrieval in v1, from expression kernels to optimizer routing and safe fallback behavior.

## 1) Vector scoring kernels

Vector expressions are evaluated against Arrow `FixedSizeList<Float32>` embedding columns and a query literal `VectorF32`.

Implemented kernels:

1. `cosine_similarity(vector_col, query_vec)`
2. `l2_distance(vector_col, query_vec)`
3. `dot_product(vector_col, query_vec)`

Key properties:

1. Analyzer enforces vector column/type compatibility before runtime.
2. Runtime validates dimensionality (`query vector dim X != column dim Y` errors clearly).
3. Null vector rows propagate null scores.
4. Numeric tests cover positive/negative values, zero vectors, nulls, and shape mismatch.

Relevant code:

1. `crates/planner/src/analyzer.rs`
2. `crates/execution/src/expressions/mod.rs`

## 2) Brute-force ranking: `TopKByScore`

The default SQL vector ranking path is brute-force `TopKByScore`.

Plan shape from SQL frontend:

1. `ORDER BY cosine_similarity(...) DESC LIMIT k`
2. lowered to logical `TopKByScore { input, score_expr, k }`

Execution behavior:

1. Evaluate score expression per batch/row.
2. Keep a min-heap of size `k` with best rows seen so far.
3. Ignore null scores.
4. Output selected rows as one compact result batch.
5. `k=0` returns an empty batch with input schema.

Determinism detail:

1. Tie-breaking includes a stable sequence number so equal-score rows are selected consistently for a fixed input stream.

Relevant code:

1. `crates/planner/src/sql_frontend.rs`
2. `crates/client/src/runtime.rs`

## 3) Index-backed path: `VectorTopK`

When conditions are satisfied, optimizer rewrites brute-force top-k to `VectorTopK` for qdrant-backed tables.

`VectorTopK` execution contract:

1. Calls `VectorIndexProvider::topk(query_vec, k, filter)`.
2. Returns stable schema columns: `id`, `score`, `payload`.
3. Produces a batch directly from provider rows (no full table scan).

Relevant code:

1. `crates/storage/src/vector_index.rs`
2. `crates/storage/src/qdrant_provider.rs`
3. `crates/client/src/runtime.rs`

## 4) Qdrant rewrite preconditions

Rewrite only applies for the specific pattern:

1. `Projection -> TopKByScore -> TableScan`

And only if all checks pass:

1. Table format is `qdrant`.
2. `k > 0`.
3. Score expression is exactly `cosine_similarity(vector_column, vector_literal)`.
4. Vector argument is a column reference.
5. Query argument is a literal `VectorF32`.
6. Projection is satisfiable from `id`, `score`, `payload`.
7. Filters are translatable to qdrant subset (below).

If any check fails, optimizer keeps the original `TopKByScore` plan.

Relevant code:

1. `crates/planner/src/optimizer.rs`

## 5) Projection contract

`VectorTopK` does not expose arbitrary table columns. The optimizer only rewrites when projection uses:

1. `id`
2. `score`
3. `payload`

Examples:

1. `SELECT id, score, payload ...` -> rewrite eligible.
2. `SELECT title ...` -> not rewrite-eligible, falls back to brute-force.

This prevents semantic breakage from missing columns.

Relevant code:

1. `crates/planner/src/optimizer.rs`
2. `crates/planner/src/explain.rs`

## 6) Filter pushdown subset

For qdrant rewrite, FFQ translates only a strict subset of SQL predicates:

1. Equality on scalar payload field: `col = literal`
2. `AND` combinations of supported equality predicates
3. Literal types: `Int64`, `Utf8`, `Boolean`

Unsupported predicates (for example `>`, `<`, `OR`, function predicates) do not fail the query; they disable rewrite and keep brute-force path.

Relevant code:

1. `crates/planner/src/optimizer.rs`

## 7) Safe fallback semantics

Fallback is intentional and central:

1. Rewrite logic returns `Apply` or `Fallback` decision.
2. Fallback preserves existing `TopKByScore` plan.
3. Query remains executable even when index rewrite is not possible.
4. Explain output marks chosen path:
   - `rewrite=index_applied` for `VectorTopK`
   - `rewrite=index_fallback` for `TopKByScore`

This gives reliability first, with opportunistic acceleration when preconditions hold.

Relevant code:

1. `crates/planner/src/optimizer.rs`
2. `crates/planner/src/explain.rs`

## 8) Practical debugging checklist

When you expect index routing but see brute-force:

1. Check table format is `qdrant` in catalog.
2. Ensure `ORDER BY` expression is exactly cosine with vector literal query argument.
3. Ensure projection only uses `id/score/payload`.
4. Simplify filters to `col = literal` with `AND`.
5. Run explain and verify rewrite marker.

If any item fails, fallback is expected behavior in v1.

## Runnable command

```bash
cargo test -p ffq-client --test embedded_vector_topk --features vector
```
