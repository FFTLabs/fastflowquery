# Vector/RAG v2 (Bootstrap)

- Status: draft
- Owner: @ffq-docs
- Last Verified Commit: TBD
- Last Verified Date: TBD
- Source: inherited/adapted from prior version docs; v2 verification pending


This document describes the bootstrapped v2 vector retrieval path as currently implemented, including brute-force rerank, qdrant-backed index routing, fallback semantics, and the two-phase retrieval pattern.

## EPIC 9 status (implemented subset)

This page documents the currently implemented subset of EPIC 9:

1. hybrid logical node and physical vector KNN execution path (`HybridVectorScan` -> `VectorKnnExec`)
2. connector-aware prefilter pushdown subset (qdrant-focused)
3. `metric` / `ef_search` vector KNN knobs
4. batched hybrid query API (`Engine::hybrid_search_batch`)
5. pluggable embedding provider trait with sample and optional HTTP provider

It does not yet claim a fully generalized multi-provider hybrid engine.

## Feature Flags

| Flag | Meaning |
|---|---|
| `vector` | Enables vector literal/type handling, vector expressions, top-k by score planning, and vector-aware optimizer rewrites. |
| `qdrant` | Enables `QdrantProvider` execution for `VectorTopKExec` against qdrant tables. |

## Vector Type and Expression

1. Embedding column type: Arrow `FixedSizeList<Float32>`.
2. Query vector literal type: `LiteralValue::VectorF32(Vec<f32>)`.
3. Scoring expression used by SQL top-k rewrite path: `cosine_similarity(vector_col, query_vector_literal)` returns float score.

## SQL shape supported for top-k scoring

v1 supports top-k vector ranking through:

```sql
SELECT ...
FROM ...
ORDER BY cosine_similarity(emb, :q) DESC
LIMIT k
```

Guardrails:

1. Exactly one `ORDER BY` expression.
2. `DESC` only.
3. `LIMIT` is required.
4. No aggregate + vector order-by in same query shape.
5. Global full sort is not implemented; this pattern lowers to top-k operators.

## Brute-force path: `TopKByScore`

`TopKByScoreExec { input, score_expr, k }` is the default vector ranking path.

Behavior:

1. Evaluates `score_expr` batch-by-batch.
2. Maintains a min-heap of top-k rows.
3. Accepts `Float32` or `Float64` score arrays.
4. Emits a compact result batch containing selected rows in descending score order.
5. Tie order is deterministic in v1 test coverage through stable normalization/snapshot checks.

Metric coverage note:
1. SQL rewrite routing is currently cosine-based.
2. L2 and dot ranking correctness is validated at operator/runtime test layer (not SQL rewrite matching).

Failure/edge behavior:

1. `k = 0` returns empty batch with input schema.
2. Non-float score array fails execution.
3. Null score rows are skipped.

## Index-backed path: `VectorTopKExec`

`VectorTopKExec { table, query_vector, k, filter }` returns index results without scanning/parsing full table data.

Execution contract:

1. Table format must be `qdrant` (or mock vector rows via `vector.mock_rows_json` in tests/dev fixtures).
2. Provider call: `VectorIndexProvider::topk(query_vec, k, filter)`.
3. Output schema is stable and fixed: `id:Int64`, `score:Float32`, `payload:Utf8?`.

If `qdrant` feature is disabled and runtime tries to execute a qdrant index operator, execution returns an unsupported-feature error.

## Hybrid node + score column (`9.1`, partial)

In the newer v2 hybrid path, planner/runtime also support:

1. logical node: `HybridVectorScan`
2. physical node: `VectorKnnExec`

`HybridVectorScan` carries:

1. `source`
2. `query_vectors`
3. `k`
4. `ef_search`
5. `prefilter`
6. `metric`
7. `provider`

Explain output includes hybrid/vector node details (query count/dim, metric, provider, prefilter).

Score-column contract:

1. qdrant/index-backed vector results expose a score column in output (`score`)
2. optimizer rewrite snapshots also validate projected score semantics and explain visibility
3. SQL-facing `_score` naming conventions are partially documented through explain/optimizer snapshots and vector execution schemas, but end-user naming contracts are still evolving by path (brute-force vs index-backed)

## Qdrant connector (v1)

`QdrantProvider` uses table options:

1. `qdrant.endpoint` (default: `http://127.0.0.1:6334`)
2. `qdrant.collection` (fallback: table uri/name)
3. `qdrant.with_payload` (`true`/`1` to include payload)

v1 filter payload accepted by provider is JSON:

```json
{
  "must": [
    { "field": "tenant_id", "value": 42 },
    { "field": "lang", "value": "en" }
  ]
}
```

## Rewrite Preconditions and Fallback

The optimizer attempts `Projection -> TopKByScore -> TableScan` rewrite to `VectorTopK` only when all checks pass.

Explain markers:

1. `rewrite=index_applied` for `VectorTopK`.
2. `rewrite=index_fallback` for `TopKByScore`.

### Rewrite vs fallback table

| Condition | Rewrite to `VectorTopK` | Fallback to `TopKByScore` |
|---|---|---|
| Projection uses only `id`, `score`, `payload` | yes | no |
| Projection needs other columns (example: `title`) | no | yes |
| Input shape is `TopKByScore` over `TableScan` | yes | no |
| Score expr is `cosine_similarity(column, vector_literal)` | yes | no |
| Query vector is not literal `VectorF32` | no | yes |
| `k > 0` | yes | no |
| Table format is `qdrant` | yes | no |
| Table format is not `qdrant` | no | yes |
| Filter translation supports all predicates (`col = literal` and `AND`) | yes | no |
| Any unsupported filter predicate (example: `col > 1`) | no | yes |

Fallback is safe by design: unsupported shapes do not hard-fail planning; the existing brute-force execution plan remains valid.

## Filter pushdown subset (qdrant rewrite path)

When rewrite candidates include table-scan filters, v1 translates only:

1. equality predicate: `column = literal`
2. conjunction: `expr1 AND expr2 ...`
3. literal types: `Int64`, `Utf8`, `Boolean`

Anything else (range, OR, functions, non-literal comparison) causes rewrite fallback.

This is the implemented `9.2` subset today: connector-aware in practice for qdrant, but not yet a generalized capability-negotiation contract across multiple vector providers.

## `VectorKnnExec` knobs (`9.3`, partial)

`VectorKnnExec` exposes tuning knobs and filter payload in physical planning/runtime:

1. `k`
2. `metric` (`cosine`, `dot`, `l2`)
3. `ef_search` (optional provider-specific HNSW search override)
4. `prefilter` (optional provider payload filter)

Sources of knob values:

1. optimizer rewrite from table options (for example `vector.metric`, `vector.ef_search`)
2. per-query overrides through `VectorKnnOverrides` in DataFrame APIs
3. direct hybrid logical plan construction (`Engine::hybrid_search_batch`)

Validation:

1. metric values are validated against supported set
2. `ef_search` must be `> 0` when provided

## Two-phase retrieval pattern

v1 also supports a two-phase retrieval rewrite for doc tables configured with vector index metadata:

1. External top-k: `VectorTopK(index_table)` returns `(id, score, payload?)`.
2. Join: docs table join on id.
3. Metadata filtering: doc predicates applied.
4. Exact rerank: `TopKByScore` over joined docs with exact `cosine_similarity`.

Required table options on docs table:

1. `vector.index_table` (qdrant table name)
2. `vector.id_column` (default `id`)
3. `vector.embedding_column` (optional validation)
4. `vector.prefetch_multiplier` (default `4`)
5. `vector.prefetch_cap` (optional hard cap)

This keeps exact ranking quality while reducing candidate set size.

## Batched query mode (`9.4`, partial)

`Engine::hybrid_search_batch(...)` provides a batched vector query API.

Behavior:

1. accepts `query_vecs: Vec<Vec<f32>>`
2. validates non-empty batch and non-empty vectors
3. builds `LogicalPlan::HybridVectorScan` directly (bypasses SQL parsing)
4. preserves `k`, `metric`, `provider`, and optional future runtime tuning hooks through the logical node

Current note:

1. API shape is implemented and contract-tested
2. dedicated throughput/recall benchmark characterization for batched mode is still limited

## Stable embedding API / provider plugin (`9.5`, partial)

`ffq-client` exposes a pluggable embedding contract:

1. `EmbeddingProvider::embed(&[String]) -> Result<Vec<Vec<f32>>>`

Built-in implementations:

1. `SampleEmbeddingProvider` (deterministic test/example provider)
2. `HttpEmbeddingProvider` (feature `embedding-http`) for remote HTTP embedding services

Engine integration:

1. `Engine::embed_texts(&provider, texts)` delegates to the provider without coupling core engine logic to a model vendor

Current limits:

1. embedding result caching is not implemented yet
2. provider registry/discovery is not a generalized plugin runtime; users pass provider instances directly

## Quick examples

Rewrite-eligible query:

```sql
SELECT id, score, payload
FROM docs_idx
ORDER BY cosine_similarity(emb, :q) DESC
LIMIT 10
```

For qdrant table format and supported filters/projection, plan uses `VectorTopK`.

Fallback query:

```sql
SELECT title
FROM docs_idx
ORDER BY cosine_similarity(emb, :q) DESC
LIMIT 10
```

Because `title` is not in the `VectorTopK` output contract, plan stays on `TopKByScore`.

Two-phase retrieval query shape:

```sql
SELECT id, title
FROM docs
WHERE lang = 'en'
ORDER BY cosine_similarity(emb, :q) DESC
LIMIT 5
```

With docs table vector options configured and qdrant index table registered, optimizer can build:
`VectorTopK -> Join -> Filter -> TopKByScore`.

## Validation references

1. Rewrite/fallback behavior and explain markers:
   - `crates/planner/src/optimizer.rs`
   - `crates/planner/src/explain.rs`
   - `crates/client/tests/qdrant_routing.rs`
2. Brute-force top-k path:
   - `crates/client/src/runtime.rs`
   - `crates/client/tests/embedded_vector_topk.rs`
   - includes cosine query-level ranking plus L2/dot operator-level ranking and tie handling checks
3. Two-phase retrieval rewrite and execution:
   - `crates/planner/src/optimizer.rs`
   - `crates/client/tests/embedded_two_phase_retrieval.rs`
4. Provider contract and qdrant implementation:
   - `crates/storage/src/vector_index.rs`
   - `crates/storage/src/qdrant_provider.rs`
5. Hybrid/batched query and embedding provider API:
   - `crates/client/src/engine.rs`
   - `crates/client/src/embedding.rs`
   - `crates/client/tests/public_api_contract.rs`
