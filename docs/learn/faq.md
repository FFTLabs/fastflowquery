# FAQ

This FAQ focuses on common learner and contributor failures, with direct pointers to deeper chapters.

## Query planning and SQL

### Why does `ORDER BY ...` fail for many SQL shapes?
FFQ v1 only supports global ordering in the vector top-k pattern (`ORDER BY cosine_similarity(...) DESC LIMIT k`).

Read more:
1. `docs/learn/10-vector-rag-internals.md`
2. `docs/learn/02-optimizer-internals.md`

### Why does `INSERT INTO ... SELECT ...` return empty result batches?
Sink plans are write-oriented. Success is in committed output files/table state, not returned rows from sink node.

Read more:
1. `docs/learn/11-writes-commit.md`

## Storage and catalog

### Why do I get table-not-found or schema mismatch errors?
Usually catalog metadata is missing/stale (wrong path/format/schema/options).

Read more:
1. `docs/learn/09-storage-catalog.md`
2. `docs/learn/01-query-lifecycle.md`

### Why does distributed query fail with join key missing in schema?
Worker-side catalog entries require explicit schema for reliable planning/execution on remote side.

Read more:
1. `docs/learn/09-storage-catalog.md`
2. `docs/learn/labs/02-distributed-run.md`

## Embedded vs distributed behavior

### Why can distributed row order differ from embedded?
Physical execution can differ in partition/task ordering, but logical result sets should match after normalization.

Read more:
1. `docs/learn/08-correctness-distributed.md`

### Why does distributed execution require numeric query IDs in some paths?
Current v1 shuffle layout includes assumptions tied to numeric query ID usage.

Read more:
1. `docs/learn/05-distributed-stages-shuffle.md`
2. `docs/learn/06-control-plane.md`

## Shuffle, stage, and retries

### Why do I see stale/old shuffle attempt behavior?
Attempt IDs are explicit; latest-attempt readers ignore stale attempts and TTL cleanup removes old attempt dirs.

Read more:
1. `docs/learn/05-distributed-stages-shuffle.md`

### Why is a stage stuck waiting?
Likely causes: parent stage not marked succeeded, scheduler backpressure, worker capacity/health issues.

Debug with:
1. scheduler metrics (`queued` vs `running`)
2. coordinator/worker trace correlation by `query_id/stage_id/task_id`

Read more:
1. `docs/learn/06-control-plane.md`
2. `docs/learn/12-observability-debugging.md`

## Vector/RAG routing

### Why didn’t my vector query rewrite to `VectorTopK`?
Common reasons:
1. table format is not `qdrant`
2. projection requests columns beyond `id/score/payload`
3. score expression not exact cosine pattern
4. filters outside supported subset (`col = literal` with `AND` only)

Read more:
1. `docs/learn/10-vector-rag-internals.md`

### Why does fallback still run successfully?
Fallback is intentional safety: planner preserves brute-force `TopKByScore` when rewrite preconditions fail.

Read more:
1. `docs/learn/10-vector-rag-internals.md`

## Writes and commit semantics

### Why is append not idempotent?
Append intentionally adds new part files each successful run. Overwrite is the retry-safe deterministic mode.

Read more:
1. `docs/learn/11-writes-commit.md`

### Why did a failed write leave no visible table?
Catalog update occurs after durable write success; failed writes should not register partial table state.

Read more:
1. `docs/learn/11-writes-commit.md`

## Observability and debugging

### Where should I start when a query is slow?
Use signal order:
1. traces for location
2. metrics for pressure
3. profiling for CPU hotspots

Read more:
1. `docs/learn/12-observability-debugging.md`

### Why are Prometheus series counts high?
Metric labels include query/stage/task IDs, which increases cardinality in long-running environments.

Read more:
1. `docs/learn/12-observability-debugging.md`

## Benchmarks and correctness gates

### Why does official benchmark fail before execution starts?
Official mode requires generated parquet fixtures (`tpch_dbgen_sf1_parquet`) and valid manifests.

Read more:
1. `docs/learn/labs/04-official-benchmark-correctness-gate.md`
2. `docs/v1/benchmarks.md`

### Why does benchmark run fail with correctness divergence?
The official benchmark path includes correctness checks for Q1/Q3; divergence is treated as hard failure.

Read more:
1. `docs/learn/labs/04-official-benchmark-correctness-gate.md`
2. `docs/v1/benchmarks.md`
