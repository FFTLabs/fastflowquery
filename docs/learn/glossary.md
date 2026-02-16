# Glossary

This glossary defines core FFQ terms used across the learner guide and links each term to deeper chapters.

## A

**Analyzer**
- Planner phase that resolves columns/types and validates plan correctness before execution.
- Deep dive: `docs/learn/01-query-lifecycle.md`, `docs/learn/02-optimizer-internals.md`

**Arrow RecordBatch**
- Columnar in-memory batch format returned by execution operators.
- Deep dive: `docs/learn/01-query-lifecycle.md`, `docs/learn/04-execution-engine.md`

## B

**Broadcast Join**
- Join strategy where one side is replicated to avoid full dual-side shuffle.
- Deep dive: `docs/learn/04-execution-engine.md`

## C

**Catalog**
- Table metadata registry (`TableDef`) with format/path/schema/options used for planning and execution.
- Deep dive: `docs/learn/09-storage-catalog.md`

**Coordinator**
- Distributed control-plane component that tracks query/task state and assigns tasks to workers.
- Deep dive: `docs/learn/06-control-plane.md`, `docs/learn/07-rpc-protocol.md`

## D

**Deterministic Normalization**
- Test/result normalization (sorting/tolerance/null handling) used to compare logically equivalent outputs.
- Deep dive: `docs/learn/08-correctness-distributed.md`

**Distributed Runtime**
- Mode where query execution is coordinated across worker processes via stage/task scheduling and shuffle.
- Deep dive: `docs/learn/05-distributed-stages-shuffle.md`, `docs/learn/06-control-plane.md`

## E

**Embedded Runtime**
- Single-process execution mode using the same physical operators without remote orchestration.
- Deep dive: `docs/learn/01-query-lifecycle.md`, `docs/learn/04-execution-engine.md`

**Exchange (ShuffleWrite/ShuffleRead)**
- Physical stage boundary where partitioned data is materialized and later consumed by downstream stages.
- Deep dive: `docs/learn/05-distributed-stages-shuffle.md`

## F

**Fallback (Vector Rewrite)**
- Safety behavior where planner keeps `TopKByScore` when index rewrite preconditions are not satisfied.
- Deep dive: `docs/learn/10-vector-rag-internals.md`

## G

**Grace Join / Grace Spill**
- Partitioned spill strategy when join build-side memory budget is exceeded.
- Deep dive: `docs/learn/04-execution-engine.md`

## H

**Hash Aggregate (Two-Phase)**
- Partial aggregate before exchange, final aggregate after exchange.
- Deep dive: `docs/learn/04-execution-engine.md`

**Hash Join**
- Join operator that builds hash table on build side and probes with other side; can spill when needed.
- Deep dive: `docs/learn/04-execution-engine.md`

## I

**Idempotency (Write Path)**
- Property that repeated execution yields same committed state (true for overwrite path in v1, not for append).
- Deep dive: `docs/learn/11-writes-commit.md`

**InsertInto**
- Logical plan node representing `INSERT INTO ... SELECT` write intent.
- Deep dive: `docs/learn/11-writes-commit.md`

## M

**Map Output Registry**
- Coordinator registry of shuffle outputs keyed by `(query_id, stage_id, map_task, attempt)`.
- Deep dive: `docs/learn/06-control-plane.md`, `docs/learn/07-rpc-protocol.md`

**Memory Budget**
- Per-query/per-task memory limit that triggers spill paths in heavy operators.
- Deep dive: `docs/learn/04-execution-engine.md`

## O

**Optimizer Rule**
- Rule-based transformation preserving semantics while improving runtime behavior.
- Deep dive: `docs/learn/02-optimizer-internals.md`

## P

**ParquetWriteExec**
- Physical sink operator that writes parquet output for DML/write API paths.
- Deep dive: `docs/learn/11-writes-commit.md`

**Physical Plan**
- Executable operator graph produced from logical plan.
- Deep dive: `docs/learn/03-physical-planning.md`

**Projection Contract (VectorTopK)**
- `VectorTopK` rewrite is valid only when requested columns are satisfiable from `id`, `score`, `payload`.
- Deep dive: `docs/learn/10-vector-rag-internals.md`

## Q

**Query ID / Stage ID / Task ID**
- Core execution identifiers for tracing, metrics labels, and control-plane correlation.
- Deep dive: `docs/learn/06-control-plane.md`, `docs/learn/12-observability-debugging.md`

## R

**Rewrite (Vector Index Rewrite)**
- Planner optimization from brute-force `TopKByScore` to `VectorTopK` under strict preconditions.
- Deep dive: `docs/learn/10-vector-rag-internals.md`

## S

**Shuffle**
- Data redistribution across partitions/stages, persisted as Arrow IPC partition files.
- Deep dive: `docs/learn/05-distributed-stages-shuffle.md`

**Spill**
- Temporary disk materialization used when memory budget is exceeded.
- Deep dive: `docs/learn/04-execution-engine.md`, `docs/learn/12-observability-debugging.md`

**Stage DAG**
- Directed acyclic graph of executable stages separated by exchange boundaries.
- Deep dive: `docs/learn/05-distributed-stages-shuffle.md`, `docs/learn/06-control-plane.md`

## T

**TableDef**
- Catalog structure describing table name/format/paths/schema/options.
- Deep dive: `docs/learn/09-storage-catalog.md`

**TopKByScore**
- Brute-force top-k operator used for vector ranking and fallback path.
- Deep dive: `docs/learn/10-vector-rag-internals.md`

**Temp-then-Commit**
- Write durability pattern: stage output to temp, then atomically commit/rename.
- Deep dive: `docs/learn/11-writes-commit.md`

## V

**VectorTopK**
- Index-backed top-k operator returning `id`, `score`, `payload` from vector index provider.
- Deep dive: `docs/learn/10-vector-rag-internals.md`

**VectorIndexProvider**
- Provider interface for external vector top-k backends (for example qdrant).
- Deep dive: `docs/learn/10-vector-rag-internals.md`

## W

**Worker**
- Distributed executor process that pulls tasks, runs operators, and reports status.
- Deep dive: `docs/learn/06-control-plane.md`, `docs/learn/07-rpc-protocol.md`
