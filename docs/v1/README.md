# FastFlowQuery v1 Documentation

This page is the canonical scope contract for FFQ v1. It defines what is included, what is intentionally out of scope, and where each v1 subsystem is documented.

## v1 Goals

1. Provide a stable engine surface for local analytics:
   - `Engine::new(config)`
   - `engine.sql(...)`
   - `DataFrame::collect()`
   - `engine.shutdown()`
2. Run end-to-end in embedded mode (SQL -> plan -> execution -> Arrow batches).
3. Support an optional distributed runtime (`distributed` feature) with coordinator/worker execution.
4. Support core storage/catalog behavior for portable table definitions.
5. Support robust joins/aggregations with minimal spill behavior.
6. Provide observability primitives (tracing + Prometheus metrics + profiling hooks).
7. Deliver vector/RAG v1 capabilities with safe fallback behavior.

## v1 Non-Goals

1. Full ANSI SQL coverage.
2. Cost-based optimizer and advanced adaptive planning.
3. Full transactional table formats or ACID lakehouse semantics.
4. Production-grade multi-tenant cluster management and autoscaling.
5. Complete connector ecosystem beyond v1 targets (parquet first; optional qdrant/object-store paths).
6. Full-blown global sort implementation as a hard requirement for vector retrieval (v1 uses top-k focused operators).

## Feature Flags (v1)

| Feature | Purpose | Default |
|---|---|---|
| `embedded` | Local in-process runtime path | on |
| `distributed` | Coordinator/worker runtime and shuffle stages | off |
| `vector` | Vector type/expressions and vector-aware operators | off |
| `qdrant` | Qdrant vector index provider integration | off |
| `s3` | Optional object store storage path | off |
| `profiling` | Profiling hooks + metrics exporter integration | off |

## What Is In v1

1. Planner pipeline: SQL frontend, analyzer, optimizer, physical planning.
2. Core physical operators: scan, filter, project, aggregate, join, limit, top-k.
3. Storage/catalog: table registration, table metadata, parquet-based reads/writes.
4. Distributed basics: stages, shuffle write/read, retries/cleanup, coordinator/worker loop.
5. Write path: insert/write operators with durable commit semantics for v1 table flows.
6. Observability: tracing fields, Prometheus metrics, profiling-enabled hooks.
7. Vector/RAG v1: cosine similarity path, top-k rerank, optional qdrant routing and fallback.

## Documentation Map

The pages below define the v1 implementation contract and operational behavior.

| Area | Document |
|---|---|
| Plan-to-code implementation status | `docs/v1/status-matrix.md` |
| Quick start (run in minutes) | `docs/v1/quickstart.md` |
| System architecture (as built) | `docs/v1/architecture.md` |
| Storage and catalog | `docs/v1/storage-catalog.md` |
| Core operators and execution semantics | `docs/v1/operators-core.md` |
| Shuffle and stage model | `docs/v1/shuffle-stage-model.md` |
| Distributed runtime (coordinator/worker) | `docs/v1/distributed-runtime.md` |
| Client runtime and result flow | `docs/v1/client-runtime.md` |
| Writes and DML semantics | `docs/v1/writes-dml.md` |
| Vector/RAG v1 | `docs/v1/vector-rag.md` |
| Observability and profiling | `docs/v1/observability.md` |
| Testing and validation playbook | `docs/v1/testing.md` |
| Integration runbook (13.2) | `docs/v1/integration-13.2.md` |
| Benchmark contract (13.3) | `docs/v1/benchmarks.md` |
| Known gaps and next steps | `docs/v1/known-gaps.md` |

## Scope Governance

1. Any behavior marked out of scope on this page is not a v1 correctness requirement.
2. If implementation and docs diverge, update this page first, then the subsystem page.
3. New v1-facing features must add or update a linked page in the documentation map.
