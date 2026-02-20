# FastFlowQuery v2 Documentation

This page is the canonical scope contract for FFQ v2.
It defines what is in v2, what is out of scope, and where each v2 topic is documented.

## v2 Goals

1. Provide a stable library-first engine API with explicit SemVer/deprecation policy.
2. Keep embedded execution as the default runtime path.
3. Harden distributed runtime behavior (liveness, requeue, retry/backoff, scheduler limits).
4. Support capability-aware custom operator execution in distributed mode.
5. Provide stable extension points:
   - optimizer rule registry
   - scalar UDF registration
   - physical operator registry
6. Provide user-facing FFI and Python bindings for core query flows.
7. Keep observability and benchmark workflows reproducible across local and CI runs.

## v2 Non-Goals

1. Full plugin ecosystem with dynamic runtime loading in this phase.
2. Full CBO/adaptive query optimization.
3. Full SQL dialect completeness beyond current planner/runtime scope.
4. Production cluster orchestration features (autoscaling, tenancy isolation, etc.).
5. Replacing all historical v1 docs immediately (v1 remains archived reference only).

## Feature Flags (v2)

| Feature | Purpose | Default |
|---|---|---|
| `core` | Embedded runtime and core SQL path | on |
| `embedded` | Legacy alias for core embedded path | on |
| `minimal` | Embedded + parquet-focused slim preset | off |
| `distributed` | Coordinator/worker runtime and gRPC flow | off |
| `s3` | Object-store storage support | off |
| `vector` | Vector types/kernels and vector-aware planning | off |
| `qdrant` | Qdrant-backed vector provider integration | off |
| `python` | `pyo3` bindings | off |
| `ffi` | Stable C ABI surface | off |
| `profiling` | Profiling-oriented instrumentation | off |

## No v1 Dependency Rule

1. `docs/v2/*` is the standalone documentation source for v2 users and contributors.
2. v2 pages must not require readers to open `docs/v1/*` to understand or run v2 behavior.
3. Cross-links to `docs/v1/*` are allowed only as historical context, never as required steps.

## Metadata Convention (All `docs/v2/*`)

Each v2 page must start with:

1. `Status: draft|verified`
2. `Owner: <team-or-handle>`
3. `Last Verified Commit: <sha|TBD>`
4. `Last Verified Date: YYYY-MM-DD|TBD`

Interpretation:

1. `draft` means structure exists but content is not yet complete/fully audited.
2. `verified` means content was reviewed against current implementation and tests.

## Required Page Matrix (v2)

The matrix below is the complete required v2 doc set. Ownership can be updated as teams split by area.

| Category | Page | Owner | Status |
|---|---|---|---|
| Core | `docs/v2/README.md` | `@ffq-docs` | verified |
| Core | `docs/v2/status-matrix.md` | `@ffq-docs` | draft |
| Core | `docs/v2/architecture.md` | `@ffq-docs` | draft |
| Core | `docs/v2/quickstart.md` | `@ffq-docs` | draft |
| Core | `docs/v2/repl.md` | `@ffq-docs` | draft |
| Core | `docs/v2/testing.md` | `@ffq-docs` | draft |
| Core | `docs/v2/integration-13.2.md` | `@ffq-docs` | draft |
| Core | `docs/v2/benchmarks.md` | `@ffq-docs` | draft |
| Core | `docs/v2/known-gaps.md` | `@ffq-docs` | draft |
| Runtime | `docs/v2/runtime-portability.md` | `@ffq-runtime` | draft |
| Runtime | `docs/v2/distributed-runtime.md` | `@ffq-runtime` | draft |
| Runtime | `docs/v2/control-plane.md` | `@ffq-runtime` | draft |
| Runtime | `docs/v2/adaptive-shuffle-tuning.md` | `@ffq-runtime` | draft |
| Runtime | `docs/v2/distributed-capabilities.md` | `@ffq-runtime` | draft |
| Runtime | `docs/v2/custom-operators-deployment.md` | `@ffq-runtime` | draft |
| Runtime | `docs/v2/shuffle-stage-model.md` | `@ffq-runtime` | draft |
| Runtime | `docs/v2/operators-core.md` | `@ffq-runtime` | draft |
| Runtime | `docs/v2/observability.md` | `@ffq-runtime` | draft |
| API | `docs/v2/api-contract.md` | `@ffq-api` | draft |
| API | `docs/v2/extensibility.md` | `@ffq-api` | draft |
| API | `docs/v2/ffi-python.md` | `@ffq-api` | draft |
| API | `docs/v2/storage-catalog.md` | `@ffq-storage` | draft |
| API | `docs/v2/client-runtime.md` | `@ffq-api` | draft |
| API | `docs/v2/sql-semantics.md` | `@ffq-planner` | verified |
| API | `docs/v2/writes-dml.md` | `@ffq-storage` | draft |
| API | `docs/v2/vector-rag.md` | `@ffq-vector` | draft |
| Ops | `docs/v2/migration-v1-to-v2.md` | `@ffq-docs` | draft |

## Learner Track

For concept-first architecture and runtime learning:

1. `docs/learn/README.md`

## Scope Governance

1. If implementation and docs diverge, update `docs/v2/*` first.
2. Every v2 behavior change must update at least one v2 page in the map above.
3. `docs/v1/*` remains archived for v1 readers and should not be treated as the v2 contract.
