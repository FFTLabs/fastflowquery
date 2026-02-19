# Runtime & Portability (v2, EPIC 1)

- Status: draft
- Owner: @ffq-runtime
- Last Verified Commit: TBD
- Last Verified Date: TBD

## Scope

This chapter documents EPIC 1 runtime/portability behavior in v2:

1. feature/build matrix
2. core-only and minimal build paths
3. distributed runtime hardening (liveness, requeue, retry/backoff, scheduler limits)
4. reproducible acceptance commands and expected outcomes

## Feature Matrix

Primary feature definitions live in:

1. `crates/client/Cargo.toml`
2. `crates/distributed/Cargo.toml`
3. workspace CI: `.github/workflows/feature-matrix.yml`

### Client features

| Feature | Meaning |
|---|---|
| `core` | default embedded runtime surface (`core -> embedded`) |
| `embedded` | legacy alias for embedded core |
| `minimal` | slim embedded preset (`minimal -> core`) |
| `distributed` | enables `ffq-distributed` + gRPC runtime path |
| `s3` | object-store storage path |
| `vector` | vector planner/execution paths |
| `qdrant` | qdrant integration on top of vector |
| `python` | `pyo3` Python bindings |
| `ffi` | C ABI surface |
| `profiling` | profiling-oriented instrumentation |

### Distributed features

| Feature | Meaning |
|---|---|
| `grpc` | coordinator/worker gRPC binaries/services |
| `vector` | vector paths in distributed execution |
| `qdrant` | qdrant-enabled vector provider path |
| `profiling` | profiling instrumentation |

## Build Profiles and Portability Checks

These commands are the canonical reproducible checks for EPIC 1.1.

### 1) Core-only build (no default features)

```bash
cargo build --no-default-features
```

Expected:

1. command succeeds
2. workspace builds without requiring distributed/python/s3

### 2) Minimal preset build

```bash
cargo build -p ffq-client --no-default-features --features minimal
```

Expected:

1. command succeeds
2. embedded core path compiles from minimal preset

### 3) Combined distributed + python + s3 build

```bash
cargo build --features distributed,python,s3
```

Expected:

1. command succeeds
2. distributed runtime, Python bindings, and S3-gated code paths all compile together

### 4) Full feature-matrix build (client)

```bash
cargo build -p ffq-client --no-default-features --features core,distributed,s3,vector,qdrant,python,ffi
```

Expected:

1. command succeeds
2. no feature-conflict compile breakage for the v2 matrix

### 5) FFI smoke in matrix

```bash
make ffi-example
```

Expected:

1. C example compiles and runs
2. IPC result fetch path is usable from C

## Distributed Runtime Hardening (EPIC 1.2)

Implementation focus:

1. worker liveness via heartbeat tracking
2. stale-worker task requeue with incremented attempts
3. retry/backoff and blacklist thresholds
4. scheduler concurrency limits (per worker and per query)
5. capability-aware assignment for custom physical operators

Primary implementation:

1. `crates/distributed/src/coordinator.rs`
2. `crates/distributed/src/worker.rs`
3. `crates/distributed/src/grpc.rs`
4. `crates/distributed/proto/ffq_distributed.proto`

### Runtime behavior contract

1. If a worker stops heartbeating beyond timeout, running tasks are requeued.
2. Retries create a new `attempt` with backoff delay.
3. Workers over failure threshold are blacklisted from new assignments.
4. Coordinator enforces:
   - `max_concurrent_tasks_per_worker`
   - `max_concurrent_tasks_per_query`
5. Custom-operator tasks are assigned only to workers advertising required capabilities in heartbeat payload.

## Reproducible Hardening Checks

### Coordinator unit tests (distributed crate)

```bash
cargo test -p ffq-distributed --features grpc coordinator_requeues_tasks_from_stale_worker
cargo test -p ffq-distributed --features grpc coordinator_enforces_worker_and_query_concurrency_limits
cargo test -p ffq-distributed --features grpc coordinator_blacklists_failing_worker
cargo test -p ffq-distributed --features grpc coordinator_assigns_custom_operator_tasks_only_to_capable_workers
```

Expected:

1. stale-worker tasks are requeued to new attempts
2. concurrency caps are enforced
3. repeated failures trigger blacklist behavior
4. capability-incompatible workers receive no custom-operator tasks

### In-process distributed custom operator execution

```bash
cargo test -p ffq-distributed --features grpc coordinator_with_workers_executes_custom_operator_stage -- --nocapture
```

Expected:

1. custom physical operator executes on workers
2. query reaches succeeded state
3. output matches test assertions

## CI Reference

Feature/build matrix CI:

1. `.github/workflows/feature-matrix.yml`

SemVer/API gate (related to runtime portability stability):

1. `.github/workflows/api-semver.yml`

## EPIC 1 Acceptance Mapping

### 1.1 Acceptance

1. `cargo build --no-default-features` works.
2. `cargo build --features distributed,python,s3` works.
3. feature matrix workflow compiles full client matrix and runs FFI smoke.

Release artifact publishing remains tracked under deferred release EPIC (`Plan_v2.md` EPIC 11).

### 1.2 Acceptance (current status)

1. distributed liveness/requeue and scheduler limits are implemented and unit-tested.
2. capability-aware custom-op scheduling is implemented and tested.
3. full external “kill live worker during query and validate terminal behavior” scenario is partially covered in local/in-process tests; additional chaos-style external integration can extend this later.
