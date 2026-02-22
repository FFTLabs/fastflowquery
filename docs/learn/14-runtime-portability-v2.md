# LEARN-14: Runtime & Portability (EPIC 1)

This chapter explains EPIC 1 from `tickets/eng/Plan_v2.md` in learner terms:

1. how FFQ feature flags map to runtime capabilities
2. what core-only/minimal builds guarantee
3. how distributed runtime liveness/requeue/scheduler limits behave

Primary v2 reference:

1. `docs/v2/runtime-portability.md`

## 1) Feature Matrix Mental Model

Main client feature surface (see `crates/client/Cargo.toml`):

1. `core` (embedded runtime baseline)
2. `minimal` (slim embedded preset)
3. `distributed`
4. `s3`
5. `vector`
6. `qdrant`
7. `python`
8. `ffi`

Why this matters:

1. you can compile only what you need
2. distributed/runtime integrations remain optional
3. CI can verify compatibility combinations

## 2) EPIC 1.1 Build Acceptance (Reproducible)

### Core-only

```bash
cargo build --no-default-features
```

Expected:

1. build succeeds without distributed/python/s3 requirements

### Minimal preset

```bash
cargo build -p ffq-client --no-default-features --features minimal
```

Expected:

1. embedded core path builds via minimal preset

### Combined feature path

```bash
cargo build --features distributed,python,s3
```

Expected:

1. distributed + python + s3 compile in one configuration

### Full matrix slice

```bash
cargo build -p ffq-client --no-default-features --features core,distributed,s3,vector,qdrant,python,ffi
```

Expected:

1. no feature-conflict compile breakage in this v2 matrix slice

## 3) EPIC 1.2 Distributed Runtime Hardening

Core behavior:

1. workers send heartbeat/liveness and capability metadata
2. stale workers are detected by timeout
3. running tasks from stale workers are requeued as new attempts
4. retry/backoff and blacklist policy bound repeated failures
5. scheduler enforces per-worker and per-query concurrency limits

Primary code:

1. `crates/distributed/src/coordinator.rs`
2. `crates/distributed/src/worker.rs`
3. `crates/distributed/src/grpc.rs`
4. `crates/distributed/proto/ffq_distributed.proto`

## 4) Hardening Checks (Reproducible)

```bash
cargo test -p ffq-distributed --features grpc coordinator_requeues_tasks_from_stale_worker
cargo test -p ffq-distributed --features grpc coordinator_enforces_worker_and_query_concurrency_limits
cargo test -p ffq-distributed --features grpc coordinator_blacklists_failing_worker
```

Expected:

1. stale-worker tasks are requeued
2. scheduler limits are enforced
3. failing workers can be blacklisted

## 5) Capability-Aware Assignment (Custom Operators)

Behavior:

1. worker heartbeat advertises `custom_operator_capabilities`
2. tasks with required custom op names are assigned only to capable workers

Verify with:

```bash
cargo test -p ffq-distributed --features grpc coordinator_assigns_custom_operator_tasks_only_to_capable_workers
```

## 6) What Is Still Deferred

EPIC 1 release-artifact pipeline acceptance remains deferred to release EPIC:

1. single server binary publishing workflow
2. crate publish orchestration
3. wheel release orchestration

See:

1. `tickets/eng/Plan_v2.md` (EPIC 11)
2. `docs/v2/status-matrix.md`
