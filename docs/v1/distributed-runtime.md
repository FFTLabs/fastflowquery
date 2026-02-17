# Distributed Runtime (Coordinator/Worker) - v1

This document describes FFQ v1 distributed runtime behavior for coordinator/worker execution.

## Scope

Covered in this page:
1. Control-plane and shuffle RPCs.
2. Worker pull scheduling model.
3. Query/task state machine.
4. Map output registry semantics.
5. Basic worker blacklisting.
6. Worker resource controls.
7. Minimal walkthrough: coordinator + 2 workers.

Core files:
1. `crates/distributed/proto/ffq_distributed.proto`
2. `crates/distributed/src/grpc.rs`
3. `crates/distributed/src/coordinator.rs`
4. `crates/distributed/src/worker.rs`
5. `crates/distributed/src/stage.rs`

## RPC Surface (tonic proto)

### ControlPlane service

RPCs:
1. `SubmitQuery`
2. `GetTask`
3. `ReportTaskStatus`
4. `GetQueryStatus`
5. `CancelQuery`
6. `RegisterQueryResults`
7. `FetchQueryResults` (server stream)

Purpose:
1. Query lifecycle control.
2. Worker task assignment loop.
3. Final result publication/fetch.

### ShuffleService

RPCs:
1. `RegisterMapOutput`
2. `FetchShufflePartition` (server stream)

Purpose:
1. Map output metadata registration.
2. Shuffle partition byte transport.

### HeartbeatService

RPC:
1. `Heartbeat`

Purpose:
1. Liveness/availability signal in worker polling loop.

## Coordinator Model

Implementation: `crates/distributed/src/coordinator.rs`.

Responsibilities:
1. Accept submitted physical plan bytes.
2. Resolve parquet scan schemas once at submit time (from coordinator catalog, with parquet inference when schema is missing).
3. Build stage DAG (`build_stage_dag`) from shuffle boundaries.
4. Track query state and task state.
5. Assign tasks via pull API (`GetTask`).
6. Store map output metadata registry.
7. Serve shuffle fetch for registered attempts.
8. Track and expose stage/task metrics.
9. Blacklist repeatedly failing workers.

Coordinator catalog note:
1. Set `FFQ_COORDINATOR_CATALOG_PATH` so coordinator can resolve/infer parquet schemas and embed them in task plans.
2. Workers then rely on schema carried in each `ParquetScanExec` assignment, which keeps schema consistent across workers.

### Query state machine

Query states:
1. `Queued`
2. `Running`
3. `Succeeded`
4. `Failed`
5. `Canceled`

Transitions:
1. Submit -> `Queued`.
2. First assignment -> `Running`.
3. All tasks succeeded -> `Succeeded`.
4. Any task failed -> `Failed` (with message).
5. `CancelQuery` -> `Canceled`.

### Task state machine

Task states:
1. `Queued`
2. `Running`
3. `Succeeded`
4. `Failed`

Scheduler policy:
1. Workers call `GetTask(worker_id, capacity)`.
2. Coordinator assigns only runnable-stage queued tasks.
3. Stage is runnable when all parent stage tasks are `Succeeded`.
4. Assignment is bounded by worker-reported `capacity`.

## Worker Pull Scheduling

Implementation: `Worker::poll_once` in `crates/distributed/src/worker.rs`.

Flow:
1. Worker computes available capacity from CPU semaphore permits.
2. Worker requests tasks from coordinator (`GetTask`).
3. If empty, worker sends heartbeat and returns.
4. For each assignment, worker acquires one CPU slot and executes asynchronously.
5. On success:
- optionally register map outputs,
- optionally publish final query result payload,
- report `Succeeded`.
6. On failure:
- report `Failed` with error message.

Notes:
1. Scheduling is pull-based, not push-based.
2. In v1, each stage has one task (`task_id=0`) per query runtime build.

## Map Output Registry

Coordinator registry key:
1. `(query_id, stage_id, map_task, attempt)`

Behavior:
1. `register_map_output` stores partition metadata by exact key.
2. `fetch_shuffle_partition_chunks` requires exact key presence.
3. Unknown key returns planning error (`map output not registered for requested attempt`).

Metrics aggregation:
1. Registered partition `rows/bytes/batches` contribute to stage metrics.

## Blacklisting

Coordinator maintains:
1. `worker_failures: HashMap<worker_id, count>`
2. `blacklisted_workers: HashSet<worker_id>`

Rule:
1. On task failure report, increment worker failure count.
2. If count >= `blacklist_failure_threshold`, worker is blacklisted.
3. Blacklisted worker receives no further assignments.

Config:
1. `CoordinatorConfig.blacklist_failure_threshold` (default `3`).

## Worker Resource Controls

Worker config (`WorkerConfig`):
1. `cpu_slots`
2. `per_task_memory_budget_bytes`
3. `spill_dir`
4. `shuffle_root`

Enforcement:
1. CPU concurrency is controlled by `tokio::sync::Semaphore` permits.
2. Per-task memory budget is passed into execution context and used by operators for spill decisions.
3. Spill files are written under task spill directory.
4. Shuffle files/indexes are read/written under shuffle root.

## Minimal Walkthrough: Coordinator + 2 Workers

This walkthrough describes the execution shape used by v1 integration tests.

### Setup

1. Coordinator starts with shared state and shuffle root.
2. Worker A and Worker B start with:
- unique `worker_id`,
- same coordinator endpoint/control plane,
- configured CPU slots and memory budget.

### Query lifecycle

1. Client submits physical plan via `SubmitQuery`.
2. Coordinator builds stage DAG and initializes per-stage tasks.
3. Worker A polls `GetTask(capacity)` and receives runnable stage task(s).
4. Worker B polls and receives remaining runnable work (if available).
5. For map stages:
- workers execute plan fragments,
- write shuffle partitions/index,
- call `RegisterMapOutput`.
6. Once parent stages succeed, downstream stage becomes runnable.
7. Worker executes final stage and produces output batches.
8. Worker publishes final result payload via `RegisterQueryResults`.
9. Client fetches results via `FetchQueryResults` stream.
10. `GetQueryStatus` transitions to `Succeeded` when all tasks succeed.

### Failure case behavior

1. If a task execution fails, worker reports `Failed`.
2. Coordinator marks query `Failed` and records message.
3. Worker failure counts may trigger blacklist threshold.

## Notes on v1 Simplifications

1. Stage DAG is correct, but task cardinality is minimal (single task per stage in current runtime build).
2. Query id for shuffle path usage is currently expected to be numeric in distributed flow.
3. Attempt handling is explicit in registry keys and fetch calls; retry policies are minimal.

## Test References

1. `crates/client/tests/distributed_runtime_roundtrip.rs`
2. `crates/distributed/src/coordinator.rs` (unit tests for scheduling and blacklisting)

## Distributed vs Embedded Parity Coverage

v1 correctness suite includes explicit parity checks where the same fixtures and SQL are run in distributed and embedded modes, then normalized outputs are compared.

Covered shapes:
1. Join + aggregate parity.
2. Raw join projection parity (same logical rows after normalization).
3. Vector-gated two-phase retrieval parity where enabled.

Primary test:
1. `crates/client/tests/distributed_runtime_roundtrip.rs`
