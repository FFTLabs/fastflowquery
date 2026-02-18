# LEARN-07: Coordinator/Worker Control Plane

This chapter explains FFQ v1 control-plane behavior: coordinator state transitions, pull scheduling, heartbeats, task status flow, map output registry, and worker blacklisting.

## 1) Control-Plane Surface (RPCs)

Proto: `crates/distributed/proto/ffq_distributed.proto`

Services:

1. `ControlPlane`
2. `ShuffleService`
3. `HeartbeatService`

Key `ControlPlane` RPCs:

1. `SubmitQuery`
2. `GetTask`
3. `ReportTaskStatus`
4. `GetQueryStatus`
5. `CancelQuery`
6. `RegisterQueryResults`
7. `FetchQueryResults` (stream)

Shuffle RPCs:

1. `RegisterMapOutput`
2. `FetchShufflePartition` (stream)

Heartbeat RPC:

1. `Heartbeat`

## 2) Coordinator State Model

Implementation: `crates/distributed/src/coordinator.rs`

### Query state machine

`QueryState`:

1. `Queued`
2. `Running`
3. `Succeeded`
4. `Failed`
5. `Canceled`

Transitions:

1. `SubmitQuery` -> `Queued`
2. first scheduling pass (`GetTask`) moves query to `Running`
3. all tasks succeeded -> `Succeeded`
4. any task failed -> `Failed`
5. explicit cancel -> `Canceled`

### Task state machine

`TaskState`:

1. `Queued`
2. `Running`
3. `Succeeded`
4. `Failed`

Tasks are keyed by `(stage_id, task_id, attempt)` inside each query runtime.

## 3) Query Submission and Runtime Materialization

`submit_query(...)`:

1. validates unique `query_id`
2. decodes physical plan JSON
3. builds stage DAG
4. creates stage runtimes and initial queued tasks

v1 simplification:

1. each stage gets one task (`task_id=0`) per attempt
2. initial attempt is `1`
3. task carries physical plan bytes as fragment payload

## 4) Pull Scheduling Model

Workers do not get pushed tasks; they pull with capacity.

Worker side:

1. `Worker::poll_once()` computes available capacity from CPU semaphore
2. calls `GetTask(worker_id, capacity)`
3. if empty, sends heartbeat

Coordinator side (`get_task(...)`):

1. skips blacklisted workers
2. considers only `Queued`/`Running` queries
3. computes runnable stages (all parent stages succeeded)
4. assigns queued tasks up to requested capacity
5. marks assigned task `Running` and updates stage metrics

Why pull scheduling:

1. workers self-advertise available capacity
2. coordinator remains simple and stateless per worker connection

## 5) Task Status Reporting Path

Worker reports terminal/intermediate status via `ReportTaskStatus`.

Coordinator `report_task_status(...)`:

1. validates task key `(query, stage, task, attempt)` exists
2. updates task state and message
3. updates stage counters (queued/running/succeeded/failed)
4. on failure:
   - increments worker failure count
   - possibly blacklists worker
   - marks query failed
5. if all tasks succeeded and query not failed:
   - marks query succeeded

Result:

1. query status polling (`GetQueryStatus`) reflects scheduler progress
2. terminal outcome is derived from explicit task reports

## 6) Heartbeats

Worker sends heartbeat when idle in polling loop.

Current v1 behavior:

1. `HeartbeatService::heartbeat` returns `accepted=true`
2. coordinator does not yet use heartbeats for timeout-based liveness eviction

Interpretation:

1. heartbeat exists as control-plane compatibility/extension point
2. correctness does not currently depend on heartbeat processing

## 7) Map Output Registry

Coordinator map output registry key:

1. `(query_id, stage_id, map_task, attempt)`

Flow:

1. worker executes map stage and calls `RegisterMapOutput`
2. coordinator stores partition metadata and aggregates stage shuffle metrics
3. later `FetchShufflePartition` requests validate attempt key exists
4. unknown key returns explicit planning error

Why this matters:

1. protects consumers from reading unregistered/incorrect shuffle outputs
2. ties shuffle visibility to explicit task success path

## 8) Blacklisting

Coordinator tracks worker failures:

1. per-worker counter increments on reported task failures
2. if failures reach `blacklist_failure_threshold`, worker is blacklisted
3. blacklisted worker gets no further assignments

Config:

1. `CoordinatorConfig.blacklist_failure_threshold` (default `3`)

Purpose:

1. isolate repeatedly failing workers
2. reduce repeated task loss from same bad executor

## 9) End-to-End Control-Plane Sequence

Minimal successful path:

1. client `SubmitQuery`
2. worker `GetTask` pull
3. worker executes task
4. worker `RegisterMapOutput` (for map stages)
5. worker `ReportTaskStatus(Succeeded)`
6. final-stage worker `RegisterQueryResults`
7. query becomes `Succeeded`
8. client polls `GetQueryStatus` and then `FetchQueryResults`

Failure path (simplified):

1. worker reports `TaskState::Failed`
2. coordinator marks task/stage failed and query failed
3. optional blacklisting if worker repeatedly fails
4. client polling sees terminal `Failed` state

## 10) Why This Works (Correctness + Fault Assumptions)

### Core correctness points

1. stage dependencies enforce parent-before-child execution
2. task identity includes attempt, preventing ambiguous status/output updates
3. map outputs are visible only after explicit registration
4. terminal query state derives from explicit task completion reports

### Fault-handling assumptions in v1

1. workers eventually report terminal status for assigned tasks
2. network/RPC errors surface as execution errors to caller
3. coordinator process is authoritative in-memory source of query/task state
4. retries/reattempt orchestration is minimal; attempt field exists and is tracked, but advanced resubmission policy is intentionally simple in v1
5. heartbeat is advisory today (not yet used for lease-expiry requeue logic)

Under these assumptions, v1 provides a minimal but coherent control plane.

## 11) Observability Hooks in Control Plane

Coordinator and worker emit:

1. structured logs for assignment, start, success/failure, blacklisting
2. scheduler metrics (queued/running/retries)
3. stage-level map output metrics (rows/bytes/batches)

Relevant files:

1. `crates/distributed/src/coordinator.rs`
2. `crates/distributed/src/worker.rs`
3. `crates/distributed/src/grpc.rs`
4. `docs/v1/observability.md`

## Runnable command

```bash
cargo test -p ffq-client --test distributed_runtime_roundtrip --features distributed
```
