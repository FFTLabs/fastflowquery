# LEARN-07: Coordinator/Worker Control Plane

This chapter explains FFQ v2 control-plane behavior: coordinator state transitions, pull scheduling, heartbeat/liveness handling, task retry/backoff, map output registry, blacklisting, and capability-aware routing.

## 1) Control-Plane Surface (RPCs)

Proto: `crates/distributed/proto/ffq_distributed.proto`

Services:

1. `ControlPlane`
2. `ShuffleService`
3. `HeartbeatService`

Key control-plane RPCs:

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
2. first assignment moves query to `Running`
3. all latest task attempts succeeded -> `Succeeded`
4. retry budget exhausted or unrecoverable failure -> `Failed`
5. explicit cancel -> `Canceled`

### Task state machine

`TaskState`:

1. `Queued`
2. `Running`
3. `Succeeded`
4. `Failed`

Tasks are keyed by `(stage_id, task_id, attempt)`.
Latest-attempt rules prevent stale attempts from winning state updates.

## 3) Pull Scheduling and Assignment Gates

Workers pull tasks with `GetTask(worker_id, capacity)`.

Coordinator assignment gates in `get_task(...)`:

1. worker is not blacklisted
2. worker capacity > 0
3. worker under `max_concurrent_tasks_per_worker`
4. query under `max_concurrent_tasks_per_query`
5. stage is runnable (all parent stages succeeded)
6. task is queued/latest attempt and ready by backoff timestamp
7. worker satisfies required custom operator capabilities

Why this works:

1. pull scheduling gives worker-side backpressure
2. coordinator caps prevent unbounded runnable assignment
3. capability filtering prevents assigning unsupported custom-op work

## 4) Heartbeats and Liveness (Active, Not Advisory)

Worker loop sends heartbeat every poll cycle with:

1. `worker_id`
2. `running_tasks`
3. `custom_operator_capabilities`

Coordinator heartbeat behavior:

1. updates `last_seen_ms`
2. stores worker capability set
3. uses liveness timeout to detect stale workers

Stale-worker handling (`requeue_stale_workers`):

1. find workers past `worker_liveness_timeout_ms`
2. requeue their `Running` tasks as new attempts
3. clear stale worker heartbeat record

This is active correctness/fault handling, not just metadata.

## 5) Retry/Backoff and Blacklisting

On `ReportTaskStatus(..., Failed, ...)`:

1. increment worker failure counter
2. blacklist worker once `blacklist_failure_threshold` is reached
3. if attempts remain (`attempt < max_task_attempts`):
   - enqueue next attempt
   - apply exponential backoff from `retry_backoff_base_ms`
4. if attempts exhausted: query -> `Failed`

On `Succeeded`:

1. clear worker failure counter for that worker

## 6) Capability-Aware Scheduling for Custom Operators

Worker advertises available custom operator names from registry.

Coordinator compares:

1. task `required_custom_ops`
2. worker `custom_operator_capabilities`

Assignment rule:

1. tasks with no custom-op requirement can run anywhere
2. custom-op tasks only go to workers advertising all required op names

Operational consequence:

1. if no capable worker exists, task remains queued
2. once capable worker heartbeats, task becomes assignable

## 7) Map Output Registry and Attempt Safety

Map output key:

1. `(query_id, stage_id, map_task, attempt)`

Flow:

1. worker runs map stage and registers partition metadata
2. fetch requests validate exact attempt identity
3. stale/non-registered attempt lookup fails explicitly

Why this matters:

1. prevents stale shuffle outputs from contaminating reduce stages
2. ties data visibility to attempt identity

## 8) End-to-End Sequence

Successful path:

1. client `SubmitQuery`
2. workers heartbeat + `GetTask`
3. coordinator assigns runnable tasks respecting limits/capabilities
4. workers execute and report status
5. map stages register shuffle outputs
6. final stage registers results
7. query reaches `Succeeded`

Failure path:

1. task fails -> retry/backoff or terminal fail
2. repeated worker failures -> blacklist
3. stale worker -> requeue running tasks as new attempts

## 9) Why This Works (Correctness + Fault Assumptions)

Correctness anchors:

1. stage dependency gating
2. latest-attempt state tracking
3. map output attempt identity
4. capability-aware custom-op routing
5. bounded scheduler concurrency

Fault handling assumptions:

1. workers continue polling/reporting unless crashed
2. coordinator heartbeat timeout detects dead/stuck workers
3. retry budget and blacklist policy isolate bad workers and transient failures

## 10) Code References

1. `crates/distributed/src/coordinator.rs`
2. `crates/distributed/src/worker.rs`
3. `crates/distributed/src/grpc.rs`
4. `crates/distributed/proto/ffq_distributed.proto`

## Runnable commands

```bash
cargo test -p ffq-distributed --features grpc coordinator_requeues_tasks_from_stale_worker
cargo test -p ffq-distributed --features grpc coordinator_enforces_worker_and_query_concurrency_limits
cargo test -p ffq-distributed --features grpc coordinator_assigns_custom_operator_tasks_only_to_capable_workers
```
