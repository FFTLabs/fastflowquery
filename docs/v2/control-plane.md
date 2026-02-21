# Control Plane (Coordinator/Worker RPC) - v2

- Status: draft
- Owner: @ffq-runtime
- Last Verified Commit: TBD
- Last Verified Date: TBD

## Scope

This page defines the control-plane and heartbeat RPC contract used by distributed execution, including capability-aware task assignment semantics.

Protocol source:

1. `crates/distributed/proto/ffq_distributed.proto`

Server/client wiring:

1. `crates/distributed/src/grpc.rs`
2. `crates/distributed/src/coordinator.rs`
3. `crates/distributed/src/worker.rs`

## RPC Surface

### ControlPlane

1. `SubmitQuery`
2. `GetTask`
3. `ReportTaskStatus`
4. `GetQueryStatus`
5. `CancelQuery`
6. `RegisterQueryResults`
7. `FetchQueryResults` (stream)

### ShuffleService

1. `RegisterMapOutput`
2. `FetchShufflePartition` (stream)

Pipelined stream contract:

1. map-side registration updates per-partition stream metadata:
   - `stream_epoch`
   - `committed_offset`
   - `finalized`
2. reducers fetch by byte range (`start_offset`, `max_bytes`) and advance local cursors.
3. fetch responses include `watermark_offset` and `finalized` so reducers can distinguish "more data coming" vs true EOF.
4. coordinator/worker reject stale epoch/layout combinations to keep retry attempts isolated.

### HeartbeatService

1. `Heartbeat`

## Call Sequences

### Query submission

1. client calls `SubmitQuery(query_id, physical_plan_json)`
2. coordinator stores query runtime state and returns initial status
3. workers poll `GetTask` and begin execution

### Worker task loop

1. worker sends `Heartbeat`
2. worker calls `GetTask(worker_id, capacity)`
3. coordinator returns zero or more task assignments
4. worker executes each assignment
5. worker calls `ReportTaskStatus` for each assignment
6. worker may call `RegisterMapOutput` for map-stage outputs
7. final stage may call `RegisterQueryResults`

When pipelined shuffle is enabled:

1. reducer tasks can be assigned before map-task completion if readiness thresholds are met.
2. coordinator emits recommended map-publish and reduce-fetch window sizes for backpressure control.

### Client result retrieval

1. client calls `GetQueryStatus` until terminal
2. on success, client calls `FetchQueryResults` stream

## Heartbeat Payload Contract

`HeartbeatRequest` carries:

1. `worker_id`
2. `at_ms`
3. `running_tasks`
4. `custom_operator_capabilities` (repeated string)

Coordinator behavior:

1. updates worker liveness timestamp
2. stores capability set for that worker
3. uses stored capability set during subsequent `GetTask` assignment filtering

Important:

1. capability payload is used for scheduling decisions
2. workers without required capabilities are filtered out for capability-bound tasks

## Capability-Aware Filtering in `GetTask`

Task attempts may require custom operators discovered from plan fragments.

Coordinator checks:

1. if task requires no custom op names: eligible worker set is unchanged
2. if task requires custom op names: worker must advertise all required names from heartbeat

If capability match fails:

1. task remains queued
2. no assignment is sent to that worker in this poll

## Failure and Recovery Semantics

### Reported task failures

1. failure increments worker failure counter
2. failures beyond threshold trigger worker blacklisting
3. failed attempts can be retried with backoff (until retry budget exhausted)

### Worker liveness failures

1. stale heartbeat timeout triggers worker-stale handling
2. coordinator requeues running tasks from stale workers as new attempts
3. stale worker record is removed

### Assignment guards

Before assignment, coordinator also enforces:

1. worker blacklist check
2. per-worker concurrency limit
3. per-query concurrency limit
4. stage-runnable and latest-attempt checks

## Known Operational Constraints

1. capability registration is process-local: each worker process must register its custom operator factories at startup so advertised capability names are truthful.
2. if no worker advertises required capabilities, capability-bound tasks will not progress.

## Reproducible Verification

```bash
cargo test -p ffq-distributed --features grpc coordinator_assigns_custom_operator_tasks_only_to_capable_workers
cargo test -p ffq-distributed --features grpc coordinator_requeues_tasks_from_stale_worker
cargo test -p ffq-distributed --features grpc coordinator_blacklists_failing_worker
```

Expected:

1. task assignment honors capability requirements
2. stale worker tasks are requeued
3. repeated failures can blacklist a worker
