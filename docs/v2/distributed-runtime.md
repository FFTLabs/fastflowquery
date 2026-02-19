# Distributed Runtime (Coordinator/Worker) - v2

- Status: draft
- Owner: @ffq-runtime
- Last Verified Commit: TBD
- Last Verified Date: TBD

## Scope

This page documents the distributed runtime execution contract in v2:

1. stage/task execution model
2. task pull scheduling and query/task lifecycle
3. map output registry and shuffle lookup
4. liveness, retry/backoff, blacklisting
5. capability-aware custom-operator assignment

Related control-plane RPC details are documented in `docs/v2/control-plane.md`.

Core implementation references:

1. `crates/distributed/src/coordinator.rs`
2. `crates/distributed/src/worker.rs`
3. `crates/distributed/src/grpc.rs`
4. `crates/distributed/proto/ffq_distributed.proto`

## Execution Model

The coordinator accepts a physical plan and schedules task attempts by stage.

1. `SubmitQuery` stores the plan and creates stage/task runtime state.
2. Workers pull assignments via `GetTask(worker_id, capacity)`.
3. Workers execute assigned task fragments and report status (`Succeeded` or `Failed`).
4. On map stages, workers register shuffle partition metadata (`RegisterMapOutput`).
5. Query completion is reached when latest task attempts are all succeeded.

## Query and Task State

### Query states

1. `Queued`
2. `Running`
3. `Succeeded`
4. `Failed`
5. `Canceled`

### Task states

1. `Queued`
2. `Running`
3. `Succeeded`
4. `Failed`

Retry behavior:

1. failed task attempts are retried up to `max_task_attempts`
2. retries are queued with exponential backoff from `retry_backoff_base_ms`
3. when retry budget is exhausted, query is marked `Failed`

## Pull Scheduling and Limits

Scheduling is pull-based: coordinator never pushes tasks.

Assignment gates in `Coordinator::get_task`:

1. worker must not be blacklisted
2. worker capacity must be non-zero
3. per-worker running limit: `max_concurrent_tasks_per_worker`
4. per-query running limit: `max_concurrent_tasks_per_query`
5. task must be from a runnable stage and latest attempt
6. worker must satisfy required custom-operator capabilities (if any)

This prevents unbounded assignment and controls memory pressure by limiting concurrent active work.

## Capability-Aware Scheduling

Capability-aware scheduling is active behavior, not advisory metadata.

1. worker heartbeats include `custom_operator_capabilities`
2. coordinator stores capabilities per worker heartbeat record
3. each task attempt includes `required_custom_ops` (derived from plan fragment)
4. coordinator only assigns a task when worker capabilities cover all required ops

Selection rule (`worker_supports_task`):

1. tasks with no required custom ops are assignable to any healthy worker
2. tasks with required custom ops are assignable only if all required op names are present in worker capabilities

Operational consequence:

1. if no worker advertises required capabilities, matching tasks remain queued and are not incorrectly assigned
2. once a capable worker heartbeats/polls, those tasks become assignable

## Liveness and Requeue

Liveness is enforced through heartbeat timeout.

1. coordinator tracks last heartbeat timestamp per worker
2. stale workers are detected using `worker_liveness_timeout_ms`
3. running tasks owned by stale workers are requeued to new attempts
4. stale worker heartbeat records are dropped

This enables recovery from worker loss without requiring manual cleanup.

## Failure Tracking and Blacklisting

On failed task status reports:

1. worker failure count is incremented
2. when count reaches `blacklist_failure_threshold`, worker is blacklisted
3. blacklisted workers receive no further assignments

On succeeded task status reports:

1. worker failure count is cleared for that worker

## Map Output Registry and Shuffle

Map output metadata is keyed by:

1. `query_id`
2. `stage_id`
3. `map_task`
4. `attempt`

`FetchShufflePartition` requires an exact key match for the requested attempt.
This ensures stale map attempts are not used by downstream stages.

## Minimal Runtime Walkthrough (Coordinator + 2 Workers)

1. client submits query plan
2. coordinator builds stage/runtime state
3. worker `w1` and `w2` heartbeat with capability sets
4. both workers poll `GetTask`
5. coordinator assigns only runnable tasks that fit worker/query limits
6. for custom-op tasks, coordinator assigns only to workers that advertised required op names
7. workers execute and report status
8. failures are retried/backed off; stale worker tasks are requeued
9. query reaches `Succeeded` when all latest attempts succeed, otherwise `Failed`

## Reproducible Checks

```bash
cargo test -p ffq-distributed --features grpc coordinator_requeues_tasks_from_stale_worker
cargo test -p ffq-distributed --features grpc coordinator_blacklists_failing_worker
cargo test -p ffq-distributed --features grpc coordinator_enforces_worker_and_query_concurrency_limits
cargo test -p ffq-distributed --features grpc coordinator_assigns_custom_operator_tasks_only_to_capable_workers
```

Expected:

1. stale-worker tasks are requeued
2. failing workers can be blacklisted
3. per-worker/per-query assignment limits are enforced
4. custom-op tasks are assigned only to capable workers
