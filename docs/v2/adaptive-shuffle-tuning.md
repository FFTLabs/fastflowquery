# Adaptive Shuffle Tuning Guide (v2)

- Status: draft
- Owner: @ffq-runtime
- Last Verified Commit: TBD
- Last Verified Date: TBD

## Scope

This guide is the production tuning reference for adaptive shuffle in v2.

It covers:

1. adaptive layout model and decision points
2. config knobs and defaults
3. observability signals for diagnosis
4. failure modes and remediation
5. practical tuning playbooks

Core implementation:

1. `crates/common/src/adaptive.rs`
2. `crates/distributed/src/coordinator.rs`
3. `crates/distributed/src/worker.rs`
4. `crates/client/src/runtime.rs`

## Adaptive Shuffle Model

Adaptive shuffle is finalized at stage barrier time.

1. Map stage runs and reports `MapOutputPartitionMeta` with bytes per reduce partition.
2. Coordinator enters barrier flow:
   - `map_running -> map_done -> layout_finalized -> reduce_schedulable`
3. Adaptive planner computes reduce-task assignments from observed partition bytes.
4. Reduce tasks are fanned out with assignment payload:
   - `assigned_reduce_partitions`
   - `assigned_reduce_split_index`
   - `assigned_reduce_split_count`
   - `layout_version` and `layout_fingerprint`
5. Workers read only assigned partitions (and split shard if applicable).

Determinism contract:

1. same partition-byte map + same config -> identical assignments
2. planner sorts partitions by id before grouping
3. split/coalesce behavior is stable across runs

## Config Knobs and Defaults

Coordinator env vars (from `ffq-coordinator`):

1. `FFQ_ADAPTIVE_SHUFFLE_TARGET_BYTES` (default `134217728`, 128 MiB)
2. `FFQ_ADAPTIVE_SHUFFLE_MIN_REDUCE_TASKS` (default `1`)
3. `FFQ_ADAPTIVE_SHUFFLE_MAX_REDUCE_TASKS` (default `0`, meaning no explicit max beyond planned count)
4. `FFQ_ADAPTIVE_SHUFFLE_MAX_PARTITIONS_PER_TASK` (default `0`, disabled)
5. `FFQ_WORKER_LIVENESS_TIMEOUT_MS` (default `15000`)
6. `FFQ_RETRY_BACKOFF_BASE_MS` (default `250`)
7. `FFQ_MAX_TASK_ATTEMPTS` (default `3`)

How each knob affects layout:

1. `target_bytes`:
   - lower value increases reduce parallelism (more split pressure)
   - higher value increases coalescing (fewer reduce tasks)
2. `min_reduce_tasks`:
   - floor for adaptive output
3. `max_reduce_tasks`:
   - hard ceiling for adaptive output
4. `max_partitions_per_task`:
   - limits number of reduce partitions grouped into one task
   - useful to avoid oversized task fan-in when bytes are small but partition count is high

## Observability Signals

Adaptive fields are exposed in stage metrics.

Use `GetQueryStatus` (distributed) or runtime report (`EXPLAIN ANALYZE` path) and inspect:

1. `planned_reduce_tasks`
2. `adaptive_reduce_tasks`
3. `adaptive_target_bytes`
4. `aqe_events`
5. `partition_bytes_histogram`
6. `skew_split_tasks`
7. `layout_finalize_count`

Quick interpretation:

1. `adaptive_reduce_tasks < planned_reduce_tasks` means coalescing happened.
2. `adaptive_reduce_tasks > planned_reduce_tasks` means split/skew handling increased fanout.
3. `layout_finalize_count` should be `1` for normal flow.
4. high `skew_split_tasks` means hot partitions are being sharded.

## Tuning Playbooks

### 1) Throughput-first (large cluster, broad parallelism)

Suggested:

1. lower `FFQ_ADAPTIVE_SHUFFLE_TARGET_BYTES` (for example 64 MiB)
2. set `FFQ_ADAPTIVE_SHUFFLE_MAX_REDUCE_TASKS` to a cluster-safe cap
3. keep `FFQ_ADAPTIVE_SHUFFLE_MAX_PARTITIONS_PER_TASK=0` unless fan-in becomes problematic

Watch for:

1. scheduler pressure from too many tiny tasks
2. increased retry traffic under worker churn

### 2) Stability-first (smaller cluster, avoid scheduling overhead)

Suggested:

1. higher `FFQ_ADAPTIVE_SHUFFLE_TARGET_BYTES` (for example 128-256 MiB)
2. conservative `FFQ_ADAPTIVE_SHUFFLE_MAX_REDUCE_TASKS`
3. non-zero `FFQ_ADAPTIVE_SHUFFLE_MAX_PARTITIONS_PER_TASK` to bound fan-in

Watch for:

1. stragglers if skewed keys dominate one partition

### 3) Skew-heavy workloads

Suggested:

1. keep moderate target bytes (for example 64-128 MiB)
2. allow higher max reduce tasks so skew splitting can activate
3. verify `skew_split_tasks > 0` and histogram tail reduction

Watch for:

1. split explosion if target is too low and max limit is unbounded

## Failure Modes and Troubleshooting

### Symptom: reduce stage starts too early / inconsistent assignments

Checks:

1. `layout_finalize_count` should stay `1`
2. `aqe_events` should include layout-finalized event

Action:

1. verify coordinator barrier transition behavior (`map_done -> layout_finalized -> reduce_schedulable`)
2. run barrier/race tests listed below

### Symptom: stale attempt reports corrupt progress

Checks:

1. task reports include current `attempt`, `layout_version`, `layout_fingerprint`
2. stale reports should be ignored

Action:

1. verify retry-attempt handling tests
2. inspect logs for stale-report ignore warnings

### Symptom: query stalls with queued tasks

Checks:

1. worker heartbeats are current
2. no broad worker blacklist condition
3. per-worker/per-query concurrency limits are not too low

Action:

1. increase `FFQ_MAX_CONCURRENT_TASKS_PER_WORKER` or `FFQ_MAX_CONCURRENT_TASKS_PER_QUERY` as needed
2. relax blacklist threshold if false positives are frequent
3. reduce retry backoff if recovery feels too slow

### Symptom: straggler-dominated completion on skew

Checks:

1. large tail bucket in `partition_bytes_histogram`
2. low or zero `skew_split_tasks`

Action:

1. lower `FFQ_ADAPTIVE_SHUFFLE_TARGET_BYTES`
2. increase `FFQ_ADAPTIVE_SHUFFLE_MAX_REDUCE_TASKS`
3. ensure split cap (`max_partitions_per_task`) is not over-constraining

## Validation Checklist

Correctness and fault tolerance:

```bash
cargo test -p ffq-distributed --features grpc coordinator_applies_barrier_time_adaptive_partition_coalescing
cargo test -p ffq-distributed --features grpc coordinator_barrier_time_hot_partition_splitting_increases_reduce_tasks
cargo test -p ffq-distributed --features grpc coordinator_ignores_stale_reports_from_old_adaptive_layout
cargo test -p ffq-distributed --features grpc coordinator_adaptive_shuffle_retries_failed_map_attempt_and_completes
cargo test -p ffq-distributed --features grpc coordinator_adaptive_shuffle_recovers_from_worker_death_during_map_and_reduce
```

Performance and regression gating:

```bash
make bench-v2-adaptive-shuffle-embedded
make bench-v2-adaptive-shuffle-compare BASELINE=<baseline.json-or-dir> CANDIDATE=<candidate.json-or-dir>
```

## Recommended Startup Template

Coordinator example:

```bash
FFQ_ADAPTIVE_SHUFFLE_TARGET_BYTES=$((128*1024*1024)) \
FFQ_ADAPTIVE_SHUFFLE_MIN_REDUCE_TASKS=1 \
FFQ_ADAPTIVE_SHUFFLE_MAX_REDUCE_TASKS=256 \
FFQ_ADAPTIVE_SHUFFLE_MAX_PARTITIONS_PER_TASK=8 \
FFQ_WORKER_LIVENESS_TIMEOUT_MS=15000 \
FFQ_RETRY_BACKOFF_BASE_MS=250 \
FFQ_MAX_TASK_ATTEMPTS=3 \
cargo run -p ffq-distributed --bin ffq-coordinator
```
