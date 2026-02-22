# LEARN-17: AQE and Adaptive Shuffle (EPIC 4)

This chapter explains EPIC 4 (AQE) in v2:

1. runtime stats flow
2. adaptive join choice
3. adaptive shuffle partitioning and skew handling
4. fault/retry safety and observability

Primary references:

1. `docs/v2/adaptive-shuffle-tuning.md`
2. `docs/v2/distributed-runtime.md`
3. `docs/v2/control-plane.md`

## 1) Runtime Stats Plumbing (4.1)

AQE decisions are driven by observed stage metrics:

1. bytes and partition sizes from map outputs
2. planned vs adaptive reduce task counts
3. stage-level events (`aqe_events`)

Why this matters:

1. planner estimates are corrected by runtime reality
2. operators can explain why adaptive layout changed

## 2) Adaptive Join Choice (4.2)

Join execution supports adaptive alternatives:

1. shuffle path
2. broadcast path

Runtime can choose broadcast when build-side bytes are below threshold.

Conceptual rule:

1. smaller observed build side -> prefer broadcast
2. otherwise remain shuffle

## 3) Adaptive Shuffle Partitions (4.3)

Barrier-time model:

1. map stage reports per-partition bytes
2. coordinator finalizes layout once (`map_done -> layout_finalized -> reduce_schedulable`)
3. reduce assignments include explicit partition/split payload

Key mechanics:

1. fanout from single reduce stage into multiple reduce tasks
2. deterministic coalesce/split algorithm
3. min/max guardrails for adaptive reduce task count
4. skew detection and hot-partition split expansion

## 4) Retry and Attempt Safety

Adaptive layouts are versioned/fingerprinted:

1. stale reports from older layout/attempt are ignored
2. worker-loss recovery requeues tasks as new attempts
3. stage correctness is preserved under retries

## 5) QueryStatus and Explain Visibility

AQE observability includes:

1. planned vs adaptive reduce tasks
2. target bytes and histogram context
3. skew split counts/events
4. barrier/layout finalize counters

Use `GetQueryStatus` / runtime reports to diagnose adaptive decisions.

## 6) EPIC 4 Verification Commands

```bash
cargo test -p ffq-distributed --features grpc coordinator_fans_out_reduce_stage_tasks_from_shuffle_layout
cargo test -p ffq-distributed --features grpc coordinator_applies_barrier_time_adaptive_partition_coalescing
cargo test -p ffq-distributed --features grpc coordinator_barrier_time_hot_partition_splitting_increases_reduce_tasks
cargo test -p ffq-distributed --features grpc coordinator_finalizes_adaptive_layout_once_before_reduce_scheduling
cargo test -p ffq-distributed --features grpc coordinator_ignores_stale_reports_from_old_adaptive_layout
cargo test -p ffq-distributed --features grpc coordinator_adaptive_shuffle_retries_failed_map_attempt_and_completes
cargo test -p ffq-distributed --features grpc coordinator_adaptive_shuffle_recovers_from_worker_death_during_map_and_reduce
```

Benchmark/tuning checks:

```bash
make bench-v2-adaptive-shuffle-embedded
make bench-v2-adaptive-shuffle-compare BASELINE=<baseline.json-or-dir> CANDIDATE=<candidate.json-or-dir>
```

## 7) Practical Tuning Notes

1. low target bytes -> more reduce tasks, better parallelism, higher scheduler overhead
2. high target bytes -> fewer tasks, lower overhead, risk of stragglers
3. skew splits should activate for hot partitions; if not, inspect skew thresholds and observed histograms

## 8) Code References

1. `crates/common/src/adaptive.rs`
2. `crates/distributed/src/coordinator.rs`
3. `crates/distributed/src/worker.rs`
4. `crates/distributed/src/grpc.rs`
5. `crates/client/src/runtime.rs`
