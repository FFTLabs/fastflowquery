# Aggregation v2 (Learner)

- Status: draft
- Owner: @ffq-runtime
- Last Verified Commit: 7888e4c
- Last Verified Date: 2026-02-21

This chapter explains EPIC 6 aggregation behavior in v2.

## Why aggregation v2 matters

Aggregation is one of the highest memory-pressure operators. v2 aggregation work focuses on:

1. predictable behavior under spill pressure
2. correct distinct aggregation
3. optional approximate counting for large cardinality workloads

## 6.1 Streaming hash aggregate with spill

Execution model:

1. batches stream through aggregate state map
2. state grows by group key
3. when estimated state exceeds budget, state is spilled
4. spilled + in-memory states are merged into final output

Key point:

1. spill is an execution strategy change, not a semantic change
2. result sets should remain deterministic between spill and non-spill runs

References:

1. `crates/client/src/runtime.rs`
2. `crates/client/tests/embedded_hash_aggregate.rs`

## 6.2 COUNT(DISTINCT) two-phase lowering

Planner lowers `COUNT(DISTINCT x)` to a distinct-friendly shape before runtime:

1. distinct arguments are normalized in planner lowering
2. runtime executes lowered aggregate plan
3. distributed parity checks validate embedded/distributed consistency

References:

1. `crates/planner/src/physical_planner.rs`
2. `crates/client/src/runtime.rs`
3. `crates/client/tests/distributed_runtime_roundtrip.rs`

## 6.3 Approximate aggregates and current limits

Implemented:

1. `APPROX_COUNT_DISTINCT` (HLL sketch state)
2. feature-gated by planner/client `approx`

Not implemented:

1. grouping sets family (`GROUPING SETS`, `ROLLUP`, `CUBE`)

References:

1. `crates/planner/src/logical_plan.rs`
2. `crates/planner/src/sql_frontend.rs`
3. `crates/client/tests/embedded_hash_aggregate.rs`

## Validation checklist

```bash
cargo test -p ffq-client --test embedded_hash_aggregate
cargo test -p ffq-client --test distributed_runtime_roundtrip distributed_embedded_roundtrip_matches_expected_snapshots_and_parity -- --exact
cargo test -p ffq-client --features approx --test embedded_hash_aggregate approx_count_distinct_is_plausible_with_tolerance -- --exact
```

Expected:

1. aggregate correctness and determinism hold with/without spill
2. distinct aggregate semantics are stable
3. approximate aggregate remains within tolerance bounds
