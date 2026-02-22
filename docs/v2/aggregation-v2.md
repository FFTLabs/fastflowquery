# Aggregation v2 (EPIC 6)

- Status: draft
- Owner: @ffq-runtime
- Last Verified Commit: 7888e4c
- Last Verified Date: 2026-02-21

## Scope

This page documents EPIC 6 aggregation behavior in v2:

1. streaming hash aggregation with spill
2. `COUNT(DISTINCT ...)` lowering and execution
3. optional approximate aggregate support

Primary references:

1. `crates/client/src/runtime.rs`
2. `crates/planner/src/physical_planner.rs`
3. `crates/planner/src/sql_frontend.rs`
4. `crates/planner/src/logical_plan.rs`

## 6.1 Streaming hash aggregate + spill

Runtime aggregate execution is streaming by input batches and keeps group state in hash maps.

When memory pressure is reached:

1. groups spill to partitioned JSONL state files
2. runtime later merges spilled state with remaining in-memory state
3. spill metrics are recorded through global metrics

This supports deterministic aggregate outputs across spill and non-spill paths.

References:

1. `crates/client/src/runtime.rs` (`run_hash_aggregate`, `maybe_spill`, `merge_spill_file`)
2. `docs/v2/operators-core.md`
3. `crates/client/tests/embedded_hash_aggregate.rs`

## 6.2 Distinct aggregation (two-phase)

Planner lowers `COUNT(DISTINCT x)` into a distinct-friendly physical strategy:

1. dedup/group shaping in planner lowering
2. runtime partial/final aggregate execution over lowered expressions

This is used in embedded and distributed paths and is validated by parity tests.

References:

1. `crates/planner/src/physical_planner.rs` (`lower_count_distinct_aggregate`)
2. `crates/client/src/runtime.rs`
3. `crates/client/tests/embedded_hash_aggregate.rs`
4. `crates/client/tests/distributed_runtime_roundtrip.rs`

## 6.3 Optional approx aggregates / grouping sets

Implemented now:

1. `APPROX_COUNT_DISTINCT(expr)` via HLL sketch state (`AggExpr::ApproxCountDistinct`)
2. planner/frontend gate under feature `approx`

Not implemented:

1. SQL grouping sets (`GROUPING SETS`, `ROLLUP`, `CUBE`)

References:

1. `crates/planner/src/logical_plan.rs`
2. `crates/planner/src/sql_frontend.rs`
3. `crates/client/src/runtime.rs`
4. `crates/client/tests/embedded_hash_aggregate.rs`

## Validation Commands

```bash
cargo test -p ffq-client --test embedded_hash_aggregate
cargo test -p ffq-client --test distributed_runtime_roundtrip distributed_embedded_roundtrip_matches_expected_snapshots_and_parity -- --exact
cargo test -p ffq-client --features approx --test embedded_hash_aggregate approx_count_distinct_is_plausible_with_tolerance -- --exact
```

Expected:

1. spill and non-spill aggregate paths are deterministic
2. `COUNT(DISTINCT ...)` correctness remains stable in embedded and distributed parity checks
3. `APPROX_COUNT_DISTINCT` remains within configured tolerance in tests
