# Join System v2 (Learner)

- Status: draft
- Owner: @ffq-runtime
- Last Verified Commit: 7888e4c
- Last Verified Date: 2026-02-21

This chapter explains EPIC 5 join behavior in v2 from a learning perspective.

## Why v2 join system matters

EPIC 5 introduces targeted improvements over baseline hash join:

1. radix partitioning for cache-friendly build/probe behavior
2. bloom prefiltering to reduce probe-side work on selective joins
3. sort-merge selection path for suitable sorted/planned inputs
4. first-class semi/anti semantics used by subquery rewrites

## 5.1 Radix-partitioned hash join

Runtime knob:

1. `join_radix_bits`

Behavior:

1. `0` keeps baseline hash path
2. `>0` partitions key-space into radix buckets before hash-table work
3. per-partition processing improves locality on larger joins

Code references:

1. `crates/client/src/runtime.rs`
2. `crates/client/examples/bench_join_radix.rs`

## 5.2 Bloom prefiltering

Runtime knobs:

1. `join_bloom_enabled`
2. `join_bloom_bits`

Behavior:

1. build keys populate a bloom filter
2. probe batches are prefiltered before full hash match
3. selective joins see lower probe-side byte/work volume

Code references:

1. `crates/client/src/runtime.rs`
2. `crates/client/examples/bench_join_bloom.rs`

## 5.3 Targeted sort-merge join path

Planner/runtime contract:

1. optimizer can emit `JoinStrategyHint::SortMerge`
2. physical planner preserves selected join strategy
3. runtime executes sort-merge path when selected/eligible

Code references:

1. `crates/planner/src/optimizer.rs`
2. `crates/planner/src/physical_planner.rs`
3. `crates/client/src/runtime.rs`
4. `crates/client/src/runtime_tests.rs`

## 5.4 Semi/anti join semantics

Logical join types:

1. `JoinType::Semi`
2. `JoinType::Anti`

Semantics:

1. semi emits left rows with at least one match
2. anti emits left rows with zero matches
3. output schema is left side only

These are reused by subquery rewrites (`EXISTS`, `IN` families).

Code references:

1. `crates/planner/src/logical_plan.rs`
2. `crates/planner/src/analyzer.rs`
3. `crates/client/src/runtime.rs`

## Validation checklist

```bash
cargo test -p ffq-client --test embedded_hash_join
cargo test -p ffq-client --test embedded_cte_subquery
make bench-v2-join-radix
make bench-v2-join-bloom
```

Expected outcomes:

1. join correctness suites pass (inner/outer/semi/anti and subquery rewrite paths)
2. microbench outputs show comparative runtime/throughput signals for radix and bloom knobs
