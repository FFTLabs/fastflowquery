# Join System v2 (EPIC 5)

- Status: draft
- Owner: @ffq-runtime
- Last Verified Commit: 7888e4c
- Last Verified Date: 2026-02-21

## Scope

This page documents EPIC 5 join-system behavior in v2:

1. radix-partitioned hash join
2. bloom-filter prefiltering for selective joins
3. targeted sort-merge join selection
4. semi/anti join semantics

Primary code references:

1. `crates/client/src/runtime.rs`
2. `crates/planner/src/physical_planner.rs`
3. `crates/planner/src/logical_plan.rs`
4. `crates/planner/src/analyzer.rs`

## 5.1 Radix-Partitioned Hash Join

Runtime hash join supports radix partitioning via config:

1. `join_radix_bits`
2. `0` means baseline hash path
3. `>0` enables radix partitioning for build/probe key buckets

Operational effect:

1. improved cache locality on large joins
2. reduced hash-table contention in large build/probe sets

Microbench entrypoint:

```bash
make bench-v2-join-radix
```

References:

1. `crates/client/examples/bench_join_radix.rs`
2. `crates/client/src/runtime.rs`

## 5.2 Bloom Filter Pushdown (Prefilter)

Hash join supports optional bloom prefiltering:

1. build side inserts join keys into bloom filter
2. probe side batches are prefiltered before full hash-match

Config knobs:

1. `join_bloom_enabled` (`true|false`)
2. `join_bloom_bits` (filter size exponent)

Microbench entrypoint:

```bash
make bench-v2-join-bloom
```

References:

1. `crates/client/examples/bench_join_bloom.rs`
2. `crates/client/src/runtime.rs`
3. `crates/client/src/runtime_tests.rs`

## 5.3 Sort-Merge Join (Targeted)

Sort-merge join strategy can be selected when configured by optimizer hinting.

Planner/runtime contract:

1. optimizer may emit `JoinStrategyHint::SortMerge`
2. runtime executes sorted-merge path for eligible join shapes
3. hash join remains fallback/default when sort-merge is not selected

Configuration:

1. `prefer_sort_merge_join` controls optimizer preference path

References:

1. `crates/planner/src/optimizer.rs`
2. `crates/planner/src/physical_planner.rs`
3. `crates/client/src/runtime.rs`
4. `crates/client/src/runtime_tests.rs`

## 5.4 Semi/Anti Joins

Semi/anti joins are first-class logical join types:

1. `JoinType::Semi`
2. `JoinType::Anti`

Semantics:

1. `SEMI`: emit left row when at least one match exists
2. `ANTI`: emit left row when no match exists
3. output schema is left-side schema

These are used directly for `EXISTS`/`IN` rewrite shapes in analyzer/decorrelation flows.

References:

1. `crates/planner/src/logical_plan.rs`
2. `crates/planner/src/analyzer.rs`
3. `crates/client/src/runtime.rs`

## Validation Commands

```bash
make bench-v2-join-radix
make bench-v2-join-bloom
cargo test -p ffq-client --test embedded_hash_join
cargo test -p ffq-client --test embedded_cte_subquery
```

Expected:

1. radix microbench reports baseline vs radix timings
2. bloom microbench reports probe reduction and timing change
3. embedded hash join suite passes (including outer/semi/anti behavior paths)
4. CTE/subquery suite passes (`EXISTS`/`IN` semijoin/antijoin rewrite semantics)
