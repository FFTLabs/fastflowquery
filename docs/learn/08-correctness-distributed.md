# LEARN-09: Distributed Correctness - Why Results Match Embedded

This chapter explains why FFQ distributed execution should return the same logical results as embedded execution, how tests compare them safely, and what non-determinism is expected vs not expected.

## 1) Core Claim

For the same SQL, catalog metadata, and compatible config, FFQ expects:

1. same logical output schema
2. same logical row set/aggregates
3. same semantics for join/aggregate/top-k operators

Embedded and distributed differ in orchestration/transport, not query meaning.

## 2) Why Semantic Equivalence Holds

### 2.1 Same planner pipeline

Both modes go through the same client planning flow:

1. SQL -> logical plan
2. optimizer rewrites
3. analyzer resolution/type checks
4. physical plan creation

Physical plan is then:

1. executed locally (embedded), or
2. serialized/submitted to coordinator+workers (distributed)

### 2.2 Same physical operator semantics

Operator semantics are intended to match:

1. `HashJoin`: same equi-join logic
2. `PartialHashAggregate` + `FinalHashAggregate`: same grouped aggregate logic
3. `TopKByScore`: same score/evaluation logic
4. `Filter`/`Project`/`Limit`: same expression and row semantics

Distributed mode adds:

1. stage scheduling
2. shuffle read/write transport
3. result IPC streaming

These are data-movement concerns, not semantic operator changes.

### 2.3 Stage/shuffle preserves partition-correctness

Shuffle contracts ensure key correctness:

1. rows with same partition key are routed to same reduce partition
2. final aggregations and join probes read the required partition data
3. attempt identity avoids mixing stale outputs into current attempt flow

## 3) Where Equivalence Is Verified in Tests

Primary parity test:

1. `crates/client/tests/integration_distributed.rs`

What it does:

1. run shared query suite in distributed mode
2. run the same queries in embedded mode (same fixture files and table schemas)
3. normalize both outputs
4. assert equality of normalized text snapshots

Queries covered in parity loop:

1. `scan_filter_project`
2. `join_projection`
3. `join_aggregate`

Shared SQL sources:

1. `tests/integration/queries/*.sql`
2. exposed via `crates/client/tests/support/mod.rs::integration_queries`

## 4) Normalization Strategy (Why Comparisons Are Stable)

Normalization helper:

1. `snapshot_text(...)` in `crates/client/tests/support/mod.rs`

Normalization behavior:

1. verify batch schemas are consistent
2. flatten all batches into row records
3. sort rows by explicit sort keys
4. render canonical row text (`col=value|...`)
5. apply float rounding/tolerance policy in value rendering path

This avoids false mismatches from:

1. batch boundary differences
2. worker scheduling order differences
3. non-semantic row ordering differences

## 5) Logical Determinism vs Physical Non-Determinism

### 5.1 Expected non-determinism (acceptable)

These may vary run-to-run without indicating correctness bugs:

1. order of rows when query does not define global ordering
2. number/shape of intermediate batches
3. task execution interleavings across workers
4. exact timing and metric values

### 5.2 Logical determinism required

These must remain stable:

1. final row set (modulo ordering when unordered)
2. final aggregates/group counts/sums/etc.
3. final join match semantics
4. schema and data types of result columns

Parity tests intentionally compare logical outputs, not incidental physical ordering.

## 6) Additional Determinism Anchors in Engine

Engine internals include explicit stabilizers:

1. aggregate output keys are sorted before output batch creation
2. top-k tie handling uses deterministic sequence tiebreak
3. shared fixtures are deterministic and reused between modes

These reduce flakiness and strengthen parity guarantees.

## 7) Known Boundaries and Assumptions

Equivalence assumes:

1. identical table definitions and schemas registered in both modes
2. distributed cluster healthy and running expected code/config
3. no unsupported operator/feature path divergence

Current v1 boundaries to keep in mind:

1. cancellation and retry orchestration are intentionally minimal
2. heartbeat is advisory in control plane
3. parity suite currently focuses on representative core queries (scan/join/agg)

## 8) Practical Parity Debug Checklist

If distributed != embedded:

1. compare optimized logical explain for the same SQL
2. validate table schemas/options match in both runs
3. inspect normalized snapshot texts for first differing row/column
4. verify shuffle attempt and partition selection behavior
5. inspect join key resolution and aggregate group key typing

Key files:

1. `crates/client/tests/integration_distributed.rs`
2. `crates/client/tests/support/mod.rs`
3. `crates/client/src/runtime.rs`
4. `crates/distributed/src/worker.rs`
5. `crates/distributed/src/coordinator.rs`

## 9) Bottom Line

FFQ distributed correctness is based on:

1. same planned semantics,
2. same operator contracts,
3. transport/scheduling layers that preserve key-partition correctness,
4. parity tests that compare normalized logical outputs rather than unstable physical ordering.

## Runnable command

```bash
make test-13.2-parity
```
