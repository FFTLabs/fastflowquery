# LEARN-09: Distributed Correctness - Why Results Match Embedded

This chapter explains why FFQ distributed execution should match embedded logical results in v2, which non-determinism is acceptable, and where parity is validated.

## 1) Core Claim

For the same SQL, tables, and relevant config, embedded and distributed should produce:

1. same logical schema
2. same logical row set / aggregate values
3. same semantics for join/aggregate/top-k behavior

Distributed mode changes orchestration and transport, not query meaning.

## 2) Why Semantic Equivalence Holds

### 2.1 Same planning path

Both modes share:

1. SQL parse
2. logical planning
3. optimizer and analyzer
4. physical plan generation

Distributed mode executes plan fragments over coordinator/worker stages; embedded runs locally.

### 2.2 Same operator contracts

Core operator logic is shared by semantics:

1. scan/filter/project
2. hash join
3. partial/final hash aggregate
4. top-k/vector scoring paths
5. sinks and result materialization

### 2.3 Shuffle and attempt identity correctness

Distributed correctness depends on:

1. stage dependency gating
2. map output registration keyed by attempt
3. fetch requiring exact attempt identity
4. stale attempt isolation (no accidental reuse)

### 2.4 Retry/liveness recovery keeps semantics

With failures:

1. stale worker running tasks are requeued as new attempts
2. failed attempts retry with backoff up to attempt budget
3. latest-attempt tracking ensures terminal state reflects current attempt lineage

These mechanisms change execution timing, not logical result semantics.

## 3) Capability-Aware Custom Operators and Correctness

For `PhysicalPlan::Custom`:

1. worker heartbeat advertises `custom_operator_capabilities`
2. coordinator assigns custom-op tasks only to capable workers
3. worker must have matching factory registered, else task fails explicitly

Why this matters for correctness:

1. avoids assigning custom-op work to workers that cannot execute required semantics
2. prevents silent fallback to wrong execution path

## 4) Where Parity Is Verified

Primary parity checks:

1. `make test-13.2-parity`
2. `crates/client/tests/distributed_runtime_roundtrip.rs`
3. `crates/client/tests/integration_distributed.rs`

Coverage includes:

1. join + aggregate parity
2. projection/filter scan parity
3. distributed vs embedded normalized output comparison

## 5) Normalization Strategy

Parity compares normalized outputs, not incidental execution layout.

Normalization includes:

1. stable schema checks
2. batch flattening
3. explicit row sorting by keys
4. canonical rendering for snapshot/compare
5. float tolerance handling in comparisons

This removes false mismatches from:

1. batch boundary differences
2. worker interleaving/scheduling order
3. unordered row emission where SQL has no final `ORDER BY`

## 6) Expected vs Unexpected Non-Determinism

### Expected and acceptable

1. batch counts and batch boundaries
2. task execution interleaving
3. timing/metric variance
4. row order for unordered queries

### Not acceptable

1. missing/extra rows after normalization
2. changed aggregate/group values
3. schema/type divergence for same query
4. stale-attempt data mixed into final output

## 7) Practical Debug Flow for Parity Failures

1. compare SQL text and table registrations in both modes
2. compare logical/physical explains
3. inspect normalized outputs (first differing row/column)
4. verify stage attempt lineage and shuffle registration keys
5. check worker capability availability for custom-op queries
6. inspect coordinator logs for requeue/blacklist/retry events

## 8) Code References

1. `crates/client/src/runtime.rs`
2. `crates/distributed/src/coordinator.rs`
3. `crates/distributed/src/worker.rs`
4. `crates/client/tests/distributed_runtime_roundtrip.rs`
5. `crates/client/tests/integration_distributed.rs`
6. `crates/client/tests/support/mod.rs`

## Runnable commands

```bash
make test-13.2-parity
cargo test -p ffq-distributed --features grpc coordinator_requeues_tasks_from_stale_worker
cargo test -p ffq-distributed --features grpc coordinator_assigns_custom_operator_tasks_only_to_capable_workers
```
