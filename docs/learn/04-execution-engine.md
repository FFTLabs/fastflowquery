# LEARN-05: Execution Engine Internals

This chapter explains how FFQ executes physical plans at runtime, with focus on streaming, batch flow, memory/spill behavior, determinism guarantees, and operator failure modes.

## 1) Runtime Entry and Stream Contract

Execution entry:

1. `DataFrame::execute_with_schema()` in `crates/client/src/dataframe.rs`
2. builds `QueryContext { batch_size_rows, mem_budget_bytes, spill_dir }`
3. calls `runtime.execute(physical_plan, ctx, catalog_snapshot)`

Runtime trait:

1. `Runtime::execute(...) -> SendableRecordBatchStream`

Stream contract:

1. every stream reports a `SchemaRef` (`RecordBatchStream::schema()`)
2. stream item type is `Result<RecordBatch>`
3. errors are propagated through the stream (not silently dropped)

Core types:

1. `crates/execution/src/stream.rs`
2. `crates/execution/src/exec_node.rs`

## 2) Batch Flow Model (Embedded)

Embedded runtime (`crates/client/src/runtime.rs`) currently evaluates operator subtrees recursively:

1. `execute_plan(...)` executes child operators first
2. materializes child output as `Vec<RecordBatch>` (`ExecOutput`)
3. applies current operator logic
4. wraps final output back into stream via `StreamAdapter`

So v1 is stream-shaped at API boundaries but mostly eager/materialized inside operator evaluation.

## 3) Operator Dispatch Path

`execute_plan(...)` pattern-matches `PhysicalPlan` and handles:

1. `ParquetScan`
2. `ParquetWrite`
3. `Project`
4. `Filter`
5. `Limit`
6. `TopKByScore`
7. `VectorTopK`
8. `Exchange` (`ShuffleWrite/ShuffleRead/Broadcast`)
9. `PartialHashAggregate`
10. `FinalHashAggregate`
11. `HashJoin`

Any unsupported physical node fails fast with `FfqError::Unsupported`.

## 4) Memory Budget and Spill Behavior

The runtime memory knob is `QueryContext.mem_budget_bytes`.

Semantics:

1. `0` means "do not enforce spill threshold"
2. `>0` enables spill threshold checks in join and aggregate paths

Spill directory:

1. `QueryContext.spill_dir`
2. runtime creates it when spill is needed
3. spill files are temporary and removed after merge/use

### 4.1 Aggregate spill

Path:

1. `run_hash_aggregate(...)`
2. accumulates group hash map `HashMap<Vec<ScalarValue>, Vec<AggState>>`
3. `maybe_spill(...)` checks estimated state size (`estimate_groups_bytes`)
4. if above budget:
   - serialize states to JSONL (`agg_spill_<suffix>.jsonl`)
   - record spill metrics
   - clear in-memory groups
5. end of input:
   - merge spill files back (`merge_spill_file`)
   - finalize output batch

Failure modes:

1. spill serialize/deserialize IO errors
2. spill state shape mismatch
3. spill state type mismatch

### 4.2 Join spill (grace-style)

Path:

1. `run_hash_join(...)` estimates build-side footprint
2. if build side exceeds budget:
   - use `grace_hash_join(...)`
3. grace join:
   - partition build/probe rows into N files by hashed join key
   - read matching partitions
   - build per-partition hash table and probe
   - clean up partition files

Failure modes:

1. join key resolution errors
2. spill encode/decode errors
3. spill file IO errors

## 5) Join Internals

In-memory join path:

1. flatten input batches into row vectors (`rows_from_batches`)
2. resolve join key indexes from schema
3. build hash table: `key -> build-row indexes`
4. probe rows and emit joined rows
5. reconstruct output batch with left+right schema concatenation

Contract:

1. v1 join type is inner join only
2. output schema is left fields followed by right fields
3. build side affects internal order/combine behavior, not SQL semantics

Primary failure modes:

1. join key missing in schema
2. row->array conversion/type mismatch during output batch build

## 6) Aggregate Internals

Two modes are used by physical plan:

1. `PartialHashAggregate`
2. `FinalHashAggregate`

State model:

1. `AggState::{Count, SumInt, SumFloat, Min, Max, Avg{sum,count}}`
2. keyed by grouped scalar key vector

Special handling:

1. for `AVG`, partial stage emits hidden count columns (`__ffq_avg_count_<name>`)
2. final stage merges partial sums/counts and computes final average
3. empty-input no-group query emits one row with initialized states

Deterministic output rule:

1. keys are sorted (`format!("{key:?}")`) before output batch construction
2. this stabilizes group row order across runs

Primary failure modes:

1. unsupported/non-resolvable group column typing
2. aggregate state merge type mismatch
3. expression evaluation failures

## 7) Top-K Internals

`run_topk_by_score(...)`:

1. evaluates score expression per row
2. maintains fixed-size min-heap (`BinaryHeap<Reverse<TopKEntry>>`)
3. keeps only top `k` rows
4. emits selected rows as one concatenated batch

Determinism mechanism:

1. heap ordering uses score then insertion sequence (`seq`)
2. ties are resolved deterministically by sequence order
3. tests assert deterministic tie behavior in runtime test module

Contract:

1. score expression must evaluate to `Float32` or `Float64`
2. null scores are ignored

Failure modes:

1. non-float score output
2. expression evaluation/concat failures

## 8) Determinism Guarantees (v1 Scope)

Engine-level deterministic behaviors:

1. aggregate output key sorting before materialization
2. deterministic top-k tie handling (`seq` tiebreaker)
3. deterministic hash partition mapping for spill files (`hash_key`)

Non-goals / caveats:

1. full global output ordering is not implied unless query explicitly enforces it
2. floating-point results follow normal FP semantics/tolerance expectations

## 9) Metrics and Tracing Hooks During Execution

Per-operator metrics:

1. runtime records rows/batches/bytes/time via `global_metrics().record_operator(...)`

Spill metrics:

1. join and aggregate spill paths call `global_metrics().record_spill(...)`

Tracing:

1. query/operator spans include `query_id`, `stage_id`, `task_id`, `operator`

This is the primary observability path for debugging slow or unstable executions.

## 10) Failure Mode Summary Checklist

When execution fails, common categories are:

1. planning/runtime mismatch (`unsupported operator`)
2. schema/key resolution (`join key not found`, group column unknown)
3. expression typing (`predicate not boolean`, top-k score not float)
4. spill IO/serialization errors
5. batch build failures (array/type/length mismatch)

## 11) Embedded vs Distributed Engine Note

Distributed mode uses coordinator/worker orchestration, but task execution semantics are aligned with the same physical-operator behavior model. The client still receives Arrow IPC-decoded `RecordBatch` streams.

For control-plane and stage execution details, see:

1. `docs/v1/distributed-runtime.md`
2. `docs/v1/shuffle-stage-model.md`

## Runnable command

```bash
cargo test -p ffq-client --test embedded_hash_join
```
