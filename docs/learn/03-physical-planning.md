# LEARN-04: Physical Planning and Operator Contracts

This chapter explains how FFQ lowers logical plans to physical plans, where boundaries are inserted, and what each physical operator promises at execution time.

## 1) Role of Physical Planning

Logical plan answers: "what query semantics?"
Physical plan answers: "what executable operator graph?"

Entry point:

1. `ffq_planner::create_physical_plan(...)` in `crates/planner/src/physical_planner.rs`

Physical operator definitions:

1. `crates/planner/src/physical_plan.rs`

## 2) Logical -> Physical Mapping

The mapping in v1 is direct for many nodes, with special expansion for aggregates and joins.

1. `LogicalPlan::TableScan` -> `PhysicalPlan::ParquetScan`
2. `LogicalPlan::Filter` -> `PhysicalPlan::Filter`
3. `LogicalPlan::Projection` -> `PhysicalPlan::Project`
4. `LogicalPlan::Limit` -> `PhysicalPlan::Limit`
5. `LogicalPlan::TopKByScore` -> `PhysicalPlan::TopKByScore`
6. `LogicalPlan::VectorTopK` -> `PhysicalPlan::VectorTopK`
7. `LogicalPlan::InsertInto` -> `PhysicalPlan::ParquetWrite`

Special expansions:

1. `LogicalPlan::Aggregate` expands into:
   - `PartialHashAggregate`
   - `Exchange(ShuffleWrite hash(keys))`
   - `Exchange(ShuffleRead hash(keys))`
   - `FinalHashAggregate`
2. `LogicalPlan::Join` expands depending on `strategy_hint`:
   - broadcast left/right adds `Exchange(Broadcast)` on build side
   - shuffle/auto adds `Exchange(ShuffleWrite/ShuffleRead)` on both sides

## 3) Operator Boundaries

### 3.1 Stage Boundaries (Distributed)

`Exchange(ShuffleRead)` is treated as stage-cut boundary by stage DAG builder.

Code:

1. `crates/distributed/src/stage.rs`

Implication:

1. upstream subtree before a `ShuffleRead` runs in a different stage
2. downstream subtree runs in consumer stage

### 3.2 Data Boundary Contract

Shuffle boundary contract is partitioned by `PartitioningSpec::HashKeys { keys, partitions }` (or `Single`).

For correctness:

1. same key hashing on write/read side
2. all rows with same key must land in same reduce partition

### 3.3 Sink Boundary

`ParquetWrite` is terminal/sink-like for write plans:

1. consumes child batches
2. persists output
3. returns empty schema/empty batches in runtime path

## 4) Schema Propagation Model

FFQ v1 schema propagation has two phases:

1. analyzer computes resolved schemas on logical plan (`crates/planner/src/analyzer.rs`)
2. runtime stream carries concrete `SchemaRef` with `RecordBatchStream::schema()`

Important detail:

1. physical plan nodes do not carry full output schema fields in struct definitions
2. executors derive/output schema during execution from child schema + expressions

Examples:

1. `Project` computes output field names/types from `(expr, alias)`
2. `Filter` keeps child schema unchanged
3. `Limit` keeps child schema unchanged
4. `TopKByScore` keeps child schema unchanged (row subset + ordering)
5. `VectorTopK` returns fixed schema contract: `id:Int64`, `score:Float32`, `payload:Utf8?`

## 5) Execution Contracts (Per Operator)

Execution interface foundations:

1. storage/execution operator node contract: `ExecNode` (`crates/execution/src/exec_node.rs`)
2. output stream contract: `SendableRecordBatchStream` + `schema()` (`crates/execution/src/stream.rs`)
3. runtime contract: `Runtime::execute(plan, ctx, catalog)` (`crates/client/src/runtime.rs`)

Below are the practical contracts for v1 physical operators.

### 5.1 ParquetScan

Input:

1. table name + optional projection + pushed filters (logical predicates as strings for provider path)

Output:

1. stream of `RecordBatch` rows from parquet paths
2. schema from table schema/catalog/provider

Failure modes:

1. non-parquet format for parquet provider
2. file open/decode failures

### 5.2 Filter

Input:

1. child stream
2. boolean predicate expression

Output:

1. same schema as child
2. subset of rows

Failure modes:

1. predicate not boolean
2. expression evaluation errors

### 5.3 Project

Input:

1. child stream
2. `(expr, output_name)` list

Output:

1. new schema from projected expressions
2. same row count as child

Failure modes:

1. expression compile/evaluation errors
2. record batch construction errors (type/length mismatch)

### 5.4 HashJoin

Input:

1. left and right child streams
2. equi-join key pairs
3. build side hint (left/right)

Output:

1. joined schema (left + right fields)
2. joined rows (inner join semantics in v1)

Failure modes:

1. missing join key columns
2. schema/type mismatches
3. spill IO errors on grace-style fallback path

### 5.5 PartialHashAggregate / FinalHashAggregate

Input:

1. grouping expressions
2. aggregate expressions
3. child stream (raw rows for partial, shuffled partial rows for final)

Output:

1. grouped aggregate rows
2. two-phase correctness equivalent to single global aggregate

Failure modes:

1. unsupported group expression shape at planning (non-column group key in v1 physical planner)
2. aggregate state/type errors
3. spill IO errors on low-memory path

### 5.6 Exchange (ShuffleWrite / ShuffleRead / Broadcast)

Input:

1. child stream
2. partitioning spec

Output:

1. boundary-preserving stream semantics for downstream operator
2. in distributed mode, materialized/transported shuffle partitions

Failure modes:

1. shuffle file/index read/write errors
2. missing map output registration
3. stale attempt handling (older attempts ignored by latest-attempt reads)

### 5.7 Limit

Input:

1. child stream
2. `n`

Output:

1. first `n` rows in child order
2. same schema as child

Failure modes:

1. no special failure beyond child failure

### 5.8 TopKByScore

Input:

1. child stream
2. score expression (must evaluate to float)
3. `k`

Output:

1. same schema as child
2. top-k rows by descending score (single/few output batches)

Failure modes:

1. non-float score expression
2. expression evaluation errors

### 5.9 VectorTopK

Input:

1. index table
2. query vector
3. `k`
4. optional serialized filter

Output:

1. fixed schema: `(id, score, payload?)`
2. top-k rows from vector provider

Failure modes:

1. table not qdrant-format for index-backed path
2. `qdrant` feature disabled
3. provider/filter errors

### 5.10 ParquetWrite

Input:

1. child stream
2. target table

Output:

1. side effect: durable parquet write
2. runtime output stream is empty-schema/empty-batches

Failure modes:

1. filesystem write/commit failures
2. catalog/table resolution errors

## 6) Contract Between Planner and Runtime

Planner guarantees:

1. physical graph shape is executable for v1-supported features
2. join/aggregate include required exchanges for distribution correctness
3. unsupported shapes fail early (planning error)

Runtime guarantees:

1. executes physical graph and returns `SendableRecordBatchStream`
2. stream reports schema via `schema()`
3. errors propagate as execution errors, not silent truncation

## 7) Minimal Example: Aggregate Lowering

Logical:

```text
Aggregate[group=l_orderkey, agg=COUNT(l_partkey)]
  Join(...)
```

Physical:

```text
FinalHashAggregate
  Exchange(ShuffleRead hash(l_orderkey))
    Exchange(ShuffleWrite hash(l_orderkey))
      PartialHashAggregate
        HashJoin(...)
```

Why this shape:

1. partial aggregates reduce data before shuffle
2. shuffle co-locates same group keys
3. final aggregate merges partial states deterministically

## 8) Practical Debug Checklist

When a query misbehaves, inspect in this order:

1. optimized logical explain (`DataFrame::explain()`)
2. expected physical expansion from mapping rules above
3. whether stage boundaries (`ShuffleRead`) are where expected
4. operator input schema and output schema contract at failing node
5. runtime mode differences (embedded vs distributed transport)
