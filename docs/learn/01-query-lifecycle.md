# LEARN-02: End-to-End Query Lifecycle

This chapter walks one query through FFQ from SQL text to returned `RecordBatch` results.

## Recurring Example Query

We use the same query in every step:

```sql
SELECT l_orderkey, COUNT(l_partkey) AS c
FROM lineitem
INNER JOIN orders ON l_orderkey = o_orderkey
GROUP BY l_orderkey
```

This is the same query used in `tests/integration/queries/join_aggregate.sql`.

## Pipeline Shape: Conceptual vs Current v1 Call Path

Conceptually, people often explain planning as:

1. parse SQL
2. build logical plan
3. analyze names/types
4. optimize
5. lower to physical plan
6. execute

Current v1 `DataFrame::collect()` path runs:

1. parse SQL
2. build logical plan
3. optimize
4. analyze
5. lower to physical plan
6. execute

Code pointers:

1. parse + logical: `crates/client/src/engine.rs`, `crates/client/src/planner_facade.rs`, `crates/planner/src/sql_frontend.rs`
2. optimize + analyze: `crates/client/src/planner_facade.rs`
3. physical planning: `crates/planner/src/physical_planner.rs`
4. runtime execution: `crates/client/src/dataframe.rs`, `crates/client/src/runtime.rs`

## 1) SQL Parse

`Engine::sql(...)` calls `PlannerFacade::plan_sql`, which calls `ffq_planner::sql_to_logical(...)`.

`sql_to_logical`:

1. parses SQL into a `sqlparser` AST (`ffq_sql::parse_sql`)
2. rejects multi-statement input in v1
3. converts `Statement::Query` into a `LogicalPlan`

For the example query, parser output is a single `SELECT` with:

1. `FROM lineitem`
2. `INNER JOIN orders ON l_orderkey = o_orderkey`
3. projection expressions (`l_orderkey`, `COUNT(l_partkey)`)
4. `GROUP BY l_orderkey`

## 2) Initial Logical Plan

`sql_frontend.rs` builds a logical tree. For our query, the initial shape is:

```text
Projection[l_orderkey, c]
  Aggregate[group=l_orderkey, aggs=COUNT(l_partkey) AS c]
    Join[on=l_orderkey=o_orderkey, type=Inner, hint=Auto]
      TableScan[lineitem]
      TableScan[orders]
```

At this point:

1. columns are still symbolic names (not positional indexes)
2. types are not fully validated yet
3. join strategy is still logical (`Auto`)

## 3) Analyzer (Schema, Types, Safety Checks)

Analyzer responsibilities are in `crates/planner/src/analyzer.rs`.

For this query, analyzer does:

1. resolves columns against catalog schemas
2. turns columns into resolved refs (`Expr::ColumnRef { index, ... }`)
3. validates join key compatibility (`l_orderkey` vs `o_orderkey`)
4. validates aggregate typing (`COUNT` output type, group expr validity)
5. produces output schema for each node

If schema lookup fails or join key types mismatch, planning fails here with clear errors.

## 4) Optimizer (Rule-Based Rewrites)

Optimizer responsibilities are in `crates/planner/src/optimizer.rs`.
Rules run in this order:

1. constant folding
2. filter merge
3. projection pushdown
4. predicate pushdown
5. join strategy hinting
6. vector rewrite (when enabled/applicable)

For our query, important effects are:

1. table scans can be pruned to needed columns
2. join strategy hint may change from `Auto` to broadcast if table stats justify it
3. otherwise join stays `Auto`, later lowered to shuffle join shape in physical planning

## 5) Physical Plan Lowering

`create_physical_plan(...)` in `crates/planner/src/physical_planner.rs` turns logical operators into executable operators.

For this query, physical shape is:

```text
FinalHashAggregate
  Exchange(ShuffleRead)
    Exchange(ShuffleWrite hash(l_orderkey))
      PartialHashAggregate
        HashJoin
          Exchange(...) or Broadcast(...) on left/right
          Exchange(...) or Broadcast(...) on left/right
            ParquetScan[lineitem]
            ParquetScan[orders]
```

Key point: aggregate is always two-phase in v1 (`PartialHashAggregate` then `FinalHashAggregate`).

## 6) Runtime Execution

`DataFrame::collect()` calls `execute_with_schema()` in `crates/client/src/dataframe.rs`:

1. optimize + analyze logical plan
2. build physical plan
3. create `QueryContext` (`batch_size_rows`, `mem_budget_bytes`, `spill_dir`)
4. call runtime `execute(...)`
5. collect stream into `Vec<RecordBatch>`

### Embedded runtime path

`EmbeddedRuntime` in `crates/client/src/runtime.rs` recursively executes physical operators:

1. `ParquetScan` reads Arrow batches from parquet
2. `HashJoin` performs in-memory hash join, with spill path when memory budget is exceeded
3. `PartialHashAggregate` groups per partition/task
4. `ShuffleWrite/ShuffleRead` are stage-boundary operators in plan shape; embedded path evaluates them in-process
5. `FinalHashAggregate` merges partial states and emits final groups

Output becomes a stream of Arrow `RecordBatch` objects.

### Distributed runtime path

With `distributed` feature and `FFQ_COORDINATOR_ENDPOINT` set, `DistributedRuntime` is used.

Client side (`crates/client/src/runtime.rs`):

1. serialize physical plan to JSON
2. `SubmitQuery` to coordinator
3. poll `GetQueryStatus` until terminal state
4. on success, stream `FetchQueryResults` bytes
5. decode Arrow IPC stream into `RecordBatch` values

Coordinator/worker side (`crates/distributed/src/*.rs`):

1. coordinator builds stage DAG from shuffle boundaries
2. workers pull tasks with `GetTask` (capacity-based scheduling)
3. workers execute plan fragments, register map outputs, and report task state
4. final stage publishes Arrow IPC payload for query result fetch

## 7) Result Batches Returned to User

`collect()` finally returns `Vec<RecordBatch>` to the caller.

For the example query, schema is expected to be equivalent to:

1. `l_orderkey: Int64`
2. `c: Int64`

And rows are grouped counts per order key.

## Why This Works (Core Invariants)

1. same logical SQL semantics are used in embedded and distributed modes
2. planning is deterministic for same SQL + catalog + config
3. analyzer guarantees resolved/typed expressions before execution
4. stage boundaries are explicit in physical plan (`ShuffleWrite`/`ShuffleRead`)
5. final user result is always Arrow batches, independent of runtime mode

## Suggested Next Chapter

Continue with the optimizer deep dive ticket (`LEARN-03`) to study each rewrite rule with before/after plan examples.
