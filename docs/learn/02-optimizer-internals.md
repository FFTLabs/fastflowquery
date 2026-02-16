# LEARN-03: Optimizer Internals (Rule-by-Rule)

This chapter explains FFQ v1 optimizer behavior directly from `crates/planner/src/optimizer.rs`.

## Where It Sits In The Pipeline

Current v1 flow in client code is:

1. SQL frontend builds logical plan
2. optimizer rewrites logical plan
3. analyzer resolves columns/types
4. physical planner lowers to physical operators

Code path: `crates/client/src/planner_facade.rs` (`optimize_analyze`).

## Rule Order (Important)

`Optimizer::optimize(...)` applies passes in this exact order:

1. constant folding
2. filter merge
3. projection pushdown
4. predicate pushdown
5. join strategy hinting
6. vector index rewrite (feature-gated)

Order matters because later passes rely on shape changes from earlier passes.

## 1) Constant Folding

### Preconditions

1. expression subtree has literal operands
2. operation is in the supported fold set (`+,-,*,/,=,!=,<,<=,>,>=`, boolean `AND/OR/NOT`)

### Transformation

Rewrites expression-only; plan shape is unchanged.

Before:

```text
Filter ((1 = 1) AND (l_orderkey > 10))
  TableScan lineitem
```

After:

```text
Filter (l_orderkey > 10)
  TableScan lineitem
```

### Why Correct

It replaces deterministic literal subexpressions with their evaluated literal result.

### Fallback / No-Rewrite Cases

1. non-literal operands
2. unsupported literal type/operator combination
3. division by zero (left unchanged)

## 2) Filter Merge

### Preconditions

Nested filters exist:

```text
Filter p2
  Filter p1
    input
```

### Transformation

Before:

```text
Filter (o_custkey < 500)
  Filter (o_orderkey > 100)
    TableScan orders
```

After:

```text
Filter ((o_orderkey > 100) AND (o_custkey < 500))
  TableScan orders
```

### Why Correct

`Filter(p2, Filter(p1, X))` is equivalent to `Filter(p1 AND p2, X)`.

### Fallback / No-Rewrite Cases

No fallback needed; pass simply leaves non-nested filters unchanged.

## 3) Projection Pushdown

### Preconditions

1. parent operators only require subset of columns
2. source schema is known via `OptimizerContext::table_schema`

### Transformation

Columns required by projection/filter/join keys are computed and pushed into `TableScan.projection`.

Before:

```text
Projection [l_orderkey]
  Filter (l_partkey > 10)
    TableScan lineitem projection=None
```

After:

```text
Projection [l_orderkey]
  Filter (l_partkey > 10)
    TableScan lineitem projection=[l_orderkey, l_partkey]
```

Join example behavior:

1. join keys are always kept
2. ambiguous columns may be kept on both sides conservatively

### Why Correct

It only removes columns proven unused by downstream operators.

### Fallback / No-Rewrite Cases

1. no required-column restriction -> existing projection kept
2. unknown/ambiguous ownership -> conservative keep (no unsafe pruning)
3. aggregate boundary: v1 does not fully prune through aggregate outputs

## 4) Predicate Pushdown

### Preconditions

Filter appears above a pushdown-capable node:

1. `TableScan`
2. passthrough `Projection`
3. `Inner Join` (for side-specific conjuncts)

### Transformation

#### a) Into TableScan

Before:

```text
Filter (l_partkey > 10)
  TableScan lineitem pushed_filters=0
```

After:

```text
TableScan lineitem pushed_filters=1
  (l_partkey > 10)
```

#### b) Through passthrough projection

Before:

```text
Filter (l_orderkey > 1)
  Projection [l_orderkey := l_orderkey]
    TableScan lineitem
```

After:

```text
Projection [l_orderkey := l_orderkey]
  Filter (l_orderkey > 1)
    TableScan lineitem
```

#### c) Split over inner join

Before:

```text
Filter ((l_partkey > 10) AND (o_custkey < 500))
  Join on l_orderkey=o_orderkey
```

After:

```text
Join on l_orderkey=o_orderkey
  left:  Filter (l_partkey > 10)
  right: Filter (o_custkey < 500)
```

### Why Correct

Predicates referencing only one side of an inner join can be evaluated before the join without changing result semantics.

### Fallback / No-Rewrite Cases

1. non-inner joins are not split
2. conjuncts touching both sides stay above join
3. non-passthrough projections keep filter above projection

## 5) Join Strategy Hinting

### Preconditions

1. join node exists
2. table/subplan byte estimates available via `OptimizerContext::table_stats` (optional)

### Transformation

`strategy_hint` rewritten to:

1. `BroadcastLeft` if left is small and under threshold
2. `BroadcastRight` if right is small and under threshold
3. `Shuffle` otherwise

Before:

```text
Join strategy=auto
```

After (example):

```text
Join strategy=broadcast_right
```

### Why Correct

Join strategy hint changes physical distribution strategy, not SQL join semantics.

### Fallback / No-Rewrite Cases

If stats are unavailable, optimizer uses conservative `Shuffle`.

## 6) Vector Index Rewrite (Feature `vector`)

This pass rewrites eligible `TopKByScore` shapes to index-backed `VectorTopK`, and can also build a two-phase retrieval+rerank plan.

### 6.1 Direct TopK -> VectorTopK Rewrite

#### Pattern

```text
Projection
  TopKByScore(score_expr, k)
    TableScan(table)
```

#### Preconditions (all required)

1. projection must be satisfiable from `{id, score, payload}`
2. input is `TopKByScore` with `k > 0`
3. `TableScan` format is `qdrant`
4. score expression is `cosine_similarity(vector_col, vector_literal)`
5. filter translation (if present) is supported (`col = literal`, `AND`)

#### Before

```text
Projection [id, score, payload]
  TopKByScore k=5 score=cosine_similarity(emb, [..])
    TableScan table=docs projection=None pushed_filters=1
```

#### After

```text
Projection [id, score, payload]
  VectorTopK table=docs k=5 query_dim=... filter=Some(...)
```

#### Why Correct

The index provider returns the same logical contract (`id`, `score`, optional `payload`) for top-k by the same scoring expression class.

#### Fallback Cases

Any failed precondition keeps original `TopKByScore` plan.

### 6.2 Two-Phase Retrieval Rewrite

For docs-table configs with `vector.index_table` and compatible metadata, optimizer can rewrite brute-force rerank into:

1. `VectorTopK` prefetch from index table
2. join back to docs table on id
3. optional metadata filter
4. final exact `TopKByScore` rerank

Before:

```text
TopKByScore(cosine_similarity(docs.emb, q), k)
  TableScan docs
```

After (shape):

```text
TopKByScore(cosine_similarity(emb, q), k)
  Filter(...)
    Projection(...)
      Join(strategy=broadcast_right)
        TableScan docs
        VectorTopK table=docs_idx k=prefetch_k
```

### Explain Marker For Applied vs Fallback

`explain_logical` includes an explicit marker:

1. `TopKByScore ... rewrite=index_fallback`
2. `VectorTopK ... rewrite=index_applied`

This is the main debug signal for whether index rewrite happened.

Code: `crates/planner/src/explain.rs`.

## End-to-End Before/After Example (Core SQL)

### Query

```sql
SELECT l_orderkey, COUNT(l_partkey) AS c
FROM lineitem
INNER JOIN orders ON l_orderkey = o_orderkey
WHERE o_custkey < 500
GROUP BY l_orderkey
```

### Typical Before (conceptual)

```text
Aggregate
  Filter (o_custkey < 500)
    Join strategy=auto
      TableScan lineitem projection=None pushed_filters=0
      TableScan orders  projection=None pushed_filters=0
```

### Typical After (conceptual)

```text
Aggregate
  Join strategy=broadcast_right|shuffle
    TableScan lineitem projection=[l_orderkey,l_partkey] pushed_filters=0
    TableScan orders  projection=[o_orderkey,o_custkey] pushed_filters=1
      (o_custkey < 500)
```

## Correctness Model Summary

Each pass preserves logical query result by one of:

1. algebraic equivalence (`Filter` merge/split, constant fold)
2. projection safety (remove only unused columns)
3. physical-strategy hinting only (join strategy)
4. contract-preserving operator substitution (`TopKByScore` -> `VectorTopK` with guarded preconditions)

When optimizer cannot prove safety, it keeps the existing subtree (fallback).

## Useful Files For Deeper Study

1. `crates/planner/src/optimizer.rs`
2. `crates/planner/src/explain.rs`
3. `crates/planner/tests/optimizer_golden.rs`
4. `crates/planner/src/physical_planner.rs`

## Runnable command

```bash
cargo test -p ffq-planner --test optimizer_golden
```
