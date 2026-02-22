# LEARN-16: SQL Semantics in v2 (EPIC 3)

This chapter explains the EPIC 3 SQL semantics surface in learner form:

1. which SQL constructs are supported in v2
2. how correctness is preserved for CTE/subquery/window paths
3. where edge cases (NULLs, scalar subquery shape, recursive limits) matter

Primary reference:

1. `docs/v2/sql-semantics.md`

## 1) Mental Model

EPIC 3 adds SQL semantics in layers:

1. join/type expressions (`OUTER JOIN`, `CASE`)
2. CTE + subquery analysis/rewrites
3. window planning/execution

The design principle is:

1. keep SQL behavior explicit and test-backed
2. expose rewrite decisions via `EXPLAIN`
3. preserve embedded/distributed parity

## 2) Outer Joins and CASE

Supported join forms:

1. `INNER`
2. `LEFT`
3. `RIGHT`
4. `FULL`
5. `SEMI`
6. `ANTI`

CASE support:

1. `CASE WHEN ... THEN ... ELSE ... END` in projection/filter
2. analyzer applies minimal coercion rules

## 3) CTE + Subquery Semantics

CTE behavior:

1. dependency graph is validated before planning
2. duplicate names/cycles are rejected
3. recursive CTE phase-1 uses bounded depth

Subquery behavior:

1. uncorrelated `IN`, `EXISTS`, `NOT EXISTS` supported
2. scalar subqueries must be one column and at most one row
3. correlated `EXISTS`/`IN` forms use decorrelation rewrites where supported

Important null semantics:

1. `IN/NOT IN` follows SQL three-valued logic (`TRUE/FALSE/NULL`)
2. in `WHERE`, only `TRUE` keeps rows

## 4) Window Semantics

Window support includes:

1. ranking/distribution (`ROW_NUMBER`, `RANK`, `DENSE_RANK`, `PERCENT_RANK`, `CUME_DIST`, `NTILE`)
2. aggregate windows (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`)
3. value windows (`LAG`, `LEAD`, `FIRST_VALUE`, `LAST_VALUE`, `NTH_VALUE`)
4. frame units (`ROWS`, `RANGE`, `GROUPS`) with exclusion forms
5. named windows and explicit null ordering

Correctness anchors:

1. deterministic tie handling
2. explicit null ordering semantics
3. parity tests between embedded and distributed execution

## 5) Explain + Error Taxonomy

`EXPLAIN` should surface:

1. subquery rewrite/decorrelation decisions
2. window frame/grouping details

Typical actionable failures:

1. unsupported correlation shape
2. scalar subquery row-shape violation
3. recursive CTE depth overflow

## 6) Reproducible Verification

```bash
cargo test -p ffq-client --test embedded_hash_join
cargo test -p ffq-client --test embedded_case_expr
cargo test -p ffq-client --test embedded_cte_subquery
cargo test -p ffq-client --test embedded_cte_subquery_golden
cargo test -p ffq-client --test embedded_window_functions
cargo test -p ffq-client --test embedded_window_golden
cargo test -p ffq-client --test distributed_runtime_roundtrip
```

## 7) Practical Notes

1. not all SQL standard set operations are in scope (`UNION` distinct / `INTERSECT` / `EXCEPT` remain limited).
2. recursive CTE and large window workloads should be configured carefully for depth/memory.
3. use `docs/v2/sql-semantics.md` as the definitive support matrix.

## 8) Code References

1. `crates/planner/src/sql_frontend.rs`
2. `crates/planner/src/analyzer.rs`
3. `crates/planner/src/optimizer.rs`
4. `crates/planner/src/explain.rs`
5. `crates/client/src/runtime.rs`
