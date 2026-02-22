# SQL Semantics (v2)

- Status: verified
- Owner: @ffq-planner
- Last Verified Commit: 7888e4c
- Last Verified Date: 2026-02-21

This page is the SQL support contract for v2 as implemented now.

## Scope

Use this page to answer:

1. which SQL forms are supported
2. what semantics apply (especially NULL/subquery/CTE behavior)
3. what is not supported yet
4. what error classes/codes to expect on failure

## Support Matrix

| Area | Form | Status | Notes |
|---|---|---|---|
| Projection/filter | `SELECT ... FROM ... WHERE ...` | supported | Core path. |
| Aggregation | `GROUP BY` + `COUNT/SUM/MIN/MAX/AVG` | supported | Existing aggregate semantics apply. |
| Join | `INNER`, `LEFT`, `RIGHT`, `FULL`, `SEMI`, `ANTI` | supported | Join strategy selected by optimizer/physical planner. |
| CASE | `CASE WHEN ... THEN ... ELSE ... END` | supported | Minimal coercion rules are applied by analyzer. |
| CTE | `WITH cte AS (...)` | supported | Multi-CTE ordering and cycle detection implemented. |
| Recursive CTE | `WITH RECURSIVE ... UNION ALL ...` | supported (phase 1) | Bounded by `recursive_cte_max_depth`. |
| Uncorrelated subquery | `IN (SELECT ...)` | supported | Requires single projected subquery column. |
| Uncorrelated subquery | `EXISTS (SELECT ...)`, `NOT EXISTS (...)` | supported | Truth-table semantics implemented. |
| Scalar subquery | `a = (SELECT ...)`, `<`, `>` etc. | supported | Must return exactly one column and at most one row. |
| Correlated subquery | Correlated `EXISTS/NOT EXISTS` | supported via decorrelation | Rewritten to semijoin/antijoin shapes when supported. |
| Correlated subquery | Correlated `IN/NOT IN` | supported via decorrelation | Null-aware semantics implemented; rewritten join pipeline. |
| Set op | `UNION ALL` | supported | Implemented as concat operator. |
| Set op | `UNION` (distinct), `INTERSECT`, `EXCEPT` | not supported | Use explicit rewrites for now. |
| Ordering | General `ORDER BY` | limited | Full global sort not generally supported; vector top-k pattern remains special-case path. |
| Window | `... OVER (...)` | supported | See detailed window contract below. |

## Window SQL Contract (v2)

This section is the authoritative support contract for window SQL in v2.

### Supported window functions

Ranking/distribution:

1. `ROW_NUMBER()`
2. `RANK()`
3. `DENSE_RANK()`
4. `PERCENT_RANK()`
5. `CUME_DIST()`
6. `NTILE(n)`

Aggregate windows:

1. `COUNT(expr|*)`
2. `SUM(expr)`
3. `AVG(expr)`
4. `MIN(expr)`
5. `MAX(expr)`

Offset/value:

1. `LAG(expr [, offset [, default]])`
2. `LEAD(expr [, offset [, default]])`
3. `FIRST_VALUE(expr)`
4. `LAST_VALUE(expr)`
5. `NTH_VALUE(expr, n)`

### Supported syntax

1. `PARTITION BY ...`
2. `ORDER BY ...` with:
   - `ASC` and `DESC`
   - `NULLS FIRST` and `NULLS LAST`
3. Named windows:
   - `WINDOW w AS (...)`
   - `... OVER w`
4. Frame units:
   - `ROWS`
   - `RANGE`
   - `GROUPS`
5. Frame bounds:
   - `UNBOUNDED PRECEDING`
   - `n PRECEDING`
   - `CURRENT ROW`
   - `n FOLLOWING`
   - `UNBOUNDED FOLLOWING`
6. Frame exclusion:
   - `EXCLUDE NO OTHERS`
   - `EXCLUDE CURRENT ROW`
   - `EXCLUDE GROUP`
   - `EXCLUDE TIES`

### Frame and validation semantics

1. Invalid frame bounds are planning errors:
   - start cannot be `UNBOUNDED FOLLOWING`
   - end cannot be `UNBOUNDED PRECEDING`
   - start bound must be `<=` end bound
2. `RANGE` and `GROUPS` require `ORDER BY`.
3. `RANGE` with offset currently requires exactly one numeric `ORDER BY` key with non-null value.
4. `RANGE` without offset supports current-row and unbounded forms.

### Type and nullability rules

1. Return type:
   - `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `NTILE`, `COUNT` -> `Int64`
   - `PERCENT_RANK`, `CUME_DIST` -> `Float64`
   - `SUM`, `AVG` -> `Float64`
   - `MIN`, `MAX`, `LAG`, `LEAD`, `FIRST_VALUE`, `LAST_VALUE`, `NTH_VALUE` -> input expression type
2. `SUM`/`AVG` arguments must be numeric.
3. `LAG`/`LEAD` default must be type-compatible with the value expression.
4. Nullability:
   - ranking/distribution/count outputs are non-null
   - value/aggregate windows may be nullable per frame/expression semantics

### Determinism and ordering behavior

1. Null ordering follows explicit clause (`NULLS FIRST/LAST`) when present.
2. Ties are handled deterministically; repeated runs on unchanged data produce stable results.
3. Embedded and distributed window semantics are parity-tested for:
   - ranking
   - frame behavior (`ROWS`/`RANGE`/`GROUPS`)
   - null ordering
   - exclusion modes

### Explain visibility for windows

`EXPLAIN` includes:

1. window expressions
2. explicit/default frame details
3. sort-reuse grouping information
4. distributed strategy context where applicable

### Known limits and failure modes

1. Window execution currently materializes/sorts partition state; very large partitions can be memory-heavy.
2. `RANGE` offset frames are restricted to one numeric `ORDER BY` key.
3. Invalid shapes fail as planning/execution errors with actionable messages (for example unsupported `RANGE` frame bounds).

### Performance notes

1. Group compatible window expressions to maximize sort reuse.
2. Prefer selective filters before wide window projections.
3. Use `docs/v2/benchmarks.md` window scenarios and thresholds for regression tracking:
   - narrow partitions
   - wide partitions
   - skewed keys
   - many window expressions

## CTE Semantics

1. CTE dependency graph is validated before planning.
2. Duplicate CTE names and CTE dependency cycles are planning errors.
3. Reuse policy:
   - `inline`: CTE is expanded per reference.
   - `materialize`: repeated references can be shared via CTE reference nodes.
4. Recursive CTE (phase 1):
   - requires `UNION ALL` seed + recursive term pattern
   - recursion depth is bounded by `recursive_cte_max_depth`
   - `recursive_cte_max_depth=0` is rejected with a planning error

## Subquery Semantics

## `IN` / `NOT IN` (SQL three-valued logic)

Behavior aligns with SQL null semantics:

1. `lhs IN (rhs)`:
   - `TRUE` if any non-null rhs value equals lhs
   - `NULL` if no match and rhs contains `NULL`, or lhs is `NULL`
   - `FALSE` if no match and rhs has no `NULL`
2. `lhs NOT IN (rhs)`:
   - `FALSE` if any non-null rhs value equals lhs
   - `NULL` if no match and rhs contains `NULL`, or lhs is `NULL`
   - `TRUE` if no match and rhs has no `NULL`
3. In `WHERE`, only `TRUE` keeps rows; `FALSE` and `NULL` are filtered out.

## `EXISTS` / `NOT EXISTS`

1. `EXISTS (subquery)` is `TRUE` when subquery returns at least one row.
2. `NOT EXISTS (subquery)` is logical negation of `EXISTS`.
3. Correlated forms are decorrelated when predicate shape is supported.

## Scalar subqueries

1. Must return exactly one column.
2. Must return at most one row.
3. Multiple rows produce execution error code:
   - `E_SUBQUERY_SCALAR_ROW_VIOLATION`

## Correlation and decorelation

Supported correlated rewrite classes:

1. `EXISTS/NOT EXISTS` with simple outer-inner equality predicates
2. `IN/NOT IN` with supported equality correlation shape

Unsupported correlation shapes fail with:

1. error class: `unsupported`
2. error code: `E_SUBQUERY_UNSUPPORTED_CORRELATION`

## Error Taxonomy (Subquery/CTE)

| Code | Class | Meaning |
|---|---|---|
| `E_SUBQUERY_UNSUPPORTED_CORRELATION` | `Unsupported` | Correlated shape cannot be decorrelated by current analyzer rules. |
| `E_SUBQUERY_SCALAR_ROW_VIOLATION` | `Planning`/`Execution` | Scalar subquery has wrong shape (not 1 column) or >1 row. |
| `E_RECURSIVE_CTE_OVERFLOW` | `Planning` | Recursive CTE depth configuration prevents expansion (for example depth=0). |

CLI/REPL classify these under `[unsupported]`, `[planning]`, or `[execution]` and print hints.

## Explain Visibility

`EXPLAIN` includes rewrite metadata for subquery-related plan nodes:

1. `InSubqueryFilter ... rewrite=none`
2. `ExistsSubqueryFilter ... rewrite=none`
3. `ScalarSubqueryFilter ... rewrite=none`
4. Decorrelated joins are annotated:
   - `rewrite=decorrelated_exists_subquery`
   - `rewrite=decorrelated_not_exists_subquery`
   - `rewrite=decorrelated_in_subquery`
   - `rewrite=decorrelated_not_in_subquery`

This makes rewrite/decorrelation decisions visible without reading source code.

## Performance Notes

1. Correlated subquery support is currently rewrite-based, not a generic nested-loop engine.
2. `materialize` CTE reuse mode can reduce repeated work for multiply referenced CTEs.
3. Recursive CTE performance is bounded by configured depth; use the smallest depth that fits query intent.
4. `NOT IN` with nullable RHS can eliminate rows due to SQL null semantics; this is correctness-first behavior, not a bug.

## Practical Examples

```sql
-- Correlated EXISTS (rewritten to semijoin shape when supported)
SELECT t.k
FROM t
WHERE EXISTS (
  SELECT s.k
  FROM s
  WHERE s.k = t.k
);

-- Correlated NOT IN with null-aware semantics
SELECT t.k
FROM t
WHERE t.k NOT IN (
  SELECT s.k
  FROM s
  WHERE s.group_id = t.group_id
);

-- Recursive CTE (phase 1, UNION ALL)
WITH RECURSIVE r AS (
  SELECT 1 AS node, 0 AS depth
  UNION ALL
  SELECT node + 1, depth + 1
  FROM r
  WHERE depth < 4
)
SELECT node
FROM r;
```

## Related Pages

1. `docs/v2/quickstart.md`
2. `docs/v2/api-contract.md`
3. `docs/v2/runtime-portability.md`
4. `docs/v2/migration-v1-to-v2.md`
5. `docs/v2/testing.md`

## Correctness Evidence Map

EPIC 3 correctness is locked by these suites/artifacts:

1. CTE/subquery behavior:
   - `crates/client/tests/embedded_cte_subquery.rs`
   - `crates/client/tests/embedded_cte_subquery_golden.rs`
   - `crates/client/tests/snapshots/subquery/embedded_cte_subquery_edge_matrix.snap`
2. window behavior:
   - `crates/client/tests/embedded_window_functions.rs`
   - `crates/client/tests/embedded_window_golden.rs`
   - `crates/client/tests/snapshots/window/embedded_window_edge_matrix.snap`
3. embedded/distributed parity:
   - `crates/client/tests/distributed_runtime_roundtrip.rs`
