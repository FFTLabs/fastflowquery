-- Canonical TPC-H Q1 for FFQ benchmark runs (official profile path).
-- Adaptation notes:
-- 1. Reduced projection set to metrics currently supported in FFQ v1 benchmark contract.
-- 2. Date predicate uses literal compare (`<= '1998-09-02'`) and avoids INTERVAL syntax.
-- 3. Query text is shared by embedded and distributed benchmark runners.
SELECT
  l_returnflag,
  COUNT(l_orderkey) AS count_order,
  SUM(l_quantity) AS sum_qty
FROM lineitem
WHERE l_shipdate <= '1998-09-02'
GROUP BY l_returnflag
