-- Canonical TPC-H Q3 for FFQ benchmark runs (official profile path).
-- Adaptation notes:
-- 1. Segment filter/join to `customer` is omitted in v1 because current fixture scope focuses on
--    `orders` + `lineitem` coverage for join/filter/aggregate performance paths.
-- 2. Revenue formula is simplified to `SUM(l_extendedprice)` (no discount multiplier) in v1.
-- 3. Query text is shared by embedded and distributed benchmark runners.
SELECT
  l_orderkey,
  SUM(l_extendedprice) AS revenue,
  o_orderdate,
  o_shippriority
FROM lineitem
INNER JOIN orders ON l_orderkey = o_orderkey
WHERE o_orderdate < '1995-03-15'
  AND l_shipdate > '1995-03-15'
GROUP BY l_orderkey, o_orderdate, o_shippriority
LIMIT 10
