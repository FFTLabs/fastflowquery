-- Adaptive shuffle scenario: mixed join + filter + aggregate workload.
SELECT
  o_shippriority AS bucket,
  COUNT(1) AS row_cnt,
  SUM(l_extendedprice) AS sum_price
FROM lineitem
INNER JOIN orders ON l_orderkey = o_orderkey
WHERE o_orderdate < '1995-03-15'
GROUP BY o_shippriority;
