-- Adaptive shuffle scenario: skewed join (few hot keys dominate output).
SELECT
  l_orderkey AS part_key,
  COUNT(1) AS row_cnt,
  SUM(l_quantity) AS sum_qty
FROM lineitem
INNER JOIN orders ON l_orderkey = o_orderkey
WHERE l_orderkey <= 2
GROUP BY l_orderkey;
