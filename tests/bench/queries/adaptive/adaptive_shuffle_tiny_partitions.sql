-- Adaptive shuffle scenario: many small reduce groups (high cardinality key).
SELECT
  l_orderkey AS part_key,
  SUM(l_quantity) AS sum_qty
FROM lineitem
GROUP BY l_orderkey
ORDER BY part_key;
