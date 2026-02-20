-- Adaptive shuffle scenario: mixed join + aggregate workload.
SELECT
  CASE
    WHEN o.o_custkey <= 20 THEN 0
    WHEN o.o_custkey <= 40 THEN 1
    ELSE 2
  END AS bucket,
  COUNT(*) AS row_cnt,
  SUM(l.l_quantity) AS sum_qty
FROM orders o
JOIN lineitem l ON o.o_orderkey = l.l_orderkey
GROUP BY
  CASE
    WHEN o.o_custkey <= 20 THEN 0
    WHEN o.o_custkey <= 40 THEN 1
    ELSE 2
  END
ORDER BY bucket;
