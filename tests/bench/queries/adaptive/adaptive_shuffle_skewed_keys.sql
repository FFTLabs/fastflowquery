-- Adaptive shuffle scenario: heavy skew on one hot key.
SELECT
  CASE
    WHEN l_orderkey <= 2 THEN 0
    ELSE l_orderkey
  END AS part_key,
  COUNT(*) AS row_cnt,
  SUM(l_quantity) AS sum_qty
FROM lineitem
GROUP BY
  CASE
    WHEN l_orderkey <= 2 THEN 0
    ELSE l_orderkey
  END
ORDER BY part_key;
