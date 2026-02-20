-- Adaptive shuffle scenario: coarse keying allows stronger coalescing.
SELECT
  CASE
    WHEN l_orderkey <= 2 THEN 0
    ELSE 1
  END AS part_key,
  SUM(l_extendedprice) AS sum_price
FROM lineitem
GROUP BY
  CASE
    WHEN l_orderkey <= 2 THEN 0
    ELSE 1
  END;
