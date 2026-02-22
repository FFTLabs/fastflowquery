-- Window benchmark scenario: skewed partitions (hot/cold bucket split).
SELECT
  CASE
    WHEN (l_orderkey % 10) = 0 THEN 'hot'
    ELSE 'cold'
  END AS skew_bucket,
  l_orderkey,
  l_shipdate,
  l_extendedprice,
  ROW_NUMBER() OVER (
    PARTITION BY CASE WHEN (l_orderkey % 10) = 0 THEN 'hot' ELSE 'cold' END
    ORDER BY l_shipdate, l_orderkey
  ) AS rn,
  SUM(l_extendedprice) OVER (
    PARTITION BY CASE WHEN (l_orderkey % 10) = 0 THEN 'hot' ELSE 'cold' END
    ORDER BY l_shipdate, l_orderkey
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS running_revenue
FROM lineitem
WHERE l_shipdate <= '1998-12-01';
