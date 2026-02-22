-- Window benchmark scenario: narrow partitions (high-cardinality partition key).
SELECT
  l_orderkey,
  l_quantity,
  ROW_NUMBER() OVER (
    PARTITION BY l_orderkey
    ORDER BY l_shipdate, l_extendedprice DESC
  ) AS rn,
  SUM(l_extendedprice) OVER (
    PARTITION BY l_orderkey
    ORDER BY l_shipdate
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS running_revenue
FROM lineitem
WHERE l_shipdate <= '1998-12-01';
