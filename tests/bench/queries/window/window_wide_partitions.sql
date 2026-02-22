-- Window benchmark scenario: wide partitions (low-cardinality partition key).
SELECT
  l_returnflag,
  l_linestatus,
  l_shipdate,
  l_quantity,
  RANK() OVER (
    PARTITION BY l_returnflag, l_linestatus
    ORDER BY l_shipdate, l_orderkey
  ) AS rnk,
  SUM(l_quantity) OVER (
    PARTITION BY l_returnflag, l_linestatus
    ORDER BY l_shipdate, l_orderkey
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS running_qty
FROM lineitem
WHERE l_shipdate <= '1998-12-01';
