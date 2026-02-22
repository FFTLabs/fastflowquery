-- Window benchmark scenario: many expressions sharing partition/order keys.
SELECT
  l_returnflag,
  l_linestatus,
  l_shipdate,
  l_orderkey,
  l_quantity,
  l_extendedprice,
  ROW_NUMBER() OVER (
    PARTITION BY l_returnflag, l_linestatus
    ORDER BY l_shipdate, l_orderkey
  ) AS row_num,
  RANK() OVER (
    PARTITION BY l_returnflag, l_linestatus
    ORDER BY l_shipdate, l_orderkey
  ) AS rank_num,
  DENSE_RANK() OVER (
    PARTITION BY l_returnflag, l_linestatus
    ORDER BY l_shipdate, l_orderkey
  ) AS dense_rank_num,
  SUM(l_quantity) OVER (
    PARTITION BY l_returnflag, l_linestatus
    ORDER BY l_shipdate, l_orderkey
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS sum_qty,
  AVG(l_quantity) OVER (
    PARTITION BY l_returnflag, l_linestatus
    ORDER BY l_shipdate, l_orderkey
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS avg_qty,
  MIN(l_quantity) OVER (
    PARTITION BY l_returnflag, l_linestatus
    ORDER BY l_shipdate, l_orderkey
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS min_qty,
  MAX(l_quantity) OVER (
    PARTITION BY l_returnflag, l_linestatus
    ORDER BY l_shipdate, l_orderkey
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS max_qty,
  COUNT(*) OVER (
    PARTITION BY l_returnflag, l_linestatus
    ORDER BY l_shipdate, l_orderkey
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS count_rows
FROM lineitem
WHERE l_shipdate <= '1998-12-01';
