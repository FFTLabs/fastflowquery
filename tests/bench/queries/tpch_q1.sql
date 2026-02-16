SELECT
  l_returnflag,
  COUNT(l_orderkey) AS count_order,
  SUM(l_quantity) AS sum_qty
FROM lineitem
WHERE l_shipdate <= '1998-09-02'
GROUP BY l_returnflag
