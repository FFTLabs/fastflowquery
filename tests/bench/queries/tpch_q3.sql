SELECT
  l_orderkey,
  SUM(l_extendedprice) AS revenue,
  o_orderdate,
  o_shippriority
FROM lineitem
INNER JOIN orders ON l_orderkey = o_orderkey
WHERE o_orderdate < '1995-03-15'
  AND l_shipdate > '1995-03-15'
GROUP BY l_orderkey, o_orderdate, o_shippriority
LIMIT 10
