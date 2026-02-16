SELECT l_orderkey, COUNT(l_partkey) AS c
FROM lineitem
INNER JOIN orders ON l_orderkey = o_orderkey
GROUP BY l_orderkey
