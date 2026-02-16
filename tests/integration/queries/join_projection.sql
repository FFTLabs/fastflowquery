SELECT l_orderkey, l_partkey, o_custkey
FROM lineitem
INNER JOIN orders ON l_orderkey = o_orderkey
