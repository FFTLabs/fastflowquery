# TPC-H dbgen SF1 Parquet Conversion Output

This directory stores deterministic parquet conversion output from:

1. `tests/bench/fixtures/tpch_dbgen_sf1/*.tbl`

Generate parquet conversion:

```bash
make tpch-dbgen-parquet
```

Expected files:

1. `customer.parquet`
2. `orders.parquet`
3. `lineitem.parquet`
4. `manifest.json`
