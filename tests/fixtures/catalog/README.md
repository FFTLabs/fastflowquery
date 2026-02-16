# Distributed Worker Catalog Fixture

This catalog is consumed by distributed worker containers in:

- `docker/compose/ffq.yml`

The worker process reads:

- `FFQ_WORKER_CATALOG_PATH=/data/catalog/tables.json`

Paths in `tables.json` are container paths and align with mounted fixture volume:

- `/data/parquet/lineitem.parquet`
- `/data/parquet/orders.parquet`
- `/data/parquet/docs.parquet`

## TPC-H official profiles (host-local)

Additional host-local catalog profiles for official dbgen parquet fixtures:

- `tpch_dbgen_sf1_parquet.tables.json`
- `tpch_dbgen_sf1_parquet.tables.toml`

These files map to:

- `tests/bench/fixtures/tpch_dbgen_sf1_parquet/customer.parquet`
- `tests/bench/fixtures/tpch_dbgen_sf1_parquet/orders.parquet`
- `tests/bench/fixtures/tpch_dbgen_sf1_parquet/lineitem.parquet`
