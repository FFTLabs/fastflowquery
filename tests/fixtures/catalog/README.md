# Distributed Worker Catalog Fixture

This catalog is consumed by distributed worker containers in:

- `docker/compose/ffq.yml`

The worker process reads:

- `FFQ_WORKER_CATALOG_PATH=/data/catalog/tables.json`

Paths in `tables.json` are container paths and align with mounted fixture volume:

- `/data/parquet/lineitem.parquet`
- `/data/parquet/orders.parquet`
- `/data/parquet/docs.parquet`
