# Benchmark Fixtures (13.3)

This directory stores deterministic benchmark fixtures and manifests for:

1. `tpch_sf1` (synthetic TPCH-style tables for Q1/Q3 benchmark paths)
2. `rag_synth` (synthetic vector embeddings dataset for top-k benchmarks)
3. `tpch_dbgen_sf1` (official dbgen `.tbl` outputs for SF1, generated on demand)
4. `tpch_dbgen_sf1_parquet` (deterministic parquet conversion of required dbgen SF1 tables)

Generate/update fixtures:

```bash
./scripts/generate-bench-fixtures.sh
```

Output layout:

1. `tests/bench/fixtures/index.json`
2. `tests/bench/fixtures/tpch_sf1/manifest.json`
3. `tests/bench/fixtures/rag_synth/manifest.json`

Generate official dbgen SF1 `.tbl` output:

```bash
make tpch-dbgen-sf1
```

Generated outputs land under:

1. `tests/bench/fixtures/tpch_dbgen_sf1/*.tbl`
2. `tests/bench/fixtures/tpch_dbgen_sf1/manifest.json`

Convert dbgen SF1 `.tbl` into FFQ benchmark-ready parquet:

```bash
make tpch-dbgen-parquet
```

Conversion outputs land under:

1. `tests/bench/fixtures/tpch_dbgen_sf1_parquet/customer.parquet`
2. `tests/bench/fixtures/tpch_dbgen_sf1_parquet/orders.parquet`
3. `tests/bench/fixtures/tpch_dbgen_sf1_parquet/lineitem.parquet`
4. `tests/bench/fixtures/tpch_dbgen_sf1_parquet/manifest.json`
4. Parquet files under each fixture directory

Determinism contract:

1. fixed deterministic seed (`42`)
2. fixed file names and directory layout
3. fixed schemas and row counts per file
