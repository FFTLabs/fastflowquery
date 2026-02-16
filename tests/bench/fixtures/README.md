# Benchmark Fixtures (13.3)

This directory stores deterministic benchmark fixtures and manifests for:

1. `tpch_sf1` (synthetic TPCH-style tables for Q1/Q3 benchmark paths)
2. `rag_synth` (synthetic vector embeddings dataset for top-k benchmarks)

Generate/update fixtures:

```bash
./scripts/generate-bench-fixtures.sh
```

Output layout:

1. `tests/bench/fixtures/index.json`
2. `tests/bench/fixtures/tpch_sf1/manifest.json`
3. `tests/bench/fixtures/rag_synth/manifest.json`
4. Parquet files under each fixture directory

Determinism contract:

1. fixed deterministic seed (`42`)
2. fixed file names and directory layout
3. fixed schemas and row counts per file
