# Lab 04: Official Benchmark + Correctness Gate

Goal: generate official TPC-H SF1 fixtures, run official benchmark flow, and verify correctness gate behavior.

## Prerequisites

1. C toolchain available for `dbgen` build (`gcc`/`make`).
2. Enough disk space for generated fixtures and result artifacts.
3. Optional: running distributed cluster for distributed benchmark mode.

## Steps

1. Build pinned `dbgen` tool:

```bash
make tpch-dbgen-build
```

2. Generate official SF1 `.tbl` data:

```bash
make tpch-dbgen-sf1
```

3. Convert `.tbl` to deterministic parquet set:

```bash
make tpch-dbgen-parquet
```

4. Validate fixture manifests:

```bash
make validate-tpch-dbgen-manifests
```

5. Run official embedded benchmark (Q1/Q3 + correctness checks):

```bash
make bench-13.4-official-embedded
```

6. Optional distributed official benchmark:

```bash
FFQ_COORDINATOR_ENDPOINT=http://127.0.0.1:50051 make bench-13.4-official-distributed
```

## Expected Output

1. Steps 1-4 finish with exit code `0` and produce manifests under:
   - `tests/bench/fixtures/tpch_dbgen_sf1/manifest.json`
   - `tests/bench/fixtures/tpch_dbgen_sf1_parquet/manifest.json`
2. Step 5 creates JSON/CSV artifacts under:
   - `tests/bench/results/official_tpch/`
3. If correctness checks fail, benchmark exits non-zero and reports divergence in `results[].error`.

## Troubleshooting

1. `pathspec ... did not match` during dbgen build:
   - your pinned ref is missing in local clone; fetch tags/branches and rerun build.
2. `Open failed for ./dists.dss` during data generation:
   - rerun `make tpch-dbgen-sf1` from repo root (script handles proper working dir).
3. Missing parquet fixture error in benchmark run:
   - run `make tpch-dbgen-parquet` before benchmark command.
4. Manifest validation failure:
   - regenerate via steps 2-3 and rerun `make validate-tpch-dbgen-manifests`.
5. Distributed benchmark endpoint failures:
   - verify coordinator/worker health and `FFQ_COORDINATOR_ENDPOINT`.
