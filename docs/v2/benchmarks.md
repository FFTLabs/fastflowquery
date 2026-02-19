# Benchmarks (v2 Bootstrap)

- Status: draft
- Owner: @ffq-docs
- Last Verified Commit: TBD
- Last Verified Date: TBD
- Source: inherited/adapted from prior version docs; v2 verification pending


This page bootstraps the v2 benchmark contract: what is measured, how runs are configured, and how outputs/regressions are evaluated.

## Scope

Benchmark scope (bootstrap from prior implementation):

1. TPC-H SF1:
   - Q1 (aggregation-heavy path)
   - Q3 (join + filter path)
2. RAG:
   - synthetic embeddings dataset with configurable `N` docs, dimension `D`
   - brute-force top-k baseline
   - optional qdrant top-k path when `qdrant` feature is enabled

Out of scope for this contract:

1. Absolute hardware-independent performance targets.
2. Cross-machine comparability without hardware metadata.
3. Full TPC-H query set beyond Q1/Q3 in v1.

## Benchmark Tracks (Synthetic vs Official)

FFQ v1 has two benchmark tracks with different goals:

| Track | Dataset source | Primary use | Query scope | Speed | Reportability |
|---|---|---|---|---|---|
| Synthetic dev loop | `tests/bench/fixtures/tpch_sf1` + `rag_synth` | fast iteration and regression triage during development | TPC-H Q1/Q3 + RAG matrix | fastest to run | not for external reporting |
| Official dbgen | `tests/bench/fixtures/tpch_dbgen_sf1_parquet` | reportable TPC-H numbers and release/perf signoff | TPC-H Q1/Q3 | slower | yes (v1 official path) |

When to use each track:

1. Use synthetic for daily PR checks, optimizer/runtime iteration, and quick performance comparisons.
2. Use official dbgen before publishing numbers, before release cut, and whenever reproducibility assertions are required.
3. Do not mix synthetic and official results in a single regression comparison baseline.

Interpretation contract:

1. Synthetic results are trend indicators only.
2. Official results are authoritative for TPC-H Q1/Q3 in v1.
3. If synthetic and official disagree on trend, treat official as the deciding signal.

## Official dbgen Integration (13.4.1)

The repository includes tooling to build and run TPC-H `dbgen` and generate official-style SF1 `.tbl` data under:

1. `tests/bench/fixtures/tpch_dbgen_sf1/`

Pinned defaults:

1. Source repo: `https://github.com/electrum/tpch-dbgen.git`
2. Source ref: `32f1c1b92d1664dba542e927d23d86ffa57aa253` (override with `TPCH_DBGEN_REF`)
3. Scale factor: `1` (SF1)

One-command generation:

```bash
make tpch-dbgen-sf1
```

This runs:

1. `scripts/build-tpch-dbgen.sh`
2. `scripts/generate-tpch-dbgen-sf1.sh`

Generation output:

1. all required `*.tbl` files for SF1
2. `manifest.json` with rows, bytes, sha256 per file and source metadata

Common overrides:

1. `TPCH_DBGEN_REPO` (alternate clone URL)
2. `TPCH_DBGEN_REF` (pinned commit/tag)
3. `TPCH_DBGEN_SRC_DIR` (local source/build dir)
4. `TPCH_DBGEN_OUTPUT_DIR` (where `.tbl` files are written)
5. `TPCH_DBGEN_MACHINE` (for make, if auto-detect is unsuitable)

Deterministic `.tbl` -> parquet conversion (tables needed for Q1/Q3):

```bash
make tpch-dbgen-parquet
```

Default output:

1. `tests/bench/fixtures/tpch_dbgen_sf1_parquet/customer.parquet`
2. `tests/bench/fixtures/tpch_dbgen_sf1_parquet/orders.parquet`
3. `tests/bench/fixtures/tpch_dbgen_sf1_parquet/lineitem.parquet`
4. `tests/bench/fixtures/tpch_dbgen_sf1_parquet/manifest.json`

Conversion characteristics:

1. Explicit schema mapping for `customer`, `orders`, `lineitem`.
2. Stable file naming (`<table>.parquet`).
3. Deterministic writer settings (uncompressed parquet).
4. Manifest contains schema + row count per output file.

## Benchmark Modes

Each benchmark result must declare one of:

1. `embedded`
2. `distributed`

Optional sub-mode tags:

1. `vector_bruteforce`
2. `vector_qdrant`

## Canonical Query Set

Logical benchmark query ids:

1. `tpch_q1`
2. `tpch_q3`
3. `rag_topk_bruteforce`
4. `rag_topk_qdrant` (optional/feature-gated)

Canonical SQL file paths:

1. `tests/bench/queries/canonical/tpch_q1.sql`
2. `tests/bench/queries/canonical/tpch_q3.sql`
3. `tests/bench/queries/rag_topk_bruteforce.sql`
4. `tests/bench/queries/rag_topk_qdrant.sql`

The IDs are stable reporting keys. Benchmark runners must load SQL from these files rather than embedding inline SQL strings.

TPC-H Q1/Q3 files include explicit FFQ v1 adaptation notes in SQL comments; those notes are part of
the canonical query contract and apply to both embedded and distributed benchmark modes.

## Required Metrics

Per query variant, runner must report:

1. `elapsed_ms`
2. `rows_out`
3. `bytes_out` (if known; else `null`)
4. `iterations`
5. `warmup_iterations`
6. `success` (`true/false`)
7. `error` (string or `null`)

Recommended (when available):

1. `rows_per_sec`
2. `bytes_per_sec`
3. `spill_bytes`
4. `shuffle_bytes_read`
5. `shuffle_bytes_written`

## Run Metadata (Required)

Every benchmark artifact must include:

1. `run_id` (stable unique id for one invocation)
2. `timestamp_unix_ms` (UTC epoch millis)
3. `mode` (`embedded`/`distributed`)
4. `feature_flags` (list)
5. `fixture_root`
6. `query_root`
7. `runtime` metadata:
   - `threads`
   - `batch_size_rows`
   - `mem_budget_bytes`
   - `shuffle_partitions`
   - `spill_dir`
   - `max_cv_pct`
   - `tz`
   - `locale`
8. `host` metadata:
   - `os`
   - `arch`
   - `logical_cpus`
9. `results[]` rows with query-level metrics/status
10. `rag_comparisons[]` (optional; present when comparable brute-force and qdrant rows exist)

## JSON Output Schema (Contract)

Runner JSON artifact shape:

```json
{
  "run_id": "string",
  "timestamp_unix_ms": 1771246767734,
  "mode": "embedded",
  "feature_flags": ["distributed", "vector"],
  "fixture_root": "tests/bench/fixtures",
  "query_root": "tests/bench/queries",
  "runtime": {
    "threads": 1,
    "batch_size_rows": 8192,
    "mem_budget_bytes": 67108864,
    "shuffle_partitions": 64,
    "spill_dir": "target/tmp/bench_spill",
    "max_cv_pct": 30.0,
    "tz": "UTC",
    "locale": "C"
  },
  "host": {
    "os": "linux",
    "arch": "x86_64",
    "logical_cpus": 8
  },
  "results": [
    {
      "query_id": "tpch_q1",
      "variant": "baseline",
      "runtime_tag": "embedded",
      "dataset": "tpch_sf1",
      "backend": "sql_baseline",
      "n_docs": null,
      "effective_dim": null,
      "top_k": null,
      "filter_selectivity": null,
      "iterations": 5,
      "warmup_iterations": 1,
      "elapsed_ms": 1234.56,
      "elapsed_stddev_ms": 42.5,
      "elapsed_cv_pct": 3.44,
      "rows_out": 4,
      "bytes_out": null,
      "success": true,
      "error": null
    }
  ],
  "rag_comparisons": []
}
```

## CSV Output Schema (Contract)

CSV must be one row per query result with at least:

1. `run_id`
2. `timestamp_unix_ms`
3. `mode`
4. `query_id`
5. `variant`
6. `runtime_tag`
7. `dataset`
8. `backend`
9. `n_docs`
10. `effective_dim`
11. `top_k`
12. `filter_selectivity`
13. `iterations`
14. `warmup_iterations`
15. `elapsed_ms`
16. `elapsed_stddev_ms`
17. `elapsed_cv_pct`
18. `rows_out`
19. `bytes_out`
20. `success`
21. `error`

Optional columns may be appended but required columns must remain stable.

## Regression Pass/Fail Semantics

Comparison inputs:

1. `baseline` artifact (JSON)
2. `candidate` artifact (JSON)

For each shared `(mode, query_id, variant)` tuple:

1. If `candidate.success` is `false` -> fail.
2. If baseline is missing tuple -> warn (not fail).
3. If `candidate.elapsed_ms > baseline.elapsed_ms * (1 + threshold)` -> fail.

Default v1 threshold:

1. `threshold = 0.10` (10% regression allowed)

Overrides:

1. Query-specific thresholds may be configured by runner/comparator config.
2. Missing/invalid metrics for required fields -> fail.

Comparator output contract:

1. Print failing tuples with baseline/candidate values.
2. Exit code `0` on pass, non-zero on fail.
3. Script: `scripts/compare-bench-13.3.py`.

Example:

```bash
./scripts/compare-bench-13.3.py \
  --baseline tests/bench/results/baseline.json \
  --candidate tests/bench/results/current.json \
  --threshold 0.10
```

The comparator prints offending tuple/metric details (for example elapsed regression percentage) and exits non-zero on failure.

## Reproducibility Rules

To reduce noise/flakiness:

1. Use fixed dataset seeds for synthetic generators.
2. Use deterministic fixture ids/paths per run where possible.
3. Run warmups before measured iterations.
4. Record full run metadata and feature flags.
5. Keep benchmark process settings stable (`TZ=UTC`, fixed locale, fixed thread count policy).

## Related Files

1. `docs/v2/testing.md`
2. `docs/v2/integration-13.2.md`
3. `Makefile`
4. `.github/workflows/integration-13_2.yml`
5. `tests/bench/queries/`
6. `scripts/run-bench-13.3.sh`
7. `crates/client/examples/run_bench_13_3.rs`
8. `.github/workflows/bench-13_3.yml`
9. `scripts/build-tpch-dbgen.sh`
10. `scripts/generate-tpch-dbgen-sf1.sh`
11. `scripts/convert-tpch-dbgen-parquet.sh`
12. `crates/client/src/tpch_tbl.rs`
13. `scripts/run-bench-13.4-tpch-official.sh`

## Embedded Baseline Runner

Run:

```bash
./scripts/run-bench-13.3.sh
```

Outputs are written to `tests/bench/results/` as one JSON and one CSV file per run.

RAG matrix configuration (embedded/vector path):

1. `FFQ_BENCH_RAG_MATRIX` with format:
   - `"N,dim,k,selectivity;N,dim,k,selectivity;..."`
   - example: `"1000,16,5,1.0;5000,32,10,0.5;10000,64,20,0.2"`
2. `N` controls candidate set (`id <= floor(N * selectivity)` on synthetic fixture).
3. `dim` controls effective query-vector dimensions (`<=64` for current fixture).
4. `k` controls top-k limit.
5. `selectivity` must be in `[0,1]`.

Normalization controls (defaulted by `scripts/run-bench-13.3.sh`):

1. `FFQ_BENCH_THREADS` (also exported to `TOKIO_WORKER_THREADS` and `RAYON_NUM_THREADS`)
2. `FFQ_BENCH_BATCH_SIZE_ROWS`
3. `FFQ_BENCH_MEM_BUDGET_BYTES`
4. `FFQ_BENCH_SHUFFLE_PARTITIONS`
5. `FFQ_BENCH_SPILL_DIR` (cleaned before run; removed after run unless `FFQ_BENCH_KEEP_SPILL=1`)
6. `FFQ_BENCH_MAX_CV_PCT` variance gate (`--no-variance-check` to disable in direct CLI usage)
7. `TZ=UTC` and `LC_ALL=C`

Per-query output now includes `elapsed_stddev_ms` and `elapsed_cv_pct` to track variance.

Synthetic track commands:

1. `make bench-13.3-embedded`
2. `make bench-13.3-rag`
3. `FFQ_COORDINATOR_ENDPOINT=http://127.0.0.1:50051 make bench-13.3-distributed` (optional distributed synthetic check)

Distributed mode:

```bash
FFQ_BENCH_MODE=distributed \
FFQ_COORDINATOR_ENDPOINT=http://127.0.0.1:50051 \
./scripts/run-bench-13.3.sh
```

In distributed mode, the runner performs endpoint readiness checks and executes the comparable TPC-H benchmark subset (`tpch_q1`, `tpch_q3`). Artifacts include `mode` and `runtime_tag` so embedded and distributed results can be compared with the same schema.

Optional qdrant matrix variant (`--features qdrant`):

1. Set `FFQ_BENCH_QDRANT_COLLECTION` (required to enable qdrant variant runs).
2. Optional `FFQ_BENCH_QDRANT_ENDPOINT` (default `http://127.0.0.1:6334`).
3. JSON includes `rag_comparisons` rows for baseline-vs-qdrant where matching variant keys exist.

## Official TPC-H SF1 Runner (13.4.5)

Run official dbgen parquet benchmark flow (Q1/Q3 only):

```bash
make bench-13.4-official-embedded
```

Distributed mode:

```bash
FFQ_COORDINATOR_ENDPOINT=http://127.0.0.1:50051 \
make bench-13.4-official-distributed
```

Notes:

1. Requires converted official parquet files in `tests/bench/fixtures/tpch_dbgen_sf1_parquet/`.
2. Uses canonical query files `tests/bench/queries/canonical/tpch_q1.sql` and `tests/bench/queries/canonical/tpch_q3.sql`.
3. Writes JSON/CSV artifacts to `tests/bench/results/official_tpch/` by default.
4. Includes correctness gate (13.4.6): before timing Q1/Q3, runner validates query outputs against an
   independent parquet-derived baseline (group/join aggregate checks with float tolerance).
5. Any mismatch marks the query as failed and the benchmark command exits non-zero.

Official track commands:

1. `make tpch-dbgen-sf1`
2. `make tpch-dbgen-parquet`
3. `make validate-tpch-dbgen-manifests`
4. `make bench-13.4-official-embedded`
5. `FFQ_COORDINATOR_ENDPOINT=http://127.0.0.1:50051 make bench-13.4-official-distributed`

Recommended official sequence:

1. regenerate `.tbl` and parquet fixtures,
2. validate manifest contract,
3. run embedded official benchmark,
4. run distributed official benchmark (if distributed path is in scope),
5. compare against official baseline artifact.

## Official Reproducibility Contract (13.4.7)

Pinned generation inputs:

1. dbgen repo: `https://github.com/electrum/tpch-dbgen.git`
2. dbgen ref: `32f1c1b92d1664dba542e927d23d86ffa57aa253` (set via `TPCH_DBGEN_REF`, defaulted in tooling/CI)
3. scale factor: `TPCH_SCALE=1`

Environment assumptions for reproducible runs:

1. `TZ=UTC`
2. `LC_ALL=C`
3. deterministic fixture paths under `tests/bench/fixtures/`
4. deterministic parquet writer settings from converter (`UNCOMPRESSED`, stable file naming)

Compiler/container assumptions:

1. CI validates on `ubuntu-latest` with `rust-toolchain@stable`
2. benchmark runtime and conversion tooling are executed in that pinned CI image context

Manifest contract validation:

1. `make validate-tpch-dbgen-manifests` validates:
   - expected SF1 `.tbl` table set + row counts,
   - pinned source repo/ref metadata,
   - converted parquet file set + row counts + schema signatures.
2. CI runs generation + validation twice and compares manifests byte-for-byte to detect drift.

## Make Command Matrix

1. `make bench-13.3-embedded`
   - Runs embedded benchmark baseline.
   - Common env knobs: `FFQ_BENCH_WARMUP`, `FFQ_BENCH_ITERATIONS`, `FFQ_BENCH_THREADS`, `FFQ_BENCH_BATCH_SIZE_ROWS`, `FFQ_BENCH_MEM_BUDGET_BYTES`, `FFQ_BENCH_SHUFFLE_PARTITIONS`.
2. `make bench-13.3-distributed`
   - Runs distributed benchmark baseline.
   - Required env: `FFQ_COORDINATOR_ENDPOINT`.
   - Optional env: `FFQ_WORKER1_ENDPOINT`, `FFQ_WORKER2_ENDPOINT`.
3. `make bench-13.3-rag`
   - Runs embedded RAG matrix path.
   - Optional env: `FFQ_BENCH_RAG_MATRIX`.
   - Optional qdrant env: `FFQ_BENCH_QDRANT_COLLECTION`, `FFQ_BENCH_QDRANT_ENDPOINT`.
4. `make bench-13.3-compare BASELINE=<json-or-dir> CANDIDATE=<json-or-dir> [THRESHOLD=0.10]`
   - Compares candidate vs baseline and fails on threshold regression.
5. `make tpch-dbgen-sf1`
   - Generates official dbgen SF1 `.tbl` dataset.
6. `make tpch-dbgen-parquet`
   - Converts dbgen `.tbl` to deterministic parquet for FFQ benchmark paths.
7. `make bench-13.4-official-embedded`
   - Runs official SF1 parquet Q1/Q3 benchmark in embedded mode.
8. `make bench-13.4-official-distributed`
   - Runs official SF1 parquet Q1/Q3 benchmark in distributed mode (`FFQ_COORDINATOR_ENDPOINT` required).

Legacy alias:

1. `make compare-13.3` forwards to `bench-13.3-compare`.

## CI Workflow

Workflow: `.github/workflows/bench-13_3.yml`

Triggers:

1. Pull requests (`opened`, `reopened`, `synchronize`): runs reduced matrix and uploads JSON/CSV artifacts.
2. Manual (`workflow_dispatch`): choose reduced/full matrix and optional regression gate.

Additional CI validation in the same workflow:

1. `official-fixture-contract` job regenerates official SF1 `.tbl` and parquet fixtures.
2. It runs manifest contract validation and reruns generation to detect reproducibility drift.
3. It uploads generated official manifests as artifacts for audit/debug.

Manual inputs:

1. `matrix_size`: `reduced` or `full`
2. `regression_gate`: boolean (only applies to reduced)
3. `baseline_path`: repo-relative baseline JSON path (required when gate is enabled)
4. `threshold`: regression threshold ratio (default `0.10`)

Artifacts:

1. Uploads `tests/bench/results/*.json` and `tests/bench/results/*.csv`.
2. Artifact name pattern: `bench-13_3-<run_id>-<matrix_mode>`.

## Runbook

This section is the practical end-to-end guide for running and interpreting 13.3/13.4 benchmarks.

### Prerequisites

1. Rust toolchain installed (`stable`).
2. Build dependencies available for Arrow/Parquet crates on your OS.
3. Repo checked out with generated benchmark fixtures or permission to generate them.
4. For distributed runs:
   - running coordinator endpoint
   - optional worker endpoints for readiness checks.
5. For qdrant comparisons:
   - qdrant instance reachable
   - collection populated and configured.

### Fixture Setup

Generate deterministic synthetic fixtures:

```bash
./scripts/generate-bench-fixtures.sh
```

Expected artifacts:

1. `tests/bench/fixtures/index.json`
2. `tests/bench/fixtures/tpch_sf1/manifest.json`
3. `tests/bench/fixtures/rag_synth/manifest.json`

Generate/validate official fixtures:

```bash
make tpch-dbgen-sf1
make tpch-dbgen-parquet
make validate-tpch-dbgen-manifests
```

Expected official artifacts:

1. `tests/bench/fixtures/tpch_dbgen_sf1/manifest.json`
2. `tests/bench/fixtures/tpch_dbgen_sf1_parquet/manifest.json`

### Standard Run Flow

Recommended contributor flow:

1. Embedded baseline:
   - `make bench-13.3-embedded`
2. RAG matrix:
   - `make bench-13.3-rag`
3. Distributed (when cluster is available):
   - `FFQ_COORDINATOR_ENDPOINT=http://127.0.0.1:50051 make bench-13.3-distributed`
4. Compare candidate vs baseline:
   - `make bench-13.3-compare BASELINE=<baseline.json-or-dir> CANDIDATE=<candidate.json-or-dir> THRESHOLD=0.10`

Recommended track-separated flow:

1. Synthetic loop:
   - `make bench-13.3-embedded`
   - optional: `make bench-13.3-rag`
   - optional: distributed synthetic check
2. Official loop:
   - `make tpch-dbgen-sf1`
   - `make tpch-dbgen-parquet`
   - `make validate-tpch-dbgen-manifests`
   - `make bench-13.4-official-embedded`
   - optional: `make bench-13.4-official-distributed`

### Important Environment Variables

Core runner settings:

1. `FFQ_BENCH_WARMUP`
2. `FFQ_BENCH_ITERATIONS`
3. `FFQ_BENCH_THREADS`
4. `FFQ_BENCH_BATCH_SIZE_ROWS`
5. `FFQ_BENCH_MEM_BUDGET_BYTES`
6. `FFQ_BENCH_SHUFFLE_PARTITIONS`
7. `FFQ_BENCH_SPILL_DIR`
8. `FFQ_BENCH_KEEP_SPILL`
9. `FFQ_BENCH_MAX_CV_PCT`

Mode-specific settings:

1. Distributed:
   - `FFQ_COORDINATOR_ENDPOINT` (required)
   - `FFQ_WORKER1_ENDPOINT` (optional)
   - `FFQ_WORKER2_ENDPOINT` (optional)
2. RAG:
   - `FFQ_BENCH_RAG_MATRIX`
3. Qdrant:
   - `FFQ_BENCH_QDRANT_COLLECTION` (required to enable qdrant variants)
   - `FFQ_BENCH_QDRANT_ENDPOINT` (optional)

### Artifact Interpretation

JSON (`tests/bench/results/*.json`):

1. `runtime` records normalization controls used in the run.
2. `results[]` is one row per query/variant tuple.
3. `elapsed_ms` is mean latency across measured iterations.
4. `elapsed_stddev_ms` and `elapsed_cv_pct` reflect variance.
5. For official track runs, any correctness divergence appears as `success=false` with explicit mismatch details in `error`.

How to interpret by track:

1. Synthetic:
   - use for relative change detection and quick bisecting,
   - expect more frequent baseline refreshes.
2. Official:
   - use for changelog/release performance claims,
   - baseline updates should be controlled and reviewed,
   - failed correctness checks invalidate latency numbers for that run.
3. `success=false` plus `error` indicates hard failure, correctness failure, or variance gate failure.
4. `rag_comparisons[]` contains brute-force vs qdrant deltas where both are present.

CSV (`tests/bench/results/*.csv`):

1. Flat row view for spreadsheet/chart workflows.
2. Includes query identifiers and matrix dimensions (`n_docs`, `effective_dim`, `top_k`, `filter_selectivity`).

### Baseline Update Policy

Use this policy when updating benchmark baselines:

1. Only update baseline after functional correctness is stable and green.
2. Record baseline from at least two clean runs with comparable CV%.
3. Prefer reduced matrix for routine gating and full matrix for periodic snapshots.
4. Keep threshold conservative (`0.10` default) unless justified by a known environment shift.
5. In PRs that intentionally change performance, include:
   - old vs new artifact references
   - rationale for threshold or baseline updates
   - impacted query keys.

### Troubleshooting

If embedded run fails:

1. Check fixture files exist under `tests/bench/fixtures/`.
2. For synthetic track, re-generate fixtures with `./scripts/generate-bench-fixtures.sh`.
3. For official track, run `make tpch-dbgen-sf1 && make tpch-dbgen-parquet` and then `make validate-tpch-dbgen-manifests`.
4. Verify query files under `tests/bench/queries/`.
5. Re-run with lower matrix size and fewer iterations for quick diagnosis.

If distributed run fails:

1. Verify `FFQ_COORDINATOR_ENDPOINT` has `http://` scheme.
2. Confirm coordinator/worker endpoints are reachable.
3. Re-run with reduced warmup/iterations for faster feedback.

If variance gate fails:

1. Inspect `elapsed_cv_pct` in result rows.
2. Increase `FFQ_BENCH_ITERATIONS` to smooth noise.
3. Reduce background load and keep thread count fixed.
4. Temporarily disable gate with `--no-variance-check` (or clear `FFQ_BENCH_MAX_CV_PCT`) only for diagnosis, not final CI policy.

If comparator fails:

1. Confirm baseline/candidate point to intended artifact files.
2. Review offending tuple in comparator output.
3. Distinguish true regression from row-shape mismatch (`rows_out` mismatch).
