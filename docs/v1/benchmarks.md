# Benchmarks Contract (13.3)

This page defines the v1 benchmark contract: what is measured, how runs are configured, and how outputs/regressions are evaluated.

## Scope

Benchmark scope for v1:

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

1. `tests/bench/queries/tpch_q1.sql`
2. `tests/bench/queries/tpch_q3.sql`
3. `tests/bench/queries/rag_topk_bruteforce.sql`
4. `tests/bench/queries/rag_topk_qdrant.sql`

The IDs are stable reporting keys. Benchmark runners must load SQL from these files rather than embedding inline SQL strings.

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
2. `timestamp_utc` (ISO-8601)
3. `git_commit` (or `unknown`)
4. `mode` (`embedded`/`distributed`)
5. `feature_flags` (list)
6. `dataset` metadata:
   - `name` (`tpch_sf1` / `rag_synth`)
   - `seed` (for synthetic data)
   - `scale_or_n` (SF1 or doc count)
   - `dimension` (for vector)
7. `runtime` metadata:
   - `batch_size_rows`
   - `mem_budget_bytes`
   - `cpu_slots` (if applicable)
8. `host` metadata:
   - `os`
   - `arch`
   - `cpu_model` (best effort)
   - `logical_cpus`

## JSON Output Schema (Contract)

Runner JSON artifact shape:

```json
{
  "run_id": "string",
  "timestamp_utc": "2026-02-16T10:00:00Z",
  "git_commit": "string",
  "mode": "embedded",
  "feature_flags": ["distributed", "vector"],
  "dataset": {
    "name": "tpch_sf1",
    "seed": 42,
    "scale_or_n": "sf1",
    "dimension": null
  },
  "runtime": {
    "batch_size_rows": 8192,
    "mem_budget_bytes": 67108864,
    "cpu_slots": 2
  },
  "host": {
    "os": "linux",
    "arch": "x86_64",
    "cpu_model": "string",
    "logical_cpus": 8
  },
  "results": [
    {
      "query_id": "tpch_q1",
      "variant": "baseline",
      "iterations": 5,
      "warmup_iterations": 1,
      "elapsed_ms": 1234.56,
      "rows_out": 4,
      "bytes_out": 256,
      "rows_per_sec": 3240.1,
      "bytes_per_sec": 207.3,
      "spill_bytes": 0,
      "shuffle_bytes_read": null,
      "shuffle_bytes_written": null,
      "success": true,
      "error": null
    }
  ]
}
```

## CSV Output Schema (Contract)

CSV must be one row per query result with at least:

1. `run_id`
2. `timestamp_utc`
3. `mode`
4. `query_id`
5. `variant`
6. `iterations`
7. `warmup_iterations`
8. `elapsed_ms`
9. `rows_out`
10. `bytes_out`
11. `success`
12. `error`

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

## Reproducibility Rules

To reduce noise/flakiness:

1. Use fixed dataset seeds for synthetic generators.
2. Use deterministic fixture ids/paths per run where possible.
3. Run warmups before measured iterations.
4. Record full run metadata and feature flags.
5. Keep benchmark process settings stable (`TZ=UTC`, fixed locale, fixed thread count policy).

## Related Files

1. `docs/v1/testing.md`
2. `docs/v1/integration-13.2.md`
3. `Makefile`
4. `.github/workflows/integration-13_2.yml`
5. `tests/bench/queries/`
6. `scripts/run-bench-13.3.sh`
7. `crates/client/examples/run_bench_13_3.rs`

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
