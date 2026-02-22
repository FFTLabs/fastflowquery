# Testing & Validation Playbook (v2)

- Status: draft
- Owner: @ffq-qa
- Last Verified Commit: TBD
- Last Verified Date: TBD

This page is the single validation checklist for implemented v2 scope.

## Scope

Subsystem coverage in this playbook:

1. core (embedded planner/runtime/storage/write)
2. distributed runtime
3. vector and RAG paths
4. FFI
5. Python bindings
6. extensibility (optimizer rules, UDFs, custom physical operators)

## Prerequisites

1. run from repo root (`fastflowquery/`)
2. Rust toolchain installed
3. Docker + Compose installed (distributed checks)
4. Python 3.9+ installed (Python checks)
5. C compiler available (FFI checks)

Quick check:

```bash
cargo --version
docker --version
docker compose version
python --version
cc --version
```

## Validation Modes

Use one of these depending on scope.

### A) Fast local validation (core + API)

```bash
cargo test --workspace --lib
make test-13.1-core
make test-13.2-embedded
make repl-smoke
```

### B) Full v2 functional validation

```bash
make test-13.1
make test-13.2-parity
make ffi-example
make python-dev-install
python -m pip install pyarrow
python - <<'PY'
import ffq
e = ffq.Engine()
e.register_table("lineitem", "tests/fixtures/parquet/lineitem.parquet")
assert e.sql("SELECT l_orderkey FROM lineitem LIMIT 1").collect().num_rows == 1
print("python binding smoke: OK")
PY
```

### C) CI-equivalent matrix validation

```bash
cargo build --no-default-features
cargo build --features distributed,python,s3
cargo build -p ffq-client --no-default-features --features core,distributed,s3,vector,qdrant,python,ffi
make test-13.1-core
make test-13.1-vector
make test-13.1-distributed
make test-13.2-embedded
make test-13.2-parity
make ffi-example
```

## Subsystem Checklist

## 1) Core (Embedded)

Commands:

```bash
cargo test --workspace --lib
make test-13.1-core
make test-13.2-embedded
cargo test -p ffq-client --test embedded_parquet_sink
cargo test -p ffq-client --test dataframe_write_api
```

Pass criteria:

1. planner and runtime lib tests pass
2. deterministic join/aggregate tests pass
3. embedded integration query suite passes
4. parquet sink/write API tests pass

Primary references:

1. `crates/client/tests/embedded_hash_join.rs`
2. `crates/client/tests/embedded_hash_aggregate.rs`
3. `crates/client/tests/integration_embedded.rs`
4. `crates/client/tests/embedded_parquet_sink.rs`
5. `crates/client/tests/dataframe_write_api.rs`

### 1.1) Storage IO cache validation (EPIC 8.3)

Commands:

```bash
cargo test -p ffq-storage block_cache_records_miss_then_hit_events -- --nocapture
cargo test -p ffq-storage partition_pruning_hive_matches_eq_and_range_filters -- --nocapture
```

Pass criteria:

1. cache metrics include `ffq_file_cache_events_total`
2. repeated read path records at least one `result="hit"` for enabled cache layer
3. pruning + cache behavior does not change query correctness

Primary references:

1. `crates/storage/src/parquet_provider.rs`
2. `crates/common/src/metrics.rs`
3. `crates/storage/src/parquet_provider.rs` (tests module)

### 1.2) Object-store parquet validation (EPIC 8.4)

Commands:

```bash
cargo test -p ffq-storage --features s3 object_store_uri_detection_requires_scheme -- --nocapture
cargo test -p ffq-storage --features s3 object_store_scan_reads_file_uri_parquet -- --nocapture
cargo test -p ffq-storage --features s3 object_store_scan_retries_then_fails_for_missing_object -- --nocapture
```

Pass criteria:

1. provider accepts URI-style object-store paths and rejects non-URI paths for object-store flow
2. file-URI object-store scan returns correct parquet rows/columns
3. missing-object path fails only after configured retry count with explicit attempt count in error text

Primary references:

1. `crates/storage/src/object_store_provider.rs`
2. `crates/client/src/runtime.rs`
3. `crates/distributed/src/worker.rs`

### 1.2b) Partition pruning + stats validation (EPIC 8.1 / 8.2)

Commands:

```bash
cargo test -p ffq-storage partition_pruning_hive_matches_eq_and_range_filters -- --nocapture
```

Pass criteria:

1. hive-style partition pruning removes non-matching file paths for equality/range filters
2. pruned scan result remains correct
3. storage metadata/stats extraction path remains compatible with parquet provider scan path

Primary references:

1. `docs/v2/storage-catalog.md`
2. `crates/storage/src/parquet_provider.rs`
3. `crates/storage/src/stats.rs`

### 1.3) Join System v2 validation (EPIC 5)

Commands:

```bash
cargo test -p ffq-client --test embedded_hash_join
cargo test -p ffq-client --test embedded_cte_subquery
cargo test -p ffq-client runtime_tests::join_prefers_sort_merge_when_hint_is_set -- --exact
make bench-v2-join-radix
make bench-v2-join-bloom
```

Pass criteria:

1. hash-join suite passes (including inner/outer/semi/anti correctness paths)
2. `EXISTS`/`IN` rewrite paths validate semi/anti behavior via subquery suite
3. targeted sort-merge selection test passes when the hint/config path is enabled
4. radix microbench reports baseline vs radix timing comparison output
5. bloom microbench reports selective prefilter impact in probe-side path

Primary references:

1. `docs/v2/join-system-v2.md`
2. `crates/client/src/runtime.rs`
3. `crates/planner/src/physical_planner.rs`
4. `crates/planner/src/optimizer.rs`
5. `crates/client/tests/embedded_hash_join.rs`
6. `crates/client/tests/embedded_cte_subquery.rs`
7. `crates/client/examples/bench_join_radix.rs`
8. `crates/client/examples/bench_join_bloom.rs`

### 1.4) Aggregation v2 validation (EPIC 6)

Commands:

```bash
cargo test -p ffq-client --test embedded_hash_aggregate
cargo test -p ffq-client --test distributed_runtime_roundtrip distributed_embedded_roundtrip_matches_expected_snapshots_and_parity -- --exact
cargo test -p ffq-client --features approx --test embedded_hash_aggregate approx_count_distinct_is_plausible_with_tolerance -- --exact
```

Pass criteria:

1. grouped aggregate spill/non-spill paths are deterministic and parity-stable
2. `COUNT(DISTINCT ...)` grouped queries are correct and spill-stable
3. distributed and embedded aggregate outputs match parity expectations for distinct paths
4. `APPROX_COUNT_DISTINCT` remains within tolerance when `approx` feature is enabled

Primary references:

1. `docs/v2/aggregation-v2.md`
2. `crates/client/src/runtime.rs`
3. `crates/planner/src/physical_planner.rs`
4. `crates/planner/src/sql_frontend.rs`
5. `crates/client/tests/embedded_hash_aggregate.rs`
6. `crates/client/tests/distributed_runtime_roundtrip.rs`

## 2) Distributed

Commands:

```bash
make test-13.2-parity
make test-13.1-distributed
```

Pass criteria:

1. coordinator + workers boot and become healthy
2. distributed integration suite returns correct non-empty join/agg output
3. embedded vs distributed parity comparison passes
4. distributed correctness test target passes

Primary references:

1. `scripts/run-distributed-integration.sh`
2. `crates/client/tests/integration_distributed.rs`
3. `crates/client/tests/distributed_runtime_roundtrip.rs`

## 2.1) AQE / Adaptive Shuffle (EPIC 4)

Commands:

```bash
cargo test -p ffq-distributed --features grpc coordinator_fans_out_reduce_stage_tasks_from_shuffle_layout
cargo test -p ffq-distributed --features grpc coordinator_applies_barrier_time_adaptive_partition_coalescing
cargo test -p ffq-distributed --features grpc coordinator_barrier_time_hot_partition_splitting_increases_reduce_tasks
cargo test -p ffq-distributed --features grpc coordinator_finalizes_adaptive_layout_once_before_reduce_scheduling
cargo test -p ffq-distributed --features grpc coordinator_ignores_stale_reports_from_old_adaptive_layout
cargo test -p ffq-distributed --features grpc coordinator_adaptive_shuffle_retries_failed_map_attempt_and_completes
cargo test -p ffq-distributed --features grpc coordinator_adaptive_shuffle_recovers_from_worker_death_during_map_and_reduce
make bench-v2-adaptive-shuffle-embedded
make bench-v2-adaptive-shuffle-compare BASELINE=<baseline.json-or-dir> CANDIDATE=<candidate.json-or-dir>
```

Pass criteria:

1. reduce stages fan out according to finalized adaptive layout
2. coalesce/split decisions are deterministic for identical metadata
3. hot partition skew splits increase effective reduce fanout when required
4. stale layout reports are ignored without corrupting query state
5. map/reduce failure-retry paths complete without deadlock
6. benchmark comparator exits `0` for adaptive-shuffle thresholds

Primary references:

1. `docs/v2/adaptive-shuffle-tuning.md`
2. `docs/v2/distributed-runtime.md`
3. `crates/common/src/adaptive.rs`
4. `crates/distributed/src/coordinator.rs`
5. `crates/client/src/runtime.rs`
6. `scripts/run-bench-v2-adaptive-shuffle.sh`
7. `tests/bench/thresholds/adaptive_shuffle_regression_thresholds.json`

## 3) Vector / RAG

Commands:

```bash
make test-13.1-vector
cargo test -p ffq-client --test embedded_two_phase_retrieval --features vector
cargo test -p ffq-client --test qdrant_routing --features "vector,qdrant"
cargo test -p ffq-client --test public_api_contract --features vector
cargo test -p ffq-client --features embedding-http --lib embedding::tests
```

Pass criteria:

1. vector kernel/ranking tests pass
2. optimizer vector rewrite goldens pass
3. fallback behavior for unsupported shapes is validated
4. qdrant routing tests pass when `qdrant` feature is enabled
5. public API contract includes hybrid batch query convenience path
6. embedding provider API tests pass (sample provider always; HTTP provider path when feature enabled)

Primary references:

1. `crates/client/tests/embedded_vector_topk.rs`
2. `crates/client/tests/embedded_two_phase_retrieval.rs`
3. `crates/client/tests/qdrant_routing.rs`
4. `crates/planner/tests/optimizer_golden.rs`
5. `crates/client/tests/public_api_contract.rs`
6. `crates/client/src/embedding.rs`

## 3.1) SQL Semantics (EPIC 3)

Commands:

```bash
cargo test -p ffq-client --test embedded_hash_join
cargo test -p ffq-client --test embedded_case_expr
cargo test -p ffq-client --test embedded_cte_subquery
cargo test -p ffq-client --test embedded_cte_subquery_golden
cargo test -p ffq-client --test embedded_window_functions
cargo test -p ffq-client --test embedded_window_golden
cargo test -p ffq-client --test distributed_runtime_roundtrip
```

Pass criteria:

1. outer join correctness snapshots pass (`LEFT/RIGHT/FULL`)
2. CASE projection/filter semantics pass
3. CTE/subquery semantics pass (including scalar/EXISTS/IN paths)
4. CTE/subquery golden edge matrix snapshot is stable
5. window function/frame/null/tie semantics pass
6. window golden edge matrix snapshot is stable
7. embedded and distributed parity checks pass for correlated/subquery/window shapes

Primary references:

1. `docs/v2/sql-semantics.md`
2. `crates/client/tests/embedded_hash_join.rs`
3. `crates/client/tests/embedded_case_expr.rs`
4. `crates/client/tests/embedded_cte_subquery.rs`
5. `crates/client/tests/embedded_cte_subquery_golden.rs`
6. `crates/client/tests/snapshots/subquery/embedded_cte_subquery_edge_matrix.snap`
7. `crates/client/tests/embedded_window_functions.rs`
8. `crates/client/tests/embedded_window_golden.rs`
9. `crates/client/tests/snapshots/window/embedded_window_edge_matrix.snap`
10. `crates/client/tests/distributed_runtime_roundtrip.rs`

## 4) FFI

Commands:

```bash
make ffi-build
make ffi-example
```

Pass criteria:

1. `ffq-client` builds with `ffi` feature
2. C example compiles and links
3. C example runs `SELECT 1` and parquet scan through ABI
4. output includes `ffi example: OK`

Primary references:

1. `crates/client/src/ffi.rs`
2. `include/ffq_ffi.h`
3. `examples/c/ffi_example.c`
4. `scripts/run-ffi-c-example.sh`

## 5) Python

Commands:

```bash
make python-dev-install
python -m pip install pyarrow
python - <<'PY'
import ffq
e = ffq.Engine()
e.register_table("lineitem", "tests/fixtures/parquet/lineitem.parquet")
assert e.sql("SELECT l_orderkey FROM lineitem LIMIT 1").collect().num_rows == 1
print("python binding smoke: OK")
PY
```

Optional wheel packaging check:

```bash
make python-wheel
```

Pass criteria:

1. extension installs in current Python environment
2. `engine.sql(...).collect()` returns `pyarrow.Table`
3. smoke script prints `python binding smoke: OK`
4. optional wheel build succeeds

Primary references:

1. `crates/client/src/python.rs`
2. `python/ffq/__init__.py`
3. `.github/workflows/python-wheels.yml`

## 6) Extensibility

Commands:

```bash
cargo test -p ffq-client --test udf_api
cargo test -p ffq-planner --test optimizer_custom_rule
cargo test -p ffq-client --test physical_registry
cargo test -p ffq-distributed --features grpc coordinator_with_workers_executes_custom_operator_stage
cargo test -p ffq-distributed --features grpc coordinator_assigns_custom_operator_tasks_only_to_capable_workers
```

Pass criteria:

1. `my_add` UDF works in SQL execution path
2. custom optimizer rule rewrite test passes
3. physical operator registry add/remove lifecycle passes
4. distributed custom operator stage executes successfully
5. capability-aware scheduling only assigns custom-op tasks to capable workers

Primary references:

1. `crates/client/tests/udf_api.rs`
2. `crates/planner/tests/optimizer_custom_rule.rs`
3. `crates/client/tests/physical_registry.rs`
4. `crates/distributed/src/worker.rs`
5. `crates/distributed/src/coordinator.rs`

## Feature Matrix and API Compatibility Gates

Commands:

```bash
cargo build --no-default-features
cargo build --features distributed,python,s3
cargo build -p ffq-client --no-default-features --features core,distributed,s3,vector,qdrant,python,ffi
cargo test -p ffq-client --test public_api_contract
```

Optional semver gate:

```bash
cargo install cargo-semver-checks --locked
cargo semver-checks check-release --manifest-path crates/client/Cargo.toml --baseline-rev origin/main
```

## 7) Benchmark Regression Gates

Commands:

```bash
make bench-v2-window-embedded
make bench-v2-adaptive-shuffle-embedded
make bench-v2-pipelined-shuffle
make bench-v2-window-compare BASELINE=<baseline.json-or-dir> CANDIDATE=<candidate.json-or-dir>
make bench-v2-adaptive-shuffle-compare BASELINE=<baseline.json-or-dir> CANDIDATE=<candidate.json-or-dir>
make bench-v2-pipelined-shuffle-gate CANDIDATE=<candidate.json>
```

Pass criteria:

1. benchmark runs complete with all rows marked `success=true`
2. comparator exits `0` for window matrix thresholds
3. comparator exits `0` for adaptive-shuffle matrix thresholds
4. pipelined-shuffle gate exits `0` (TTFR improvement and throughput bounds)
5. CI `bench-13_3` workflow can run benchmark gates without manual patching

Primary references:

1. `.github/workflows/bench-13_3.yml`
2. `scripts/run-bench-v2-window.sh`
3. `scripts/run-bench-v2-adaptive-shuffle.sh`
4. `tests/bench/thresholds/window_regression_thresholds.json`
5. `tests/bench/thresholds/adaptive_shuffle_regression_thresholds.json`
6. `tests/bench/thresholds/pipelined_shuffle_ttfr_thresholds.json`
7. `scripts/run-bench-v2-pipelined-shuffle.sh`
8. `scripts/check-bench-v2-pipelined-ttfr.py`
9. `docs/v2/adaptive-shuffle-tuning.md`

Pass criteria:

1. feature combinations compile
2. public API contract tests pass
3. semver-check shows no unintended breaking change

## Full v2 Validation Checklist (One Path)

Run in this order:

1. `cargo build --no-default-features`
2. `cargo build --features distributed,python,s3`
3. `make test-13.1`
4. `make test-13.2-parity`
5. `make repl-smoke`
6. `make ffi-example`
7. Python smoke script from section 5
8. Extensibility command set from section 6

Overall acceptance criteria:

1. all commands exit `0`
2. no parity mismatches in distributed vs embedded checks
3. no snapshot drift unless intentionally blessed
4. FFI and Python binding smokes return successful query results
5. extensibility tests prove optimizer/UDF/custom-op behavior

## Troubleshooting Quick Map

1. distributed fails to connect:
   - check `docker compose -f docker/compose/ffq.yml ps`
   - ensure `FFQ_COORDINATOR_ENDPOINT=http://127.0.0.1:50051`
2. schema/key errors in distributed:
   - validate `tests/fixtures/catalog/tables.json`
3. Python import/collect errors:
   - rerun `make python-dev-install`; install `pyarrow`
4. FFI link/runtime errors:
   - rerun `make ffi-build`; verify `cc` and runtime library path from script
5. custom-operator distributed mismatch:
   - ensure worker bootstrap registers factories and capability heartbeat includes names
   - see `docs/v2/custom-operators-deployment.md`

## CI Workflows (Reference)

1. `.github/workflows/feature-matrix.yml`
2. `.github/workflows/correctness-13_1.yml`
3. `.github/workflows/integration-13_2.yml`
4. `.github/workflows/python-wheels.yml`
5. `.github/workflows/api-semver.yml`
6. `.github/workflows/rustdoc.yml`
