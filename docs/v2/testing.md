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

## 3) Vector / RAG

Commands:

```bash
make test-13.1-vector
cargo test -p ffq-client --test embedded_two_phase_retrieval --features vector
cargo test -p ffq-client --test qdrant_routing --features "vector,qdrant"
```

Pass criteria:

1. vector kernel/ranking tests pass
2. optimizer vector rewrite goldens pass
3. fallback behavior for unsupported shapes is validated
4. qdrant routing tests pass when `qdrant` feature is enabled

Primary references:

1. `crates/client/tests/embedded_vector_topk.rs`
2. `crates/client/tests/embedded_two_phase_retrieval.rs`
3. `crates/client/tests/qdrant_routing.rs`
4. `crates/planner/tests/optimizer_golden.rs`

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
