# Testing and Validation Playbook

This page is the v1 validation runbook. It defines test layers, key fixtures, command matrix by feature flags, and acceptance checks per subsystem.

## Goals

1. Verify v1 behavior in embedded mode.
2. Verify optional distributed mode is runnable and returns real results.
3. Verify vector/rag paths (rewrite and fallback) work as designed.
4. Verify write durability semantics (overwrite/append/restart/failure cleanup).
5. Verify observability surfaces expose meaningful metrics.

## Correctness Contract (v1)

This section is the normative definition of "correct" for v1 tests.

## Canonical sorting and normalization

1. Any comparison of multi-row query output must be order-insensitive unless the query semantics guarantee order.
2. Tests must normalize rows before comparison using explicit sort keys (for example `["id"]`, `["l_orderkey", "l_partkey"]`).
3. Use shared normalization helpers from `crates/client/tests/support/mod.rs`:
   - `snapshot_text(...)`
   - `assert_batches_deterministic(...)`
4. Never assert raw batch row order for hash join/aggregate/top-k internals unless the operator contract requires strict ordering.

## Float tolerance policy

1. Float comparisons must use tolerance; do not assert exact binary equality for computed metrics.
2. Default tolerance for normalized snapshots is `1e-9` unless a test requires looser tolerance.
3. For direct scalar checks, use absolute-difference assertions:
   - `abs(actual - expected) < tolerance`
4. If a test needs non-default tolerance, document the reason in the test body.

## Null semantics policy

1. Nulls are part of correctness and must be asserted explicitly in edge-case tests.
2. Snapshot normalization encodes nulls as `NULL`; treat this as stable contract text.
3. For vector/scoring paths, null input rows must remain null in output score arrays unless operator contract says otherwise.

## Snapshot update policy

1. Golden snapshots are authoritative expected outputs.
2. Update snapshots only when behavior changes are intentional.
3. Use blessed update flow:
   - `BLESS=1 ...`
   - or `UPDATE_SNAPSHOTS=1 ...`
4. Required review rule:
   - PRs that modify `*.snap` files must include a short explanation of why the change is expected.
5. Never mix unrelated refactors with snapshot updates in one commit.

## Flaky-test policy

1. Correctness tests must be deterministic; flaky tests are treated as failures, not tolerated noise.
2. If flakiness appears:
   - capture and document repro conditions,
   - fix determinism (sorting, stable fixtures, explicit tolerances, isolated temp dirs),
   - re-enable only after deterministic reruns pass.
3. Do not add retry loops inside assertions to hide nondeterminism.
4. Distributed tests that require socket/network binding should be isolated and clearly labeled; failures due to sandbox or environment restrictions must be called out separately from product correctness failures.

## Contributor checklist for new correctness tests

1. Use fixed fixtures with deterministic seed/data.
2. Normalize output with explicit sort keys.
3. Use tolerance for floats and explicit checks for nulls.
4. Add/maintain snapshots through bless flow when applicable.
5. Ensure the test runs in the appropriate feature matrix (`core`, `vector`, `distributed`).
6. Add the test command to the 13.1 matrix if it introduces a new coverage area.

## Test Strategy by Layer

## 1) Unit tests (`--lib`)

Scope:

1. Planner rules and transformations.
2. Metrics registry and exporter behavior.
3. Storage/provider helper logic.
4. Runtime helper logic that does not need end-to-end cluster setup.

Command:

```bash
cargo test --workspace --lib
```

## 2) Integration tests (`crates/*/tests`)

Scope:

1. End-to-end behavior inside one crate boundary (planner/client/distributed).
2. Real parquet read/write to temp files.
3. Feature-gated behavior (distributed/vector/qdrant/profiling).

Command:

```bash
cargo test
```

## 3) End-to-end scenario validation

Scope:

1. Embedded query flows and write flows.
2. Coordinator + workers distributed execution.
3. Vector rewrite + two-phase retrieval behavior.

Approach:

1. Run the command matrix below.
2. Verify each major subsystem acceptance check.

## Important Fixtures

## Data fixtures

1. Temp parquet tables generated in tests (`std::env::temp_dir()` + unique names).
2. Small deterministic row sets for join/aggregate correctness checks.
3. Vector embedding fixtures (`FixedSizeList<Float32>`) for cosine similarity ranking.

## Catalog and write fixtures

1. `FFQ_CATALOG_PATH` temporary json files in write API tests.
2. Managed table output dirs under `./ffq_tables` or catalog-adjacent dirs.
3. Write mode scenarios: overwrite, append, restart persistence, failed write cleanup, deterministic retry.

## Distributed fixtures

1. In-process gRPC coordinator service on ephemeral localhost port.
2. Worker instances with temp spill and shuffle dirs.
3. Test-level lock to avoid concurrent distributed test interference.

## Vector/qdrant fixtures

1. `format = "qdrant"` table metadata.
2. Mock vector provider rows via `vector.mock_rows_json` for deterministic tests without external qdrant.
3. Query vectors provided as `LiteralValue::VectorF32`.

## Feature-Flag Command Matrix

Run from repo root.

## 13.1 single-checklist commands (local + CI)

Local one-shot:

```bash
make test-13.1
```

Or run grouped phases:

```bash
make test-13.1-core
make test-13.1-vector
make test-13.1-distributed
```

Snapshot maintenance for optimizer goldens:

```bash
make bless-13.1-snapshots
```

CI uses the same grouped commands via:

1. `.github/workflows/correctness-13_1.yml`
2. `make test-13.1-core`
3. `make test-13.1-vector`
4. `make test-13.1-distributed`

## Baseline (embedded default)

```bash
cargo test -p ffq-client --test embedded_parquet_scan
cargo test -p ffq-client --test embedded_hash_aggregate
cargo test -p ffq-client --test embedded_hash_join
cargo test -p ffq-client --test embedded_parquet_sink
cargo test -p ffq-client --test dataframe_write_api
cargo test -p ffq-planner --test physical_plan_serde
```

## Distributed runtime

```bash
cargo test -p ffq-client --test distributed_runtime_roundtrip --features distributed
```

## Vector (brute-force + two-phase local)

```bash
cargo test -p ffq-client --test embedded_vector_topk --features vector
cargo test -p ffq-client --test embedded_two_phase_retrieval --features vector
```

## Vector + qdrant rewrite routing

```bash
cargo test -p ffq-client --test qdrant_routing --features "vector,qdrant"
```

## Distributed + vector two-phase

```bash
cargo test -p ffq-client --test distributed_runtime_roundtrip --features "distributed,vector"
```

## Profiling/metrics exporter surface

```bash
cargo test -p ffq-common --features profiling metrics_handler_returns_prometheus_text
```

## Full workspace sanity

```bash
cargo test
```

Optional broad feature build/test sweep:

```bash
cargo test -p ffq-client --features "distributed,vector,qdrant,profiling"
```

## Acceptance Checks by Subsystem

## Storage and catalog

1. Register parquet table and scan returns expected row count.
2. Table metadata/schema wiring is respected in planning.
3. Save/load catalog flow keeps persisted tables queryable after restart.

Primary tests:

1. `crates/client/tests/embedded_parquet_scan.rs`
2. `crates/client/tests/dataframe_write_api.rs`

## Planner and serialization

1. SQL to logical/physical plan path is serializable.
2. Vector and rewrite plan nodes serialize/deserialize.

Primary test:

1. `crates/planner/tests/physical_plan_serde.rs`

## Core operators (scan/filter/project/agg/join/topk)

1. Hash aggregate returns correct grouped results and handles spill path.
2. Hash join returns correct rows for broadcast and shuffle/spill scenarios.
3. Vector top-k returns ordered best matches for cosine similarity queries.

Primary tests:

1. `crates/client/tests/embedded_hash_aggregate.rs`
2. `crates/client/tests/embedded_hash_join.rs`
3. `crates/client/tests/embedded_vector_topk.rs`

## Shuffle and distributed runtime

1. Distributed collect returns same join/agg result as embedded baseline.
2. Coordinator/worker loop executes task assignment, completion, and result retrieval.
3. Two-worker execution stays deterministic on test fixtures.

Primary test:

1. `crates/client/tests/distributed_runtime_roundtrip.rs`

## Writes and commit semantics

1. `INSERT INTO ... SELECT` writes parquet sink output.
2. DataFrame write APIs support overwrite/append file layout correctly.
3. `save_as_table` is immediately queryable and restart-persistent.
4. Failed writes leave no committed partial table.
5. Overwrite retries remain deterministic (single committed part set).

Primary tests:

1. `crates/client/tests/embedded_parquet_sink.rs`
2. `crates/client/tests/dataframe_write_api.rs`

## Vector/RAG rewrite and fallback

1. Supported qdrant projection rewrites to `VectorTopK`.
2. Unsupported projection falls back to `TopKByScore`.
3. Two-phase retrieval (`VectorTopK -> Join -> rerank`) returns expected rows.

Primary tests:

1. `crates/client/tests/qdrant_routing.rs`
2. `crates/client/tests/embedded_two_phase_retrieval.rs`
3. `crates/client/tests/distributed_runtime_roundtrip.rs` (vector-gated test)

## Observability

1. Prometheus text includes operator/shuffle/spill/scheduler metric families.
2. `/metrics` handler returns scrapeable payload when `profiling` is enabled.

Primary tests:

1. `crates/common/src/metrics.rs` test module
2. `crates/common/src/metrics_exporter.rs` test module (`profiling` feature)

## End-to-End v1 Validation Sequence

Run in this order for a full v1 check:

1. `cargo test --workspace --lib`
2. Baseline embedded integration tests (scan/join/agg/sink/write).
3. Distributed runtime roundtrip (`--features distributed`).
4. Vector local tests (`--features vector`).
5. Qdrant routing rewrite/fallback tests (`--features vector,qdrant`).
6. Distributed + vector roundtrip (`--features distributed,vector`).
7. Profiling metrics handler test (`-p ffq-common --features profiling ...`).
8. Final `cargo test` workspace sweep.

If all steps pass, v1 is validated end-to-end for embedded, distributed (optional), write durability flows, vector/rag routing, and observability surfaces.
