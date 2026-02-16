# FFQ v1 Quickstart

This page is the fastest way to run FFQ v1 end-to-end.

## Prerequisites

1. Rust toolchain (`cargo`)
2. Docker + Compose (only for distributed mode)
3. Run from repo root

Quick checks:

```bash
cargo --version
docker --version
docker compose version
```

## 10-minute Path (Embedded)

1. Build:

```bash
cargo build
```

2. Run core embedded validation:

```bash
make test-13.2-embedded
```

3. Run synthetic benchmark baseline:

```bash
make bench-13.3-embedded
```

Success signals:

1. Integration tests pass.
2. Benchmark JSON/CSV artifacts are created under `tests/bench/results/`.

## Distributed Smoke Path

1. Start cluster:

```bash
docker compose -f docker/compose/ffq.yml up --build -d
docker compose -f docker/compose/ffq.yml ps
```

2. Run distributed integration:

```bash
FFQ_COORDINATOR_ENDPOINT=http://127.0.0.1:50051 make test-13.2-distributed
```

3. Optional distributed benchmark:

```bash
FFQ_COORDINATOR_ENDPOINT=http://127.0.0.1:50051 make bench-13.3-distributed
```

4. Cleanup:

```bash
docker compose -f docker/compose/ffq.yml down -v
```

## Benchmarks: Which Track to Use

1. Synthetic track (`13.3`): fast dev loop, trend checks.
2. Official track (`13.4`): reportable TPC-H Q1/Q3 numbers.

## Official TPC-H Flow (dbgen)

1. Build dbgen and generate `.tbl`:

```bash
make tpch-dbgen-sf1
```

2. Convert to parquet:

```bash
make tpch-dbgen-parquet
```

3. Validate manifest contract:

```bash
make validate-tpch-dbgen-manifests
```

4. Run official benchmark (embedded):

```bash
make bench-13.4-official-embedded
```

5. Optional official benchmark (distributed):

```bash
FFQ_COORDINATOR_ENDPOINT=http://127.0.0.1:50051 make bench-13.4-official-distributed
```

Success signals:

1. `make validate-tpch-dbgen-manifests` exits `0`.
2. Official benchmark artifacts are written under `tests/bench/results/official_tpch/`.
3. Any correctness divergence fails the run with explicit error in artifact `results[].error`.

## Most Common Failures

1. `FFQ_COORDINATOR_ENDPOINT` missing/invalid:
   - set `FFQ_COORDINATOR_ENDPOINT=http://127.0.0.1:50051`
2. `join key ... not found in schema` in distributed runs:
   - ensure `tests/fixtures/catalog/tables.json` contains schemas.
3. `Open failed for ./dists.dss` during dbgen:
   - fixed by current scripts; rerun `make tpch-dbgen-sf1`.
4. Manifest validation failure:
   - regenerate with pinned ref path:
     - `make tpch-dbgen-sf1`
     - `make tpch-dbgen-parquet`
     - `make validate-tpch-dbgen-manifests`

## Next Docs

1. Integration runbook: `docs/v1/integration-13.2.md`
2. Benchmark contract: `docs/v1/benchmarks.md`
3. Full test playbook: `docs/v1/testing.md`
