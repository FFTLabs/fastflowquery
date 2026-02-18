# Integration Runbook (13.2)

This runbook describes how to run and debug the v1 integration suite for:

1. Embedded mode.
2. Distributed mode against docker compose (`coordinator + 2 workers`).
3. Embedded vs distributed parity checks.

Use this page as the source of truth for `13.2.*`.

## Prerequisites

1. Rust toolchain installed (`cargo` available).
2. Docker + Docker Compose available and daemon running.
3. Run commands from repository root.

Quick checks:

```bash
cargo --version
docker --version
docker compose version
```

## Fixtures and Inputs

1. Shared SQL suite:
   - `tests/integration/queries/scan_filter_project.sql`
   - `tests/integration/queries/join_projection.sql`
   - `tests/integration/queries/join_aggregate.sql`
2. Deterministic parquet fixtures:
   - generated/maintained via `crates/client/tests/support/mod.rs`
   - materialized under `tests/fixtures/parquet/`
3. Distributed worker catalog fixture:
   - `tests/fixtures/catalog/tables.json`

## One-command Targets

```bash
make test-13.2-embedded
make test-13.2-distributed
make test-13.2-parity
```

Meaning:

1. `test-13.2-embedded`:
   - runs embedded integration tests only.
2. `test-13.2-distributed`:
   - runs external-cluster distributed integration test via script.
3. `test-13.2-parity`:
   - boots docker compose stack, runs embedded + distributed checks, tears down stack.

## Embedded Flow

Command:

```bash
make test-13.2-embedded
```

Expected result:

1. `integration_parquet_fixtures` passes.
2. `integration_embedded` passes.
3. Snapshot-based normalized outputs remain stable unless intentionally changed.

## Distributed Flow (against compose)

### 1) Start stack

```bash
docker compose -f docker/compose/ffq.yml up --build -d
docker compose -f docker/compose/ffq.yml ps
```

Expected:

1. `coordinator`, `worker-1`, `worker-2` are `healthy`.

### 2) Run distributed integration

```bash
FFQ_COORDINATOR_ENDPOINT=http://127.0.0.1:50051 make test-13.2-distributed
```

What the script does:

1. Waits for coordinator + worker endpoints (`50051`, `50061`, `50062` by default).
2. Uses deterministic temp path: `target/tmp/integration_distributed`.
3. Runs ignored external-cluster test:
   - `crates/client/tests/integration_distributed.rs`

Expected:

1. join + aggregate queries return non-empty rows.
2. asserted expected rows for join/agg pass.
3. normalized parity with embedded results passes for shared query set.

### 3) Cleanup

```bash
docker compose -f docker/compose/ffq.yml down -v
```

## Full parity flow in one command

```bash
make test-13.2-parity
```

Expected:

1. Stack starts.
2. Embedded checks pass.
3. Distributed checks pass.
4. Stack is torn down automatically.

## Debugging and Troubleshooting

### Inspect service state

```bash
docker compose -f docker/compose/ffq.yml ps
docker compose -f docker/compose/ffq.yml logs -f coordinator worker-1 worker-2
```

### Common failures

1. `there is no reactor running`:
   - cause: distributed test executed without Tokio runtime.
   - fix: keep distributed integration test as `#[tokio::test]` and use `.await` (already implemented).

2. `join key ... not found in schema: Valid fields: []`:
   - cause: worker catalog table missing schema.
   - fix: ensure `tests/fixtures/catalog/tables.json` has schemas for `lineitem` and `orders` (and docs when needed).
   - restart compose after catalog changes.

3. `connect coordinator failed: transport error`:
   - cause: coordinator endpoint not reachable.
   - fix: verify compose health and `FFQ_COORDINATOR_ENDPOINT`.

4. `Endpoint not reachable ... after 60s` in script:
   - cause: coordinator/worker ports not ready or blocked.
   - fix: check compose logs; verify ports `50051`, `50061`, `50062`.

### Keep integration temp artifacts

To keep temp files for debugging:

```bash
FFQ_KEEP_INTEGRATION_TMP=1 make test-13.2-distributed
```

Path:

1. `target/tmp/integration_distributed`

## CI mapping

Workflow:

1. `.github/workflows/integration-13_2.yml`

Jobs:

1. `embedded` -> `make test-13.2-embedded`
2. `parity` -> `make test-13.2-parity`

Failure policy:

1. Any embedded failure fails the workflow.
2. Any distributed/parity mismatch fails the workflow.
