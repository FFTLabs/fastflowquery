# FFQ v2 Quickstart

- Status: draft
- Owner: @ffq-docs
- Last Verified Commit: TBD
- Last Verified Date: TBD

This page is standalone: a new contributor can run first query, REPL, FFI/Python bindings, and distributed flow from here only.

## Prerequisites

1. Run from repo root: `fastflowquery/`
2. Rust toolchain installed (`cargo`)
3. Docker + Docker Compose (distributed flow)
4. Python 3.9+ (Python bindings flow)
5. C compiler (`cc`) (FFI flow)

Quick checks:

```bash
cargo --version
docker --version
docker compose version
python --version
cc --version
```

## 1) First Query (Embedded, CLI)

Use fixture parquet via catalog profile:

```bash
cargo run -p ffq-client -- query \
  --catalog tests/fixtures/catalog/tables.json \
  --sql "SELECT l_orderkey, l_quantity FROM lineitem LIMIT 5"
```

Expected:

1. command exits `0`
2. result rows are printed (non-empty output)

Plan-only check:

```bash
cargo run -p ffq-client -- query \
  --catalog tests/fixtures/catalog/tables.json \
  --sql "SELECT l_orderkey FROM lineitem LIMIT 5" \
  --plan
```

Expected:

1. optimized plan text is printed
2. no execution-time output rows (plan mode only)

## 2) REPL First Session

Start REPL with catalog:

```bash
cargo run -p ffq-client -- repl --catalog tests/fixtures/catalog/tables.json
```

Inside REPL:

```sql
\tables
\schema lineitem
SELECT l_orderkey, l_quantity FROM lineitem LIMIT 3;
\mode csv
SELECT l_orderkey FROM lineitem LIMIT 3;
\timing on
SELECT COUNT(*) AS c FROM lineitem;
\q
```

Expected:

1. `\tables` lists tables
2. `\schema` shows columns/types and schema origin
3. `SELECT` returns rows
4. `\mode csv` changes rendering
5. `\timing on` prints elapsed query time

Non-interactive REPL smoke:

```bash
make repl-smoke
```

## 3) Distributed Flow (Coordinator + 2 Workers)

Start cluster:

```bash
docker compose -f docker/compose/ffq.yml up --build -d
docker compose -f docker/compose/ffq.yml ps
```

Run distributed integration suite:

```bash
FFQ_COORDINATOR_ENDPOINT=http://127.0.0.1:50051 make test-13.2-distributed
```

Expected:

1. distributed integration test passes
2. join/agg query returns correct non-empty results

Optional full parity run (boots cluster + embedded + distributed checks):

```bash
make test-13.2-parity
```

Stop cluster:

```bash
docker compose -f docker/compose/ffq.yml down -v
```

## 4) FFI First Flow (C ABI)

Run C example end-to-end:

```bash
make ffi-example
```

What this runs:

1. builds `ffq-client` with `ffi`
2. compiles `examples/c/ffi_example.c`
3. executes `SELECT 1` and parquet scan through C API

Expected output contains:

1. `select1: ...`
2. `parquet_scan: ...`
3. `ffi example: OK`

## 5) Python First Flow

Install dev binding:

```bash
make python-dev-install
python -m pip install pyarrow
```

Run first Python query:

```bash
python - <<'PY'
import ffq

e = ffq.Engine()
e.register_table("lineitem", "tests/fixtures/parquet/lineitem.parquet")
df = e.sql("SELECT l_orderkey FROM lineitem LIMIT 1")
t = df.collect()
assert t.num_rows == 1
print("python quickstart OK", t.to_pydict())
PY
```

Expected:

1. script exits `0`
2. prints `python quickstart OK ...`

Wheel build path (optional):

```bash
make python-wheel
```

## 6) Schema Inference Quick Toggle

If catalog table `schema` entries are omitted for parquet tables, enable inference:

```bash
FFQ_SCHEMA_INFERENCE=on \
FFQ_SCHEMA_DRIFT_POLICY=refresh \
cargo run -p ffq-client -- query \
  --catalog tests/fixtures/catalog/tables.json \
  --sql "SELECT l_orderkey FROM lineitem LIMIT 5"
```

Optional persistence of inferred schema:

```bash
FFQ_SCHEMA_WRITEBACK=true
```

## 7) Common Errors and Fixes

1. `there is no reactor running`:
   - cause: async collection called outside Tokio runtime in test/tooling code
   - fix: run async query collection inside a Tokio runtime (not `futures::executor::block_on` where Tokio IO is required)

2. `join key '...' not found in schema` (distributed):
   - cause: coordinator catalog entry missing/incorrect schema for scanned table
   - fix: verify catalog profile and table schema/path consistency
   - check file: `tests/fixtures/catalog/tables.json`

3. `type mismatch while building Int64 array` on aggregate/query:
   - cause: schema drift or wrong declared type vs actual parquet field type
   - fix: align catalog schema or use schema inference (`FFQ_SCHEMA_INFERENCE=on`)

4. `schema drift detected`:
   - cause: parquet files changed after cached/writeback fingerprint
   - fix: `FFQ_SCHEMA_DRIFT_POLICY=refresh` or regenerate/update catalog metadata

5. `incompatible parquet files`:
   - cause: multi-file table has incompatible schemas beyond allowed merge policy
   - fix: split into separate tables or normalize file schemas

6. `custom physical operator '...' is not registered on worker`:
   - cause: worker process missing custom operator bootstrap registration
   - fix: register factories in every worker process before poll loop
   - see: `docs/v2/custom-operators-deployment.md`

7. `/bin/sh: set: Illegal option -o pipefail` (CI/make context):
   - cause: shell mismatch
   - fix: ensure `Makefile` uses `SHELL := /bin/bash`

8. `Permission denied ... tpch_dbgen_sf1/*.tbl` in CI:
   - cause: fixture file permissions/ownership mismatch
   - fix: regenerate fixture directory with writable permissions in workflow step before generation

## 8) Where to Go Next

1. Distributed runtime details: `docs/v2/distributed-runtime.md`
2. Control-plane RPC details: `docs/v2/control-plane.md`
3. API compatibility contract: `docs/v2/api-contract.md`
4. FFI + Python deep guide: `docs/v2/ffi-python.md`
5. Extensibility and UDF/custom operators: `docs/v2/extensibility.md`
6. Custom operator deployment contract: `docs/v2/custom-operators-deployment.md`
