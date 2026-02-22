# FFI + Python Bindings (v2)

- Status: draft
- Owner: @ffq-api
- Last Verified Commit: 7888e4c
- Last Verified Date: 2026-02-21

## Scope

This page is the user-facing bindings guide for v2 EPIC 2.2/2.3.

It covers:

1. C ABI (`ffi` feature)
2. Python bindings (`python` feature)
3. local build/packaging flows
4. wheel CI/wheel smoke behavior
5. constraints and troubleshooting

Primary references:

1. `crates/client/src/ffi.rs`
2. `include/ffq_ffi.h`
3. `examples/c/ffi_example.c`
4. `scripts/run-ffi-c-example.sh`
5. `crates/client/src/python.rs`
6. `python/ffq/__init__.py`
7. `pyproject.toml`
8. `.github/workflows/python-wheels.yml`

## C ABI (Feature `ffi`)

### What the stable C API provides

The C ABI exposes a minimal engine lifecycle:

1. create engine (`ffq_engine_new_default`, `ffq_engine_new_from_config_json`, `ffq_engine_new_from_config_kv`)
2. register data (`ffq_engine_register_table_json`, `ffq_engine_register_catalog_path`)
3. execute SQL (`ffq_engine_execute_sql`)
4. fetch results as Arrow IPC bytes (`ffq_result_ipc_bytes`)
5. inspect row/batch counts (`ffq_result_row_count`, `ffq_result_batch_count`)
6. release handles (`ffq_result_free`, `ffq_engine_free`)

Error contract:

1. all fallible calls return `FfqStatusCode`
2. optional `err_buf` receives message text on failure
3. status names are available via `ffq_status_name`

Header: `include/ffq_ffi.h`

### End-to-end runnable C flow

Prerequisites:

1. Rust toolchain
2. C compiler (`cc`)
3. parquet fixture file (default uses `tests/fixtures/parquet/lineitem.parquet`)

Run:

```bash
make ffi-example
```

Equivalent manual run:

```bash
cargo build -p ffq-client --features ffi
./scripts/run-ffi-c-example.sh tests/fixtures/parquet/lineitem.parquet
```

What this does:

1. builds `ffq-client` as `cdylib` with `ffi`
2. compiles `examples/c/ffi_example.c`
3. runs two queries through C ABI:
   - `SELECT 1 AS one FROM lineitem LIMIT 1`
   - `SELECT l_orderkey FROM lineitem LIMIT 5`

Expected output includes lines like:

1. `select1: batches=... rows=... ipc_bytes=...`
2. `parquet_scan: batches=... rows=... ipc_bytes=...`
3. `ffi example: OK`

## Python Bindings (Feature `python`)

### Python API surface

Python module package: `ffq` (native module `ffq._native`)

Classes:

1. `ffq.Engine`
2. `ffq.DataFrame`

Core methods:

1. `Engine(config_json=None, config=None)`
2. `Engine.register_table(name, uri, format=None, options=None)`
3. `Engine.register_table_json(table_json)`
4. `Engine.register_catalog(catalog_path)`
5. `Engine.sql(query)`
6. `Engine.list_tables()`
7. `DataFrame.explain()`
8. `DataFrame.collect_ipc()` -> Arrow IPC bytes
9. `DataFrame.collect()` -> `pyarrow.Table` (requires `pyarrow`)

### End-to-end runnable Python flow (local dev)

Prerequisites:

1. Python 3.9+
2. Rust toolchain
3. `maturin`
4. `pyarrow` (if using `collect()`)

Install development binding:

```bash
make python-dev-install
python -m pip install pyarrow
```

Run query flow:

```bash
python - <<'PY'
import ffq

lineitem = "tests/fixtures/parquet/lineitem.parquet"
engine = ffq.Engine()
engine.register_table("lineitem", lineitem)

df = engine.sql("SELECT l_orderkey FROM lineitem LIMIT 3")
print(df.explain())

tbl = df.collect()
print("rows:", tbl.num_rows)
print(tbl.to_pydict())
PY
```

Expected:

1. `explain()` prints optimized logical plan text
2. `tbl.num_rows` equals `3`
3. printed rows contain `l_orderkey`

### IPC-only Python flow (without `pyarrow`)

Use `collect_ipc()` if `pyarrow` is not installed:

```bash
python - <<'PY'
import ffq

e = ffq.Engine()
e.register_table("lineitem", "tests/fixtures/parquet/lineitem.parquet")
ipc_bytes = e.sql("SELECT l_orderkey FROM lineitem LIMIT 1").collect_ipc()
print("ipc bytes:", len(ipc_bytes))
PY
```

## Packaging and Wheels

### Local wheel build

```bash
make python-wheel
```

This runs `maturin build --release` and produces wheel(s).

### CI wheel matrix

Workflow: `.github/workflows/python-wheels.yml`

Jobs:

1. `wheel-linux`
2. `wheel-macos`

Each job:

1. builds wheel via `PyO3/maturin-action`
2. installs wheel + `pyarrow`
3. runs smoke query (`engine.sql(...).collect()`)
4. uploads wheel artifact

## Configuration Notes

Both C and Python flows support config overrides for runtime/schema behavior.

Common keys:

1. `batch_size_rows`
2. `mem_budget_bytes`
3. `shuffle_partitions`
4. `broadcast_threshold_bytes`
5. `spill_dir`
6. `catalog_path`
7. `coordinator_endpoint`
8. `schema_inference` (`off|on|strict|permissive`)
9. `schema_drift_policy` (`fail|refresh`)
10. `schema_writeback` (`true|false`)

## Constraints

1. C API returns Arrow IPC bytes, not C Data Interface pointers.
2. Python `collect()` requires `pyarrow`; otherwise use `collect_ipc()`.
3. FFI ABI stability is tied to exported functions in `include/ffq_ffi.h` and `crates/client/src/ffi.rs`.
4. Distributed runtime in bindings requires building with `distributed` and setting coordinator endpoint.

## Troubleshooting

### C flow

1. `missing parquet fixture`:
   - verify path passed to `scripts/run-ffi-c-example.sh`
2. linker cannot find `ffq_client`:
   - run from repo root; ensure `cargo build -p ffq-client --features ffi` succeeded
3. non-`OK` `FfqStatusCode` from query:
   - print `err_buf`; validate SQL/table registration and file paths

### Python flow

1. `ModuleNotFoundError: ffq`:
   - run `make python-dev-install` in active virtual environment
2. `pyarrow is required for DataFrame.collect()`:
   - install `pyarrow` or switch to `collect_ipc()`
3. invalid config errors:
   - ensure config key names match accepted list above
4. planning/execution errors for parquet tables:
   - check table path, schema inference policy, and file availability

## Verification Commands

```bash
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

Expected:

1. C flow prints `ffi example: OK`
2. Python flow prints `python binding smoke: OK`
