# LEARN-15: API Contract, FFI, and Python Bindings (EPIC 2)

This chapter explains EPIC 2 from `tickets/eng/Plan_v2.md` as a learner-focused contract:

1. what is stable in `Engine`/`DataFrame`
2. how SemVer/deprecation rules are enforced
3. how C ABI and Python bindings map to the same core execution model
4. where extensibility hooks fit into the public API

Primary v2 references:

1. `docs/v2/api-contract.md`
2. `docs/v2/ffi-python.md`
3. `docs/v2/extensibility.md`

## 1) Public API Contract (2.1)

Stable v2 surface centers on:

1. `Engine`
2. `DataFrame`
3. `GroupedDataFrame`

Core workflow contract:

1. `Engine::new/config`
2. table/catalog registration
3. `sql(...)`
4. `collect_stream/collect`

SemVer/deprecation model:

1. incompatible changes are major-version only
2. deprecations require a migration path before removal
3. CI checks both API shape and semver diffs

## 2) C ABI Contract (2.2)

`ffi` feature exposes minimal, stable C lifecycle:

1. engine creation from default/config JSON/config key-value
2. table/catalog registration
3. SQL execution
4. Arrow IPC bytes result retrieval
5. explicit status code + error buffer contract

Why Arrow IPC:

1. language-neutral result transport
2. integrates cleanly with downstream Arrow tooling

## 3) Python Binding Contract (2.3)

`python` feature exposes:

1. `Engine`
2. `DataFrame`
3. `collect()` -> `pyarrow.Table` (or `collect_ipc()` without `pyarrow`)
4. `explain()`

Packaging model:

1. local dev install path
2. wheel build path (`maturin`)
3. CI wheel matrix (linux + macOS) with smoke query checks

## 4) Extensibility Contract (2.4)

Public extension points:

1. `OptimizerRule` register/deregister
2. scalar UDF register/deregister
3. custom physical operator factory register/deregister

Contract-level examples:

1. `my_add(col, 3)` scalar UDF
2. optimizer test rewrite (`x > 10` -> `x >= 11`)
3. custom physical operator factory with capability-aware distributed routing

## 5) EPIC 2 Acceptance Checks (Reproducible)

### API + SemVer

```bash
cargo test -p ffq-client --test public_api_contract
```

### FFI end-to-end

```bash
make ffi-example
```

### Python binding smoke

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

### Extensibility checks

```bash
cargo test -p ffq-client --test udf_api
cargo test -p ffq-client --test physical_registry
cargo test -p ffq-planner --test optimizer_custom_rule
```

## 6) Common Failure Modes

1. API contract break:
   - semver/API CI fails on signature/behavior changes
2. FFI call returns non-OK status:
   - check `err_buf` for planning/execution/config path details
3. Python `collect()` fails:
   - install `pyarrow` or use `collect_ipc()`
4. custom operator in distributed not scheduled:
   - workers do not advertise required capability names in heartbeat

## 7) Code References

1. `crates/client/src/engine.rs`
2. `crates/client/src/dataframe.rs`
3. `crates/client/src/ffi.rs`
4. `crates/client/src/python.rs`
5. `crates/execution/src/udf.rs`
6. `crates/execution/src/physical_registry.rs`
7. `crates/planner/tests/optimizer_custom_rule.rs`
