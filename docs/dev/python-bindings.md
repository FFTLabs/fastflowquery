# Python Bindings (`pyo3`)

FFQ exposes Python bindings from `ffq-client` behind the `python` feature.

## API

Python classes:

1. `ffq.Engine`
2. `ffq.DataFrame`

Key methods:

1. `Engine(...).sql(query) -> DataFrame`
2. `DataFrame.explain() -> str`
3. `DataFrame.collect_ipc() -> bytes` (Arrow IPC stream)
4. `DataFrame.collect() -> pyarrow.Table` (requires `pyarrow`)

## Local build/install

Build wheel:

```bash
make python-wheel
```

Editable install into current Python env:

```bash
make python-dev-install
```

## Quick usage

```python
import ffq

e = ffq.Engine()
e.register_table("lineitem", "/abs/path/to/lineitem.parquet")

df = e.sql("SELECT l_orderkey FROM lineitem LIMIT 5")
print(df.explain())
table = df.collect()          # pyarrow.Table
ipc_bytes = df.collect_ipc()  # bytes
```

## Packaging

`pyproject.toml` + `maturin` are configured for wheel builds.

CI workflow:

- `.github/workflows/python-wheels.yml`
  - builds manylinux and macOS wheels
  - installs wheel with `pip`
  - runs a smoke query (`engine.sql(...).collect()`).
