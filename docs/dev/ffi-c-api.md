# FFQ C ABI (`ffi` feature)

FFQ exposes a minimal stable C API from `ffq-client` when built with `--features ffi`.

## Build

```bash
cargo build -p ffq-client --features ffi
```

Public header:

- `include/ffq_ffi.h`

## API Surface

Core functions:

1. Engine creation
   - `ffq_engine_new_default`
   - `ffq_engine_new_from_config_json`
   - `ffq_engine_new_from_config_kv`
2. Registration
   - `ffq_engine_register_table_json`
   - `ffq_engine_register_catalog_path`
3. Execution
   - `ffq_engine_execute_sql`
4. Result access
   - `ffq_result_ipc_bytes` (Arrow IPC stream bytes)
   - `ffq_result_row_count`
   - `ffq_result_batch_count`
5. Resource lifecycle
   - `ffq_engine_free`
   - `ffq_result_free`

Error handling:

- return code: `FfqStatusCode`
- optional message buffer: `(char* err_buf, size_t err_buf_len)`

## C Example

Run compile + execute smoke:

```bash
make ffi-example
```

Manual path override:

```bash
PARQUET_PATH=/abs/path/to/lineitem.parquet make ffi-example
```

Example source:

- `examples/c/ffi_example.c`
