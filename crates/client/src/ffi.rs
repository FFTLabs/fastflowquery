//! Stable C ABI for embedding FFQ from non-Rust runtimes.
//!
//! This module is enabled by the `ffi` feature and exports a minimal API:
//! - create engine from JSON config or key/value config
//! - register tables/catalog
//! - execute SQL
//! - fetch Arrow IPC stream bytes for result batches
//! - free resources
//!
//! Error handling contract:
//! - all fallible functions return [`FfqStatusCode`]
//! - optional `err_buf`/`err_buf_len` receives a UTF-8 message on failure
//! - success clears `err_buf` (empty string) when buffer is provided

use std::ffi::{CStr, c_char};
use std::panic::{AssertUnwindSafe, catch_unwind};

use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use ffq_common::{
    CteReusePolicy, EngineConfig, FfqError, SchemaDriftPolicy, SchemaInferencePolicy,
};
use ffq_storage::{Catalog, TableDef};
use futures::TryStreamExt;

use crate::Engine;

struct EngineHandle {
    engine: Engine,
}

struct ResultHandle {
    ipc_payload: Vec<u8>,
    rows: usize,
    batches: usize,
}

/// Opaque C handle for an FFQ engine instance.
#[repr(C)]
pub struct FfqEngineHandle {
    _private: [u8; 0],
}

/// Opaque C handle for SQL execution results.
#[repr(C)]
pub struct FfqResultHandle {
    _private: [u8; 0],
}

/// Stable status code set for C ABI calls.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FfqStatusCode {
    /// Operation succeeded.
    Ok = 0,
    /// Invalid configuration or catalog contract failure.
    InvalidConfig = 1,
    /// Planning/analyzer/optimizer failure.
    Planning = 2,
    /// Runtime execution failure.
    Execution = 3,
    /// I/O failure.
    Io = 4,
    /// Unsupported feature/query shape.
    Unsupported = 5,
    /// Panic or unknown internal failure.
    Internal = 6,
}

fn map_error(err: &FfqError) -> FfqStatusCode {
    match err {
        FfqError::InvalidConfig(_) => FfqStatusCode::InvalidConfig,
        FfqError::Planning(_) => FfqStatusCode::Planning,
        FfqError::Execution(_) => FfqStatusCode::Execution,
        FfqError::Io(_) => FfqStatusCode::Io,
        FfqError::Unsupported(_) => FfqStatusCode::Unsupported,
    }
}

fn write_error(buf: *mut c_char, buf_len: usize, msg: &str) {
    if buf.is_null() || buf_len == 0 {
        return;
    }
    let bytes = msg.as_bytes();
    let to_copy = bytes.len().min(buf_len.saturating_sub(1));
    // SAFETY: caller provides a writable C buffer of size `buf_len`.
    unsafe {
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), buf.cast::<u8>(), to_copy);
        *buf.add(to_copy) = 0;
    }
}

fn clear_error(buf: *mut c_char, buf_len: usize) {
    if buf.is_null() || buf_len == 0 {
        return;
    }
    // SAFETY: caller provides a writable C buffer of size `buf_len`.
    unsafe {
        *buf = 0;
    }
}

fn parse_cstr_owned(ptr: *const c_char, field: &str) -> std::result::Result<String, FfqError> {
    if ptr.is_null() {
        return Err(FfqError::InvalidConfig(format!("{field} pointer is null")));
    }
    // SAFETY: ptr checked for null; caller promises NUL-terminated string.
    let raw = unsafe { CStr::from_ptr(ptr) };
    let val = raw
        .to_str()
        .map_err(|e| FfqError::InvalidConfig(format!("{field} is not valid UTF-8: {e}")))?;
    Ok(val.to_string())
}

fn parse_bool(raw: &str) -> std::result::Result<bool, FfqError> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Ok(true),
        "false" | "0" | "no" | "off" => Ok(false),
        other => Err(FfqError::InvalidConfig(format!(
            "invalid bool value '{other}'"
        ))),
    }
}

fn apply_config_kv(config: &mut EngineConfig, kv: &str) -> std::result::Result<(), FfqError> {
    for pair in kv
        .split([',', ';'])
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        let Some((k, v)) = pair.split_once('=') else {
            return Err(FfqError::InvalidConfig(format!(
                "invalid config pair '{pair}', expected key=value"
            )));
        };
        let key = k.trim().to_ascii_lowercase();
        let value = v.trim();
        match key.as_str() {
            "batch_size_rows" => {
                config.batch_size_rows = value.parse().map_err(|e| {
                    FfqError::InvalidConfig(format!("invalid batch_size_rows '{value}': {e}"))
                })?
            }
            "mem_budget_bytes" => {
                config.mem_budget_bytes = value.parse().map_err(|e| {
                    FfqError::InvalidConfig(format!("invalid mem_budget_bytes '{value}': {e}"))
                })?
            }
            "shuffle_partitions" => {
                config.shuffle_partitions = value.parse().map_err(|e| {
                    FfqError::InvalidConfig(format!("invalid shuffle_partitions '{value}': {e}"))
                })?
            }
            "broadcast_threshold_bytes" => {
                config.broadcast_threshold_bytes = value.parse().map_err(|e| {
                    FfqError::InvalidConfig(format!(
                        "invalid broadcast_threshold_bytes '{value}': {e}"
                    ))
                })?
            }
            "join_radix_bits" => {
                config.join_radix_bits = value.parse().map_err(|e| {
                    FfqError::InvalidConfig(format!("invalid join_radix_bits '{value}': {e}"))
                })?
            }
            "spill_dir" => config.spill_dir = value.to_string(),
            "catalog_path" => config.catalog_path = Some(value.to_string()),
            "coordinator_endpoint" => config.coordinator_endpoint = Some(value.to_string()),
            "schema_inference" => {
                config.schema_inference = match value.to_ascii_lowercase().as_str() {
                    "off" => SchemaInferencePolicy::Off,
                    "on" => SchemaInferencePolicy::On,
                    "strict" => SchemaInferencePolicy::Strict,
                    "permissive" => SchemaInferencePolicy::Permissive,
                    other => {
                        return Err(FfqError::InvalidConfig(format!(
                            "invalid schema_inference '{other}'"
                        )));
                    }
                };
            }
            "schema_drift_policy" => {
                config.schema_drift_policy = match value.to_ascii_lowercase().as_str() {
                    "fail" => SchemaDriftPolicy::Fail,
                    "refresh" => SchemaDriftPolicy::Refresh,
                    other => {
                        return Err(FfqError::InvalidConfig(format!(
                            "invalid schema_drift_policy '{other}'"
                        )));
                    }
                };
            }
            "schema_writeback" => config.schema_writeback = parse_bool(value)?,
            "cte_reuse_policy" => {
                config.cte_reuse_policy = match value.to_ascii_lowercase().as_str() {
                    "inline" => CteReusePolicy::Inline,
                    "materialize" => CteReusePolicy::Materialize,
                    other => {
                        return Err(FfqError::InvalidConfig(format!(
                            "invalid cte_reuse_policy '{other}'"
                        )));
                    }
                };
            }
            other => {
                return Err(FfqError::InvalidConfig(format!(
                    "unknown config key '{other}'"
                )));
            }
        }
    }
    Ok(())
}

fn encode_ipc(
    schema: arrow_schema::SchemaRef,
    batches: &[RecordBatch],
) -> ffq_common::Result<Vec<u8>> {
    let mut out = Vec::new();
    let mut writer = StreamWriter::try_new(&mut out, schema.as_ref())
        .map_err(|e| FfqError::Execution(format!("ipc writer init failed: {e}")))?;
    for batch in batches {
        writer
            .write(batch)
            .map_err(|e| FfqError::Execution(format!("ipc write failed: {e}")))?;
    }
    writer
        .finish()
        .map_err(|e| FfqError::Execution(format!("ipc finish failed: {e}")))?;
    Ok(out)
}

fn with_unwind_guard<F>(err_buf: *mut c_char, err_buf_len: usize, f: F) -> FfqStatusCode
where
    F: FnOnce() -> std::result::Result<(), FfqError>,
{
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok(())) => {
            clear_error(err_buf, err_buf_len);
            FfqStatusCode::Ok
        }
        Ok(Err(err)) => {
            write_error(err_buf, err_buf_len, &err.to_string());
            map_error(&err)
        }
        Err(_) => {
            write_error(err_buf, err_buf_len, "panic crossed FFI boundary");
            FfqStatusCode::Internal
        }
    }
}

/// Creates an engine from default config.
///
/// `out_engine` must be a valid non-null pointer to receive an opaque handle.
#[unsafe(no_mangle)]
pub extern "C" fn ffq_engine_new_default(
    out_engine: *mut *mut FfqEngineHandle,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> FfqStatusCode {
    with_unwind_guard(err_buf, err_buf_len, || {
        if out_engine.is_null() {
            return Err(FfqError::InvalidConfig("out_engine is null".to_string()));
        }
        let engine = Engine::new(EngineConfig::default())?;
        let handle = Box::new(EngineHandle { engine });
        // SAFETY: out_engine was validated non-null above.
        unsafe {
            *out_engine = Box::into_raw(handle).cast::<FfqEngineHandle>();
        }
        Ok(())
    })
}

/// Creates an engine from JSON-encoded [`EngineConfig`].
#[unsafe(no_mangle)]
pub extern "C" fn ffq_engine_new_from_config_json(
    config_json: *const c_char,
    out_engine: *mut *mut FfqEngineHandle,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> FfqStatusCode {
    with_unwind_guard(err_buf, err_buf_len, || {
        if out_engine.is_null() {
            return Err(FfqError::InvalidConfig("out_engine is null".to_string()));
        }
        let raw = parse_cstr_owned(config_json, "config_json")?;
        let config: EngineConfig = serde_json::from_str(&raw)
            .map_err(|e| FfqError::InvalidConfig(format!("invalid config JSON: {e}")))?;
        let engine = Engine::new(config)?;
        let handle = Box::new(EngineHandle { engine });
        // SAFETY: out_engine was validated non-null above.
        unsafe {
            *out_engine = Box::into_raw(handle).cast::<FfqEngineHandle>();
        }
        Ok(())
    })
}

/// Creates an engine from key/value config pairs (`key=value,key=value`).
#[unsafe(no_mangle)]
pub extern "C" fn ffq_engine_new_from_config_kv(
    config_kv: *const c_char,
    out_engine: *mut *mut FfqEngineHandle,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> FfqStatusCode {
    with_unwind_guard(err_buf, err_buf_len, || {
        if out_engine.is_null() {
            return Err(FfqError::InvalidConfig("out_engine is null".to_string()));
        }
        let raw = parse_cstr_owned(config_kv, "config_kv")?;
        let mut config = EngineConfig::default();
        apply_config_kv(&mut config, &raw)?;
        let engine = Engine::new(config)?;
        let handle = Box::new(EngineHandle { engine });
        // SAFETY: out_engine was validated non-null above.
        unsafe {
            *out_engine = Box::into_raw(handle).cast::<FfqEngineHandle>();
        }
        Ok(())
    })
}

/// Frees an engine handle created by `ffq_engine_new_*`.
#[unsafe(no_mangle)]
pub extern "C" fn ffq_engine_free(engine: *mut FfqEngineHandle) {
    if engine.is_null() {
        return;
    }
    // SAFETY: ownership is transferred back to Rust exactly once by caller.
    let boxed = unsafe { Box::from_raw(engine.cast::<EngineHandle>()) };
    let _ = futures::executor::block_on(boxed.engine.shutdown());
}

/// Registers a single table from JSON-encoded [`TableDef`].
#[unsafe(no_mangle)]
pub extern "C" fn ffq_engine_register_table_json(
    engine: *mut FfqEngineHandle,
    table_json: *const c_char,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> FfqStatusCode {
    with_unwind_guard(err_buf, err_buf_len, || {
        if engine.is_null() {
            return Err(FfqError::InvalidConfig("engine is null".to_string()));
        }
        let raw = parse_cstr_owned(table_json, "table_json")?;
        let table: TableDef = serde_json::from_str(&raw)
            .map_err(|e| FfqError::InvalidConfig(format!("invalid table JSON: {e}")))?;
        let name = table.name.clone();
        // SAFETY: engine pointer validated non-null above and points to valid EngineHandle.
        let h = unsafe { &mut *engine.cast::<EngineHandle>() };
        h.engine.register_table_checked(name, table)?;
        Ok(())
    })
}

/// Loads catalog file (`.json`/`.toml`) and registers all tables into the engine.
#[unsafe(no_mangle)]
pub extern "C" fn ffq_engine_register_catalog_path(
    engine: *mut FfqEngineHandle,
    catalog_path: *const c_char,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> FfqStatusCode {
    with_unwind_guard(err_buf, err_buf_len, || {
        if engine.is_null() {
            return Err(FfqError::InvalidConfig("engine is null".to_string()));
        }
        let path = parse_cstr_owned(catalog_path, "catalog_path")?;
        let catalog = Catalog::load(&path)?;
        // SAFETY: engine pointer validated non-null above and points to valid EngineHandle.
        let h = unsafe { &mut *engine.cast::<EngineHandle>() };
        for table in catalog.tables() {
            h.engine.register_table_checked(table.name.clone(), table)?;
        }
        Ok(())
    })
}

/// Executes SQL and returns a result handle with Arrow IPC stream payload.
#[unsafe(no_mangle)]
pub extern "C" fn ffq_engine_execute_sql(
    engine: *mut FfqEngineHandle,
    sql: *const c_char,
    out_result: *mut *mut FfqResultHandle,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> FfqStatusCode {
    with_unwind_guard(err_buf, err_buf_len, || {
        if engine.is_null() {
            return Err(FfqError::InvalidConfig("engine is null".to_string()));
        }
        if out_result.is_null() {
            return Err(FfqError::InvalidConfig("out_result is null".to_string()));
        }
        let query = parse_cstr_owned(sql, "sql")?;
        // SAFETY: engine pointer validated non-null above and points to valid EngineHandle.
        let h = unsafe { &mut *engine.cast::<EngineHandle>() };
        let df = h.engine.sql(&query)?;
        let stream = futures::executor::block_on(df.collect_stream())?;
        let schema = stream.schema();
        let batches = futures::executor::block_on(stream.try_collect::<Vec<_>>())?;
        let rows = batches.iter().map(RecordBatch::num_rows).sum();
        let payload = encode_ipc(schema, &batches)?;
        let result = Box::new(ResultHandle {
            ipc_payload: payload,
            rows,
            batches: batches.len(),
        });
        // SAFETY: out_result validated non-null above.
        unsafe {
            *out_result = Box::into_raw(result).cast::<FfqResultHandle>();
        }
        Ok(())
    })
}

/// Frees a result handle created by [`ffq_engine_execute_sql`].
#[unsafe(no_mangle)]
pub extern "C" fn ffq_result_free(result: *mut FfqResultHandle) {
    if result.is_null() {
        return;
    }
    // SAFETY: ownership is transferred back to Rust exactly once by caller.
    let _ = unsafe { Box::from_raw(result.cast::<ResultHandle>()) };
}

/// Returns result payload as Arrow IPC stream bytes.
///
/// Pointers remain valid until `ffq_result_free` is called.
#[unsafe(no_mangle)]
pub extern "C" fn ffq_result_ipc_bytes(
    result: *const FfqResultHandle,
    out_ptr: *mut *const u8,
    out_len: *mut usize,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> FfqStatusCode {
    with_unwind_guard(err_buf, err_buf_len, || {
        if result.is_null() {
            return Err(FfqError::InvalidConfig("result is null".to_string()));
        }
        if out_ptr.is_null() || out_len.is_null() {
            return Err(FfqError::InvalidConfig(
                "out_ptr/out_len must be non-null".to_string(),
            ));
        }
        // SAFETY: result pointer validated non-null above and points to valid ResultHandle.
        let r = unsafe { &*result.cast::<ResultHandle>() };
        // SAFETY: output pointers validated non-null above.
        unsafe {
            *out_ptr = r.ipc_payload.as_ptr();
            *out_len = r.ipc_payload.len();
        }
        Ok(())
    })
}

/// Returns row count across all batches in this result.
#[unsafe(no_mangle)]
pub extern "C" fn ffq_result_row_count(result: *const FfqResultHandle) -> usize {
    if result.is_null() {
        return 0;
    }
    // SAFETY: pointer checked for null; caller promises valid handle.
    let r = unsafe { &*result.cast::<ResultHandle>() };
    r.rows
}

/// Returns batch count in this result.
#[unsafe(no_mangle)]
pub extern "C" fn ffq_result_batch_count(result: *const FfqResultHandle) -> usize {
    if result.is_null() {
        return 0;
    }
    // SAFETY: pointer checked for null; caller promises valid handle.
    let r = unsafe { &*result.cast::<ResultHandle>() };
    r.batches
}

/// Returns the FFQ status code symbolic name.
#[unsafe(no_mangle)]
pub extern "C" fn ffq_status_name(code: FfqStatusCode) -> *const c_char {
    static OK: &[u8] = b"OK\0";
    static INVALID_CONFIG: &[u8] = b"INVALID_CONFIG\0";
    static PLANNING: &[u8] = b"PLANNING\0";
    static EXECUTION: &[u8] = b"EXECUTION\0";
    static IO: &[u8] = b"IO\0";
    static UNSUPPORTED: &[u8] = b"UNSUPPORTED\0";
    static INTERNAL: &[u8] = b"INTERNAL\0";
    match code {
        FfqStatusCode::Ok => OK.as_ptr().cast::<c_char>(),
        FfqStatusCode::InvalidConfig => INVALID_CONFIG.as_ptr().cast::<c_char>(),
        FfqStatusCode::Planning => PLANNING.as_ptr().cast::<c_char>(),
        FfqStatusCode::Execution => EXECUTION.as_ptr().cast::<c_char>(),
        FfqStatusCode::Io => IO.as_ptr().cast::<c_char>(),
        FfqStatusCode::Unsupported => UNSUPPORTED.as_ptr().cast::<c_char>(),
        FfqStatusCode::Internal => INTERNAL.as_ptr().cast::<c_char>(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_kv_updates_config() {
        let mut cfg = EngineConfig::default();
        apply_config_kv(
            &mut cfg,
            "batch_size_rows=1024,mem_budget_bytes=2048,schema_inference=permissive",
        )
        .expect("kv parse");
        assert_eq!(cfg.batch_size_rows, 1024);
        assert_eq!(cfg.mem_budget_bytes, 2048);
        assert_eq!(cfg.schema_inference, SchemaInferencePolicy::Permissive);
    }
}
