//! Python bindings for `ffq-client` via `pyo3`.
//!
//! Exposes `Engine`/`DataFrame` with:
//! - SQL execution
//! - `collect_ipc()` returning Arrow IPC bytes
//! - `collect()` returning `pyarrow.Table` when `pyarrow` is installed
//! - `explain()` for optimized logical plan text

use std::collections::HashMap;

use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use ffq_common::{
    CteReusePolicy, EngineConfig, FfqError, SchemaDriftPolicy, SchemaInferencePolicy,
};
use ffq_storage::{Catalog, TableDef, TableStats};
use futures::TryStreamExt;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyModule};

use crate::{DataFrame, Engine};

fn map_ffq_err(err: FfqError) -> PyErr {
    match err {
        FfqError::InvalidConfig(m) => PyValueError::new_err(format!("invalid config: {m}")),
        FfqError::Planning(m) => PyRuntimeError::new_err(format!("planning error: {m}")),
        FfqError::Execution(m) => PyRuntimeError::new_err(format!("execution error: {m}")),
        FfqError::Io(e) => PyRuntimeError::new_err(format!("io error: {e}")),
        FfqError::Unsupported(m) => PyRuntimeError::new_err(format!("unsupported: {m}")),
    }
}

fn apply_config_map(
    config: &mut EngineConfig,
    kv: &HashMap<String, String>,
) -> std::result::Result<(), FfqError> {
    for (key, value) in kv {
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
            "spill_dir" => config.spill_dir = value.clone(),
            "catalog_path" => config.catalog_path = Some(value.clone()),
            "coordinator_endpoint" => config.coordinator_endpoint = Some(value.clone()),
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
            "schema_writeback" => {
                config.schema_writeback = match value.to_ascii_lowercase().as_str() {
                    "true" | "1" | "yes" | "on" => true,
                    "false" | "0" | "no" | "off" => false,
                    other => {
                        return Err(FfqError::InvalidConfig(format!(
                            "invalid schema_writeback '{other}'"
                        )));
                    }
                };
            }
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
) -> std::result::Result<Vec<u8>, FfqError> {
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

#[pyclass(name = "Engine")]
struct PyEngine {
    inner: Engine,
}

#[pymethods]
impl PyEngine {
    #[new]
    #[pyo3(signature = (config_json=None, config=None))]
    fn new(config_json: Option<&str>, config: Option<HashMap<String, String>>) -> PyResult<Self> {
        let mut cfg = if let Some(raw) = config_json {
            serde_json::from_str::<EngineConfig>(raw)
                .map_err(|e| PyValueError::new_err(format!("invalid config JSON: {e}")))?
        } else {
            EngineConfig::default()
        };
        if let Some(kv) = &config {
            apply_config_map(&mut cfg, kv).map_err(map_ffq_err)?;
        }
        let inner = Engine::new(cfg).map_err(map_ffq_err)?;
        Ok(Self { inner })
    }

    fn register_table(
        &self,
        name: &str,
        uri: &str,
        format: Option<&str>,
        options: Option<HashMap<String, String>>,
    ) -> PyResult<()> {
        let table = TableDef {
            name: name.to_string(),
            uri: uri.to_string(),
            paths: vec![],
            format: format.unwrap_or("parquet").to_string(),
            schema: None,
            stats: TableStats::default(),
            options: options.unwrap_or_default(),
        };
        self.inner
            .register_table_checked(name.to_string(), table)
            .map_err(map_ffq_err)
    }

    fn register_table_json(&self, table_json: &str) -> PyResult<()> {
        let table: TableDef = serde_json::from_str(table_json)
            .map_err(|e| PyValueError::new_err(format!("invalid table JSON: {e}")))?;
        self.inner
            .register_table_checked(table.name.clone(), table)
            .map_err(map_ffq_err)
    }

    fn register_catalog(&self, catalog_path: &str) -> PyResult<()> {
        let catalog = Catalog::load(catalog_path).map_err(map_ffq_err)?;
        for table in catalog.tables() {
            self.inner
                .register_table_checked(table.name.clone(), table)
                .map_err(map_ffq_err)?;
        }
        Ok(())
    }

    fn sql(&self, query: &str) -> PyResult<PyDataFrame> {
        let df = self.inner.sql(query).map_err(map_ffq_err)?;
        Ok(PyDataFrame { inner: df })
    }

    fn list_tables(&self) -> Vec<String> {
        self.inner.list_tables()
    }
}

#[pyclass(name = "DataFrame")]
struct PyDataFrame {
    inner: DataFrame,
}

#[pymethods]
impl PyDataFrame {
    fn explain(&self) -> PyResult<String> {
        self.inner.explain().map_err(map_ffq_err)
    }

    fn collect_ipc<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        let stream =
            futures::executor::block_on(self.inner.collect_stream()).map_err(map_ffq_err)?;
        let schema = stream.schema();
        let batches =
            futures::executor::block_on(stream.try_collect::<Vec<_>>()).map_err(map_ffq_err)?;
        let payload = encode_ipc(schema, &batches).map_err(map_ffq_err)?;
        Ok(PyBytes::new_bound(py, &payload))
    }

    fn collect<'py>(&self, py: Python<'py>) -> PyResult<PyObject> {
        let ipc_bytes = self.collect_ipc(py)?;
        let pyarrow = PyModule::import_bound(py, "pyarrow").map_err(|_| {
            PyRuntimeError::new_err(
                "pyarrow is required for DataFrame.collect(); use collect_ipc() if unavailable",
            )
        })?;
        let ipc = PyModule::import_bound(py, "pyarrow.ipc").map_err(|_| {
            PyRuntimeError::new_err(
                "pyarrow.ipc is required for DataFrame.collect(); use collect_ipc() if unavailable",
            )
        })?;
        let reader = ipc.call_method1("open_stream", (ipc_bytes,))?;
        let table = reader.call_method0("read_all")?;
        let _ = pyarrow; // imported for clearer error classification and future extension
        Ok(table.into_py(py))
    }
}

/// Python extension module entrypoint.
#[pymodule]
fn _native(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyEngine>()?;
    m.add_class::<PyDataFrame>()?;
    Ok(())
}
