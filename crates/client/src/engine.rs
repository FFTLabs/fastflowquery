use std::collections::HashMap;
#[cfg(feature = "profiling")]
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow_schema::Schema;
use ffq_common::{EngineConfig, Result, SchemaInferencePolicy};
use ffq_planner::LiteralValue;
use ffq_storage::parquet_provider::{FileFingerprint, ParquetProvider};
use ffq_storage::TableDef;

use crate::session::{Session, SharedSession};
use crate::DataFrame;

#[derive(Clone)]
pub struct Engine {
    session: SharedSession,
}

impl Engine {
    pub fn new(config: EngineConfig) -> Result<Self> {
        let session = Arc::new(Session::new(config)?);
        Ok(Self { session })
    }

    /// Register a table under a given name.
    /// We override `table.name` to avoid ambiguity.
    pub fn register_table(&self, name: impl Into<String>, table: TableDef) {
        self.register_table_checked(name, table)
            .expect("table registration failed");
    }

    pub fn register_table_checked(&self, name: impl Into<String>, mut table: TableDef) -> Result<()> {
        table.name = name.into();
        maybe_infer_table_schema_on_register(self.session.config.schema_inference, &mut table)?;
        self.session
            .catalog
            .write()
            .expect("catalog lock poisoned")
            .register_table(table);
        Ok(())
    }

    pub fn sql(&self, query: &str) -> Result<DataFrame> {
        let logical = self.session.planner.plan_sql(query)?;
        Ok(DataFrame::new(self.session.clone(), logical))
    }

    pub fn sql_with_params(
        &self,
        query: &str,
        params: HashMap<String, LiteralValue>,
    ) -> Result<DataFrame> {
        let logical = self.session.planner.plan_sql_with_params(query, &params)?;
        Ok(DataFrame::new(self.session.clone(), logical))
    }

    pub fn table(&self, name: &str) -> Result<DataFrame> {
        Ok(DataFrame::table(self.session.clone(), name))
    }

    pub fn list_tables(&self) -> Vec<String> {
        self.session
            .catalog
            .read()
            .expect("catalog lock poisoned")
            .tables()
            .into_iter()
            .map(|t| t.name)
            .collect()
    }

    pub fn table_schema(&self, name: &str) -> Result<Option<Schema>> {
        let cat = self.session.catalog.read().expect("catalog lock poisoned");
        let table = cat.get(name)?;
        Ok(table.schema.clone())
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.session.runtime.shutdown().await
    }

    pub fn prometheus_metrics(&self) -> String {
        self.session.prometheus_metrics()
    }

    #[cfg(feature = "profiling")]
    pub async fn serve_metrics_exporter(&self, addr: SocketAddr) -> Result<()> {
        self.session.serve_metrics_exporter(addr).await
    }
}

pub(crate) fn maybe_infer_table_schema_on_register(
    inference_policy: SchemaInferencePolicy,
    table: &mut TableDef,
) -> Result<bool> {
    if !inference_policy.allows_inference()
        || !table.format.eq_ignore_ascii_case("parquet")
        || table.schema.is_some()
    {
        return Ok(false);
    }
    let paths = table.data_paths()?;
    let fingerprint = ParquetProvider::fingerprint_paths(&paths)?;
    let schema = ParquetProvider::infer_parquet_schema_with_policy(
        &paths,
        inference_policy.is_permissive_merge(),
    )?;
    table.schema = Some(schema);
    annotate_schema_inference_metadata(table, &fingerprint)?;
    Ok(true)
}

pub(crate) fn annotate_schema_inference_metadata(
    table: &mut TableDef,
    fingerprint: &[FileFingerprint],
) -> Result<()> {
    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    table.options.insert(
        "schema.inferred_at".to_string(),
        now_secs.to_string(),
    );
    table.options.insert(
        "schema.fingerprint".to_string(),
        serde_json::to_string(fingerprint).map_err(|e| {
            ffq_common::FfqError::InvalidConfig(format!(
                "failed to encode schema fingerprint metadata: {e}"
            ))
        })?,
    );
    Ok(())
}

pub(crate) fn read_schema_fingerprint_metadata(
    table: &TableDef,
) -> Result<Option<Vec<FileFingerprint>>> {
    let Some(raw) = table.options.get("schema.fingerprint") else {
        return Ok(None);
    };
    let fp: Vec<FileFingerprint> = serde_json::from_str(raw).map_err(|e| {
        ffq_common::FfqError::InvalidConfig(format!(
            "invalid schema fingerprint metadata for table '{}': {e}",
            table.name
        ))
    })?;
    Ok(Some(fp))
}
