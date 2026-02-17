use std::collections::HashMap;
#[cfg(feature = "profiling")]
use std::net::SocketAddr;
use std::sync::Arc;

use arrow_schema::Schema;
use ffq_common::{EngineConfig, Result};
use ffq_planner::LiteralValue;
use ffq_storage::parquet_provider::ParquetProvider;
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
        maybe_infer_table_schema_on_register(self.session.config.infer_on_register, &mut table)?;
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
    infer_on_register: bool,
    table: &mut TableDef,
) -> Result<()> {
    if !infer_on_register || !table.format.eq_ignore_ascii_case("parquet") || table.schema.is_some() {
        return Ok(());
    }
    let paths = table.data_paths()?;
    let schema = ParquetProvider::infer_parquet_schema(&paths)?;
    table.schema = Some(schema);
    Ok(())
}
