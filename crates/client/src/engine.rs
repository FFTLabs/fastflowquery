use std::collections::HashMap;
use std::sync::Arc;

use ffq_common::{EngineConfig, Result};
use ffq_planner::LiteralValue;
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
    pub fn register_table(&self, name: impl Into<String>, mut table: TableDef) {
        table.name = name.into();
        self.session
            .catalog
            .write()
            .expect("catalog lock poisoned")
            .register_table(table);
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

    pub async fn shutdown(&self) -> Result<()> {
        self.session.runtime.shutdown().await
    }
}
