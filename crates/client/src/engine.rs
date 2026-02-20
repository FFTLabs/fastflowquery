use std::collections::HashMap;
#[cfg(feature = "profiling")]
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow_schema::Schema;
use ffq_common::{EngineConfig, Result, SchemaInferencePolicy};
use ffq_execution::{ScalarUdf, deregister_scalar_udf, register_scalar_udf};
use ffq_planner::{LiteralValue, OptimizerRule, ScalarUdfTypeResolver};
use ffq_storage::TableDef;
use ffq_storage::parquet_provider::{FileFingerprint, ParquetProvider};

use crate::DataFrame;
use crate::physical_registry::PhysicalOperatorFactory;
use crate::session::{Session, SharedSession};

/// Primary entry point for planning and executing queries.
///
/// `Engine` owns a shared session containing planner, catalog, runtime, and metrics state.
/// Clone is cheap and shares the same underlying session.
#[derive(Clone)]
pub struct Engine {
    session: SharedSession,
}

/// Source of a table schema returned by [`Engine::table_schema_with_origin`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableSchemaOrigin {
    /// Schema came directly from catalog definition.
    CatalogDefined,
    /// Schema was inferred from parquet files and cached/persisted as metadata.
    Inferred,
}

impl Engine {
    /// Constructs a new engine with the provided configuration.
    ///
    /// Runtime selection behavior:
    /// - when built **without** `distributed` feature: always embedded runtime
    /// - when built **with** `distributed` feature:
    ///   - uses distributed runtime if `config.coordinator_endpoint` is set, or
    ///   - uses `FFQ_COORDINATOR_ENDPOINT` when set, otherwise falls back to embedded.
    ///
    /// Schema policy env overrides are also applied from session bootstrap:
    /// `FFQ_SCHEMA_INFERENCE`, `FFQ_SCHEMA_WRITEBACK`, `FFQ_SCHEMA_DRIFT_POLICY`.
    ///
    /// # Examples
    /// ```no_run
    /// use ffq_client::Engine;
    /// use ffq_common::EngineConfig;
    ///
    /// let engine = Engine::new(EngineConfig::default())?;
    /// # let _ = engine;
    /// # Ok::<(), ffq_common::FfqError>(())
    /// ```
    ///
    /// # Errors
    /// Returns an error if session initialization fails (for example catalog load or invalid config).
    pub fn new(config: EngineConfig) -> Result<Self> {
        let session = Arc::new(Session::new(config)?);
        Ok(Self { session })
    }

    /// Returns the effective engine configuration for this session.
    ///
    /// This reflects env-driven overrides applied during session bootstrap.
    pub fn config(&self) -> EngineConfig {
        self.session.config.clone()
    }

    /// Register a table under a given name.
    /// We override `table.name` to avoid ambiguity.
    pub fn register_table(&self, name: impl Into<String>, table: TableDef) {
        self.register_table_checked(name, table)
            .expect("table registration failed");
    }

    /// Registers a table and returns a fallible result.
    ///
    /// For parquet tables with inference enabled and no explicit schema,
    /// registration may infer schema immediately.
    ///
    /// # Examples
    /// ```no_run
    /// use arrow_schema::{DataType, Field, Schema};
    /// use ffq_client::Engine;
    /// use ffq_common::EngineConfig;
    /// use ffq_storage::{TableDef, TableStats};
    /// use std::collections::HashMap;
    ///
    /// let engine = Engine::new(EngineConfig::default())?;
    /// engine.register_table_checked(
    ///     "lineitem",
    ///     TableDef {
    ///         name: "lineitem".to_string(),
    ///         uri: "tests/fixtures/parquet/lineitem.parquet".to_string(),
    ///         paths: vec![],
    ///         format: "parquet".to_string(),
    ///         schema: Some(Schema::new(vec![
    ///             Field::new("l_orderkey", DataType::Int64, false),
    ///             Field::new("l_quantity", DataType::Float64, false),
    ///         ])),
    ///         stats: TableStats::default(),
    ///         options: HashMap::new(),
    ///     },
    /// )?;
    /// # Ok::<(), ffq_common::FfqError>(())
    /// ```
    ///
    /// # Errors
    /// Returns an error when schema inference/validation fails or table metadata is invalid.
    pub fn register_table_checked(
        &self,
        name: impl Into<String>,
        mut table: TableDef,
    ) -> Result<()> {
        table.name = name.into();
        maybe_infer_table_schema_on_register(self.session.config.schema_inference, &mut table)?;
        self.session
            .catalog
            .write()
            .expect("catalog lock poisoned")
            .register_table(table);
        Ok(())
    }

    /// Parses SQL into a query [`DataFrame`].
    ///
    /// The query is planned/analyzed during execution (`collect`, write methods, etc.).
    ///
    /// # Examples
    /// ```no_run
    /// use ffq_client::Engine;
    /// use ffq_common::EngineConfig;
    ///
    /// let engine = Engine::new(EngineConfig::default())?;
    /// let df = engine.sql("SELECT 1")?;
    /// let batches = futures::executor::block_on(df.collect())?;
    /// # let _ = batches;
    /// # Ok::<(), ffq_common::FfqError>(())
    /// ```
    ///
    /// # Errors
    /// Returns an error when SQL parsing fails.
    pub fn sql(&self, query: &str) -> Result<DataFrame> {
        let logical = self.session.planner.plan_sql_with_params(
            query,
            &HashMap::new(),
            &self.session.config,
        )?;
        Ok(DataFrame::new(self.session.clone(), logical))
    }

    /// Same as [`Engine::sql`] but binds named parameters.
    ///
    /// # Errors
    /// Returns an error when SQL parsing/binding fails.
    pub fn sql_with_params(
        &self,
        query: &str,
        params: HashMap<String, LiteralValue>,
    ) -> Result<DataFrame> {
        let logical =
            self.session
                .planner
                .plan_sql_with_params(query, &params, &self.session.config)?;
        Ok(DataFrame::new(self.session.clone(), logical))
    }

    #[cfg(feature = "vector")]
    /// Convenience helper for vector top-k search.
    ///
    /// This constructs a query equivalent to:
    /// `SELECT <id_col>, cosine_similarity(<vector_col>, :query_vec) AS score
    ///  FROM <table> ORDER BY cosine_similarity(<vector_col>, :query_vec) DESC LIMIT <k>`.
    ///
    /// # Errors
    /// Returns an error when SQL planning fails.
    pub fn hybrid_search(
        &self,
        table: &str,
        id_col: &str,
        vector_col: &str,
        query_vector: Vec<f32>,
        k: usize,
    ) -> Result<DataFrame> {
        let sql = format!(
            "SELECT {id_col}, cosine_similarity({vector_col}, :query_vec) AS score \
             FROM {table} \
             ORDER BY cosine_similarity({vector_col}, :query_vec) DESC \
             LIMIT {k}"
        );
        let mut params = HashMap::new();
        params.insert(
            "query_vec".to_string(),
            LiteralValue::VectorF32(query_vector),
        );
        self.sql_with_params(&sql, params)
    }

    /// Returns a [`DataFrame`] that scans a registered table.
    ///
    /// # Errors
    /// Returns an error if table lookup/planning fails.
    pub fn table(&self, name: &str) -> Result<DataFrame> {
        Ok(DataFrame::table(self.session.clone(), name))
    }

    /// Lists all currently registered table names.
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

    /// Returns schema for a registered table when available.
    ///
    /// # Errors
    /// Returns an error if table lookup fails.
    pub fn table_schema(&self, name: &str) -> Result<Option<Schema>> {
        let cat = self.session.catalog.read().expect("catalog lock poisoned");
        let table = cat.get(name)?;
        Ok(table.schema.clone())
    }

    /// Returns schema together with origin metadata.
    ///
    /// # Errors
    /// Returns an error if table lookup fails.
    pub fn table_schema_with_origin(
        &self,
        name: &str,
    ) -> Result<Option<(Schema, TableSchemaOrigin)>> {
        let cat = self.session.catalog.read().expect("catalog lock poisoned");
        let table = cat.get(name)?;
        let Some(schema) = table.schema.clone() else {
            return Ok(None);
        };
        let origin = if table.options.contains_key("schema.inferred_at")
            || table.options.contains_key("schema.fingerprint")
        {
            TableSchemaOrigin::Inferred
        } else {
            TableSchemaOrigin::CatalogDefined
        };
        Ok(Some((schema, origin)))
    }

    /// Gracefully shuts down runtime resources.
    pub async fn shutdown(&self) -> Result<()> {
        self.session.runtime.shutdown().await
    }

    /// Renders current Prometheus metrics exposition text.
    pub fn prometheus_metrics(&self) -> String {
        self.session.prometheus_metrics()
    }

    /// Returns the most recent query execution stats report captured by this engine session.
    ///
    /// The report is populated by query execution paths (`collect`, write methods).
    pub fn last_query_stats_report(&self) -> Option<String> {
        self.session
            .last_query_stats_report
            .read()
            .expect("query stats lock poisoned")
            .clone()
    }

    /// Register a custom optimizer rule.
    ///
    /// Rules are applied after built-in optimizer passes in deterministic name order.
    /// Returns `true` when an existing rule with same name was replaced.
    pub fn register_optimizer_rule(&self, rule: Arc<dyn OptimizerRule>) -> bool {
        self.session.planner.register_optimizer_rule(rule)
    }

    /// Deregister a custom optimizer rule by name.
    ///
    /// Returns `true` when an existing rule was removed.
    pub fn deregister_optimizer_rule(&self, name: &str) -> bool {
        self.session.planner.deregister_optimizer_rule(name)
    }

    /// Register a scalar UDF for SQL/DataFrame execution.
    ///
    /// This registers:
    /// - planner-side return type resolver
    /// - execution-side batch invocation implementation
    ///
    /// Returns `true` when existing UDF with same name was replaced.
    pub fn register_scalar_udf(&self, udf: Arc<dyn ScalarUdf>) -> bool {
        let udf_name = udf.name().to_ascii_lowercase();
        let resolver_udf = Arc::clone(&udf);
        let resolver: ScalarUdfTypeResolver =
            Arc::new(move |arg_types| resolver_udf.return_type(arg_types));
        let replaced_analyzer = self
            .session
            .planner
            .register_scalar_udf_type(udf_name.clone(), resolver);
        let replaced_exec = register_scalar_udf(udf);
        replaced_analyzer || replaced_exec
    }

    /// Register a numeric scalar UDF type resolver only.
    ///
    /// Useful when expression type can be inferred as numeric passthrough.
    pub fn register_numeric_udf_type(&self, name: impl Into<String>) -> bool {
        self.session
            .planner
            .register_numeric_passthrough_udf_type(name)
    }

    /// Deregister a scalar UDF by name from planner and execution registries.
    ///
    /// Returns `true` when an existing registration was removed.
    pub fn deregister_scalar_udf(&self, name: &str) -> bool {
        let a = self.session.planner.deregister_scalar_udf_type(name);
        let b = deregister_scalar_udf(name);
        a || b
    }

    /// Register a custom physical operator factory.
    ///
    /// This registry is used as the extension point for custom runtime
    /// operators in v2.
    pub fn register_physical_operator_factory(
        &self,
        factory: Arc<dyn PhysicalOperatorFactory>,
    ) -> bool {
        self.session.physical_registry.register(factory)
    }

    /// Deregister a custom physical operator factory by name.
    pub fn deregister_physical_operator_factory(&self, name: &str) -> bool {
        self.session.physical_registry.deregister(name)
    }

    /// List registered custom physical operator factory names.
    pub fn list_physical_operator_factories(&self) -> Vec<String> {
        self.session.physical_registry.names()
    }

    #[cfg(feature = "profiling")]
    /// Serves metrics exporter endpoint for profiling/observability workflows.
    ///
    /// # Errors
    /// Returns an error if binding or serving fails.
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
    )
    .map_err(|e| {
        ffq_common::FfqError::InvalidConfig(format!(
            "schema inference failed for table '{}': {e}",
            table.name
        ))
    })?;
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
    table
        .options
        .insert("schema.inferred_at".to_string(), now_secs.to_string());
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
