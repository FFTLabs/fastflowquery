use std::collections::HashMap;
#[cfg(feature = "profiling")]
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::{env, path::Path, path::PathBuf};

use arrow_schema::Schema;
use ffq_common::{
    CteReusePolicy, EngineConfig, MetricsRegistry, Result, SchemaDriftPolicy, SchemaInferencePolicy,
};
use ffq_storage::Catalog;
use ffq_storage::parquet_provider::FileFingerprint;

use crate::engine::maybe_infer_table_schema_on_register;
use crate::physical_registry::{PhysicalOperatorRegistry, global_physical_operator_registry};
use crate::planner_facade::PlannerFacade;
#[cfg(feature = "distributed")]
use crate::runtime::DistributedRuntime;
use crate::runtime::{EmbeddedRuntime, Runtime};

pub type SharedSession = Arc<Session>;

#[derive(Debug, Clone)]
pub(crate) struct SchemaCacheEntry {
    pub schema: Schema,
    pub fingerprint: Vec<FileFingerprint>,
}

#[derive(Debug)]
pub struct Session {
    pub config: EngineConfig,
    pub catalog: RwLock<Catalog>,
    pub catalog_path: String,
    pub metrics: MetricsRegistry,
    pub planner: PlannerFacade,
    pub physical_registry: Arc<PhysicalOperatorRegistry>,
    pub runtime: Arc<dyn Runtime>,
    pub(crate) schema_cache: RwLock<HashMap<String, SchemaCacheEntry>>,
}

impl Session {
    pub fn new(config: EngineConfig) -> Result<Self> {
        // Best-effort local env loading for FFQ_COORDINATOR_ENDPOINT and other dev vars.
        let _ = dotenvy::dotenv();
        let mut config = config;
        apply_schema_policy_env_overrides(&mut config)?;

        let runtime: Arc<dyn Runtime> = {
            #[cfg(feature = "distributed")]
            {
                let endpoint = config
                    .coordinator_endpoint
                    .clone()
                    .or_else(|| std::env::var("FFQ_COORDINATOR_ENDPOINT").ok());
                if let Some(endpoint) = endpoint {
                    Arc::new(DistributedRuntime::new(endpoint))
                } else {
                    Arc::new(EmbeddedRuntime::new())
                }
            }
            #[cfg(not(feature = "distributed"))]
            {
                Arc::new(EmbeddedRuntime::new())
            }
        };
        let catalog_path = config
            .catalog_path
            .clone()
            .or_else(|| env::var("FFQ_CATALOG_PATH").ok())
            .unwrap_or_else(|| "./ffq_tables/tables.json".to_string());
        let mut catalog = if Path::new(&catalog_path).exists() {
            Catalog::load(&catalog_path)?
        } else {
            Catalog::new()
        };
        if config.schema_inference.allows_inference() {
            let mut changed = false;
            for mut table in catalog.tables() {
                let inferred =
                    maybe_infer_table_schema_on_register(config.schema_inference, &mut table)?;
                changed |= inferred;
                catalog.register_table(table);
            }
            if changed && config.schema_writeback {
                catalog.save(&catalog_path)?;
            }
        }

        Ok(Self {
            config,
            catalog: RwLock::new(catalog),
            catalog_path,
            metrics: MetricsRegistry::new(),
            planner: PlannerFacade::new(),
            physical_registry: global_physical_operator_registry(),
            runtime,
            schema_cache: RwLock::new(HashMap::new()),
        })
    }

    pub fn persist_catalog(&self) -> Result<()> {
        let catalog = self.catalog.read().expect("catalog lock poisoned");
        catalog.save(&self.catalog_path)
    }

    pub fn managed_table_path(&self, name: &str) -> PathBuf {
        Path::new(&self.catalog_path)
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(name)
    }

    pub fn prometheus_metrics(&self) -> String {
        ffq_common::metrics::global_metrics().render_prometheus()
    }

    #[cfg(feature = "profiling")]
    pub async fn serve_metrics_exporter(&self, addr: SocketAddr) -> Result<()> {
        ffq_common::run_metrics_exporter(addr)
            .await
            .map_err(Into::into)
    }
}

fn apply_schema_policy_env_overrides(config: &mut EngineConfig) -> Result<()> {
    if let Ok(raw) = env::var("FFQ_SCHEMA_INFERENCE") {
        config.schema_inference = parse_schema_inference_policy(&raw)?;
    }
    if let Ok(raw) = env::var("FFQ_SCHEMA_WRITEBACK") {
        config.schema_writeback = parse_bool_flag(&raw, "FFQ_SCHEMA_WRITEBACK")?;
    }
    if let Ok(raw) = env::var("FFQ_SCHEMA_DRIFT_POLICY") {
        config.schema_drift_policy = parse_schema_drift_policy(&raw)?;
    }
    if let Ok(raw) = env::var("FFQ_CTE_REUSE_POLICY") {
        config.cte_reuse_policy = parse_cte_reuse_policy(&raw)?;
    }
    Ok(())
}

fn parse_schema_inference_policy(raw: &str) -> Result<SchemaInferencePolicy> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "off" => Ok(SchemaInferencePolicy::Off),
        "on" => Ok(SchemaInferencePolicy::On),
        "strict" => Ok(SchemaInferencePolicy::Strict),
        "permissive" => Ok(SchemaInferencePolicy::Permissive),
        other => Err(ffq_common::FfqError::InvalidConfig(format!(
            "invalid FFQ_SCHEMA_INFERENCE='{other}'; expected off|on|strict|permissive"
        ))),
    }
}

fn parse_schema_drift_policy(raw: &str) -> Result<SchemaDriftPolicy> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "fail" => Ok(SchemaDriftPolicy::Fail),
        "refresh" => Ok(SchemaDriftPolicy::Refresh),
        other => Err(ffq_common::FfqError::InvalidConfig(format!(
            "invalid FFQ_SCHEMA_DRIFT_POLICY='{other}'; expected fail|refresh"
        ))),
    }
}

fn parse_cte_reuse_policy(raw: &str) -> Result<CteReusePolicy> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "inline" => Ok(CteReusePolicy::Inline),
        "materialize" => Ok(CteReusePolicy::Materialize),
        other => Err(ffq_common::FfqError::InvalidConfig(format!(
            "invalid FFQ_CTE_REUSE_POLICY='{other}'; expected inline|materialize"
        ))),
    }
}

fn parse_bool_flag(raw: &str, key: &str) -> Result<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        other => Err(ffq_common::FfqError::InvalidConfig(format!(
            "invalid {key}='{other}'; expected true|false"
        ))),
    }
}
