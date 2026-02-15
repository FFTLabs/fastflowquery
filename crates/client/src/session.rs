use std::sync::{Arc, RwLock};
use std::{env, path::Path, path::PathBuf};

use ffq_common::{EngineConfig, MetricsRegistry, Result};
use ffq_storage::Catalog;

use crate::planner_facade::PlannerFacade;
#[cfg(feature = "distributed")]
use crate::runtime::DistributedRuntime;
use crate::runtime::{EmbeddedRuntime, Runtime};

pub type SharedSession = Arc<Session>;

#[derive(Debug)]
pub struct Session {
    pub config: EngineConfig,
    pub catalog: RwLock<Catalog>,
    pub catalog_path: String,
    pub metrics: MetricsRegistry,
    pub planner: PlannerFacade,
    pub runtime: Arc<dyn Runtime>,
}

impl Session {
    pub fn new(config: EngineConfig) -> Result<Self> {
        // Best-effort local env loading for FFQ_COORDINATOR_ENDPOINT and other dev vars.
        let _ = dotenvy::dotenv();

        let runtime: Arc<dyn Runtime> = {
            #[cfg(feature = "distributed")]
            {
                if let Ok(endpoint) = std::env::var("FFQ_COORDINATOR_ENDPOINT") {
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
        let catalog_path =
            env::var("FFQ_CATALOG_PATH").unwrap_or_else(|_| "./ffq_tables/tables.json".to_string());
        let catalog = if Path::new(&catalog_path).exists() {
            Catalog::load(&catalog_path)?
        } else {
            Catalog::new()
        };

        Ok(Self {
            config,
            catalog: RwLock::new(catalog),
            catalog_path,
            metrics: MetricsRegistry::new(),
            planner: PlannerFacade::new(),
            runtime,
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
}
