use std::sync::{Arc, RwLock};

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
    pub metrics: MetricsRegistry,
    pub planner: PlannerFacade,
    pub runtime: Arc<dyn Runtime>,
}

impl Session {
    pub fn new(config: EngineConfig) -> Result<Self> {
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
        Ok(Self {
            config,
            catalog: RwLock::new(Catalog::new()),
            metrics: MetricsRegistry::new(),
            planner: PlannerFacade::new(),
            runtime,
        })
    }
}
