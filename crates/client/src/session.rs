use std::sync::{Arc, RwLock};

use ffq_common::{EngineConfig, MetricsRegistry, Result};
use ffq_storage::Catalog;

use crate::planner_facade::PlannerFacade;
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
        Ok(Self {
            config,
            catalog: RwLock::new(Catalog::new()),
            metrics: MetricsRegistry::new(),
            planner: PlannerFacade::new(),
            runtime: Arc::new(EmbeddedRuntime::new()),
        })
    }
}
