use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};

use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use ffq_common::Result;

/// Factory contract for custom physical operators.
///
/// Implementations consume fully materialized input batches and produce a new
/// schema plus output batches.
pub trait PhysicalOperatorFactory: Send + Sync {
    /// Stable operator factory name used by `PhysicalPlan::Custom.op_name`.
    fn name(&self) -> &str;

    /// Execute custom operator logic.
    fn execute(
        &self,
        input_schema: SchemaRef,
        input_batches: Vec<RecordBatch>,
        config: &HashMap<String, String>,
    ) -> Result<(SchemaRef, Vec<RecordBatch>)>;
}

/// Registry for custom physical operator factories.
#[derive(Default)]
pub struct PhysicalOperatorRegistry {
    inner: RwLock<HashMap<String, Arc<dyn PhysicalOperatorFactory>>>,
}

impl std::fmt::Debug for PhysicalOperatorRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let count = self.inner.read().map(|m| m.len()).unwrap_or_default();
        f.debug_struct("PhysicalOperatorRegistry")
            .field("factories", &count)
            .finish()
    }
}

impl PhysicalOperatorRegistry {
    /// Register or replace a factory.
    ///
    /// Returns `true` when an existing factory with the same name was replaced.
    pub fn register(&self, factory: Arc<dyn PhysicalOperatorFactory>) -> bool {
        self.inner
            .write()
            .expect("physical registry lock poisoned")
            .insert(factory.name().to_string(), factory)
            .is_some()
    }

    /// Deregister a factory by name.
    ///
    /// Returns `true` when an existing factory was removed.
    pub fn deregister(&self, name: &str) -> bool {
        self.inner
            .write()
            .expect("physical registry lock poisoned")
            .remove(name)
            .is_some()
    }

    /// Fetch a factory by name.
    pub fn get(&self, name: &str) -> Option<Arc<dyn PhysicalOperatorFactory>> {
        self.inner
            .read()
            .expect("physical registry lock poisoned")
            .get(name)
            .cloned()
    }

    /// List registered factory names in sorted order.
    pub fn names(&self) -> Vec<String> {
        let mut names = self
            .inner
            .read()
            .expect("physical registry lock poisoned")
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        names.sort();
        names
    }
}

fn global_registry() -> &'static Arc<PhysicalOperatorRegistry> {
    static REGISTRY: OnceLock<Arc<PhysicalOperatorRegistry>> = OnceLock::new();
    REGISTRY.get_or_init(|| Arc::new(PhysicalOperatorRegistry::default()))
}

/// Return the global physical operator registry shared by default runtimes.
pub fn global_physical_operator_registry() -> Arc<PhysicalOperatorRegistry> {
    Arc::clone(global_registry())
}

/// Register a factory in the global physical operator registry.
///
/// Returns `true` when an existing factory with the same name was replaced.
pub fn register_global_physical_operator_factory(
    factory: Arc<dyn PhysicalOperatorFactory>,
) -> bool {
    global_registry().register(factory)
}

/// Deregister a factory from the global physical operator registry.
///
/// Returns `true` when an existing factory was removed.
pub fn deregister_global_physical_operator_factory(name: &str) -> bool {
    global_registry().deregister(name)
}
