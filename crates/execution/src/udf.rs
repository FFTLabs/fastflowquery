//! Scalar UDF registry and runtime interface.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};

use arrow::array::ArrayRef;
use arrow_schema::DataType;
use ffq_common::Result;

/// Runtime scalar UDF contract.
pub trait ScalarUdf: Send + Sync {
    /// Stable lowercase function name used in SQL (`my_add`).
    fn name(&self) -> &str;
    /// Return type inference from analyzed argument types.
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType>;
    /// Batch-wise invocation with Arrow arrays.
    fn invoke(&self, args: &[ArrayRef]) -> Result<ArrayRef>;
}

type UdfMap = HashMap<String, Arc<dyn ScalarUdf>>;

fn registry() -> &'static RwLock<UdfMap> {
    static REGISTRY: OnceLock<RwLock<UdfMap>> = OnceLock::new();
    REGISTRY.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Register or replace a scalar UDF.
///
/// Returns `true` when an existing UDF with same name was replaced.
pub fn register_scalar_udf(udf: Arc<dyn ScalarUdf>) -> bool {
    registry()
        .write()
        .expect("udf registry lock poisoned")
        .insert(udf.name().to_ascii_lowercase(), udf)
        .is_some()
}

/// Deregister scalar UDF by name.
///
/// Returns `true` when an existing UDF was removed.
pub fn deregister_scalar_udf(name: &str) -> bool {
    registry()
        .write()
        .expect("udf registry lock poisoned")
        .remove(&name.to_ascii_lowercase())
        .is_some()
}

/// Lookup scalar UDF by name.
pub fn get_scalar_udf(name: &str) -> Option<Arc<dyn ScalarUdf>> {
    registry()
        .read()
        .expect("udf registry lock poisoned")
        .get(&name.to_ascii_lowercase())
        .cloned()
}
