use std::collections::HashMap;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use ffq_client::{Engine, PhysicalOperatorFactory};
use ffq_common::EngineConfig;

struct DummyFactory;

impl PhysicalOperatorFactory for DummyFactory {
    fn name(&self) -> &str {
        "dummy_factory"
    }

    fn execute(
        &self,
        input_schema: SchemaRef,
        input_batches: Vec<RecordBatch>,
        _config: &HashMap<String, String>,
    ) -> ffq_common::Result<(SchemaRef, Vec<RecordBatch>)> {
        Ok((input_schema, input_batches))
    }
}

#[test]
fn physical_operator_registry_registers_and_deregisters() {
    let engine = Engine::new(EngineConfig::default()).expect("engine");
    assert!(!engine.register_physical_operator_factory(Arc::new(DummyFactory)));
    let names = engine.list_physical_operator_factories();
    assert!(names.iter().any(|n| n == "dummy_factory"));
    assert!(engine.deregister_physical_operator_factory("dummy_factory"));
    let names = engine.list_physical_operator_factories();
    assert!(!names.iter().any(|n| n == "dummy_factory"));
}
