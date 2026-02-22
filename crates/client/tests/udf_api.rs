use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int64Array};
use arrow::compute::kernels::numeric::add;
use arrow_schema::DataType;
use ffq_client::{Engine, ScalarUdf};
use ffq_common::EngineConfig;
use ffq_storage::{TableDef, TableStats};

struct MyAddUdf;

impl ScalarUdf for MyAddUdf {
    fn name(&self) -> &str {
        "my_add"
    }

    fn return_type(&self, arg_types: &[DataType]) -> ffq_common::Result<DataType> {
        if arg_types.len() != 2 {
            return Err(ffq_common::FfqError::Planning(
                "my_add requires exactly 2 arguments".to_string(),
            ));
        }
        match (&arg_types[0], &arg_types[1]) {
            (DataType::Int64, DataType::Int64) => Ok(DataType::Int64),
            (DataType::Float64, DataType::Float64) => Ok(DataType::Float64),
            _ => Err(ffq_common::FfqError::Planning(
                "my_add supports Int64/Float64 argument pairs".to_string(),
            )),
        }
    }

    fn invoke(&self, args: &[ArrayRef]) -> ffq_common::Result<ArrayRef> {
        if args.len() != 2 {
            return Err(ffq_common::FfqError::Execution(
                "my_add expected 2 arrays".to_string(),
            ));
        }
        if let (Some(a), Some(b)) = (
            args[0].as_any().downcast_ref::<Int64Array>(),
            args[1].as_any().downcast_ref::<Int64Array>(),
        ) {
            return Ok(Arc::new(add(a, b).map_err(|e| {
                ffq_common::FfqError::Execution(format!("my_add int64 failed: {e}"))
            })?));
        }
        if let (Some(a), Some(b)) = (
            args[0].as_any().downcast_ref::<Float64Array>(),
            args[1].as_any().downcast_ref::<Float64Array>(),
        ) {
            return Ok(Arc::new(add(a, b).map_err(|e| {
                ffq_common::FfqError::Execution(format!("my_add float64 failed: {e}"))
            })?));
        }
        Err(ffq_common::FfqError::Execution(
            "my_add received unsupported array types".to_string(),
        ))
    }
}

#[test]
fn scalar_udf_my_add_works_in_sql() {
    let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/fixtures/parquet/lineitem.parquet");
    let engine = Engine::new(EngineConfig::default()).expect("engine");
    engine.register_table(
        "lineitem",
        TableDef {
            name: "lineitem".to_string(),
            uri: fixture.to_string_lossy().to_string(),
            paths: vec![],
            format: "parquet".to_string(),
            schema: None,
            stats: TableStats::default(),
            options: HashMap::new(),
        },
    );
    engine.register_scalar_udf(Arc::new(MyAddUdf));

    let batches = futures::executor::block_on(
        engine
            .sql("SELECT my_add(l_orderkey, 3) AS v, l_orderkey FROM lineitem LIMIT 1")
            .expect("sql")
            .collect(),
    )
    .expect("collect");
    assert!(!batches.is_empty());
    let batch = &batches[0];
    let v = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("v int64")
        .value(0);
    let k = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("k int64")
        .value(0);
    assert_eq!(v, k + 3);
}
