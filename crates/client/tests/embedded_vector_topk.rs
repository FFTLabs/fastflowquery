#![cfg(feature = "vector")]

use std::collections::HashMap;
use std::fs::File;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::{FixedSizeListBuilder, Float32Builder, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use ffq_client::Engine;
use ffq_common::EngineConfig;
use ffq_planner::LiteralValue;
use ffq_storage::TableDef;
use parquet::arrow::ArrowWriter;

fn unique_path(prefix: &str, ext: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}_{nanos}.{ext}"))
}

fn write_docs_parquet(path: &std::path::Path, schema: Arc<Schema>) {
    let mut emb_builder = FixedSizeListBuilder::new(Float32Builder::new(), 3);
    let vecs = [
        [1.0_f32, 0.0, 0.0],
        [0.8_f32, 0.2, 0.0],
        [0.0_f32, 1.0, 0.0],
    ];
    for v in vecs {
        for x in v {
            emb_builder.values().append_value(x);
        }
        emb_builder.append(true);
    }

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
            Arc::new(emb_builder.finish()),
        ],
    )
    .expect("batch");
    let file = File::create(path).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, schema, None).expect("writer");
    writer.write(&batch).expect("write");
    writer.close().expect("close");
}

#[test]
fn order_by_cosine_similarity_limit_uses_topk() {
    let src_path = unique_path("ffq_vec_topk_src", "parquet");
    let emb_field = Field::new("item", DataType::Float32, true);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("title", DataType::Utf8, false),
        Field::new("emb", DataType::FixedSizeList(Arc::new(emb_field), 3), true),
    ]));
    write_docs_parquet(&src_path, schema.clone());

    let engine = Engine::new(EngineConfig::default()).expect("engine");
    engine.register_table(
        "docs",
        TableDef {
            name: "docs".to_string(),
            uri: src_path.to_string_lossy().to_string(),
            paths: Vec::new(),
            format: "parquet".to_string(),
            schema: Some((*schema).clone()),
            stats: ffq_storage::TableStats::default(),
            options: HashMap::new(),
        },
    );

    let mut params = HashMap::new();
    params.insert(
        "q".to_string(),
        LiteralValue::VectorF32(vec![1.0, 0.0, 0.0]),
    );

    let batches = futures::executor::block_on(
        engine
            .sql_with_params(
                "SELECT id, title FROM docs ORDER BY cosine_similarity(emb, :q) DESC LIMIT 2",
                params,
            )
            .expect("sql")
            .collect(),
    )
    .expect("collect");

    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 2);
    let ids = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("ids");
    assert_eq!(ids.value(0), 1);
    assert_eq!(ids.value(1), 2);

    let _ = std::fs::remove_file(src_path);
}
