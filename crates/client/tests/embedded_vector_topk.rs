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
#[path = "support/mod.rs"]
mod support;

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
            .sql_with_params(support::integration_queries::vector_topk_cosine(), params)
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

#[test]
fn cosine_topk_tie_order_is_deterministic() {
    let src_path = unique_path("ffq_vec_topk_tie", "parquet");
    let emb_field = Field::new("item", DataType::Float32, true);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("title", DataType::Utf8, false),
        Field::new("emb", DataType::FixedSizeList(Arc::new(emb_field), 3), true),
    ]));
    // ids 1 and 2 tie on cosine score for query [1,0,0].
    let mut emb_builder = FixedSizeListBuilder::new(Float32Builder::new(), 3);
    for v in [[1.0_f32, 0.0, 0.0], [2.0_f32, 0.0, 0.0], [0.0_f32, 1.0, 0.0]] {
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
    let file = File::create(&src_path).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, schema.clone(), None).expect("writer");
    writer.write(&batch).expect("write");
    writer.close().expect("close");

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

    let q = support::integration_queries::vector_topk_cosine();
    let b1 = futures::executor::block_on(
        engine
            .sql_with_params(q, params.clone())
            .expect("sql")
            .collect(),
    )
    .expect("collect1");
    let b2 = futures::executor::block_on(
        engine
            .sql_with_params(q, params)
            .expect("sql")
            .collect(),
    )
    .expect("collect2");
    support::assert_batches_deterministic(&b1, &b2, &["id"], 1e-9);

    let ids = b1[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("ids");
    assert_eq!(ids.value(0), 2);
    assert_eq!(ids.value(1), 1);

    let _ = std::fs::remove_file(src_path);
}

#[test]
fn parquet_vector_topk_uses_bruteforce_fallback_plan() {
    let src_path = unique_path("ffq_vec_topk_fallback", "parquet");
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
    let df = engine
        .sql_with_params(support::integration_queries::vector_topk_cosine(), params)
        .expect("sql");
    let explain = df.explain().expect("explain");
    assert!(explain.contains("TopKByScore"));
    assert!(explain.contains("rewrite=index_fallback"));

    let _ = std::fs::remove_file(src_path);
}
