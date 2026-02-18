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
use ffq_storage::{TableDef, TableStats};
use parquet::arrow::ArrowWriter;

fn unique_path(prefix: &str, ext: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}_{nanos}.{ext}"))
}

fn write_docs(path: &std::path::Path, schema: Arc<Schema>) {
    let mut emb = FixedSizeListBuilder::new(Float32Builder::new(), 3);
    let vectors = [
        [1.0_f32, 0.0, 0.0],
        [0.8_f32, 0.2, 0.0],
        [0.0_f32, 1.0, 0.0],
    ];
    for v in vectors {
        for x in v {
            emb.values().append_value(x);
        }
        emb.append(true);
    }
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 2, 3])),
            Arc::new(StringArray::from(vec!["doc-1", "doc-2", "doc-3"])),
            Arc::new(StringArray::from(vec!["en", "en", "de"])),
            Arc::new(emb.finish()),
        ],
    )
    .expect("batch");
    let file = File::create(path).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, schema, None).expect("writer");
    writer.write(&batch).expect("write");
    writer.close().expect("close");
}

#[test]
fn two_phase_vector_join_rerank_runs_embedded() {
    let docs_path = unique_path("ffq_two_phase_docs", "parquet");
    let emb_item = Field::new("item", DataType::Float32, true);
    let docs_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("title", DataType::Utf8, false),
        Field::new("lang", DataType::Utf8, false),
        Field::new("emb", DataType::FixedSizeList(Arc::new(emb_item), 3), true),
    ]));
    write_docs(&docs_path, docs_schema.clone());

    let mut docs_opts = HashMap::new();
    docs_opts.insert("vector.index_table".to_string(), "docs_idx".to_string());
    docs_opts.insert("vector.id_column".to_string(), "id".to_string());
    docs_opts.insert("vector.embedding_column".to_string(), "emb".to_string());
    docs_opts.insert("vector.prefetch_multiplier".to_string(), "3".to_string());

    let index_mock = r#"[
        {"id":2,"score":0.99,"payload":"{\"source\":\"idx\"}"},
        {"id":1,"score":0.95,"payload":"{\"source\":\"idx\"}"},
        {"id":3,"score":0.10,"payload":"{\"source\":\"idx\"}"}
    ]"#;
    let mut idx_opts = HashMap::new();
    idx_opts.insert("vector.mock_rows_json".to_string(), index_mock.to_string());

    let engine = Engine::new(EngineConfig::default()).expect("engine");
    engine.register_table(
        "docs",
        TableDef {
            name: "docs".to_string(),
            uri: docs_path.to_string_lossy().to_string(),
            paths: Vec::new(),
            format: "parquet".to_string(),
            schema: Some((*docs_schema).clone()),
            stats: TableStats::default(),
            options: docs_opts,
        },
    );
    engine.register_table(
        "docs_idx",
        TableDef {
            name: "docs_idx".to_string(),
            uri: "docs_idx".to_string(),
            paths: Vec::new(),
            format: "qdrant".to_string(),
            schema: Some(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("score", DataType::Float32, false),
                Field::new("payload", DataType::Utf8, true),
            ])),
            stats: TableStats::default(),
            options: idx_opts,
        },
    );

    let mut params = HashMap::new();
    params.insert(
        "q".to_string(),
        LiteralValue::VectorF32(vec![1.0_f32, 0.0, 0.0]),
    );
    let query = "SELECT id, title FROM docs WHERE lang = 'en' ORDER BY cosine_similarity(emb, :q) DESC LIMIT 1";
    let explain = engine
        .sql_with_params(query, params.clone())
        .expect("sql")
        .explain()
        .expect("explain");
    assert!(explain.contains("VectorTopK table=docs_idx"));
    assert!(explain.contains("Join type=Inner"));

    let batches = futures::executor::block_on(
        engine
            .sql_with_params(query, params)
            .expect("sql")
            .collect(),
    )
    .expect("collect");
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    let ids = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("id column");
    assert_eq!(ids.value(0), 1);

    let _ = std::fs::remove_file(docs_path);
}
