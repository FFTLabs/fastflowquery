#![cfg(all(feature = "vector", feature = "qdrant"))]

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use ffq_client::Engine;
use ffq_common::EngineConfig;
use ffq_planner::LiteralValue;
use ffq_storage::{TableDef, TableStats};

fn qdrant_table() -> TableDef {
    let emb_item = Field::new("item", DataType::Float32, true);
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("title", DataType::Utf8, true),
        Field::new("payload", DataType::Utf8, true),
        Field::new("emb", DataType::FixedSizeList(Arc::new(emb_item), 3), true),
    ]);
    let mut options = HashMap::new();
    options.insert(
        "qdrant.endpoint".to_string(),
        "http://127.0.0.1:6334".to_string(),
    );
    options.insert("qdrant.collection".to_string(), "docs_idx".to_string());
    options.insert("qdrant.with_payload".to_string(), "true".to_string());
    TableDef {
        name: "docs_idx".to_string(),
        uri: "docs_idx".to_string(),
        paths: Vec::new(),
        format: "qdrant".to_string(),
        schema: Some(schema),
        stats: TableStats::default(),
        options,
    }
}

#[test]
fn explain_uses_vector_topk_for_supported_projection() {
    let engine = Engine::new(EngineConfig::default()).expect("engine");
    engine.register_table("docs_idx", qdrant_table());

    let mut params = HashMap::new();
    params.insert(
        "q".to_string(),
        LiteralValue::VectorF32(vec![1.0, 0.0, 0.0]),
    );
    let df = engine
        .sql_with_params(
            "SELECT id, payload FROM docs_idx ORDER BY cosine_similarity(emb, :q) DESC LIMIT 2",
            params,
        )
        .expect("sql");
    let explain = df.explain().expect("explain");
    assert!(explain.contains("VectorTopK table=docs_idx"));
    assert!(explain.contains("rewrite=index_applied"));
}

#[test]
fn explain_falls_back_for_unsupported_projection() {
    let engine = Engine::new(EngineConfig::default()).expect("engine");
    engine.register_table("docs_idx", qdrant_table());

    let mut params = HashMap::new();
    params.insert(
        "q".to_string(),
        LiteralValue::VectorF32(vec![1.0, 0.0, 0.0]),
    );
    let df = engine
        .sql_with_params(
            "SELECT title FROM docs_idx ORDER BY cosine_similarity(emb, :q) DESC LIMIT 2",
            params,
        )
        .expect("sql");
    let explain = df.explain().expect("explain");
    assert!(explain.contains("TopKByScore"));
    assert!(explain.contains("rewrite=index_fallback"));
}
