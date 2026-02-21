use ffq_client::Engine;
use ffq_client::SampleEmbeddingProvider;
use ffq_common::EngineConfig;
use ffq_storage::{TableDef, TableStats};
use futures::TryStreamExt;
use std::collections::HashMap;
use std::path::PathBuf;

#[test]
fn public_api_engine_and_dataframe_contract_v2() {
    let config = EngineConfig::default();
    let engine = Engine::new(config.clone()).expect("engine");
    let effective = engine.config();
    assert_eq!(effective.batch_size_rows, config.batch_size_rows);

    let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/fixtures/parquet/lineitem.parquet");
    engine.register_table(
        "api_contract_dummy",
        TableDef {
            name: "ignored".to_string(),
            uri: fixture.to_string_lossy().to_string(),
            paths: vec![],
            format: "parquet".to_string(),
            schema: None,
            stats: TableStats::default(),
            options: HashMap::new(),
        },
    );

    let df = engine
        .sql("SELECT l_orderkey FROM api_contract_dummy LIMIT 1")
        .expect("sql");
    let stream = futures::executor::block_on(df.collect_stream()).expect("collect_stream");
    let batches = futures::executor::block_on(stream.try_collect::<Vec<_>>()).expect("stream");
    assert!(!batches.is_empty());

    let batches2 = futures::executor::block_on(df.collect()).expect("collect");
    assert!(!batches2.is_empty());

    let emb = SampleEmbeddingProvider::new(8).expect("embedding provider");
    let vectors = engine
        .embed_texts(&emb, &["alpha".to_string(), "beta".to_string()])
        .expect("embed texts");
    assert_eq!(vectors.len(), 2);
    assert_eq!(vectors[0].len(), 8);
}

#[cfg(feature = "vector")]
#[test]
fn public_api_hybrid_search_convenience_exists() {
    let engine = Engine::new(EngineConfig::default()).expect("engine");
    let fixture =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/parquet/docs.parquet");
    engine.register_table(
        "docs",
        TableDef {
            name: "ignored".to_string(),
            uri: fixture.to_string_lossy().to_string(),
            paths: vec![],
            format: "parquet".to_string(),
            schema: None,
            stats: TableStats::default(),
            options: HashMap::new(),
        },
    );
    let _ = engine
        .hybrid_search("docs", "id", "emb", vec![0.1_f32, 0.2, 0.3], 5)
        .expect("hybrid_search");
    let _ = engine
        .hybrid_search_batch(
            "docs",
            vec![vec![0.1_f32, 0.2, 0.3], vec![0.3_f32, 0.2, 0.1]],
            5,
        )
        .expect("hybrid_search_batch");
}
