#![cfg(feature = "distributed")]

use std::collections::HashMap;
use std::fs::File;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use arrow::array::Int64Array;
#[cfg(feature = "vector")]
use arrow::array::{FixedSizeListBuilder, Float32Builder, StringArray};
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use ffq_client::Engine;
use ffq_common::EngineConfig;
use ffq_distributed::grpc::{
    ControlPlaneServer, CoordinatorServices, HeartbeatServiceServer, ShuffleServiceServer,
};
use ffq_distributed::{
    Coordinator, CoordinatorConfig, DefaultTaskExecutor, GrpcControlPlane, Worker, WorkerConfig,
};
#[cfg(feature = "vector")]
use ffq_planner::LiteralValue;
use ffq_storage::{TableDef, TableStats};
use parquet::arrow::ArrowWriter;
use tokio::sync::Mutex;
use tonic::transport::Server;
#[path = "support/mod.rs"]
mod support;

static DIST_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn register_tables(
    engine: &Engine,
    lineitem_path: &std::path::Path,
    orders_path: &std::path::Path,
) {
    let lineitem_schema = Schema::new(vec![
        Field::new("l_orderkey", DataType::Int64, false),
        Field::new("l_partkey", DataType::Int64, false),
    ]);
    let orders_schema = Schema::new(vec![
        Field::new("o_orderkey", DataType::Int64, false),
        Field::new("o_custkey", DataType::Int64, false),
    ]);

    engine.register_table(
        "lineitem",
        TableDef {
            name: "lineitem".to_string(),
            uri: lineitem_path.to_string_lossy().to_string(),
            paths: Vec::new(),
            format: "parquet".to_string(),
            schema: Some(lineitem_schema),
            stats: TableStats::default(),
            options: HashMap::new(),
        },
    );
    engine.register_table(
        "orders",
        TableDef {
            name: "orders".to_string(),
            uri: orders_path.to_string_lossy().to_string(),
            paths: Vec::new(),
            format: "parquet".to_string(),
            schema: Some(orders_schema),
            stats: TableStats::default(),
            options: HashMap::new(),
        },
    );
}

fn register_tables_without_schema(
    engine: &Engine,
    lineitem_path: &std::path::Path,
    orders_path: &std::path::Path,
) {
    engine.register_table(
        "lineitem",
        TableDef {
            name: "lineitem".to_string(),
            uri: lineitem_path.to_string_lossy().to_string(),
            paths: Vec::new(),
            format: "parquet".to_string(),
            schema: None,
            stats: TableStats::default(),
            options: HashMap::new(),
        },
    );
    engine.register_table(
        "orders",
        TableDef {
            name: "orders".to_string(),
            uri: orders_path.to_string_lossy().to_string(),
            paths: Vec::new(),
            format: "parquet".to_string(),
            schema: None,
            stats: TableStats::default(),
            options: HashMap::new(),
        },
    );
}

fn register_window_case_table(engine: &Engine, window_path: &std::path::Path, with_schema: bool) {
    let schema = Schema::new(vec![
        Field::new("grp", DataType::Int64, false),
        Field::new("ord", DataType::Int64, false),
        Field::new("score", DataType::Int64, true),
    ]);
    engine.register_table(
        "window_case",
        TableDef {
            name: "window_case".to_string(),
            uri: window_path.to_string_lossy().to_string(),
            paths: Vec::new(),
            format: "parquet".to_string(),
            schema: with_schema.then_some(schema),
            stats: TableStats::default(),
            options: HashMap::new(),
        },
    );
}

fn write_window_case_parquet(path: &std::path::Path) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("grp", DataType::Int64, false),
        Field::new("ord", DataType::Int64, false),
        Field::new("score", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 1, 1, 1, 2, 2, 2, 2])),
            Arc::new(Int64Array::from(vec![1_i64, 2, 3, 4, 1, 2, 3, 4])),
            Arc::new(Int64Array::from(vec![
                Some(10_i64),
                Some(10),
                None,
                Some(20),
                None,
                Some(5),
                Some(5),
                Some(8),
            ])),
        ],
    )
    .expect("window_case batch");
    let file = File::create(path).expect("create window_case parquet");
    let mut writer = ArrowWriter::try_new(file, schema, None).expect("window_case writer");
    writer.write(&batch).expect("window_case write");
    writer.close().expect("window_case close");
}

fn collect_group_counts(batches: &[RecordBatch]) -> Vec<(i64, i64)> {
    let mut out = Vec::new();
    for batch in batches {
        let k = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("k");
        let c = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("c");
        for i in 0..batch.num_rows() {
            out.push((k.value(i), c.value(i)));
        }
    }
    out.sort_unstable();
    out
}

fn collect_join_rows(batches: &[RecordBatch]) -> Vec<(i64, i64, i64)> {
    let mut out = Vec::new();
    for batch in batches {
        let k = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("l_orderkey");
        let part = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("l_partkey");
        let cust = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("o_custkey");
        for i in 0..batch.num_rows() {
            out.push((k.value(i), part.value(i), cust.value(i)));
        }
    }
    out.sort_unstable();
    out
}

fn collect_scan_rows(batches: &[RecordBatch]) -> Vec<(i64, i64)> {
    let mut out = Vec::new();
    for batch in batches {
        let orderkey = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("l_orderkey");
        let partkey = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("l_partkey");
        for i in 0..batch.num_rows() {
            out.push((orderkey.value(i), partkey.value(i)));
        }
    }
    out.sort_unstable();
    out
}

#[cfg(feature = "vector")]
fn write_docs_vector(path: &std::path::Path, schema: Arc<Schema>) {
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

#[cfg(feature = "vector")]
fn register_two_phase_tables(engine: &Engine, docs_path: &std::path::Path) {
    let emb_field = Field::new("item", DataType::Float32, true);
    let docs_schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("title", DataType::Utf8, false),
        Field::new("lang", DataType::Utf8, false),
        Field::new("emb", DataType::FixedSizeList(Arc::new(emb_field), 3), true),
    ]);
    let mut docs_opts = HashMap::new();
    docs_opts.insert("vector.index_table".to_string(), "docs_idx".to_string());
    docs_opts.insert("vector.id_column".to_string(), "id".to_string());
    docs_opts.insert("vector.embedding_column".to_string(), "emb".to_string());
    docs_opts.insert("vector.prefetch_multiplier".to_string(), "3".to_string());
    engine.register_table(
        "docs",
        TableDef {
            name: "docs".to_string(),
            uri: docs_path.to_string_lossy().to_string(),
            paths: Vec::new(),
            format: "parquet".to_string(),
            schema: Some(docs_schema),
            stats: TableStats::default(),
            options: docs_opts,
        },
    );

    let mock_rows = r#"[
        {"id":2,"score":0.99,"payload":"{\"source\":\"idx\"}"},
        {"id":1,"score":0.95,"payload":"{\"source\":\"idx\"}"},
        {"id":3,"score":0.10,"payload":"{\"source\":\"idx\"}"}
    ]"#;
    let mut idx_opts = HashMap::new();
    idx_opts.insert("vector.mock_rows_json".to_string(), mock_rows.to_string());
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
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distributed_runtime_collect_matches_embedded_for_join_agg() {
    let _lock = DIST_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let fixtures = support::ensure_integration_parquet_fixtures();
    let lineitem_path = fixtures.lineitem;
    let orders_path = fixtures.orders;
    let window_path = support::unique_path("ffq_client_window_case", "parquet");
    write_window_case_parquet(&window_path);
    let spill_dir = support::unique_path("ffq_client_dist_spill", "dir");
    let shuffle_root = support::unique_path("ffq_client_dist_shuffle", "dir");
    let _ = std::fs::create_dir_all(&shuffle_root);

    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    drop(listener);
    let endpoint = format!("http://{addr}");

    let mut coordinator_catalog = ffq_storage::Catalog::new();
    coordinator_catalog.register_table(TableDef {
        name: "lineitem".to_string(),
        uri: lineitem_path.to_string_lossy().to_string(),
        paths: Vec::new(),
        format: "parquet".to_string(),
        schema: None,
        stats: TableStats::default(),
        options: HashMap::new(),
    });
    coordinator_catalog.register_table(TableDef {
        name: "orders".to_string(),
        uri: orders_path.to_string_lossy().to_string(),
        paths: Vec::new(),
        format: "parquet".to_string(),
        schema: None,
        stats: TableStats::default(),
        options: HashMap::new(),
    });
    coordinator_catalog.register_table(TableDef {
        name: "window_case".to_string(),
        uri: window_path.to_string_lossy().to_string(),
        paths: Vec::new(),
        format: "parquet".to_string(),
        schema: None,
        stats: TableStats::default(),
        options: HashMap::new(),
    });
    let coordinator = Arc::new(Mutex::new(Coordinator::with_catalog(
        CoordinatorConfig {
            blacklist_failure_threshold: 3,
            shuffle_root: shuffle_root.clone(),
            ..CoordinatorConfig::default()
        },
        coordinator_catalog,
    )));
    let services = CoordinatorServices::from_shared(Arc::clone(&coordinator));
    let server_handle = tokio::spawn(async move {
        Server::builder()
            .add_service(ControlPlaneServer::new(services.clone()))
            .add_service(ShuffleServiceServer::new(services.clone()))
            .add_service(HeartbeatServiceServer::new(services))
            .serve(addr)
            .await
            .expect("grpc server");
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut worker_catalog = ffq_storage::Catalog::new();
    worker_catalog.register_table(TableDef {
        name: "lineitem".to_string(),
        uri: lineitem_path.to_string_lossy().to_string(),
        paths: Vec::new(),
        format: "parquet".to_string(),
        schema: None,
        stats: TableStats::default(),
        options: HashMap::new(),
    });
    worker_catalog.register_table(TableDef {
        name: "orders".to_string(),
        uri: orders_path.to_string_lossy().to_string(),
        paths: Vec::new(),
        format: "parquet".to_string(),
        schema: None,
        stats: TableStats::default(),
        options: HashMap::new(),
    });
    worker_catalog.register_table(TableDef {
        name: "window_case".to_string(),
        uri: window_path.to_string_lossy().to_string(),
        paths: Vec::new(),
        format: "parquet".to_string(),
        schema: None,
        stats: TableStats::default(),
        options: HashMap::new(),
    });
    let executor = Arc::new(DefaultTaskExecutor::new(Arc::new(worker_catalog)));

    let cp1 = Arc::new(
        GrpcControlPlane::connect(&endpoint)
            .await
            .expect("cp1 connect"),
    );
    let cp2 = Arc::new(
        GrpcControlPlane::connect(&endpoint)
            .await
            .expect("cp2 connect"),
    );

    let worker1 = Worker::new(
        WorkerConfig {
            worker_id: "w1".to_string(),
            cpu_slots: 1,
            per_task_memory_budget_bytes: 1024 * 1024,
            spill_dir: spill_dir.clone(),
            shuffle_root: shuffle_root.clone(),
        },
        cp1,
        Arc::clone(&executor),
    );
    let worker2 = Worker::new(
        WorkerConfig {
            worker_id: "w2".to_string(),
            cpu_slots: 1,
            per_task_memory_budget_bytes: 1024 * 1024,
            spill_dir: spill_dir.clone(),
            shuffle_root: shuffle_root.clone(),
        },
        cp2,
        executor,
    );

    let stop = Arc::new(AtomicBool::new(false));
    let stop1 = Arc::clone(&stop);
    let w1 = tokio::spawn(async move {
        while !stop1.load(Ordering::Relaxed) {
            let _ = worker1.poll_once().await;
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    });
    let stop2 = Arc::clone(&stop);
    let w2 = tokio::spawn(async move {
        while !stop2.load(Ordering::Relaxed) {
            let _ = worker2.poll_once().await;
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    });

    let mut cfg = EngineConfig::default();
    cfg.spill_dir = spill_dir.to_string_lossy().to_string();
    cfg.coordinator_endpoint = Some(endpoint.clone());
    let dist_engine = Engine::new(cfg.clone()).expect("distributed engine");
    register_tables(&dist_engine, &lineitem_path, &orders_path);
    register_window_case_table(&dist_engine, &window_path, true);
    let sql_scan = support::integration_queries::scan_filter_project();
    let sql_agg = support::integration_queries::join_aggregate();
    let sql_join = support::integration_queries::join_projection();
    let sql_cte = "WITH filtered AS (
        SELECT l_orderkey, l_partkey
        FROM lineitem
        WHERE l_orderkey >= 2
    )
    SELECT l_orderkey, l_partkey FROM filtered";
    let sql_in_subquery = "SELECT l_orderkey, l_partkey
        FROM lineitem
        WHERE l_orderkey IN (
            SELECT o_orderkey FROM orders WHERE o_custkey >= 100
        )";
    let sql_correlated_exists = "SELECT l_orderkey, l_partkey
        FROM lineitem
        WHERE EXISTS (
            SELECT o_orderkey
            FROM orders
            WHERE orders.o_orderkey = lineitem.l_orderkey
        )";
    let sql_cte_join_heavy = "WITH c AS (
        SELECT l_orderkey, l_partkey
        FROM lineitem
        WHERE l_orderkey >= 2
    )
    SELECT a.l_orderkey, a.l_partkey, b.l_partkey AS other_part
    FROM c a
    JOIN c b
      ON a.l_orderkey = b.l_orderkey";
    let sql_window = "SELECT l_orderkey, l_partkey,
        ROW_NUMBER() OVER (PARTITION BY l_orderkey ORDER BY l_partkey) AS rn
        FROM lineitem
        WHERE l_orderkey >= 2";
    let sql_window_rank = "SELECT grp, ord, score,
        ROW_NUMBER() OVER (PARTITION BY grp ORDER BY score ASC NULLS LAST) AS rn,
        RANK() OVER (PARTITION BY grp ORDER BY score ASC NULLS LAST) AS rnk,
        DENSE_RANK() OVER (PARTITION BY grp ORDER BY score ASC NULLS LAST) AS dr
        FROM window_case";
    let sql_window_frame = "SELECT grp, ord,
        SUM(score) OVER (
            PARTITION BY grp
            ORDER BY ord
            ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
        ) AS s_rows,
        SUM(score) OVER (
            PARTITION BY grp
            ORDER BY score
            GROUPS BETWEEN CURRENT ROW AND 1 FOLLOWING
        ) AS s_groups
        FROM window_case";
    let sql_window_nulls = "SELECT grp, ord,
        ROW_NUMBER() OVER (PARTITION BY grp ORDER BY score ASC NULLS FIRST) AS rn_nf,
        ROW_NUMBER() OVER (PARTITION BY grp ORDER BY score ASC NULLS LAST) AS rn_nl
        FROM window_case";
    let sql_window_exclude = "SELECT grp, ord,
        SUM(score) OVER (
            PARTITION BY grp
            ORDER BY ord
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            EXCLUDE CURRENT ROW
        ) AS s_ex
        FROM window_case";

    let dist_scan_batches = dist_engine
        .sql(sql_scan)
        .expect("dist sql")
        .collect()
        .await
        .expect("dist scan collect");

    let dist_agg_batches = dist_engine
        .sql(sql_agg)
        .expect("dist sql")
        .collect()
        .await
        .expect("dist agg collect");
    let dist_join_batches = dist_engine
        .sql(sql_join)
        .expect("dist sql")
        .collect()
        .await
        .expect("dist join collect");
    let dist_cte_batches = dist_engine
        .sql(sql_cte)
        .expect("dist cte sql")
        .collect()
        .await
        .expect("dist cte collect");
    let dist_in_subquery_batches = dist_engine
        .sql(sql_in_subquery)
        .expect("dist in-subquery sql")
        .collect()
        .await
        .expect("dist in-subquery collect");
    let dist_correlated_exists_batches = dist_engine
        .sql(sql_correlated_exists)
        .expect("dist correlated exists sql")
        .collect()
        .await
        .expect("dist correlated exists collect");
    let dist_cte_join_heavy_batches = dist_engine
        .sql(sql_cte_join_heavy)
        .expect("dist cte join-heavy sql")
        .collect()
        .await
        .expect("dist cte join-heavy collect");
    let dist_window_batches = dist_engine
        .sql(sql_window)
        .expect("dist window sql")
        .collect()
        .await
        .expect("dist window collect");
    let dist_window_rank_batches = dist_engine
        .sql(sql_window_rank)
        .expect("dist window rank sql")
        .collect()
        .await
        .expect("dist window rank collect");
    let dist_window_frame_batches = dist_engine
        .sql(sql_window_frame)
        .expect("dist window frame sql")
        .collect()
        .await
        .expect("dist window frame collect");
    let dist_window_nulls_batches = dist_engine
        .sql(sql_window_nulls)
        .expect("dist window nulls sql")
        .collect()
        .await
        .expect("dist window nulls collect");
    let dist_window_exclude_batches = dist_engine
        .sql(sql_window_exclude)
        .expect("dist window exclude sql")
        .collect()
        .await
        .expect("dist window exclude collect");

    cfg.coordinator_endpoint = None;

    let embedded_engine = Engine::new(cfg).expect("embedded engine");
    register_tables(&embedded_engine, &lineitem_path, &orders_path);
    register_window_case_table(&embedded_engine, &window_path, true);
    let embedded_scan_batches = embedded_engine
        .sql(sql_scan)
        .expect("embedded scan sql")
        .collect()
        .await
        .expect("embedded scan collect");
    let embedded_agg_batches = embedded_engine
        .sql(sql_agg)
        .expect("embedded agg sql")
        .collect()
        .await
        .expect("embedded agg collect");
    let embedded_join_batches = embedded_engine
        .sql(sql_join)
        .expect("embedded join sql")
        .collect()
        .await
        .expect("embedded join collect");
    let embedded_cte_batches = embedded_engine
        .sql(sql_cte)
        .expect("embedded cte sql")
        .collect()
        .await
        .expect("embedded cte collect");
    let embedded_in_subquery_batches = embedded_engine
        .sql(sql_in_subquery)
        .expect("embedded in-subquery sql")
        .collect()
        .await
        .expect("embedded in-subquery collect");
    let embedded_correlated_exists_batches = embedded_engine
        .sql(sql_correlated_exists)
        .expect("embedded correlated exists sql")
        .collect()
        .await
        .expect("embedded correlated exists collect");
    let embedded_cte_join_heavy_batches = embedded_engine
        .sql(sql_cte_join_heavy)
        .expect("embedded cte join-heavy sql")
        .collect()
        .await
        .expect("embedded cte join-heavy collect");
    let embedded_window_batches = embedded_engine
        .sql(sql_window)
        .expect("embedded window sql")
        .collect()
        .await
        .expect("embedded window collect");
    let embedded_window_rank_batches = embedded_engine
        .sql(sql_window_rank)
        .expect("embedded window rank sql")
        .collect()
        .await
        .expect("embedded window rank collect");
    let embedded_window_frame_batches = embedded_engine
        .sql(sql_window_frame)
        .expect("embedded window frame sql")
        .collect()
        .await
        .expect("embedded window frame collect");
    let embedded_window_nulls_batches = embedded_engine
        .sql(sql_window_nulls)
        .expect("embedded window nulls sql")
        .collect()
        .await
        .expect("embedded window nulls collect");
    let embedded_window_exclude_batches = embedded_engine
        .sql(sql_window_exclude)
        .expect("embedded window exclude sql")
        .collect()
        .await
        .expect("embedded window exclude collect");

    let dist_agg_norm = support::snapshot_text(&dist_agg_batches, &["l_orderkey"], 1e-9);
    let emb_agg_norm = support::snapshot_text(&embedded_agg_batches, &["l_orderkey"], 1e-9);
    assert_eq!(
        dist_agg_norm, emb_agg_norm,
        "distributed and embedded aggregate outputs differ"
    );

    let dist_join_norm = support::snapshot_text(
        &dist_join_batches,
        &["l_orderkey", "l_partkey", "o_custkey"],
        1e-9,
    );
    let emb_join_norm = support::snapshot_text(
        &embedded_join_batches,
        &["l_orderkey", "l_partkey", "o_custkey"],
        1e-9,
    );
    assert_eq!(
        dist_join_norm, emb_join_norm,
        "distributed and embedded join outputs differ"
    );

    let dist_scan_norm =
        support::snapshot_text(&dist_scan_batches, &["l_orderkey", "l_partkey"], 1e-9);
    let emb_scan_norm =
        support::snapshot_text(&embedded_scan_batches, &["l_orderkey", "l_partkey"], 1e-9);
    assert_eq!(
        dist_scan_norm, emb_scan_norm,
        "distributed and embedded scan/filter/project outputs differ"
    );

    let dist_cte_norm = support::snapshot_text(&dist_cte_batches, &["l_orderkey", "l_partkey"], 1e-9);
    let emb_cte_norm = support::snapshot_text(&embedded_cte_batches, &["l_orderkey", "l_partkey"], 1e-9);
    assert_eq!(
        dist_cte_norm, emb_cte_norm,
        "distributed and embedded CTE outputs differ"
    );

    let dist_in_norm =
        support::snapshot_text(&dist_in_subquery_batches, &["l_orderkey", "l_partkey"], 1e-9);
    let emb_in_norm =
        support::snapshot_text(&embedded_in_subquery_batches, &["l_orderkey", "l_partkey"], 1e-9);
    assert_eq!(
        dist_in_norm, emb_in_norm,
        "distributed and embedded IN-subquery outputs differ"
    );

    let dist_exists_norm = support::snapshot_text(
        &dist_correlated_exists_batches,
        &["l_orderkey", "l_partkey"],
        1e-9,
    );
    let emb_exists_norm = support::snapshot_text(
        &embedded_correlated_exists_batches,
        &["l_orderkey", "l_partkey"],
        1e-9,
    );
    assert_eq!(
        dist_exists_norm, emb_exists_norm,
        "distributed and embedded correlated EXISTS outputs differ"
    );

    let dist_cte_join_heavy_norm = support::snapshot_text(
        &dist_cte_join_heavy_batches,
        &["l_orderkey", "l_partkey", "other_part"],
        1e-9,
    );
    let emb_cte_join_heavy_norm = support::snapshot_text(
        &embedded_cte_join_heavy_batches,
        &["l_orderkey", "l_partkey", "other_part"],
        1e-9,
    );
    assert_eq!(
        dist_cte_join_heavy_norm, emb_cte_join_heavy_norm,
        "distributed and embedded CTE join-heavy outputs differ"
    );
    let dist_window_norm =
        support::snapshot_text(&dist_window_batches, &["l_orderkey", "l_partkey", "rn"], 1e-9);
    let emb_window_norm =
        support::snapshot_text(&embedded_window_batches, &["l_orderkey", "l_partkey", "rn"], 1e-9);
    assert_eq!(
        dist_window_norm, emb_window_norm,
        "distributed and embedded window outputs differ"
    );
    let dist_window_rank_norm =
        support::snapshot_text(&dist_window_rank_batches, &["grp", "ord"], 1e-9);
    let emb_window_rank_norm =
        support::snapshot_text(&embedded_window_rank_batches, &["grp", "ord"], 1e-9);
    assert_eq!(
        dist_window_rank_norm, emb_window_rank_norm,
        "distributed and embedded window rank outputs differ"
    );
    let dist_window_frame_norm =
        support::snapshot_text(&dist_window_frame_batches, &["grp", "ord"], 1e-9);
    let emb_window_frame_norm =
        support::snapshot_text(&embedded_window_frame_batches, &["grp", "ord"], 1e-9);
    assert_eq!(
        dist_window_frame_norm, emb_window_frame_norm,
        "distributed and embedded window frame outputs differ"
    );
    let dist_window_nulls_norm =
        support::snapshot_text(&dist_window_nulls_batches, &["grp", "ord"], 1e-9);
    let emb_window_nulls_norm =
        support::snapshot_text(&embedded_window_nulls_batches, &["grp", "ord"], 1e-9);
    assert_eq!(
        dist_window_nulls_norm, emb_window_nulls_norm,
        "distributed and embedded window null-order outputs differ"
    );
    let dist_window_exclude_norm =
        support::snapshot_text(&dist_window_exclude_batches, &["grp", "ord"], 1e-9);
    let emb_window_exclude_norm =
        support::snapshot_text(&embedded_window_exclude_batches, &["grp", "ord"], 1e-9);
    assert_eq!(
        dist_window_exclude_norm, emb_window_exclude_norm,
        "distributed and embedded window exclusion outputs differ"
    );

    let dist_agg = collect_group_counts(&dist_agg_batches);
    let emb_agg = collect_group_counts(&embedded_agg_batches);
    assert_eq!(dist_agg, emb_agg);
    assert_eq!(dist_agg, vec![(2, 2), (3, 3)]);

    let expected_join = vec![
        (2, 20, 100),
        (2, 21, 100),
        (3, 30, 200),
        (3, 31, 200),
        (3, 32, 200),
    ];
    let dist_join = collect_join_rows(&dist_join_batches);
    let emb_join = collect_join_rows(&embedded_join_batches);
    assert_eq!(dist_join, emb_join);
    assert_eq!(dist_join, expected_join);

    let dist_scan = collect_scan_rows(&dist_scan_batches);
    let emb_scan = collect_scan_rows(&embedded_scan_batches);
    assert_eq!(dist_scan, emb_scan);

    stop.store(true, Ordering::Relaxed);
    w1.abort();
    w2.abort();
    server_handle.abort();

    let _ = std::fs::remove_dir_all(&spill_dir);
    let _ = std::fs::remove_dir_all(&shuffle_root);
    let _ = std::fs::remove_file(&window_path);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distributed_runtime_no_schema_parity_matches_embedded() {
    let _lock = DIST_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let fixtures = support::ensure_integration_parquet_fixtures();
    let lineitem_path = fixtures.lineitem;
    let orders_path = fixtures.orders;
    let spill_dir = support::unique_path("ffq_client_dist_spill_noschema", "dir");
    let shuffle_root = support::unique_path("ffq_client_dist_shuffle_noschema", "dir");
    let _ = std::fs::create_dir_all(&shuffle_root);

    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    drop(listener);
    let endpoint = format!("http://{addr}");

    let mut coordinator_catalog = ffq_storage::Catalog::new();
    coordinator_catalog.register_table(TableDef {
        name: "lineitem".to_string(),
        uri: lineitem_path.to_string_lossy().to_string(),
        paths: Vec::new(),
        format: "parquet".to_string(),
        schema: None,
        stats: TableStats::default(),
        options: HashMap::new(),
    });
    coordinator_catalog.register_table(TableDef {
        name: "orders".to_string(),
        uri: orders_path.to_string_lossy().to_string(),
        paths: Vec::new(),
        format: "parquet".to_string(),
        schema: None,
        stats: TableStats::default(),
        options: HashMap::new(),
    });
    let coordinator = Arc::new(Mutex::new(Coordinator::with_catalog(
        CoordinatorConfig {
            blacklist_failure_threshold: 3,
            shuffle_root: shuffle_root.clone(),
            ..CoordinatorConfig::default()
        },
        coordinator_catalog,
    )));
    let services = CoordinatorServices::from_shared(Arc::clone(&coordinator));
    let server_handle = tokio::spawn(async move {
        Server::builder()
            .add_service(ControlPlaneServer::new(services.clone()))
            .add_service(ShuffleServiceServer::new(services.clone()))
            .add_service(HeartbeatServiceServer::new(services))
            .serve(addr)
            .await
            .expect("grpc server");
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut worker_catalog = ffq_storage::Catalog::new();
    worker_catalog.register_table(TableDef {
        name: "lineitem".to_string(),
        uri: lineitem_path.to_string_lossy().to_string(),
        paths: Vec::new(),
        format: "parquet".to_string(),
        schema: None,
        stats: TableStats::default(),
        options: HashMap::new(),
    });
    worker_catalog.register_table(TableDef {
        name: "orders".to_string(),
        uri: orders_path.to_string_lossy().to_string(),
        paths: Vec::new(),
        format: "parquet".to_string(),
        schema: None,
        stats: TableStats::default(),
        options: HashMap::new(),
    });
    let executor = Arc::new(DefaultTaskExecutor::new(Arc::new(worker_catalog)));

    let cp1 = Arc::new(
        GrpcControlPlane::connect(&endpoint)
            .await
            .expect("cp1 connect"),
    );
    let cp2 = Arc::new(
        GrpcControlPlane::connect(&endpoint)
            .await
            .expect("cp2 connect"),
    );

    let worker1 = Worker::new(
        WorkerConfig {
            worker_id: "w1".to_string(),
            cpu_slots: 1,
            per_task_memory_budget_bytes: 1024 * 1024,
            spill_dir: spill_dir.clone(),
            shuffle_root: shuffle_root.clone(),
        },
        cp1,
        Arc::clone(&executor),
    );
    let worker2 = Worker::new(
        WorkerConfig {
            worker_id: "w2".to_string(),
            cpu_slots: 1,
            per_task_memory_budget_bytes: 1024 * 1024,
            spill_dir: spill_dir.clone(),
            shuffle_root: shuffle_root.clone(),
        },
        cp2,
        executor,
    );

    let stop = Arc::new(AtomicBool::new(false));
    let stop1 = Arc::clone(&stop);
    let w1 = tokio::spawn(async move {
        while !stop1.load(Ordering::Relaxed) {
            let _ = worker1.poll_once().await;
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    });
    let stop2 = Arc::clone(&stop);
    let w2 = tokio::spawn(async move {
        while !stop2.load(Ordering::Relaxed) {
            let _ = worker2.poll_once().await;
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    });

    let mut cfg = EngineConfig::default();
    cfg.spill_dir = spill_dir.to_string_lossy().to_string();
    cfg.coordinator_endpoint = Some(endpoint.clone());
    cfg.schema_inference = ffq_common::SchemaInferencePolicy::On;
    let dist_engine = Engine::new(cfg.clone()).expect("distributed engine");
    register_tables_without_schema(&dist_engine, &lineitem_path, &orders_path);

    let sql = support::integration_queries::join_aggregate();
    let dist_batches = dist_engine
        .sql(sql)
        .expect("dist sql")
        .collect()
        .await
        .expect("dist collect");

    cfg.coordinator_endpoint = None;
    let embedded_engine = Engine::new(cfg).expect("embedded engine");
    register_tables_without_schema(&embedded_engine, &lineitem_path, &orders_path);
    let emb_batches = embedded_engine
        .sql(sql)
        .expect("embedded sql")
        .collect()
        .await
        .expect("embedded collect");

    let dist_norm = support::snapshot_text(&dist_batches, &["l_orderkey"], 1e-9);
    let emb_norm = support::snapshot_text(&emb_batches, &["l_orderkey"], 1e-9);
    assert_eq!(dist_norm, emb_norm, "distributed no-schema parity mismatch");

    stop.store(true, Ordering::Relaxed);
    w1.await.expect("worker1 join");
    w2.await.expect("worker2 join");
    server_handle.abort();
    let _ = std::fs::remove_dir_all(spill_dir);
    let _ = std::fs::remove_dir_all(shuffle_root);
}

#[cfg(feature = "vector")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distributed_runtime_two_phase_vector_join_rerank_matches_embedded() {
    let _lock = DIST_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let docs_path = support::unique_path("ffq_client_dist_docs", "parquet");
    let spill_dir = support::unique_path("ffq_client_dist_vec_spill", "dir");
    let shuffle_root = support::unique_path("ffq_client_dist_vec_shuffle", "dir");
    let _ = std::fs::create_dir_all(&shuffle_root);

    let emb_field = Field::new("item", DataType::Float32, true);
    let docs_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("title", DataType::Utf8, false),
        Field::new("lang", DataType::Utf8, false),
        Field::new("emb", DataType::FixedSizeList(Arc::new(emb_field), 3), true),
    ]));
    write_docs_vector(&docs_path, docs_schema);

    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    drop(listener);
    let endpoint = format!("http://{addr}");

    let coordinator = Arc::new(Mutex::new(Coordinator::new(CoordinatorConfig {
        blacklist_failure_threshold: 3,
        shuffle_root: shuffle_root.clone(),
        ..CoordinatorConfig::default()
    })));
    let services = CoordinatorServices::from_shared(Arc::clone(&coordinator));
    let server_handle = tokio::spawn(async move {
        Server::builder()
            .add_service(ControlPlaneServer::new(services.clone()))
            .add_service(ShuffleServiceServer::new(services.clone()))
            .add_service(HeartbeatServiceServer::new(services))
            .serve(addr)
            .await
            .expect("grpc server");
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut catalog = ffq_storage::Catalog::new();
    let mut docs_opts = HashMap::new();
    docs_opts.insert("vector.index_table".to_string(), "docs_idx".to_string());
    docs_opts.insert("vector.id_column".to_string(), "id".to_string());
    docs_opts.insert("vector.embedding_column".to_string(), "emb".to_string());
    docs_opts.insert("vector.prefetch_multiplier".to_string(), "3".to_string());
    catalog.register_table(TableDef {
        name: "docs".to_string(),
        uri: docs_path.to_string_lossy().to_string(),
        paths: Vec::new(),
        format: "parquet".to_string(),
        schema: Some(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("title", DataType::Utf8, false),
            Field::new("lang", DataType::Utf8, false),
            Field::new(
                "emb",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 3),
                true,
            ),
        ])),
        stats: TableStats::default(),
        options: docs_opts,
    });
    let mut idx_opts = HashMap::new();
    idx_opts.insert(
        "vector.mock_rows_json".to_string(),
        r#"[
            {"id":2,"score":0.99,"payload":"{\"source\":\"idx\"}"},
            {"id":1,"score":0.95,"payload":"{\"source\":\"idx\"}"},
            {"id":3,"score":0.10,"payload":"{\"source\":\"idx\"}"}
        ]"#
        .to_string(),
    );
    catalog.register_table(TableDef {
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
    });
    let executor = Arc::new(DefaultTaskExecutor::new(Arc::new(catalog)));

    let cp1 = Arc::new(
        GrpcControlPlane::connect(&endpoint)
            .await
            .expect("cp1 connect"),
    );
    let cp2 = Arc::new(
        GrpcControlPlane::connect(&endpoint)
            .await
            .expect("cp2 connect"),
    );
    let worker1 = Worker::new(
        WorkerConfig {
            worker_id: "w1".to_string(),
            cpu_slots: 1,
            per_task_memory_budget_bytes: 1024 * 1024,
            spill_dir: spill_dir.clone(),
            shuffle_root: shuffle_root.clone(),
        },
        cp1,
        Arc::clone(&executor),
    );
    let worker2 = Worker::new(
        WorkerConfig {
            worker_id: "w2".to_string(),
            cpu_slots: 1,
            per_task_memory_budget_bytes: 1024 * 1024,
            spill_dir: spill_dir.clone(),
            shuffle_root: shuffle_root.clone(),
        },
        cp2,
        executor,
    );
    let stop = Arc::new(AtomicBool::new(false));
    let stop1 = Arc::clone(&stop);
    let w1 = tokio::spawn(async move {
        while !stop1.load(Ordering::Relaxed) {
            let _ = worker1.poll_once().await;
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    });
    let stop2 = Arc::clone(&stop);
    let w2 = tokio::spawn(async move {
        while !stop2.load(Ordering::Relaxed) {
            let _ = worker2.poll_once().await;
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    });

    let mut cfg = EngineConfig::default();
    cfg.spill_dir = spill_dir.to_string_lossy().to_string();
    cfg.coordinator_endpoint = Some(endpoint.clone());
    let dist_engine = Engine::new(cfg.clone()).expect("distributed engine");
    register_two_phase_tables(&dist_engine, &docs_path);

    let sql = support::integration_queries::vector_two_phase_rerank();
    let mut params = HashMap::new();
    params.insert(
        "q".to_string(),
        LiteralValue::VectorF32(vec![1.0, 0.0, 0.0]),
    );
    let dist_batches = dist_engine
        .sql_with_params(sql, params.clone())
        .expect("dist sql")
        .collect()
        .await
        .expect("dist collect");
    cfg.coordinator_endpoint = None;

    let embedded_engine = Engine::new(cfg).expect("embedded engine");
    register_two_phase_tables(&embedded_engine, &docs_path);
    let embedded_batches = embedded_engine
        .sql_with_params(sql, params)
        .expect("embedded sql")
        .collect()
        .await
        .expect("embedded collect");

    let dist_ids: Vec<i64> = dist_batches
        .iter()
        .flat_map(|b| {
            let ids = b
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("ids");
            (0..b.num_rows()).map(|i| ids.value(i)).collect::<Vec<_>>()
        })
        .collect();
    let emb_ids: Vec<i64> = embedded_batches
        .iter()
        .flat_map(|b| {
            let ids = b
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("ids");
            (0..b.num_rows()).map(|i| ids.value(i)).collect::<Vec<_>>()
        })
        .collect();

    assert_eq!(dist_ids, emb_ids);
    assert_eq!(dist_ids, vec![1_i64]);

    stop.store(true, Ordering::Relaxed);
    w1.abort();
    w2.abort();
    server_handle.abort();

    let _ = std::fs::remove_file(&docs_path);
    let _ = std::fs::remove_dir_all(&spill_dir);
    let _ = std::fs::remove_dir_all(&shuffle_root);
}
