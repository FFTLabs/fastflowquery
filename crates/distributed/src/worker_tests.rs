use super::*;
use crate::coordinator::CoordinatorConfig;
use ffq_execution::{
    PhysicalOperatorFactory, deregister_global_physical_operator_factory,
    register_global_physical_operator_factory,
};
use ffq_planner::{
    AggExpr, Expr, JoinStrategyHint, JoinType, LogicalPlan, ParquetScanExec, ParquetWriteExec,
    PhysicalPlan, PhysicalPlannerConfig, create_physical_plan,
};
use ffq_storage::{TableDef, TableStats};
use parquet::arrow::ArrowWriter;
use std::collections::HashMap;
use std::fs::File;

use arrow::array::Int64Array;
use arrow_schema::{DataType, Field, Schema};

struct AddConstFactory;

impl PhysicalOperatorFactory for AddConstFactory {
    fn name(&self) -> &str {
        "add_const_i64"
    }

    fn execute(
        &self,
        input_schema: SchemaRef,
        input_batches: Vec<RecordBatch>,
        config: &HashMap<String, String>,
    ) -> Result<(SchemaRef, Vec<RecordBatch>)> {
        let col = config.get("column").cloned().ok_or_else(|| {
            FfqError::InvalidConfig("custom operator missing 'column' config".to_string())
        })?;
        let addend: i64 = config
            .get("addend")
            .ok_or_else(|| {
                FfqError::InvalidConfig("custom operator missing 'addend' config".to_string())
            })?
            .parse()
            .map_err(|e| {
                FfqError::InvalidConfig(format!("custom operator invalid addend value: {e}"))
            })?;
        let idx = input_schema
            .index_of(&col)
            .map_err(|e| FfqError::InvalidConfig(format!("column lookup failed: {e}")))?;

        let mut out = Vec::with_capacity(input_batches.len());
        for batch in input_batches {
            let mut cols = batch.columns().to_vec();
            let base = cols[idx]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    FfqError::Execution("add_const_i64 expects Int64 input column".to_string())
                })?;
            let mut builder = Int64Builder::with_capacity(base.len());
            for v in base.iter() {
                match v {
                    Some(x) => builder.append_value(x + addend),
                    None => builder.append_null(),
                }
            }
            cols[idx] = Arc::new(builder.finish());
            out.push(
                RecordBatch::try_new(Arc::clone(&input_schema), cols)
                    .map_err(|e| FfqError::Execution(format!("custom batch build failed: {e}")))?,
            );
        }
        Ok((input_schema, out))
    }
}

fn unique_path(prefix: &str, ext: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}_{nanos}.{ext}"))
}

fn write_parquet(
    path: &std::path::Path,
    schema: Arc<Schema>,
    cols: Vec<Arc<dyn arrow::array::Array>>,
) {
    let batch = RecordBatch::try_new(schema.clone(), cols).expect("build batch");
    let file = File::create(path).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, schema, None).expect("writer");
    writer.write(&batch).expect("write");
    writer.close().expect("close");
}

fn test_task_context(
    query_id: &str,
    stage_id: u64,
    task_id: u64,
    attempt: u32,
    shuffle_root: &std::path::Path,
) -> TaskContext {
    TaskContext {
        query_id: query_id.to_string(),
        stage_id,
        task_id,
        attempt,
        per_task_memory_budget_bytes: 1,
        batch_size_rows: 8192,
        spill_trigger_ratio_num: 1,
        spill_trigger_ratio_den: 1,
        join_radix_bits: 8,
        join_bloom_enabled: true,
        join_bloom_bits: 20,
        shuffle_compression_codec: ffq_shuffle::ShuffleCompressionCodec::Lz4,
        reduce_fetch_window_partitions: 4,
        map_output_publish_window_partitions: 1,
        spill_dir: std::env::temp_dir(),
        shuffle_root: shuffle_root.to_path_buf(),
        assigned_reduce_partitions: Vec::new(),
        assigned_reduce_split_index: 0,
        assigned_reduce_split_count: 1,
    }
}

#[tokio::test]
async fn coordinator_with_two_workers_runs_join_and_agg_query() {
    let lineitem_path = unique_path("ffq_dist_lineitem", "parquet");
    let orders_path = unique_path("ffq_dist_orders", "parquet");
    let spill_dir = unique_path("ffq_dist_spill", "dir");
    let shuffle_root = unique_path("ffq_dist_shuffle", "dir");
    let _ = std::fs::create_dir_all(&shuffle_root);

    let lineitem_schema = Arc::new(Schema::new(vec![
        Field::new("l_orderkey", DataType::Int64, false),
        Field::new("l_partkey", DataType::Int64, false),
    ]));
    write_parquet(
        &lineitem_path,
        lineitem_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 2, 2, 3, 3, 3])),
            Arc::new(Int64Array::from(vec![10_i64, 20, 21, 30, 31, 32])),
        ],
    );

    let orders_schema = Arc::new(Schema::new(vec![
        Field::new("o_orderkey", DataType::Int64, false),
        Field::new("o_custkey", DataType::Int64, false),
    ]));
    write_parquet(
        &orders_path,
        orders_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![2_i64, 3, 4])),
            Arc::new(Int64Array::from(vec![100_i64, 200, 300])),
        ],
    );

    let mut coordinator_catalog = Catalog::new();
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
    let mut worker_catalog = Catalog::new();
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
    let worker_catalog = Arc::new(worker_catalog);

    let physical = create_physical_plan(
        &LogicalPlan::Aggregate {
            group_exprs: vec![Expr::Column("l_orderkey".to_string())],
            aggr_exprs: vec![(
                AggExpr::Count(Expr::Column("l_partkey".to_string())),
                "c".to_string(),
            )],
            input: Box::new(LogicalPlan::Join {
                left: Box::new(LogicalPlan::TableScan {
                    table: "lineitem".to_string(),
                    projection: None,
                    filters: vec![],
                }),
                right: Box::new(LogicalPlan::TableScan {
                    table: "orders".to_string(),
                    projection: None,
                    filters: vec![],
                }),
                on: vec![("l_orderkey".to_string(), "o_orderkey".to_string())],
                join_type: JoinType::Inner,
                strategy_hint: JoinStrategyHint::BroadcastRight,
            }),
        },
        &PhysicalPlannerConfig {
            shuffle_partitions: 4,
            ..PhysicalPlannerConfig::default()
        },
    )
    .expect("physical plan");
    let physical_json = serde_json::to_vec(&physical).expect("physical json");

    let coordinator = Arc::new(Mutex::new(Coordinator::with_catalog(
        CoordinatorConfig::default(),
        coordinator_catalog,
    )));
    {
        let mut c = coordinator.lock().await;
        c.submit_query("1001".to_string(), &physical_json)
            .expect("submit");
    }

    let control = Arc::new(InProcessControlPlane::new(Arc::clone(&coordinator)));
    let exec = Arc::new(DefaultTaskExecutor::new(Arc::clone(&worker_catalog)));
    let worker1 = Worker::new(
        WorkerConfig {
            worker_id: "w1".to_string(),
            cpu_slots: 1,
            spill_dir: spill_dir.clone(),
            shuffle_root: shuffle_root.clone(),
            ..WorkerConfig::default()
        },
        Arc::clone(&control),
        Arc::clone(&exec),
    );
    let worker2 = Worker::new(
        WorkerConfig {
            worker_id: "w2".to_string(),
            cpu_slots: 1,
            spill_dir: spill_dir.clone(),
            shuffle_root: shuffle_root.clone(),
            ..WorkerConfig::default()
        },
        control,
        Arc::clone(&exec),
    );

    for _ in 0..16 {
        let _ = worker1.poll_once().await.expect("worker1 poll");
        let _ = worker2.poll_once().await.expect("worker2 poll");
        let state = {
            let c = coordinator.lock().await;
            c.get_query_status("1001").expect("status").state
        };
        if state == crate::coordinator::QueryState::Succeeded {
            let batches = exec.take_query_output("1001").await.expect("sink output");
            assert!(!batches.is_empty());
            let encoded = {
                let c = coordinator.lock().await;
                c.fetch_query_results("1001").expect("coordinator results")
            };
            assert!(!encoded.is_empty());
            let _ = std::fs::remove_file(&lineitem_path);
            let _ = std::fs::remove_file(&orders_path);
            let _ = std::fs::remove_dir_all(&spill_dir);
            let _ = std::fs::remove_dir_all(&shuffle_root);
            return;
        }
        assert_ne!(state, crate::coordinator::QueryState::Failed);
    }

    let _ = std::fs::remove_file(lineitem_path);
    let _ = std::fs::remove_file(orders_path);
    let _ = std::fs::remove_dir_all(spill_dir);
    let _ = std::fs::remove_dir_all(shuffle_root);
    panic!("query did not finish in allotted polls");
}

#[tokio::test]
async fn worker_executes_parquet_write_sink() {
    let src_path = unique_path("ffq_worker_sink_src", "parquet");
    let out_dir = unique_path("ffq_worker_sink_out", "dir");
    let out_file = out_dir.join("part-00000.parquet");
    let spill_dir = unique_path("ffq_worker_sink_spill", "dir");

    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, false),
    ]));
    write_parquet(
        &src_path,
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 2, 3])),
            Arc::new(Int64Array::from(vec![10_i64, 20, 30])),
        ],
    );

    let mut catalog = Catalog::new();
    catalog.register_table(TableDef {
        name: "src".to_string(),
        uri: src_path.to_string_lossy().to_string(),
        paths: Vec::new(),
        format: "parquet".to_string(),
        schema: Some((*schema).clone()),
        stats: TableStats::default(),
        options: HashMap::new(),
    });
    catalog.register_table(TableDef {
        name: "dst".to_string(),
        uri: out_dir.to_string_lossy().to_string(),
        paths: Vec::new(),
        format: "parquet".to_string(),
        schema: Some((*schema).clone()),
        stats: TableStats::default(),
        options: HashMap::new(),
    });
    let catalog = Arc::new(catalog);

    let plan = PhysicalPlan::ParquetWrite(ParquetWriteExec {
        table: "dst".to_string(),
        input: Box::new(PhysicalPlan::ParquetScan(ParquetScanExec {
            table: "src".to_string(),
            schema: None,
            projection: Some(vec!["a".to_string(), "b".to_string()]),
            filters: vec![],
        })),
    });
    let plan_json = serde_json::to_vec(&plan).expect("plan json");

    let coordinator = Arc::new(Mutex::new(Coordinator::new(CoordinatorConfig {
        blacklist_failure_threshold: 3,
        shuffle_root: out_dir.clone(),
        ..CoordinatorConfig::default()
    })));
    {
        let mut c = coordinator.lock().await;
        c.submit_query("2001".to_string(), &plan_json)
            .expect("submit");
    }
    let control = Arc::new(InProcessControlPlane::new(Arc::clone(&coordinator)));
    let worker = Worker::new(
        WorkerConfig {
            worker_id: "w1".to_string(),
            cpu_slots: 1,
            spill_dir: spill_dir.clone(),
            shuffle_root: out_dir.clone(),
            ..WorkerConfig::default()
        },
        control,
        Arc::new(DefaultTaskExecutor::new(catalog)),
    );

    for _ in 0..16 {
        let _ = worker.poll_once().await.expect("worker poll");
        let state = {
            let c = coordinator.lock().await;
            c.get_query_status("2001").expect("status").state
        };
        if state == crate::coordinator::QueryState::Succeeded {
            assert!(out_file.exists(), "sink file missing");
            let file = File::open(&out_file).expect("open sink");
            let reader =
                parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
                    .expect("reader build")
                    .build()
                    .expect("reader");
            let rows = reader.map(|b| b.expect("decode").num_rows()).sum::<usize>();
            assert_eq!(rows, 3);
            let _ = std::fs::remove_file(src_path);
            let _ = std::fs::remove_file(out_file);
            let _ = std::fs::remove_dir_all(out_dir);
            let _ = std::fs::remove_dir_all(spill_dir);
            return;
        }
        assert_ne!(state, crate::coordinator::QueryState::Failed);
    }

    let _ = std::fs::remove_file(src_path);
    let _ = std::fs::remove_file(out_file);
    let _ = std::fs::remove_dir_all(out_dir);
    let _ = std::fs::remove_dir_all(spill_dir);
    panic!("sink query did not finish");
}

#[tokio::test]
async fn coordinator_with_workers_executes_custom_operator_stage() {
    let _ = deregister_global_physical_operator_factory("add_const_i64");
    let _ = register_global_physical_operator_factory(Arc::new(AddConstFactory));

    let src_path = unique_path("ffq_dist_custom_src", "parquet");
    let spill_dir = unique_path("ffq_dist_custom_spill", "dir");
    let shuffle_root = unique_path("ffq_dist_custom_shuffle", "dir");
    let _ = std::fs::create_dir_all(&shuffle_root);

    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    write_parquet(
        &src_path,
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 2, 3])),
            Arc::new(Int64Array::from(vec![10_i64, 20, 30])),
        ],
    );

    let mut coordinator_catalog = Catalog::new();
    coordinator_catalog.register_table(TableDef {
        name: "t".to_string(),
        uri: src_path.to_string_lossy().to_string(),
        paths: Vec::new(),
        format: "parquet".to_string(),
        schema: None,
        stats: TableStats::default(),
        options: HashMap::new(),
    });
    let mut worker_catalog = Catalog::new();
    worker_catalog.register_table(TableDef {
        name: "t".to_string(),
        uri: src_path.to_string_lossy().to_string(),
        paths: Vec::new(),
        format: "parquet".to_string(),
        schema: None,
        stats: TableStats::default(),
        options: HashMap::new(),
    });
    let worker_catalog = Arc::new(worker_catalog);

    let mut cfg = HashMap::new();
    cfg.insert("column".to_string(), "v".to_string());
    cfg.insert("addend".to_string(), "5".to_string());
    let plan = PhysicalPlan::Custom(ffq_planner::CustomExec {
        op_name: "add_const_i64".to_string(),
        config: cfg,
        input: Box::new(PhysicalPlan::ParquetScan(ParquetScanExec {
            table: "t".to_string(),
            schema: None,
            projection: Some(vec!["k".to_string(), "v".to_string()]),
            filters: vec![],
        })),
    });
    let physical_json = serde_json::to_vec(&plan).expect("physical json");

    let coordinator = Arc::new(Mutex::new(Coordinator::with_catalog(
        CoordinatorConfig::default(),
        coordinator_catalog,
    )));
    {
        let mut c = coordinator.lock().await;
        c.submit_query("3001".to_string(), &physical_json)
            .expect("submit");
    }

    let control = Arc::new(InProcessControlPlane::new(Arc::clone(&coordinator)));
    let exec = Arc::new(DefaultTaskExecutor::new(Arc::clone(&worker_catalog)));
    let worker1 = Worker::new(
        WorkerConfig {
            worker_id: "w1".to_string(),
            cpu_slots: 1,
            spill_dir: spill_dir.clone(),
            shuffle_root: shuffle_root.clone(),
            ..WorkerConfig::default()
        },
        Arc::clone(&control),
        Arc::clone(&exec),
    );
    let worker2 = Worker::new(
        WorkerConfig {
            worker_id: "w2".to_string(),
            cpu_slots: 1,
            spill_dir: spill_dir.clone(),
            shuffle_root: shuffle_root.clone(),
            ..WorkerConfig::default()
        },
        control,
        Arc::clone(&exec),
    );

    for _ in 0..16 {
        let _ = worker1.poll_once().await.expect("worker1 poll");
        let _ = worker2.poll_once().await.expect("worker2 poll");
        let state = {
            let c = coordinator.lock().await;
            c.get_query_status("3001").expect("status").state
        };
        if state == crate::coordinator::QueryState::Succeeded {
            let batches = exec.take_query_output("3001").await.expect("sink output");
            let all = concat_batches(&batches[0].schema(), &batches).expect("concat");
            let values = all
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int64 values");
            assert_eq!(values.values(), &[15_i64, 25, 35]);

            let _ = std::fs::remove_file(&src_path);
            let _ = std::fs::remove_dir_all(&spill_dir);
            let _ = std::fs::remove_dir_all(&shuffle_root);
            let _ = deregister_global_physical_operator_factory("add_const_i64");
            return;
        }
        assert_ne!(state, crate::coordinator::QueryState::Failed);
    }

    let _ = std::fs::remove_file(src_path);
    let _ = std::fs::remove_dir_all(spill_dir);
    let _ = std::fs::remove_dir_all(shuffle_root);
    let _ = deregister_global_physical_operator_factory("add_const_i64");
    panic!("custom query did not finish in allotted polls");
}

#[test]
fn shuffle_read_hash_requires_assigned_partitions() {
    let shuffle_root = unique_path("ffq_shuffle_read_assign_required", "dir");
    let _ = std::fs::create_dir_all(&shuffle_root);
    let ctx = test_task_context("5001", 0, 0, 1, &shuffle_root);
    let err = read_stage_input_from_shuffle(
        1,
        &ffq_planner::PartitioningSpec::HashKeys {
            keys: vec!["k".to_string()],
            partitions: 4,
        },
        5001,
        &ctx,
    )
    .err()
    .expect("missing assignment should error");
    match err {
        FfqError::Execution(msg) => assert!(msg.contains("missing assigned_reduce_partitions")),
        other => panic!("unexpected error: {other:?}"),
    }
    let _ = std::fs::remove_dir_all(shuffle_root);
}

#[test]
fn shuffle_read_hash_reads_only_assigned_partition_subset() {
    let shuffle_root = unique_path("ffq_shuffle_read_scoped", "dir");
    let _ = std::fs::create_dir_all(&shuffle_root);
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
    let input_batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(
            (1_i64..=64_i64).collect::<Vec<_>>(),
        ))],
    )
    .expect("input batch");
    let child = ExecOutput {
        schema,
        batches: vec![input_batch],
    };

    let map_ctx = test_task_context("5002", 1, 0, 1, &shuffle_root);
    let partitioning = ffq_planner::PartitioningSpec::HashKeys {
        keys: vec!["k".to_string()],
        partitions: 4,
    };
    let metas =
        write_stage_shuffle_outputs(&child, &partitioning, 5002, &map_ctx).expect("write map");
    assert!(!metas.is_empty());
    let target = metas[0].clone();

    let reduce_ctx = TaskContext {
        task_id: target.reduce_partition as u64,
        assigned_reduce_partitions: vec![target.reduce_partition],
        ..test_task_context("5002", 0, target.reduce_partition as u64, 1, &shuffle_root)
    };
    let out = read_stage_input_from_shuffle(1, &partitioning, 5002, &reduce_ctx)
        .expect("read assigned partition");
    let rows = out.batches.iter().map(|b| b.num_rows() as u64).sum::<u64>();
    assert_eq!(rows, target.rows);

    let _ = std::fs::remove_dir_all(shuffle_root);
}

#[test]
fn shuffle_read_hash_split_assignment_shards_one_partition_deterministically() {
    let shuffle_root = unique_path("ffq_shuffle_read_split_shard", "dir");
    let _ = std::fs::create_dir_all(&shuffle_root);
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
    let input_batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(
            (1_i64..=128_i64).collect::<Vec<_>>(),
        ))],
    )
    .expect("input batch");
    let child = ExecOutput {
        schema,
        batches: vec![input_batch],
    };
    let partitioning = ffq_planner::PartitioningSpec::HashKeys {
        keys: vec!["k".to_string()],
        partitions: 4,
    };

    let map_ctx = test_task_context("5003", 1, 0, 1, &shuffle_root);
    let metas =
        write_stage_shuffle_outputs(&child, &partitioning, 5003, &map_ctx).expect("write map");
    let target = metas
        .iter()
        .max_by_key(|m| m.rows)
        .expect("some partition")
        .clone();

    let read_rows = |split_index: u32| -> u64 {
        let reduce_ctx = TaskContext {
            task_id: target.reduce_partition as u64,
            assigned_reduce_partitions: vec![target.reduce_partition],
            assigned_reduce_split_index: split_index,
            assigned_reduce_split_count: 2,
            ..test_task_context("5003", 0, target.reduce_partition as u64, 1, &shuffle_root)
        };
        let out = read_stage_input_from_shuffle(1, &partitioning, 5003, &reduce_ctx)
            .expect("read assigned partition");
        out.batches.iter().map(|b| b.num_rows() as u64).sum::<u64>()
    };
    let left = read_rows(0);
    let right = read_rows(1);
    assert_eq!(left + right, target.rows);
    let _ = std::fs::remove_dir_all(shuffle_root);
}

#[test]
fn shuffle_read_incremental_cursor_reads_only_unseen_bytes() {
    let shuffle_root = unique_path("ffq_shuffle_read_incremental_cursor", "dir");
    let _ = std::fs::create_dir_all(&shuffle_root);
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
    let partitioning = ffq_planner::PartitioningSpec::HashKeys {
        keys: vec!["k".to_string()],
        partitions: 1,
    };

    let batch1 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1_i64, 2]))],
    )
    .expect("batch1");
    let batch2 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![3_i64, 4]))],
    )
    .expect("batch2");

    let map_ctx = test_task_context("5004", 1, 0, 1, &shuffle_root);
    let out1 = ExecOutput {
        schema: Arc::clone(&schema),
        batches: vec![batch1],
    };
    let out2 = ExecOutput {
        schema,
        batches: vec![batch2],
    };
    write_stage_shuffle_outputs(&out1, &partitioning, 5004, &map_ctx).expect("write chunk1");
    let metas = write_stage_shuffle_outputs(&out2, &partitioning, 5004, &map_ctx)
        .expect("write chunk2 and aggregate index");
    assert_eq!(metas.len(), 1);
    let target = metas[0].reduce_partition;

    let reader = ShuffleReader::new(&shuffle_root);
    let mut cursors = HashMap::<u32, (u32, u64)>::new();

    let (_attempt, first_batches) =
        read_partition_incremental_latest(&reader, 5004, 1, 0, target, &mut cursors)
            .expect("first incremental read");
    let first_rows = first_batches
        .iter()
        .map(|b| b.num_rows() as u64)
        .sum::<u64>();
    assert_eq!(first_rows, 2);

    let (_attempt, second_batches) =
        read_partition_incremental_latest(&reader, 5004, 1, 0, target, &mut cursors)
            .expect("second incremental read");
    let second_rows = second_batches
        .iter()
        .map(|b| b.num_rows() as u64)
        .sum::<u64>();
    assert_eq!(
        second_rows, 0,
        "second incremental read should not decode already consumed bytes"
    );

    let _ = std::fs::remove_dir_all(shuffle_root);
}

#[test]
fn shuffle_read_incremental_cursor_resets_when_latest_attempt_changes() {
    let shuffle_root = unique_path("ffq_shuffle_retry_cursor_reset", "dir");
    let _ = std::fs::create_dir_all(&shuffle_root);
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
    let partitioning = ffq_planner::PartitioningSpec::HashKeys {
        keys: vec!["k".to_string()],
        partitions: 1,
    };

    let base_ctx = test_task_context("5006", 1, 0, 1, &shuffle_root);

    write_stage_shuffle_outputs(
        &ExecOutput {
            schema: Arc::clone(&schema),
            batches: vec![
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))],
                )
                .expect("attempt1 batch"),
            ],
        },
        &partitioning,
        5006,
        &base_ctx,
    )
    .expect("write attempt1");

    let reader = ShuffleReader::new(&shuffle_root);
    let mut cursors = HashMap::<u32, (u32, u64)>::new();
    let (attempt1, first) = read_partition_incremental_latest(&reader, 5006, 1, 0, 0, &mut cursors)
        .expect("read attempt1");
    assert_eq!(attempt1, 1);
    assert_eq!(first.iter().map(|b| b.num_rows() as u64).sum::<u64>(), 3);

    let mut retry_ctx = base_ctx.clone();
    retry_ctx.attempt = 2;
    write_stage_shuffle_outputs(
        &ExecOutput {
            schema,
            batches: vec![
                RecordBatch::try_new(
                    Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)])),
                    vec![Arc::new(Int64Array::from(vec![42_i64]))],
                )
                .expect("attempt2 batch"),
            ],
        },
        &partitioning,
        5006,
        &retry_ctx,
    )
    .expect("write attempt2");

    let (attempt2, second) =
        read_partition_incremental_latest(&reader, 5006, 1, 0, 0, &mut cursors)
            .expect("read attempt2");
    assert_eq!(attempt2, 2, "reader should switch to latest attempt");
    assert_eq!(
        second.iter().map(|b| b.num_rows() as u64).sum::<u64>(),
        1,
        "cursor must reset when attempt changes to avoid row loss"
    );

    let _ = std::fs::remove_dir_all(shuffle_root);
}
