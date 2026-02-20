use std::collections::HashMap;
use std::fs::{self, File};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::Int64Array;
#[cfg(feature = "vector")]
use arrow::array::{FixedSizeListBuilder, Float32Array, Float32Builder};
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use ffq_common::adaptive::plan_adaptive_reduce_layout;
use ffq_execution::PhysicalOperatorFactory;
#[cfg(feature = "vector")]
use ffq_planner::LiteralValue;
use ffq_planner::VectorTopKExec;
use ffq_planner::{
    CteRefExec, CustomExec, Expr, ParquetScanExec, PartitioningSpec, PhysicalPlan, UnionAllExec,
    WindowExpr, WindowFrameBound, WindowFrameExclusion, WindowFrameSpec, WindowFrameUnits,
    WindowFunction, WindowOrderExpr,
};
use ffq_storage::vector_index::{VectorIndexProvider, VectorTopKRow};
use ffq_storage::{Catalog, TableDef, TableStats};
use futures::TryStreamExt;
use futures::future::BoxFuture;
use parquet::arrow::ArrowWriter;

#[cfg(feature = "vector")]
use super::run_topk_by_score;
use super::{
    EmbeddedRuntime, ExecOutput, QueryContext, Runtime, TraceIds,
    embedded_adaptive_plan_for_partitioning_with_target, hash_key, join_key_from_row,
    resolve_key_indexes, rows_from_batches, rows_to_vector_topk_output,
    run_vector_topk_with_provider, run_window_exec, run_window_exec_with_ctx,
    scalar_estimate_bytes,
};
use crate::physical_registry::PhysicalOperatorRegistry;

struct MockVectorProvider;

impl VectorIndexProvider for MockVectorProvider {
    fn topk<'a>(
        &'a self,
        _query_vec: Vec<f32>,
        _k: usize,
        _filter: Option<String>,
    ) -> BoxFuture<'a, ffq_common::Result<Vec<VectorTopKRow>>> {
        Box::pin(async {
            Ok(vec![
                VectorTopKRow {
                    id: 7,
                    score: 0.77,
                    payload_json: Some("{\"tenant\":\"a\"}".to_string()),
                },
                VectorTopKRow {
                    id: 2,
                    score: 0.65,
                    payload_json: None,
                },
            ])
        })
    }
}

struct CountingFactory {
    calls: Arc<AtomicUsize>,
}

impl PhysicalOperatorFactory for CountingFactory {
    fn name(&self) -> &str {
        "counting_passthrough"
    }

    fn execute(
        &self,
        input_schema: arrow_schema::SchemaRef,
        input_batches: Vec<RecordBatch>,
        _config: &HashMap<String, String>,
    ) -> ffq_common::Result<(arrow_schema::SchemaRef, Vec<RecordBatch>)> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok((input_schema, input_batches))
    }
}

#[test]
fn vector_topk_rows_are_encoded_as_batch() {
    let rows = vec![
        ffq_storage::vector_index::VectorTopKRow {
            id: 10,
            score: 0.9,
            payload_json: Some("{\"title\":\"a\"}".to_string()),
        },
        ffq_storage::vector_index::VectorTopKRow {
            id: 20,
            score: 0.8,
            payload_json: None,
        },
    ];
    let out = rows_to_vector_topk_output(rows).expect("build output");
    assert_eq!(out.batches.len(), 1);
    let b = &out.batches[0];
    assert_eq!(b.num_rows(), 2);
    assert_eq!(b.schema().field(0).name(), "id");
    assert_eq!(b.schema().field(1).name(), "score");
    assert_eq!(b.schema().field(2).name(), "payload");
}

#[test]
fn vector_topk_exec_uses_provider_rows() {
    let exec = VectorTopKExec {
        table: "docs_idx".to_string(),
        query_vector: vec![1.0, 0.0, 0.0],
        k: 2,
        filter: Some("{\"must\":[]}".to_string()),
    };
    let provider = MockVectorProvider;
    let out = futures::executor::block_on(run_vector_topk_with_provider(&exec, &provider))
        .expect("vector topk output");
    assert_eq!(out.batches.len(), 1);
    let b = &out.batches[0];
    assert_eq!(b.num_rows(), 2);
    assert_eq!(b.schema().field(0).name(), "id");
    assert_eq!(b.schema().field(1).name(), "score");
    assert_eq!(b.schema().field(2).name(), "payload");
}

#[test]
fn window_exclude_current_row_changes_sum_frame_results() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("ord", DataType::Int64, false),
        Field::new("score", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 2, 3])),
            Arc::new(Int64Array::from(vec![10_i64, 20, 30])),
        ],
    )
    .expect("batch");
    let input = ExecOutput {
        schema: schema.clone(),
        batches: vec![batch],
    };
    let w = WindowExpr {
        func: WindowFunction::Sum(Expr::ColumnRef {
            name: "score".to_string(),
            index: 1,
        }),
        partition_by: vec![],
        order_by: vec![WindowOrderExpr {
            expr: Expr::ColumnRef {
                name: "ord".to_string(),
                index: 0,
            },
            asc: true,
            nulls_first: false,
        }],
        frame: Some(WindowFrameSpec {
            units: WindowFrameUnits::Rows,
            start_bound: WindowFrameBound::UnboundedPreceding,
            end_bound: WindowFrameBound::UnboundedFollowing,
            exclusion: WindowFrameExclusion::CurrentRow,
        }),
        output_name: "s".to_string(),
    };
    let out = run_window_exec(input, &[w]).expect("window");
    let arr = out.batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .expect("f64");
    let vals = (0..arr.len()).map(|i| arr.value(i)).collect::<Vec<_>>();
    assert_eq!(vals, vec![50.0, 40.0, 30.0]);
}

#[test]
fn window_sum_supports_all_exclusion_modes() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("ord", DataType::Int64, false),
        Field::new("score", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 2, 3])),
            Arc::new(Int64Array::from(vec![10_i64, 10, 20])),
        ],
    )
    .expect("batch");
    let mk_input = || ExecOutput {
        schema: schema.clone(),
        batches: vec![batch.clone()],
    };
    let run = |exclusion: WindowFrameExclusion| -> Vec<f64> {
        let w = WindowExpr {
            func: WindowFunction::Sum(Expr::ColumnRef {
                name: "score".to_string(),
                index: 1,
            }),
            partition_by: vec![],
            order_by: vec![WindowOrderExpr {
                expr: Expr::ColumnRef {
                    name: "score".to_string(),
                    index: 1,
                },
                asc: true,
                nulls_first: false,
            }],
            frame: Some(WindowFrameSpec {
                units: WindowFrameUnits::Rows,
                start_bound: WindowFrameBound::UnboundedPreceding,
                end_bound: WindowFrameBound::UnboundedFollowing,
                exclusion,
            }),
            output_name: "s".to_string(),
        };
        let out = run_window_exec(mk_input(), &[w]).expect("window");
        let arr = out.batches[0]
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .expect("f64");
        (0..arr.len()).map(|i| arr.value(i)).collect::<Vec<_>>()
    };

    assert_eq!(run(WindowFrameExclusion::NoOthers), vec![40.0, 40.0, 40.0]);
    assert_eq!(
        run(WindowFrameExclusion::CurrentRow),
        vec![30.0, 30.0, 20.0]
    );
    assert_eq!(run(WindowFrameExclusion::Group), vec![20.0, 20.0, 20.0]);
    assert_eq!(run(WindowFrameExclusion::Ties), vec![30.0, 30.0, 40.0]);
}

#[test]
fn window_exclusion_does_not_change_rank_results() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("ord", DataType::Int64, false),
        Field::new("score", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 2, 3])),
            Arc::new(Int64Array::from(vec![10_i64, 10, 20])),
        ],
    )
    .expect("batch");
    let input = ExecOutput {
        schema: schema.clone(),
        batches: vec![batch],
    };
    let w = WindowExpr {
        func: WindowFunction::Rank,
        partition_by: vec![],
        order_by: vec![WindowOrderExpr {
            expr: Expr::ColumnRef {
                name: "score".to_string(),
                index: 1,
            },
            asc: true,
            nulls_first: false,
        }],
        frame: Some(WindowFrameSpec {
            units: WindowFrameUnits::Rows,
            start_bound: WindowFrameBound::UnboundedPreceding,
            end_bound: WindowFrameBound::CurrentRow,
            exclusion: WindowFrameExclusion::Group,
        }),
        output_name: "r".to_string(),
    };
    let out = run_window_exec(input, &[w]).expect("window");
    let arr = out.batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("i64");
    let vals = (0..arr.len()).map(|i| arr.value(i)).collect::<Vec<_>>();
    assert_eq!(vals, vec![1, 1, 3]);
}

#[test]
fn window_exec_spills_under_tight_memory_budget_and_cleans_temp_files() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("ord", DataType::Int64, false),
        Field::new("score", DataType::Int64, false),
    ]));
    let n = 2048_i64;
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values(1_i64..=n)),
            Arc::new(Int64Array::from_iter_values(
                (1_i64..=n).map(|v| (v % 17) + 1),
            )),
        ],
    )
    .expect("batch");
    let input = ExecOutput {
        schema: schema.clone(),
        batches: vec![batch],
    };
    let w = WindowExpr {
        func: WindowFunction::Sum(Expr::ColumnRef {
            name: "score".to_string(),
            index: 1,
        }),
        partition_by: vec![],
        order_by: vec![WindowOrderExpr {
            expr: Expr::ColumnRef {
                name: "ord".to_string(),
                index: 0,
            },
            asc: true,
            nulls_first: false,
        }],
        frame: Some(WindowFrameSpec {
            units: WindowFrameUnits::Rows,
            start_bound: WindowFrameBound::UnboundedPreceding,
            end_bound: WindowFrameBound::CurrentRow,
            exclusion: WindowFrameExclusion::NoOthers,
        }),
        output_name: "running_sum".to_string(),
    };
    let spill_dir = std::env::temp_dir().join(format!(
        "ffq_window_spill_test_{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    let ctx = QueryContext {
        batch_size_rows: 512,
        mem_budget_bytes: 256,
        broadcast_threshold_bytes: u64::MAX,
        join_radix_bits: 8,
        spill_dir: spill_dir.to_string_lossy().into_owned(),
        stats_collector: None,
    };
    let trace = TraceIds {
        query_id: "window-spill-test".to_string(),
        stage_id: 7,
        task_id: 9,
    };
    let out = run_window_exec_with_ctx(input, &[w], &ctx, Some(&trace)).expect("window with spill");
    let arr = out.batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .expect("running sum");
    assert_eq!(arr.len(), n as usize);
    assert!(arr.value(arr.len() - 1) > 0.0);

    let leftover = fs::read_dir(&ctx.spill_dir)
        .ok()
        .into_iter()
        .flat_map(|it| it.filter_map(|e| e.ok()))
        .filter(|e| e.file_name().to_string_lossy().contains("window_spill_q"))
        .count();
    assert_eq!(leftover, 0, "window spill files must be cleaned up");
    let _ = fs::remove_dir_all(&ctx.spill_dir);
}

#[test]
fn materialized_cte_ref_executes_shared_subplan_once() {
    let tmp = std::env::temp_dir().join(format!(
        "ffq_runtime_cte_ref_{}.parquet",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))],
    )
    .expect("batch");
    let file = File::create(&tmp).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, schema.clone(), None).expect("writer");
    writer.write(&batch).expect("write");
    writer.close().expect("close");

    let mut catalog = Catalog::new();
    catalog.register_table(TableDef {
        name: "t".to_string(),
        uri: tmp.to_string_lossy().into_owned(),
        paths: Vec::new(),
        format: "parquet".to_string(),
        schema: Some((*schema).clone()),
        stats: TableStats::default(),
        options: HashMap::new(),
    });
    let catalog = Arc::new(catalog);

    let calls = Arc::new(AtomicUsize::new(0));
    let registry = Arc::new(PhysicalOperatorRegistry::default());
    assert!(!registry.register(Arc::new(CountingFactory {
        calls: Arc::clone(&calls),
    })));

    let shared = PhysicalPlan::Custom(CustomExec {
        op_name: "counting_passthrough".to_string(),
        config: HashMap::new(),
        input: Box::new(PhysicalPlan::ParquetScan(ParquetScanExec {
            table: "t".to_string(),
            schema: None,
            projection: None,
            filters: Vec::new(),
        })),
    });
    let plan = PhysicalPlan::UnionAll(UnionAllExec {
        left: Box::new(PhysicalPlan::CteRef(CteRefExec {
            name: "shared_cte".to_string(),
            plan: Box::new(shared.clone()),
        })),
        right: Box::new(PhysicalPlan::CteRef(CteRefExec {
            name: "shared_cte".to_string(),
            plan: Box::new(shared),
        })),
    });

    let runtime = EmbeddedRuntime::new();
    let stream = futures::executor::block_on(runtime.execute(
        plan,
        QueryContext {
            batch_size_rows: 1024,
            mem_budget_bytes: 64 * 1024 * 1024,
            broadcast_threshold_bytes: u64::MAX,
            join_radix_bits: 8,
            spill_dir: "./ffq_spill_test".to_string(),
            stats_collector: None,
        },
        Arc::clone(&catalog),
        Arc::clone(&registry),
    ))
    .expect("execute");
    let batches =
        futures::executor::block_on(stream.try_collect::<Vec<RecordBatch>>()).expect("collect");
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 6);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "shared CTE subplan should execute exactly once"
    );
    let _ = std::fs::remove_file(tmp);
}

#[test]
fn embedded_adaptive_partitioning_matches_shared_planner_on_same_stats() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 2, 3, 4, 5, 6, 7, 8])),
            Arc::new(Int64Array::from(vec![10_i64, 20, 30, 40, 50, 60, 70, 80])),
        ],
    )
    .expect("batch");
    let input = ExecOutput {
        schema: schema.clone(),
        batches: vec![batch],
    };
    let partitioning = PartitioningSpec::HashKeys {
        keys: vec!["k".to_string()],
        partitions: 4,
    };
    let target_bytes = 32_u64;
    let embedded =
        embedded_adaptive_plan_for_partitioning_with_target(&input, &partitioning, target_bytes)
            .expect("embedded adaptive plan");

    let rows = rows_from_batches(&input).expect("rows");
    let key_idx = resolve_key_indexes(&schema, &["k".to_string()]).expect("key idx");
    let mut bytes_by_partition = HashMap::<u32, u64>::new();
    for row in &rows {
        let key = join_key_from_row(row, &key_idx);
        let partition = (hash_key(&key) % 4) as u32;
        let row_bytes = row
            .iter()
            .map(|v| scalar_estimate_bytes(v) as u64)
            .sum::<u64>();
        bytes_by_partition
            .entry(partition)
            .and_modify(|b| *b = b.saturating_add(row_bytes))
            .or_insert(row_bytes);
    }
    let shared = plan_adaptive_reduce_layout(4, target_bytes, &bytes_by_partition, 1, 0, 0);
    assert_eq!(embedded.assignments, shared.assignments);
    assert_eq!(embedded.adaptive_reduce_tasks, shared.adaptive_reduce_tasks);
    assert_eq!(
        embedded.partition_bytes_histogram,
        shared.partition_bytes_histogram
    );
}

#[cfg(feature = "vector")]
fn sample_vector_output() -> ExecOutput {
    let mut emb_builder = FixedSizeListBuilder::new(Float32Builder::new(), 3);
    let rows = [
        [1.0_f32, 0.0, 0.0], // id 10
        [2.0_f32, 0.0, 0.0], // id 20 (cosine tie with id 10 vs [1,0,0])
        [0.0_f32, 1.0, 0.0], // id 30
    ];
    for v in rows {
        for x in v {
            emb_builder.values().append_value(x);
        }
        emb_builder.append(true);
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "emb",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 3),
            true,
        ),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![10_i64, 20, 30])),
            Arc::new(emb_builder.finish()),
        ],
    )
    .expect("batch");
    ExecOutput {
        schema,
        batches: vec![batch],
    }
}

#[cfg(feature = "vector")]
fn collect_ids(out: &ExecOutput) -> Vec<i64> {
    out.batches
        .iter()
        .flat_map(|b| {
            let ids = b
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id array");
            (0..b.num_rows()).map(|i| ids.value(i)).collect::<Vec<_>>()
        })
        .collect()
}

#[cfg(feature = "vector")]
fn collect_scores(out: &ExecOutput) -> Vec<f32> {
    let mut scores = Vec::new();
    for b in &out.batches {
        // rank tests below project full row, so score is computed from emb; we re-evaluate by query expr output not stored.
        let emb = b
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::FixedSizeListArray>()
            .expect("emb list");
        let vals = emb
            .values()
            .as_any()
            .downcast_ref::<Float32Array>()
            .expect("emb values");
        for row in 0..b.num_rows() {
            scores.push(vals.value(row * 3));
        }
    }
    scores
}

#[cfg(feature = "vector")]
#[test]
fn topk_by_score_cosine_ranking_tie_is_deterministic() {
    let input = sample_vector_output();
    let expr = Expr::CosineSimilarity {
        vector: Box::new(Expr::Column("emb".to_string())),
        query: Box::new(Expr::Literal(LiteralValue::VectorF32(vec![1.0, 0.0, 0.0]))),
    };
    let out = run_topk_by_score(input, expr, 2).expect("topk");
    // tie between id=10 and id=20; implementation is deterministic and keeps later row first
    assert_eq!(collect_ids(&out), vec![20, 10]);
}

#[cfg(feature = "vector")]
#[test]
fn topk_by_score_l2_ranking_order_matches_expected() {
    let input = sample_vector_output();
    let expr = Expr::L2Distance {
        vector: Box::new(Expr::Column("emb".to_string())),
        query: Box::new(Expr::Literal(LiteralValue::VectorF32(vec![1.0, 0.0, 0.0]))),
    };
    // TopKByScore is descending, so largest L2 distance first.
    let out = run_topk_by_score(input, expr, 3).expect("topk");
    assert_eq!(collect_ids(&out), vec![30, 20, 10]);
}

#[cfg(feature = "vector")]
#[test]
fn topk_by_score_dot_ranking_order_matches_expected() {
    let input = sample_vector_output();
    let expr = Expr::DotProduct {
        vector: Box::new(Expr::Column("emb".to_string())),
        query: Box::new(Expr::Literal(LiteralValue::VectorF32(vec![1.0, 0.0, 0.0]))),
    };
    let out = run_topk_by_score(input, expr, 3).expect("topk");
    assert_eq!(collect_ids(&out), vec![20, 10, 30]);
    let first_component_scores = collect_scores(&out);
    assert_eq!(first_component_scores, vec![2.0, 1.0, 0.0]);
}
