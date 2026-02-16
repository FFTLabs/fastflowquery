use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::collections::{hash_map::DefaultHasher, HashMap};
use std::fmt::Debug;
use std::fs::{self, File};
use std::hash::{Hash, Hasher};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use arrow::array::{
    Array, ArrayRef, BooleanBuilder, FixedSizeListBuilder, Float32Builder, Float64Builder,
    Int64Array, Int64Builder, StringBuilder,
};
use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use ffq_common::metrics::global_metrics;
use ffq_common::{FfqError, Result};
use ffq_execution::{compile_expr, SendableRecordBatchStream, StreamAdapter, TaskContext};
use ffq_planner::{AggExpr, BuildSide, ExchangeExec, Expr, PhysicalPlan};
use ffq_storage::parquet_provider::ParquetProvider;
#[cfg(feature = "qdrant")]
use ffq_storage::qdrant_provider::QdrantProvider;
#[cfg(any(feature = "qdrant", test))]
use ffq_storage::vector_index::VectorIndexProvider;
use ffq_storage::{Catalog, StorageProvider};
use futures::future::BoxFuture;
use futures::{FutureExt, TryStreamExt};
use parquet::arrow::ArrowWriter;
use serde::{Deserialize, Serialize};
#[cfg(feature = "distributed")]
use tracing::{debug, error};
use tracing::{info, info_span, Instrument};

#[derive(Debug, Clone)]
pub struct QueryContext {
    pub batch_size_rows: usize,
    pub mem_budget_bytes: usize,
    pub spill_dir: String,
}

/// Runtime = something that can execute a PhysicalPlan and return a stream of RecordBatches.
pub trait Runtime: Send + Sync + Debug {
    fn execute(
        &self,
        plan: PhysicalPlan,
        ctx: QueryContext,
        catalog: Arc<Catalog>,
    ) -> BoxFuture<'static, Result<SendableRecordBatchStream>>;

    fn shutdown(&self) -> BoxFuture<'static, Result<()>> {
        async { Ok(()) }.boxed()
    }
}

#[derive(Debug, Default)]
pub struct EmbeddedRuntime;

impl EmbeddedRuntime {
    pub fn new() -> Self {
        Self
    }
}

impl Runtime for EmbeddedRuntime {
    fn execute(
        &self,
        plan: PhysicalPlan,
        ctx: QueryContext,
        catalog: Arc<Catalog>,
    ) -> BoxFuture<'static, Result<SendableRecordBatchStream>> {
        async move {
            let trace = Arc::new(TraceIds {
                query_id: local_query_id()?,
                stage_id: 0,
                task_id: 0,
            });
            info!(
                query_id = %trace.query_id,
                stage_id = trace.stage_id,
                task_id = trace.task_id,
                mode = "embedded",
                "query execution started"
            );
            let exec = execute_plan(plan, ctx, catalog, Arc::clone(&trace)).await?;
            info!(
                query_id = %trace.query_id,
                stage_id = trace.stage_id,
                task_id = trace.task_id,
                rows = exec.batches.iter().map(|b| b.num_rows()).sum::<usize>(),
                batches = exec.batches.len(),
                "query execution completed"
            );
            let stream = futures::stream::iter(exec.batches.into_iter().map(Ok));
            Ok(Box::pin(StreamAdapter::new(exec.schema, stream)) as SendableRecordBatchStream)
        }
        .boxed()
    }
}

struct ExecOutput {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

#[derive(Debug, Clone)]
struct TraceIds {
    query_id: String,
    stage_id: u64,
    task_id: u64,
}

fn execute_plan(
    plan: PhysicalPlan,
    ctx: QueryContext,
    catalog: Arc<Catalog>,
    trace: Arc<TraceIds>,
) -> BoxFuture<'static, Result<ExecOutput>> {
    let operator = operator_name(&plan);
    let span = info_span!(
        "operator_execute",
        query_id = %trace.query_id,
        stage_id = trace.stage_id,
        task_id = trace.task_id,
        operator = operator
    );
    async move {
        let started = Instant::now();
        let eval = match plan {
            PhysicalPlan::ParquetScan(scan) => {
                let table = catalog.get(&scan.table)?.clone();
                let provider = ParquetProvider::new();
                let node = provider.scan(
                    &table,
                    scan.projection,
                    scan.filters.into_iter().map(|f| format!("{f:?}")).collect(),
                )?;
                let stream = node.execute(Arc::new(TaskContext {
                    batch_size_rows: ctx.batch_size_rows,
                    mem_budget_bytes: ctx.mem_budget_bytes,
                }))?;
                let schema = stream.schema();
                let batches = stream.try_collect::<Vec<RecordBatch>>().await?;
                Ok(OpEval {
                    out: ExecOutput { schema, batches },
                    in_rows: 0,
                    in_batches: 0,
                    in_bytes: 0,
                })
            }
            PhysicalPlan::ParquetWrite(write) => {
                let child =
                    execute_plan(*write.input, ctx, catalog.clone(), Arc::clone(&trace)).await?;
                let table = catalog.get(&write.table)?.clone();
                write_parquet_sink(&table, &child)?;
                let (in_rows, in_batches, in_bytes) = batch_stats(&child.batches);
                Ok(OpEval {
                    out: ExecOutput {
                        schema: Arc::new(Schema::empty()),
                        batches: Vec::new(),
                    },
                    in_rows,
                    in_batches,
                    in_bytes,
                })
            }
            PhysicalPlan::Project(project) => {
                let child = execute_plan(*project.input, ctx, catalog, Arc::clone(&trace)).await?;
                let mut out_batches = Vec::with_capacity(child.batches.len());
                let schema = Arc::new(Schema::new(
                    project
                        .exprs
                        .iter()
                        .map(|(expr, name)| {
                            let dt = compile_expr(expr, &child.schema)?.data_type();
                            Ok(Field::new(name, dt, true))
                        })
                        .collect::<Result<Vec<_>>>()?,
                ));
                for batch in &child.batches {
                    let cols = project
                        .exprs
                        .iter()
                        .map(|(expr, _)| compile_expr(expr, &child.schema)?.evaluate(batch))
                        .collect::<Result<Vec<_>>>()?;
                    out_batches.push(RecordBatch::try_new(schema.clone(), cols).map_err(|e| {
                        FfqError::Execution(format!("project build batch failed: {e}"))
                    })?);
                }
                let (in_rows, in_batches, in_bytes) = batch_stats(&child.batches);
                Ok(OpEval {
                    out: ExecOutput {
                        schema,
                        batches: out_batches,
                    },
                    in_rows,
                    in_batches,
                    in_bytes,
                })
            }
            PhysicalPlan::Filter(filter) => {
                let child = execute_plan(*filter.input, ctx, catalog, Arc::clone(&trace)).await?;
                let pred = compile_expr(&filter.predicate, &child.schema)?;
                let mut out = Vec::new();
                for batch in &child.batches {
                    let mask = pred.evaluate(batch)?;
                    let mask = mask
                        .as_any()
                        .downcast_ref::<arrow::array::BooleanArray>()
                        .ok_or_else(|| {
                            FfqError::Execution(
                                "filter predicate must evaluate to boolean".to_string(),
                            )
                        })?;
                    let filtered = arrow::compute::filter_record_batch(batch, mask)
                        .map_err(|e| FfqError::Execution(format!("filter batch failed: {e}")))?;
                    out.push(filtered);
                }
                let (in_rows, in_batches, in_bytes) = batch_stats(&child.batches);
                Ok(OpEval {
                    out: ExecOutput {
                        schema: child.schema,
                        batches: out,
                    },
                    in_rows,
                    in_batches,
                    in_bytes,
                })
            }
            PhysicalPlan::Limit(limit) => {
                let child = execute_plan(*limit.input, ctx, catalog, Arc::clone(&trace)).await?;
                let mut out = Vec::new();
                let mut remaining = limit.n;
                for batch in &child.batches {
                    if remaining == 0 {
                        break;
                    }
                    let take = remaining.min(batch.num_rows());
                    out.push(batch.slice(0, take));
                    remaining -= take;
                }
                let (in_rows, in_batches, in_bytes) = batch_stats(&child.batches);
                Ok(OpEval {
                    out: ExecOutput {
                        schema: child.schema,
                        batches: out,
                    },
                    in_rows,
                    in_batches,
                    in_bytes,
                })
            }
            PhysicalPlan::TopKByScore(topk) => {
                let child = execute_plan(*topk.input, ctx, catalog, Arc::clone(&trace)).await?;
                let (in_rows, in_batches, in_bytes) = batch_stats(&child.batches);
                Ok(OpEval {
                    out: run_topk_by_score(child, topk.score_expr, topk.k)?,
                    in_rows,
                    in_batches,
                    in_bytes,
                })
            }
            PhysicalPlan::VectorTopK(exec) => Ok(OpEval {
                out: execute_vector_topk(exec, catalog).await?,
                in_rows: 0,
                in_batches: 0,
                in_bytes: 0,
            }),
            PhysicalPlan::Exchange(exchange) => match exchange {
                ExchangeExec::ShuffleWrite(x) => {
                    let child = execute_plan(*x.input, ctx, catalog, Arc::clone(&trace)).await?;
                    let (in_rows, in_batches, in_bytes) = batch_stats(&child.batches);
                    Ok(OpEval {
                        out: child,
                        in_rows,
                        in_batches,
                        in_bytes,
                    })
                }
                ExchangeExec::ShuffleRead(x) => {
                    let child = execute_plan(*x.input, ctx, catalog, Arc::clone(&trace)).await?;
                    let (in_rows, in_batches, in_bytes) = batch_stats(&child.batches);
                    Ok(OpEval {
                        out: child,
                        in_rows,
                        in_batches,
                        in_bytes,
                    })
                }
                ExchangeExec::Broadcast(x) => {
                    let child = execute_plan(*x.input, ctx, catalog, Arc::clone(&trace)).await?;
                    let (in_rows, in_batches, in_bytes) = batch_stats(&child.batches);
                    Ok(OpEval {
                        out: child,
                        in_rows,
                        in_batches,
                        in_bytes,
                    })
                }
            },
            PhysicalPlan::PartialHashAggregate(agg) => {
                let child =
                    execute_plan(*agg.input, ctx.clone(), catalog, Arc::clone(&trace)).await?;
                let (in_rows, in_batches, in_bytes) = batch_stats(&child.batches);
                Ok(OpEval {
                    out: run_hash_aggregate(
                        child,
                        agg.group_exprs,
                        agg.aggr_exprs,
                        AggregateMode::Partial,
                        &ctx,
                        &trace,
                    )?,
                    in_rows,
                    in_batches,
                    in_bytes,
                })
            }
            PhysicalPlan::FinalHashAggregate(agg) => {
                let child =
                    execute_plan(*agg.input, ctx.clone(), catalog, Arc::clone(&trace)).await?;
                let (in_rows, in_batches, in_bytes) = batch_stats(&child.batches);
                Ok(OpEval {
                    out: run_hash_aggregate(
                        child,
                        agg.group_exprs,
                        agg.aggr_exprs,
                        AggregateMode::Final,
                        &ctx,
                        &trace,
                    )?,
                    in_rows,
                    in_batches,
                    in_bytes,
                })
            }
            PhysicalPlan::HashJoin(join) => {
                let ffq_planner::HashJoinExec {
                    left: left_plan,
                    right: right_plan,
                    on,
                    build_side,
                    ..
                } = join;
                let left =
                    execute_plan(*left_plan, ctx.clone(), catalog.clone(), Arc::clone(&trace))
                        .await?;
                let right =
                    execute_plan(*right_plan, ctx.clone(), catalog, Arc::clone(&trace)).await?;
                let (l_rows, l_batches, l_bytes) = batch_stats(&left.batches);
                let (r_rows, r_batches, r_bytes) = batch_stats(&right.batches);
                Ok(OpEval {
                    out: run_hash_join(left, right, on, build_side, &ctx, &trace)?,
                    in_rows: l_rows + r_rows,
                    in_batches: l_batches + r_batches,
                    in_bytes: l_bytes + r_bytes,
                })
            }
            other => Err(FfqError::Unsupported(format!(
                "embedded runtime does not support operator yet: {other:?}"
            ))),
        }?;
        let (out_rows, out_batches, out_bytes) = batch_stats(&eval.out.batches);
        global_metrics().record_operator(
            &trace.query_id,
            trace.stage_id,
            trace.task_id,
            operator,
            eval.in_rows,
            out_rows,
            eval.in_batches,
            out_batches,
            eval.in_bytes,
            out_bytes,
            started.elapsed().as_secs_f64(),
        );
        Ok(eval.out)
    }
    .instrument(span)
    .boxed()
}

struct OpEval {
    out: ExecOutput,
    in_rows: u64,
    in_batches: u64,
    in_bytes: u64,
}

fn batch_stats(batches: &[RecordBatch]) -> (u64, u64, u64) {
    let rows = batches.iter().map(|b| b.num_rows() as u64).sum::<u64>();
    let batch_count = batches.len() as u64;
    let bytes = batches
        .iter()
        .map(|b| {
            b.columns()
                .iter()
                .map(|a| a.get_array_memory_size() as u64)
                .sum::<u64>()
        })
        .sum::<u64>();
    (rows, batch_count, bytes)
}

fn operator_name(plan: &PhysicalPlan) -> &'static str {
    match plan {
        PhysicalPlan::ParquetScan(_) => "ParquetScan",
        PhysicalPlan::ParquetWrite(_) => "ParquetWrite",
        PhysicalPlan::Filter(_) => "Filter",
        PhysicalPlan::Project(_) => "Project",
        PhysicalPlan::CoalesceBatches(_) => "CoalesceBatches",
        PhysicalPlan::PartialHashAggregate(_) => "PartialHashAggregate",
        PhysicalPlan::FinalHashAggregate(_) => "FinalHashAggregate",
        PhysicalPlan::HashJoin(_) => "HashJoin",
        PhysicalPlan::Exchange(ExchangeExec::ShuffleWrite(_)) => "ShuffleWrite",
        PhysicalPlan::Exchange(ExchangeExec::ShuffleRead(_)) => "ShuffleRead",
        PhysicalPlan::Exchange(ExchangeExec::Broadcast(_)) => "Broadcast",
        PhysicalPlan::Limit(_) => "Limit",
        PhysicalPlan::TopKByScore(_) => "TopKByScore",
        PhysicalPlan::VectorTopK(_) => "VectorTopK",
    }
}

fn local_query_id() -> Result<String> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| FfqError::Execution(format!("clock error: {e}")))?
        .as_nanos();
    Ok(format!("local-{nanos}"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AggregateMode {
    Partial,
    Final,
}

#[derive(Debug, Clone)]
struct AggSpec {
    expr: AggExpr,
    name: String,
    out_type: DataType,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum ScalarValue {
    Int64(i64),
    Float64Bits(u64),
    VectorF32Bits(Vec<u32>),
    Utf8(String),
    Boolean(bool),
    Null,
}

impl Hash for ScalarValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Self::Int64(v) => {
                0_u8.hash(state);
                v.hash(state);
            }
            Self::Float64Bits(v) => {
                1_u8.hash(state);
                v.hash(state);
            }
            Self::VectorF32Bits(v) => {
                2_u8.hash(state);
                v.len().hash(state);
                for x in v {
                    x.hash(state);
                }
            }
            Self::Utf8(v) => {
                3_u8.hash(state);
                v.hash(state);
            }
            Self::Boolean(v) => {
                4_u8.hash(state);
                v.hash(state);
            }
            Self::Null => 5_u8.hash(state),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum AggState {
    Count(i64),
    SumInt(i64),
    SumFloat(f64),
    Min(Option<ScalarValue>),
    Max(Option<ScalarValue>),
    Avg { sum: f64, count: i64 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SpillRow {
    key: Vec<ScalarValue>,
    states: Vec<AggState>,
}

#[derive(Debug, Clone)]
struct TopKEntry {
    score: f64,
    batch_idx: usize,
    row_idx: usize,
    seq: usize,
}

impl PartialEq for TopKEntry {
    fn eq(&self, other: &Self) -> bool {
        self.score.to_bits() == other.score.to_bits() && self.seq == other.seq
    }
}
impl Eq for TopKEntry {}
impl PartialOrd for TopKEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for TopKEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.score
            .total_cmp(&other.score)
            .then_with(|| self.seq.cmp(&other.seq))
    }
}

#[cfg_attr(feature = "profiling", inline(never))]
fn run_topk_by_score(child: ExecOutput, score_expr: Expr, k: usize) -> Result<ExecOutput> {
    #[cfg(feature = "profiling")]
    let _profile_span = info_span!("profile_topk_by_score").entered();
    if k == 0 {
        return Ok(ExecOutput {
            schema: child.schema.clone(),
            batches: vec![RecordBatch::new_empty(child.schema)],
        });
    }

    let score_eval = compile_expr(&score_expr, &child.schema)?;
    let mut heap: BinaryHeap<Reverse<TopKEntry>> = BinaryHeap::new();
    let mut seq = 0usize;

    for (batch_idx, batch) in child.batches.iter().enumerate() {
        let score_arr = score_eval.evaluate(batch)?;
        for row_idx in 0..batch.num_rows() {
            let score = score_at(&score_arr, row_idx)?;
            if let Some(score) = score {
                let entry = Reverse(TopKEntry {
                    score,
                    batch_idx,
                    row_idx,
                    seq,
                });
                seq += 1;
                if heap.len() < k {
                    heap.push(entry);
                } else if let Some(min) = heap.peek() {
                    if entry.0 > min.0 {
                        let _ = heap.pop();
                        heap.push(entry);
                    }
                }
            }
        }
    }

    let mut picked = heap.into_vec();
    picked.sort_by(|a, b| b.0.cmp(&a.0));

    if picked.is_empty() {
        return Ok(ExecOutput {
            schema: child.schema.clone(),
            batches: vec![RecordBatch::new_empty(child.schema)],
        });
    }

    let mut one_row_batches = Vec::with_capacity(picked.len());
    for Reverse(e) in picked {
        one_row_batches.push(child.batches[e.batch_idx].slice(e.row_idx, 1));
    }
    let out = concat_batches(&child.schema, &one_row_batches)
        .map_err(|e| FfqError::Execution(format!("top-k concat failed: {e}")))?;
    Ok(ExecOutput {
        schema: child.schema,
        batches: vec![out],
    })
}

fn score_at(arr: &ArrayRef, idx: usize) -> Result<Option<f64>> {
    if arr.is_null(idx) {
        return Ok(None);
    }
    if let Some(a) = arr.as_any().downcast_ref::<arrow::array::Float32Array>() {
        return Ok(Some(a.value(idx) as f64));
    }
    if let Some(a) = arr.as_any().downcast_ref::<arrow::array::Float64Array>() {
        return Ok(Some(a.value(idx)));
    }
    Err(FfqError::Execution(format!(
        "top-k score expression must evaluate to Float32/Float64, got {:?}",
        arr.data_type()
    )))
}

fn execute_vector_topk(
    exec: ffq_planner::VectorTopKExec,
    catalog: Arc<Catalog>,
) -> BoxFuture<'static, Result<ExecOutput>> {
    async move {
        let table = catalog.get(&exec.table)?.clone();
        if let Some(rows) = mock_vector_rows_from_table(&table, exec.k)? {
            return rows_to_vector_topk_output(rows);
        }
        if table.format != "qdrant" {
            return Err(FfqError::Unsupported(format!(
                "VectorTopKExec requires table format='qdrant', got '{}'",
                table.format
            )));
        }
        #[cfg(not(feature = "qdrant"))]
        {
            let _ = table;
            let _ = exec;
            return Err(FfqError::Unsupported(
                "qdrant feature is disabled; build ffq-client with --features qdrant".to_string(),
            ));
        }
        #[cfg(feature = "qdrant")]
        {
            let provider = QdrantProvider::from_table(&table)?;
            run_vector_topk_with_provider(&exec, &provider).await
        }
    }
    .boxed()
}

#[cfg(any(feature = "qdrant", test))]
async fn run_vector_topk_with_provider(
    exec: &ffq_planner::VectorTopKExec,
    provider: &dyn VectorIndexProvider,
) -> Result<ExecOutput> {
    let rows = provider
        .topk(exec.query_vector.clone(), exec.k, exec.filter.clone())
        .await?;
    rows_to_vector_topk_output(rows)
}

#[derive(Debug, Clone, serde::Deserialize)]
struct MockVectorRow {
    id: i64,
    score: f32,
    #[serde(default)]
    payload: Option<String>,
}

fn mock_vector_rows_from_table(
    table: &ffq_storage::TableDef,
    k: usize,
) -> Result<Option<Vec<ffq_storage::vector_index::VectorTopKRow>>> {
    let Some(raw) = table.options.get("vector.mock_rows_json") else {
        return Ok(None);
    };
    let mut rows: Vec<MockVectorRow> = serde_json::from_str(raw).map_err(|e| {
        FfqError::Execution(format!(
            "invalid vector.mock_rows_json for table '{}': {e}",
            table.name
        ))
    })?;
    rows.sort_by(|a, b| b.score.total_cmp(&a.score));
    rows.truncate(k);
    Ok(Some(
        rows.into_iter()
            .map(|r| ffq_storage::vector_index::VectorTopKRow {
                id: r.id,
                score: r.score,
                payload_json: r.payload,
            })
            .collect(),
    ))
}

#[allow(dead_code)]
fn rows_to_vector_topk_output(
    rows: Vec<ffq_storage::vector_index::VectorTopKRow>,
) -> Result<ExecOutput> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("score", DataType::Float32, false),
        Field::new("payload", DataType::Utf8, true),
    ]));
    let mut id_b = Int64Builder::with_capacity(rows.len());
    let mut score_b = arrow::array::Float32Builder::with_capacity(rows.len());
    let mut payload_b = StringBuilder::with_capacity(rows.len(), rows.len() * 16);
    for row in rows {
        id_b.append_value(row.id);
        score_b.append_value(row.score);
        if let Some(p) = row.payload_json {
            payload_b.append_value(p);
        } else {
            payload_b.append_null();
        }
    }
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(id_b.finish()),
            Arc::new(score_b.finish()),
            Arc::new(payload_b.finish()),
        ],
    )
    .map_err(|e| FfqError::Execution(format!("build VectorTopK record batch failed: {e}")))?;
    Ok(ExecOutput {
        schema,
        batches: vec![batch],
    })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct JoinSpillRow {
    key: Vec<ScalarValue>,
    row: Vec<ScalarValue>,
}

#[derive(Debug, Clone, Copy)]
enum JoinInputSide {
    Left,
    Right,
}

#[derive(Debug, Clone, Copy)]
enum JoinExecSide {
    Build,
    Probe,
}

#[cfg_attr(feature = "profiling", inline(never))]
fn run_hash_join(
    left: ExecOutput,
    right: ExecOutput,
    on: Vec<(String, String)>,
    build_side: BuildSide,
    ctx: &QueryContext,
    trace: &TraceIds,
) -> Result<ExecOutput> {
    #[cfg(feature = "profiling")]
    let _profile_span = info_span!(
        "profile_hash_join",
        query_id = %trace.query_id,
        stage_id = trace.stage_id,
        task_id = trace.task_id
    )
    .entered();
    let left_rows = rows_from_batches(&left)?;
    let right_rows = rows_from_batches(&right)?;

    let (build_rows, probe_rows, build_schema, probe_schema, build_input_side) = match build_side {
        BuildSide::Left => (
            &left_rows,
            &right_rows,
            left.schema.clone(),
            right.schema.clone(),
            JoinInputSide::Left,
        ),
        BuildSide::Right => (
            &right_rows,
            &left_rows,
            right.schema.clone(),
            left.schema.clone(),
            JoinInputSide::Right,
        ),
    };

    let build_key_names = join_key_names(&on, build_input_side, JoinExecSide::Build);
    let probe_key_names = join_key_names(&on, build_input_side, JoinExecSide::Probe);

    let build_key_idx = resolve_key_indexes(&build_schema, &build_key_names)?;
    let probe_key_idx = resolve_key_indexes(&probe_schema, &probe_key_names)?;

    let output_schema = Arc::new(Schema::new(
        left.schema
            .fields()
            .iter()
            .chain(right.schema.fields().iter())
            .map(|f| (**f).clone())
            .collect::<Vec<_>>(),
    ));

    let joined_rows = if ctx.mem_budget_bytes > 0
        && estimate_join_rows_bytes(build_rows) > ctx.mem_budget_bytes
    {
        grace_hash_join(
            build_rows,
            probe_rows,
            &build_key_idx,
            &probe_key_idx,
            build_input_side,
            ctx,
            trace,
        )?
    } else {
        in_memory_hash_join(
            build_rows,
            probe_rows,
            &build_key_idx,
            &probe_key_idx,
            build_input_side,
        )
    };

    let batch = rows_to_batch(&output_schema, &joined_rows)?;
    Ok(ExecOutput {
        schema: output_schema,
        batches: vec![batch],
    })
}

fn rows_from_batches(input: &ExecOutput) -> Result<Vec<Vec<ScalarValue>>> {
    let mut out = Vec::new();
    for batch in &input.batches {
        for row in 0..batch.num_rows() {
            let mut values = Vec::with_capacity(batch.num_columns());
            for col in 0..batch.num_columns() {
                values.push(scalar_from_array(batch.column(col), row)?);
            }
            out.push(values);
        }
    }
    Ok(out)
}

fn rows_to_batch(schema: &SchemaRef, rows: &[Vec<ScalarValue>]) -> Result<RecordBatch> {
    let mut cols = vec![Vec::<ScalarValue>::with_capacity(rows.len()); schema.fields().len()];
    for row in rows {
        for (idx, value) in row.iter().enumerate() {
            cols[idx].push(value.clone());
        }
    }
    let arrays = cols
        .iter()
        .enumerate()
        .map(|(idx, col)| scalars_to_array(col, schema.field(idx).data_type()))
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(schema.clone(), arrays)
        .map_err(|e| FfqError::Execution(format!("join output batch failed: {e}")))
}

fn join_key_names(
    on: &[(String, String)],
    build_side: JoinInputSide,
    exec_side: JoinExecSide,
) -> Vec<String> {
    let use_left = match (build_side, exec_side) {
        (JoinInputSide::Left, JoinExecSide::Build) => true,
        (JoinInputSide::Left, JoinExecSide::Probe) => false,
        (JoinInputSide::Right, JoinExecSide::Build) => false,
        (JoinInputSide::Right, JoinExecSide::Probe) => true,
    };
    on.iter()
        .map(|(l, r)| if use_left { l.clone() } else { r.clone() })
        .collect()
}

fn resolve_key_indexes(schema: &SchemaRef, names: &[String]) -> Result<Vec<usize>> {
    names
        .iter()
        .map(|name| {
            let direct = schema.index_of(name);
            match direct {
                Ok(idx) => Ok(idx),
                Err(_) => {
                    let short = strip_qual(name);
                    schema.index_of(&short).map_err(|e| {
                        FfqError::Execution(format!("join key '{name}' not found in schema: {e}"))
                    })
                }
            }
        })
        .collect()
}

fn strip_qual(name: &str) -> String {
    name.rsplit('.').next().unwrap_or(name).to_string()
}

fn join_key_from_row(row: &[ScalarValue], idxs: &[usize]) -> Vec<ScalarValue> {
    idxs.iter().map(|i| row[*i].clone()).collect()
}

fn in_memory_hash_join(
    build_rows: &[Vec<ScalarValue>],
    probe_rows: &[Vec<ScalarValue>],
    build_key_idx: &[usize],
    probe_key_idx: &[usize],
    build_side: JoinInputSide,
) -> Vec<Vec<ScalarValue>> {
    let mut ht: HashMap<Vec<ScalarValue>, Vec<usize>> = HashMap::new();
    for (idx, row) in build_rows.iter().enumerate() {
        ht.entry(join_key_from_row(row, build_key_idx))
            .or_default()
            .push(idx);
    }

    let mut out = Vec::new();
    for probe in probe_rows {
        let probe_key = join_key_from_row(probe, probe_key_idx);
        if let Some(build_matches) = ht.get(&probe_key) {
            for build_idx in build_matches {
                let build = &build_rows[*build_idx];
                out.push(combine_join_rows(build, probe, build_side));
            }
        }
    }
    out
}

fn combine_join_rows(
    build: &[ScalarValue],
    probe: &[ScalarValue],
    build_side: JoinInputSide,
) -> Vec<ScalarValue> {
    match build_side {
        JoinInputSide::Left => build.iter().cloned().chain(probe.iter().cloned()).collect(),
        JoinInputSide::Right => probe.iter().cloned().chain(build.iter().cloned()).collect(),
    }
}

fn estimate_join_rows_bytes(rows: &[Vec<ScalarValue>]) -> usize {
    rows.iter()
        .map(|r| 64 + r.iter().map(scalar_estimate_bytes).sum::<usize>())
        .sum()
}

#[cfg_attr(feature = "profiling", inline(never))]
fn grace_hash_join(
    build_rows: &[Vec<ScalarValue>],
    probe_rows: &[Vec<ScalarValue>],
    build_key_idx: &[usize],
    probe_key_idx: &[usize],
    build_side: JoinInputSide,
    ctx: &QueryContext,
    trace: &TraceIds,
) -> Result<Vec<Vec<ScalarValue>>> {
    #[cfg(feature = "profiling")]
    let _profile_span = info_span!(
        "profile_grace_hash_join",
        query_id = %trace.query_id,
        stage_id = trace.stage_id,
        task_id = trace.task_id
    )
    .entered();
    let spill_started = Instant::now();
    fs::create_dir_all(&ctx.spill_dir)?;
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| FfqError::Execution(format!("clock error: {e}")))?
        .as_nanos();
    let parts = 16_usize;

    let build_paths = (0..parts)
        .map(|p| PathBuf::from(&ctx.spill_dir).join(format!("join_build_{suffix}_{p}.jsonl")))
        .collect::<Vec<_>>();
    let probe_paths = (0..parts)
        .map(|p| PathBuf::from(&ctx.spill_dir).join(format!("join_probe_{suffix}_{p}.jsonl")))
        .collect::<Vec<_>>();

    spill_join_partitions(build_rows, build_key_idx, &build_paths)?;
    spill_join_partitions(probe_rows, probe_key_idx, &probe_paths)?;
    let spill_bytes = estimate_join_rows_bytes(build_rows) + estimate_join_rows_bytes(probe_rows);
    global_metrics().record_spill(
        &trace.query_id,
        trace.stage_id,
        trace.task_id,
        "join",
        spill_bytes as u64,
        spill_started.elapsed().as_secs_f64(),
    );

    let mut out = Vec::<Vec<ScalarValue>>::new();
    for p in 0..parts {
        let mut ht: HashMap<Vec<ScalarValue>, Vec<Vec<ScalarValue>>> = HashMap::new();

        if let Ok(file) = File::open(&build_paths[p]) {
            let reader = BufReader::new(file);
            for line in reader.lines() {
                let line = line?;
                if line.trim().is_empty() {
                    continue;
                }
                let rec: JoinSpillRow = serde_json::from_str(&line)
                    .map_err(|e| FfqError::Execution(format!("join spill decode failed: {e}")))?;
                ht.entry(rec.key).or_default().push(rec.row);
            }
        }

        if let Ok(file) = File::open(&probe_paths[p]) {
            let reader = BufReader::new(file);
            for line in reader.lines() {
                let line = line?;
                if line.trim().is_empty() {
                    continue;
                }
                let rec: JoinSpillRow = serde_json::from_str(&line)
                    .map_err(|e| FfqError::Execution(format!("join spill decode failed: {e}")))?;
                if let Some(build_matches) = ht.get(&rec.key) {
                    for build in build_matches {
                        out.push(combine_join_rows(build, &rec.row, build_side));
                    }
                }
            }
        }

        let _ = fs::remove_file(&build_paths[p]);
        let _ = fs::remove_file(&probe_paths[p]);
    }

    Ok(out)
}

fn spill_join_partitions(
    rows: &[Vec<ScalarValue>],
    key_idx: &[usize],
    paths: &[PathBuf],
) -> Result<()> {
    let mut writers = Vec::with_capacity(paths.len());
    for path in paths {
        let file = File::create(path)?;
        writers.push(BufWriter::new(file));
    }

    for row in rows {
        let key = join_key_from_row(row, key_idx);
        let part = (hash_key(&key) as usize) % writers.len();
        let rec = JoinSpillRow {
            key,
            row: row.clone(),
        };
        let line = serde_json::to_string(&rec)
            .map_err(|e| FfqError::Execution(format!("join spill encode failed: {e}")))?;
        writers[part].write_all(line.as_bytes())?;
        writers[part].write_all(b"\n")?;
    }

    for mut w in writers {
        w.flush()?;
    }

    Ok(())
}

fn hash_key(key: &[ScalarValue]) -> u64 {
    let mut h = DefaultHasher::new();
    key.hash(&mut h);
    h.finish()
}

#[cfg_attr(feature = "profiling", inline(never))]
fn run_hash_aggregate(
    child: ExecOutput,
    group_exprs: Vec<Expr>,
    aggr_exprs: Vec<(AggExpr, String)>,
    mode: AggregateMode,
    ctx: &QueryContext,
    trace: &TraceIds,
) -> Result<ExecOutput> {
    #[cfg(feature = "profiling")]
    let _profile_span = info_span!(
        "profile_hash_aggregate",
        query_id = %trace.query_id,
        stage_id = trace.stage_id,
        task_id = trace.task_id,
        mode = ?mode
    )
    .entered();
    let input_schema = child.schema;
    let specs = build_agg_specs(&aggr_exprs, &input_schema, &group_exprs, mode)?;

    let mut groups: HashMap<Vec<ScalarValue>, Vec<AggState>> = HashMap::new();
    let mut spills = Vec::<PathBuf>::new();

    for batch in &child.batches {
        accumulate_batch(
            mode,
            &specs,
            &group_exprs,
            &input_schema,
            batch,
            &mut groups,
        )?;
        maybe_spill(&mut groups, &mut spills, ctx, trace)?;
    }

    if group_exprs.is_empty() && groups.is_empty() {
        groups.insert(vec![], init_states(&specs));
    }

    if !groups.is_empty() {
        maybe_spill(&mut groups, &mut spills, ctx, trace)?;
    }

    if !spills.is_empty() {
        for path in &spills {
            merge_spill_file(path, &mut groups)?;
            let _ = fs::remove_file(path);
        }
    }

    build_output(groups, &specs, &group_exprs, &input_schema, mode)
}

fn build_agg_specs(
    aggr_exprs: &[(AggExpr, String)],
    input_schema: &SchemaRef,
    group_exprs: &[Expr],
    mode: AggregateMode,
) -> Result<Vec<AggSpec>> {
    let mut specs = Vec::with_capacity(aggr_exprs.len());
    for (idx, (expr, name)) in aggr_exprs.iter().enumerate() {
        let out_type = match mode {
            AggregateMode::Partial => match expr {
                AggExpr::Count(_) => DataType::Int64,
                AggExpr::Sum(e) | AggExpr::Min(e) | AggExpr::Max(e) => {
                    expr_data_type(e, input_schema)?
                }
                AggExpr::Avg(_) => DataType::Float64,
            },
            AggregateMode::Final => {
                let col_idx = group_exprs.len() + idx;
                input_schema.field(col_idx).data_type().clone()
            }
        };
        specs.push(AggSpec {
            expr: expr.clone(),
            name: name.clone(),
            out_type,
        });
    }
    Ok(specs)
}

fn expr_data_type(expr: &Expr, schema: &SchemaRef) -> Result<DataType> {
    let compiled = compile_expr(expr, schema)?;
    Ok(compiled.data_type())
}

fn init_states(specs: &[AggSpec]) -> Vec<AggState> {
    specs
        .iter()
        .map(|s| match s.expr {
            AggExpr::Count(_) => AggState::Count(0),
            AggExpr::Sum(_) => match s.out_type {
                DataType::Int64 => AggState::SumInt(0),
                _ => AggState::SumFloat(0.0),
            },
            AggExpr::Min(_) => AggState::Min(None),
            AggExpr::Max(_) => AggState::Max(None),
            AggExpr::Avg(_) => AggState::Avg { sum: 0.0, count: 0 },
        })
        .collect()
}

fn accumulate_batch(
    mode: AggregateMode,
    specs: &[AggSpec],
    group_exprs: &[Expr],
    input_schema: &SchemaRef,
    batch: &RecordBatch,
    groups: &mut HashMap<Vec<ScalarValue>, Vec<AggState>>,
) -> Result<()> {
    let group_arrays = match mode {
        AggregateMode::Partial => {
            let mut arrays = Vec::with_capacity(group_exprs.len());
            for expr in group_exprs {
                let compiled = compile_expr(expr, input_schema)?;
                arrays.push(compiled.evaluate(batch)?);
            }
            arrays
        }
        AggregateMode::Final => {
            let mut arrays = Vec::with_capacity(group_exprs.len());
            for idx in 0..group_exprs.len() {
                arrays.push(batch.column(idx).clone());
            }
            arrays
        }
    };

    let agg_arrays = match mode {
        AggregateMode::Partial => {
            let mut arrays = Vec::with_capacity(specs.len());
            for spec in specs {
                let expr = match &spec.expr {
                    AggExpr::Count(e)
                    | AggExpr::Sum(e)
                    | AggExpr::Min(e)
                    | AggExpr::Max(e)
                    | AggExpr::Avg(e) => e,
                };
                let compiled = compile_expr(expr, input_schema)?;
                arrays.push(compiled.evaluate(batch)?);
            }
            arrays
        }
        AggregateMode::Final => {
            let mut arrays = Vec::with_capacity(specs.len());
            for idx in 0..specs.len() {
                arrays.push(batch.column(group_exprs.len() + idx).clone());
            }
            arrays
        }
    };

    let avg_count_arrays = if mode == AggregateMode::Final {
        let mut map = HashMap::<String, ArrayRef>::new();
        for spec in specs {
            if matches!(spec.expr, AggExpr::Avg(_)) {
                let key = avg_count_col_name(&spec.name);
                if let Ok(i) = input_schema.index_of(&key) {
                    map.insert(spec.name.clone(), batch.column(i).clone());
                }
            }
        }
        map
    } else {
        HashMap::new()
    };

    for row in 0..batch.num_rows() {
        let key = group_arrays
            .iter()
            .map(|a| scalar_from_array(a, row))
            .collect::<Result<Vec<_>>>()?;

        let state_vec = groups.entry(key).or_insert_with(|| init_states(specs));

        for (idx, spec) in specs.iter().enumerate() {
            let value = scalar_from_array(&agg_arrays[idx], row)?;
            update_state(
                &mut state_vec[idx],
                spec,
                value,
                mode,
                avg_count_arrays.get(&spec.name).map(|a| (a, row)),
            )?;
        }
    }

    Ok(())
}

fn update_state(
    state: &mut AggState,
    spec: &AggSpec,
    value: ScalarValue,
    mode: AggregateMode,
    avg_count_src: Option<(&ArrayRef, usize)>,
) -> Result<()> {
    match state {
        AggState::Count(acc) => {
            if mode == AggregateMode::Final {
                if let ScalarValue::Int64(v) = value {
                    *acc += v;
                }
            } else if value != ScalarValue::Null {
                *acc += 1;
            }
        }
        AggState::SumInt(acc) => {
            if let ScalarValue::Int64(v) = value {
                *acc += v;
            }
        }
        AggState::SumFloat(acc) => {
            if let Some(v) = as_f64(&value) {
                *acc += v;
            }
        }
        AggState::Min(cur) => {
            if value != ScalarValue::Null {
                match cur {
                    None => *cur = Some(value),
                    Some(existing) => {
                        if scalar_lt(&value, existing)? {
                            *cur = Some(value);
                        }
                    }
                }
            }
        }
        AggState::Max(cur) => {
            if value != ScalarValue::Null {
                match cur {
                    None => *cur = Some(value),
                    Some(existing) => {
                        if scalar_gt(&value, existing)? {
                            *cur = Some(value);
                        }
                    }
                }
            }
        }
        AggState::Avg { sum, count } => match mode {
            AggregateMode::Partial => {
                if let Some(v) = as_f64(&value) {
                    *sum += v;
                    *count += 1;
                }
            }
            AggregateMode::Final => {
                if let Some(v) = as_f64(&value) {
                    *sum += v;
                }
                let add_count = if let Some((arr, row)) = avg_count_src {
                    match scalar_from_array(arr, row)? {
                        ScalarValue::Int64(v) => v,
                        _ => 0,
                    }
                } else if value != ScalarValue::Null {
                    1
                } else {
                    0
                };
                *count += add_count;
            }
        },
    }

    if let (AggExpr::Count(_), AggState::Count(acc)) = (&spec.expr, state) {
        if *acc < 0 {
            return Err(FfqError::Execution("count overflow".to_string()));
        }
    }

    Ok(())
}

fn build_output(
    groups: HashMap<Vec<ScalarValue>, Vec<AggState>>,
    specs: &[AggSpec],
    group_exprs: &[Expr],
    input_schema: &SchemaRef,
    mode: AggregateMode,
) -> Result<ExecOutput> {
    let mut keys: Vec<Vec<ScalarValue>> = groups.keys().cloned().collect();
    keys.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));

    let mut fields = Vec::<Field>::new();
    let mut cols = Vec::<Vec<ScalarValue>>::new();

    for gidx in 0..group_exprs.len() {
        let (name, dt) = group_field(group_exprs, input_schema, gidx)?;
        fields.push(Field::new(&name, dt.clone(), true));
        let mut values = Vec::with_capacity(keys.len());
        for key in &keys {
            values.push(key[gidx].clone());
        }
        cols.push(values);
    }

    let mut avg_hidden_counts: Vec<(String, Vec<ScalarValue>)> = Vec::new();

    for (aidx, spec) in specs.iter().enumerate() {
        fields.push(Field::new(&spec.name, spec.out_type.clone(), true));
        let mut values = Vec::with_capacity(keys.len());
        let mut hidden_counts = Vec::new();

        for key in &keys {
            let states = groups
                .get(key)
                .ok_or_else(|| FfqError::Execution("missing aggregate state".to_string()))?;
            let state = &states[aidx];
            values.push(state_to_scalar(state, &spec.expr, mode));
            if matches!(spec.expr, AggExpr::Avg(_)) {
                let c = match state {
                    AggState::Avg { count, .. } => *count,
                    _ => 0,
                };
                hidden_counts.push(ScalarValue::Int64(c));
            }
        }

        cols.push(values);

        if mode == AggregateMode::Partial && matches!(spec.expr, AggExpr::Avg(_)) {
            avg_hidden_counts.push((avg_count_col_name(&spec.name), hidden_counts));
        }
    }

    for (name, values) in avg_hidden_counts {
        fields.push(Field::new(&name, DataType::Int64, true));
        cols.push(values);
    }

    let schema = Arc::new(Schema::new(fields));
    let arrays = cols
        .iter()
        .enumerate()
        .map(|(idx, col)| scalars_to_array(col, schema.field(idx).data_type()))
        .collect::<Result<Vec<_>>>()?;

    let batch = RecordBatch::try_new(schema.clone(), arrays)
        .map_err(|e| FfqError::Execution(format!("aggregate output batch failed: {e}")))?;

    Ok(ExecOutput {
        schema,
        batches: vec![batch],
    })
}

fn group_field(
    group_exprs: &[Expr],
    input_schema: &SchemaRef,
    idx: usize,
) -> Result<(String, DataType)> {
    match &group_exprs[idx] {
        Expr::ColumnRef { name, index } => Ok((
            name.clone(),
            if *index < input_schema.fields().len() {
                input_schema.field(*index).data_type().clone()
            } else {
                input_schema.field(idx).data_type().clone()
            },
        )),
        Expr::Column(name) => {
            let i = input_schema
                .index_of(name)
                .map_err(|e| FfqError::Execution(format!("unknown group column: {e}")))?;
            Ok((name.clone(), input_schema.field(i).data_type().clone()))
        }
        e => Ok((format!("{e:?}"), DataType::Utf8)),
    }
}

fn state_to_scalar(state: &AggState, expr: &AggExpr, mode: AggregateMode) -> ScalarValue {
    match (state, expr) {
        (AggState::Count(v), _) => ScalarValue::Int64(*v),
        (AggState::SumInt(v), _) => ScalarValue::Int64(*v),
        (AggState::SumFloat(v), _) => ScalarValue::Float64Bits(v.to_bits()),
        (AggState::Min(Some(v)), _) => v.clone(),
        (AggState::Min(None), _) => ScalarValue::Null,
        (AggState::Max(Some(v)), _) => v.clone(),
        (AggState::Max(None), _) => ScalarValue::Null,
        (AggState::Avg { sum, count }, AggExpr::Avg(_)) => {
            if mode == AggregateMode::Partial {
                ScalarValue::Float64Bits(sum.to_bits())
            } else if *count == 0 {
                ScalarValue::Null
            } else {
                ScalarValue::Float64Bits((sum / (*count as f64)).to_bits())
            }
        }
        _ => ScalarValue::Null,
    }
}

fn maybe_spill(
    groups: &mut HashMap<Vec<ScalarValue>, Vec<AggState>>,
    spills: &mut Vec<PathBuf>,
    ctx: &QueryContext,
    trace: &TraceIds,
) -> Result<()> {
    if groups.is_empty() || ctx.mem_budget_bytes == 0 {
        return Ok(());
    }

    let estimated = estimate_groups_bytes(groups);
    if estimated <= ctx.mem_budget_bytes {
        return Ok(());
    }

    let spill_started = Instant::now();
    fs::create_dir_all(&ctx.spill_dir)?;
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| FfqError::Execution(format!("clock error: {e}")))?
        .as_nanos();
    let path = PathBuf::from(&ctx.spill_dir).join(format!("agg_spill_{suffix}.jsonl"));

    let file = File::create(&path)?;
    let mut writer = BufWriter::new(file);
    for (key, states) in groups.iter() {
        let row = SpillRow {
            key: key.clone(),
            states: states.clone(),
        };
        let line = serde_json::to_string(&row)
            .map_err(|e| FfqError::Execution(format!("spill serialize failed: {e}")))?;
        writer.write_all(line.as_bytes()).map_err(FfqError::from)?;
        writer.write_all(b"\n").map_err(FfqError::from)?;
    }
    writer.flush().map_err(FfqError::from)?;
    let spill_bytes = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
    global_metrics().record_spill(
        &trace.query_id,
        trace.stage_id,
        trace.task_id,
        "aggregate",
        spill_bytes,
        spill_started.elapsed().as_secs_f64(),
    );

    groups.clear();
    spills.push(path);
    Ok(())
}

fn merge_spill_file(
    path: &PathBuf,
    groups: &mut HashMap<Vec<ScalarValue>, Vec<AggState>>,
) -> Result<()> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let row: SpillRow = serde_json::from_str(&line)
            .map_err(|e| FfqError::Execution(format!("spill deserialize failed: {e}")))?;
        if let Some(existing) = groups.get_mut(&row.key) {
            merge_states(existing, &row.states)?;
        } else {
            groups.insert(row.key, row.states);
        }
    }
    Ok(())
}

fn merge_states(target: &mut [AggState], other: &[AggState]) -> Result<()> {
    if target.len() != other.len() {
        return Err(FfqError::Execution(
            "spill state shape mismatch".to_string(),
        ));
    }

    for (t, o) in target.iter_mut().zip(other.iter()) {
        match (t, o) {
            (AggState::Count(a), AggState::Count(b)) => *a += *b,
            (AggState::SumInt(a), AggState::SumInt(b)) => *a += *b,
            (AggState::SumFloat(a), AggState::SumFloat(b)) => *a += *b,
            (AggState::Min(a), AggState::Min(b)) => {
                if let Some(bv) = b {
                    if a.as_ref()
                        .map(|av| scalar_lt(bv, av))
                        .transpose()?
                        .unwrap_or(true)
                    {
                        *a = Some(bv.clone());
                    }
                }
            }
            (AggState::Max(a), AggState::Max(b)) => {
                if let Some(bv) = b {
                    if a.as_ref()
                        .map(|av| scalar_gt(bv, av))
                        .transpose()?
                        .unwrap_or(true)
                    {
                        *a = Some(bv.clone());
                    }
                }
            }
            (
                AggState::Avg {
                    sum: asum,
                    count: acount,
                },
                AggState::Avg {
                    sum: bsum,
                    count: bcount,
                },
            ) => {
                *asum += *bsum;
                *acount += *bcount;
            }
            _ => return Err(FfqError::Execution("spill state type mismatch".to_string())),
        }
    }

    Ok(())
}

fn estimate_groups_bytes(groups: &HashMap<Vec<ScalarValue>, Vec<AggState>>) -> usize {
    let mut total = 0_usize;
    for (k, v) in groups {
        total += 96;
        total += k.iter().map(scalar_estimate_bytes).sum::<usize>();
        total += v.iter().map(agg_state_estimate_bytes).sum::<usize>();
    }
    total
}

fn scalar_estimate_bytes(v: &ScalarValue) -> usize {
    match v {
        ScalarValue::Int64(_) => 8,
        ScalarValue::Float64Bits(_) => 8,
        ScalarValue::VectorF32Bits(v) => v.len() * std::mem::size_of::<f32>(),
        ScalarValue::Utf8(s) => s.len(),
        ScalarValue::Boolean(_) => 1,
        ScalarValue::Null => 0,
    }
}

fn agg_state_estimate_bytes(v: &AggState) -> usize {
    match v {
        AggState::Count(_) => 8,
        AggState::SumInt(_) => 8,
        AggState::SumFloat(_) => 8,
        AggState::Min(x) | AggState::Max(x) => x.as_ref().map_or(0, scalar_estimate_bytes),
        AggState::Avg { .. } => 16,
    }
}

fn avg_count_col_name(name: &str) -> String {
    format!("__ffq_avg_count_{name}")
}

fn scalar_from_array(array: &ArrayRef, row: usize) -> Result<ScalarValue> {
    if array.is_null(row) {
        return Ok(ScalarValue::Null);
    }

    match array.data_type() {
        DataType::Int64 => {
            let a = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| FfqError::Execution("expected Int64Array".to_string()))?;
            Ok(ScalarValue::Int64(a.value(row)))
        }
        DataType::Float64 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .ok_or_else(|| FfqError::Execution("expected Float64Array".to_string()))?;
            Ok(ScalarValue::Float64Bits(a.value(row).to_bits()))
        }
        DataType::Float32 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::Float32Array>()
                .ok_or_else(|| FfqError::Execution("expected Float32Array".to_string()))?;
            Ok(ScalarValue::Float64Bits((a.value(row) as f64).to_bits()))
        }
        DataType::Utf8 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .ok_or_else(|| FfqError::Execution("expected StringArray".to_string()))?;
            Ok(ScalarValue::Utf8(a.value(row).to_string()))
        }
        DataType::Boolean => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::BooleanArray>()
                .ok_or_else(|| FfqError::Execution("expected BooleanArray".to_string()))?;
            Ok(ScalarValue::Boolean(a.value(row)))
        }
        DataType::FixedSizeList(field, size) => {
            if field.data_type() != &DataType::Float32 {
                return Err(FfqError::Unsupported(format!(
                    "only FixedSizeList<Float32> is supported in scalar conversion, got {:?}",
                    array.data_type()
                )));
            }
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::FixedSizeListArray>()
                .ok_or_else(|| FfqError::Execution("expected FixedSizeListArray".to_string()))?;
            let vals = a
                .values()
                .as_any()
                .downcast_ref::<arrow::array::Float32Array>()
                .ok_or_else(|| FfqError::Execution("expected Float32 list values".to_string()))?;
            let len = *size as usize;
            let start = row * len;
            let mut out = Vec::with_capacity(len);
            for i in 0..len {
                out.push(vals.value(start + i));
            }
            Ok(ScalarValue::VectorF32Bits(
                out.into_iter().map(f32::to_bits).collect(),
            ))
        }
        other => Err(FfqError::Unsupported(format!(
            "aggregate scalar type not supported yet: {other:?}"
        ))),
    }
}

fn scalars_to_array(values: &[ScalarValue], dt: &DataType) -> Result<ArrayRef> {
    match dt {
        DataType::Int64 => {
            let mut b = Int64Builder::with_capacity(values.len());
            for v in values {
                match v {
                    ScalarValue::Int64(x) => b.append_value(*x),
                    ScalarValue::Null => b.append_null(),
                    _ => {
                        return Err(FfqError::Execution(
                            "type mismatch while building Int64 array".to_string(),
                        ));
                    }
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Float64 => {
            let mut b = Float64Builder::with_capacity(values.len());
            for v in values {
                match v {
                    ScalarValue::Float64Bits(x) => b.append_value(f64::from_bits(*x)),
                    ScalarValue::Int64(x) => b.append_value(*x as f64),
                    ScalarValue::Null => b.append_null(),
                    _ => {
                        return Err(FfqError::Execution(
                            "type mismatch while building Float64 array".to_string(),
                        ));
                    }
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Float32 => {
            let mut b = arrow::array::Float32Builder::with_capacity(values.len());
            for v in values {
                match v {
                    ScalarValue::Float64Bits(x) => b.append_value(f64::from_bits(*x) as f32),
                    ScalarValue::Int64(x) => b.append_value(*x as f32),
                    ScalarValue::Null => b.append_null(),
                    _ => {
                        return Err(FfqError::Execution(
                            "type mismatch while building Float32 array".to_string(),
                        ));
                    }
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Utf8 => {
            let mut b = StringBuilder::with_capacity(values.len(), values.len() * 8);
            for v in values {
                match v {
                    ScalarValue::Utf8(x) => b.append_value(x),
                    ScalarValue::Null => b.append_null(),
                    _ => {
                        return Err(FfqError::Execution(
                            "type mismatch while building Utf8 array".to_string(),
                        ));
                    }
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Boolean => {
            let mut b = BooleanBuilder::with_capacity(values.len());
            for v in values {
                match v {
                    ScalarValue::Boolean(x) => b.append_value(*x),
                    ScalarValue::Null => b.append_null(),
                    _ => {
                        return Err(FfqError::Execution(
                            "type mismatch while building Boolean array".to_string(),
                        ));
                    }
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::FixedSizeList(field, size) => {
            if field.data_type() != &DataType::Float32 {
                return Err(FfqError::Unsupported(format!(
                    "aggregate output FixedSizeList item type not supported: {:?}",
                    field.data_type()
                )));
            }
            let mut b = FixedSizeListBuilder::new(Float32Builder::new(), *size);
            for v in values {
                match v {
                    ScalarValue::VectorF32Bits(xs) => {
                        if xs.len() != *size as usize {
                            return Err(FfqError::Execution(format!(
                                "vector length mismatch while building FixedSizeList: expected {}, got {}",
                                *size,
                                xs.len()
                            )));
                        }
                        for x in xs {
                            b.values().append_value(f32::from_bits(*x));
                        }
                        b.append(true);
                    }
                    ScalarValue::Null => b.append(false),
                    _ => {
                        return Err(FfqError::Execution(
                            "type mismatch while building FixedSizeList array".to_string(),
                        ));
                    }
                }
            }
            Ok(Arc::new(b.finish()))
        }
        other => Err(FfqError::Unsupported(format!(
            "aggregate output type not supported yet: {other:?}"
        ))),
    }
}

fn as_f64(v: &ScalarValue) -> Option<f64> {
    match v {
        ScalarValue::Int64(x) => Some(*x as f64),
        ScalarValue::Float64Bits(x) => Some(f64::from_bits(*x)),
        _ => None,
    }
}

fn scalar_lt(a: &ScalarValue, b: &ScalarValue) -> Result<bool> {
    match (a, b) {
        (ScalarValue::Int64(x), ScalarValue::Int64(y)) => Ok(x < y),
        (ScalarValue::Float64Bits(x), ScalarValue::Float64Bits(y)) => {
            Ok(f64::from_bits(*x) < f64::from_bits(*y))
        }
        (ScalarValue::Utf8(x), ScalarValue::Utf8(y)) => Ok(x < y),
        (ScalarValue::Boolean(x), ScalarValue::Boolean(y)) => Ok((!*x) & *y),
        _ => Err(FfqError::Execution(
            "cannot compare values of different types".to_string(),
        )),
    }
}

fn scalar_gt(a: &ScalarValue, b: &ScalarValue) -> Result<bool> {
    scalar_lt(b, a)
}

fn write_parquet_sink(table: &ffq_storage::TableDef, child: &ExecOutput) -> Result<()> {
    let out_path = resolve_sink_output_path(table)?;
    let staged_path = temp_sibling_path(&out_path, "staged");
    if let Some(parent) = staged_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let file = File::create(&staged_path)?;
    let mut writer = ArrowWriter::try_new(file, child.schema.clone(), None)
        .map_err(|e| FfqError::Execution(format!("parquet writer init failed: {e}")))?;
    for batch in &child.batches {
        writer
            .write(batch)
            .map_err(|e| FfqError::Execution(format!("parquet write failed: {e}")))?;
    }
    writer
        .close()
        .map_err(|e| FfqError::Execution(format!("parquet writer close failed: {e}")))?;
    if let Err(err) = replace_file_atomically(&staged_path, &out_path) {
        let _ = fs::remove_file(&staged_path);
        return Err(err);
    }
    Ok(())
}

fn resolve_sink_output_path(table: &ffq_storage::TableDef) -> Result<PathBuf> {
    let raw = if !table.uri.is_empty() {
        table.uri.clone()
    } else if let Some(first) = table.paths.first() {
        first.clone()
    } else {
        return Err(FfqError::InvalidConfig(format!(
            "table '{}' must define uri or paths for sink writes",
            table.name
        )));
    };

    let path = PathBuf::from(raw);
    let as_text = path.to_string_lossy();
    if as_text.ends_with(".parquet") {
        Ok(path)
    } else {
        Ok(path.join("part-00000.parquet"))
    }
}

fn temp_sibling_path(path: &PathBuf, label: &str) -> PathBuf {
    let parent = path
        .parent()
        .map(std::borrow::ToOwned::to_owned)
        .unwrap_or_else(|| PathBuf::from("."));
    let stem = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("target");
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    parent.join(format!(".ffq_{label}_{stem}_{nanos}.tmp"))
}

fn replace_file_atomically(staged: &PathBuf, target: &PathBuf) -> Result<()> {
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent)?;
    }
    if !target.exists() {
        fs::rename(staged, target).map_err(|e| {
            FfqError::Execution(format!(
                "file commit failed: {} -> {} ({e})",
                staged.display(),
                target.display()
            ))
        })?;
        return Ok(());
    }

    let backup = temp_sibling_path(target, "backup");
    fs::rename(target, &backup).map_err(|e| {
        FfqError::Execution(format!(
            "file backup rename failed: {} -> {} ({e})",
            target.display(),
            backup.display()
        ))
    })?;

    match fs::rename(staged, target) {
        Ok(_) => {
            let _ = fs::remove_file(backup);
            Ok(())
        }
        Err(e) => {
            let _ = fs::rename(&backup, target);
            Err(FfqError::Execution(format!(
                "file commit failed: {} -> {} ({e})",
                staged.display(),
                target.display()
            )))
        }
    }
}

#[cfg(feature = "distributed")]
use ffq_distributed::grpc::v1::QueryState as DistQueryState;
#[cfg(feature = "distributed")]
use ffq_distributed::grpc::ControlPlaneClient;
#[cfg(feature = "distributed")]
use ffq_distributed::DistributedRuntime as InnerDistributedRuntime;

#[cfg(feature = "distributed")]
#[derive(Debug)]
pub struct DistributedRuntime {
    _inner: InnerDistributedRuntime,
    coordinator_endpoint: String,
}

#[cfg(feature = "distributed")]
impl DistributedRuntime {
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            _inner: InnerDistributedRuntime::default(),
            coordinator_endpoint: endpoint.into(),
        }
    }
}

#[cfg(feature = "distributed")]
impl Default for DistributedRuntime {
    fn default() -> Self {
        Self::new("http://127.0.0.1:50051")
    }
}

#[cfg(feature = "distributed")]
impl Runtime for DistributedRuntime {
    fn execute(
        &self,
        plan: PhysicalPlan,
        _ctx: QueryContext,
        _catalog: Arc<Catalog>,
    ) -> BoxFuture<'static, Result<SendableRecordBatchStream>> {
        let endpoint = self.coordinator_endpoint.clone();
        let stage_dag = self._inner.build_stage_dag(&plan);
        async move {
            let _stage_dag = stage_dag?;
            let plan_bytes = serde_json::to_vec(&plan)
                .map_err(|e| FfqError::Execution(format!("encode physical plan failed: {e}")))?;
            let query_id = distributed_query_id()?;
            let span = info_span!(
                "distributed_query_execute",
                query_id = %query_id,
                stage_id = 0_u64,
                task_id = 0_u64,
                operator = "DistributedRuntime"
            );
            async move {
                info!(query_id = %query_id, endpoint = %endpoint, "submitting distributed query");

                let mut client = ControlPlaneClient::connect(endpoint.clone())
                    .await
                    .map_err(|e| FfqError::Execution(format!("connect coordinator failed: {e}")))?;
                client
                    .submit_query(ffq_distributed::grpc::v1::SubmitQueryRequest {
                        query_id: query_id.clone(),
                        physical_plan_json: plan_bytes,
                    })
                    .await
                    .map_err(|e| FfqError::Execution(format!("submit query failed: {e}")))?;

                let mut polls = 0_u32;
                let terminal = loop {
                    let status = client
                        .get_query_status(ffq_distributed::grpc::v1::GetQueryStatusRequest {
                            query_id: query_id.clone(),
                        })
                        .await
                        .map_err(|e| FfqError::Execution(format!("get query status failed: {e}")))?
                        .into_inner()
                        .status
                        .ok_or_else(|| {
                            FfqError::Execution("empty query status response".to_string())
                        })?;

                    let qstate = DistQueryState::try_from(status.state).map_err(|_| {
                        FfqError::Execution(format!("invalid query state value: {}", status.state))
                    })?;
                    debug!(
                        query_id = %query_id,
                        polls,
                        state = ?qstate,
                        "polled distributed query status"
                    );
                    if matches!(
                        qstate,
                        DistQueryState::Succeeded
                            | DistQueryState::Failed
                            | DistQueryState::Canceled
                    ) {
                        break (qstate, status.message);
                    }

                    polls = polls.saturating_add(1);
                    if polls > 600 {
                        return Err(FfqError::Execution(
                            "distributed query timed out waiting for completion".to_string(),
                        ));
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                };

                match terminal.0 {
                    DistQueryState::Succeeded => {
                        info!(query_id = %query_id, "distributed query succeeded");
                    }
                    DistQueryState::Failed => {
                        error!(query_id = %query_id, message = %terminal.1, "distributed query failed");
                        return Err(FfqError::Execution(format!(
                            "distributed query failed: {}",
                            terminal.1
                        )));
                    }
                    DistQueryState::Canceled => {
                        error!(query_id = %query_id, message = %terminal.1, "distributed query canceled");
                        return Err(FfqError::Execution(format!(
                            "distributed query canceled: {}",
                            terminal.1
                        )));
                    }
                    _ => {
                        return Err(FfqError::Execution(
                            "unexpected terminal query state".to_string(),
                        ));
                    }
                }

                let mut stream = client
                    .fetch_query_results(ffq_distributed::grpc::v1::FetchQueryResultsRequest {
                        query_id,
                    })
                    .await
                    .map_err(|e| FfqError::Execution(format!("fetch query results failed: {e}")))?
                    .into_inner();

                let mut payload = Vec::<u8>::new();
                while let Some(chunk) = stream
                    .message()
                    .await
                    .map_err(|e| FfqError::Execution(format!("results stream failed: {e}")))?
                {
                    payload.extend_from_slice(&chunk.payload);
                }

                let (schema, batches) = decode_record_batches_ipc(&payload)?;
                info!(batches = batches.len(), "received distributed query results");
                let out_stream = futures::stream::iter(batches.into_iter().map(Ok));
                Ok(Box::pin(StreamAdapter::new(schema, out_stream)) as SendableRecordBatchStream)
            }
            .instrument(span)
            .await
        }
        .boxed()
    }
}

#[cfg(feature = "distributed")]
fn distributed_query_id() -> Result<String> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| FfqError::Execution(format!("clock error: {e}")))?
        .as_nanos();
    Ok(nanos.to_string())
}

#[cfg(feature = "distributed")]
fn decode_record_batches_ipc(payload: &[u8]) -> Result<(SchemaRef, Vec<RecordBatch>)> {
    if payload.is_empty() {
        return Ok((Arc::new(Schema::empty()), Vec::new()));
    }
    let cursor = std::io::Cursor::new(payload.to_vec());
    let reader = arrow::ipc::reader::StreamReader::try_new(cursor, None)
        .map_err(|e| FfqError::Execution(format!("ipc reader init failed: {e}")))?;
    let schema = reader.schema();
    let batches = reader
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| FfqError::Execution(format!("ipc decode failed: {e}")))?;
    Ok((schema, batches))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{FixedSizeListBuilder, Float32Array, Float32Builder, Int64Array};
    use arrow::record_batch::RecordBatch;
    use arrow_schema::{DataType, Field, Schema};
    use ffq_planner::VectorTopKExec;
    #[cfg(feature = "vector")]
    use ffq_planner::{Expr, LiteralValue};
    use ffq_storage::vector_index::{VectorIndexProvider, VectorTopKRow};
    use futures::future::BoxFuture;

    use super::{
        rows_to_vector_topk_output, run_topk_by_score, run_vector_topk_with_provider, ExecOutput,
    };

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
}
