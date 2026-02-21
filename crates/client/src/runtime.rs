//! Query runtime implementations and operator execution helpers.
//!
//! This module executes physical plans in embedded and distributed modes.
//! Operator contracts in v1:
//! - scan/filter/project preserve row alignment per batch;
//! - join/aggregate may reorder rows, but preserve logical SQL semantics;
//! - sink operators return empty result batches and persist side effects;
//! - memory budget (`QueryContext.mem_budget_bytes`) triggers spill paths for
//!   hash join/hash aggregate when estimates exceed the budget.

use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::collections::{HashMap, HashSet, hash_map::DefaultHasher};
use std::fmt::Debug;
use std::fs::{self, File};
use std::hash::{Hash, Hasher};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use crate::physical_registry::PhysicalOperatorRegistry;
use arrow::array::{
    Array, ArrayRef, BooleanBuilder, FixedSizeListBuilder, Float32Builder, Float64Builder,
    Int64Array, Int64Builder, StringBuilder,
};
use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use ffq_common::adaptive::{AdaptiveReducePlan, plan_adaptive_reduce_layout};
use ffq_common::metrics::global_metrics;
use ffq_common::{FfqError, MemoryPressureSignal, MemorySpillManager, Result};
use ffq_execution::{SendableRecordBatchStream, StreamAdapter, TaskContext, compile_expr};
use ffq_planner::{
    AggExpr, BinaryOp, BuildSide, ExchangeExec, Expr, JoinType, LiteralValue, PartitioningSpec,
    PhysicalPlan, WindowExpr, WindowFrameBound, WindowFrameExclusion, WindowFrameSpec,
    WindowFrameUnits, WindowFunction, WindowOrderExpr,
};
#[cfg(feature = "s3")]
use ffq_storage::object_store_provider::{ObjectStoreProvider, is_object_store_uri};
use ffq_storage::parquet_provider::ParquetProvider;
#[cfg(feature = "qdrant")]
use ffq_storage::qdrant_provider::QdrantProvider;
#[cfg(any(feature = "qdrant", test))]
use ffq_storage::vector_index::VectorIndexProvider;
#[cfg(any(feature = "qdrant", test))]
use ffq_storage::vector_index::VectorQueryOptions;
use ffq_storage::{Catalog, StorageProvider};
use futures::future::BoxFuture;
use futures::{FutureExt, TryStreamExt};
use parquet::arrow::ArrowWriter;
use serde::{Deserialize, Serialize};
use tracing::{Instrument, info, info_span};
#[cfg(feature = "distributed")]
use tracing::{debug, error};

const E_SUBQUERY_SCALAR_ROW_VIOLATION: &str = "E_SUBQUERY_SCALAR_ROW_VIOLATION";
const MIN_RUNTIME_BATCH_SIZE_ROWS: usize = 256;

#[derive(Debug, Clone)]
/// Per-query runtime controls.
///
/// `mem_budget_bytes` is a soft threshold used by join/aggregate operators to
/// choose in-memory vs spill-enabled execution.
pub struct QueryContext {
    pub batch_size_rows: usize,
    pub mem_budget_bytes: usize,
    pub spill_trigger_ratio_num: u32,
    pub spill_trigger_ratio_den: u32,
    pub broadcast_threshold_bytes: u64,
    pub join_radix_bits: u8,
    pub join_bloom_enabled: bool,
    pub join_bloom_bits: u8,
    pub spill_dir: String,
    pub(crate) stats_collector: Option<Arc<RuntimeStatsCollector>>,
}

fn embedded_memory_manager(base_batch_size_rows: usize) -> Arc<MemorySpillManager> {
    static MANAGER: OnceLock<Arc<MemorySpillManager>> = OnceLock::new();
    Arc::clone(MANAGER.get_or_init(|| {
        let engine_budget = std::env::var("FFQ_ENGINE_MEM_BUDGET_BYTES")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(usize::MAX);
        MemorySpillManager::new(
            engine_budget,
            base_batch_size_rows,
            MIN_RUNTIME_BATCH_SIZE_ROWS,
        )
    }))
}

fn spill_signal_for_ctx(ctx: &QueryContext) -> MemoryPressureSignal {
    MemoryPressureSignal {
        pressure: ffq_common::MemoryPressure::Normal,
        effective_mem_budget_bytes: ctx.mem_budget_bytes,
        suggested_batch_size_rows: ctx.batch_size_rows,
        spill_trigger_ratio_num: ctx.spill_trigger_ratio_num.max(1),
        spill_trigger_ratio_den: ctx.spill_trigger_ratio_den.max(1),
    }
}

#[derive(Debug, Clone)]
struct OperatorExecutionStats {
    stage_id: u64,
    task_id: u64,
    operator: &'static str,
    rows_in: u64,
    rows_out: u64,
    batches_in: u64,
    batches_out: u64,
    bytes_in: u64,
    bytes_out: u64,
    elapsed_ms: f64,
    partition_sizes_bytes: Vec<u64>,
}

#[derive(Debug, Default, Clone)]
struct StageExecutionSummary {
    operator_count: u64,
    task_count: u64,
    rows_in: u64,
    rows_out: u64,
    batches_in: u64,
    batches_out: u64,
    bytes_in: u64,
    bytes_out: u64,
    partition_sizes_bytes: Vec<u64>,
    aqe_planned_reduce_tasks: u32,
    aqe_adaptive_reduce_tasks: u32,
    aqe_target_bytes: u64,
    aqe_events: Vec<String>,
    aqe_layout_finalize_count: u32,
    aqe_skew_split_tasks: u32,
    streaming_first_chunk_ms: u64,
    streaming_first_reduce_row_ms: u64,
    streaming_lag_ms: u64,
    streaming_buffered_bytes: u64,
    streaming_active_streams: u32,
    streaming_backpressure_events: Vec<String>,
}

#[derive(Debug, Default)]
struct RuntimeStatsInner {
    query_id: Option<String>,
    operators: Vec<OperatorExecutionStats>,
    stages: HashMap<u64, StageExecutionSummary>,
}

#[derive(Debug, Default)]
pub(crate) struct RuntimeStatsCollector {
    inner: Mutex<RuntimeStatsInner>,
}

impl RuntimeStatsCollector {
    fn record_operator(&self, query_id: &str, op: OperatorExecutionStats) {
        let mut guard = self.inner.lock().expect("stats collector lock poisoned");
        if guard.query_id.is_none() {
            guard.query_id = Some(query_id.to_string());
        }
        let stage = guard.stages.entry(op.stage_id).or_default();
        stage.operator_count = stage.operator_count.saturating_add(1);
        stage.rows_in = stage.rows_in.saturating_add(op.rows_in);
        stage.rows_out = stage.rows_out.saturating_add(op.rows_out);
        stage.batches_in = stage.batches_in.saturating_add(op.batches_in);
        stage.batches_out = stage.batches_out.saturating_add(op.batches_out);
        stage.bytes_in = stage.bytes_in.saturating_add(op.bytes_in);
        stage.bytes_out = stage.bytes_out.saturating_add(op.bytes_out);
        stage.task_count = stage.task_count.max(op.task_id.saturating_add(1));
        stage
            .partition_sizes_bytes
            .extend(op.partition_sizes_bytes.iter().copied());
        guard.operators.push(op);
    }

    fn record_stage_summary(
        &self,
        query_id: &str,
        stage_id: u64,
        task_count: u64,
        rows_out: u64,
        bytes_out: u64,
        batches_out: u64,
        planned_reduce_tasks: u32,
        adaptive_reduce_tasks: u32,
        adaptive_target_bytes: u64,
        aqe_events: Vec<String>,
        partition_histogram_upper_bounds: Vec<u64>,
        layout_finalize_count: u32,
        skew_split_tasks: u32,
        first_chunk_ms: u64,
        first_reduce_row_ms: u64,
        stream_lag_ms: u64,
        stream_buffered_bytes: u64,
        stream_active_count: u32,
        backpressure_events: Vec<String>,
    ) {
        let mut guard = self.inner.lock().expect("stats collector lock poisoned");
        if guard.query_id.is_none() {
            guard.query_id = Some(query_id.to_string());
        }
        let stage = guard.stages.entry(stage_id).or_default();
        stage.task_count = stage.task_count.max(task_count);
        stage.rows_out = stage.rows_out.max(rows_out);
        stage.bytes_out = stage.bytes_out.max(bytes_out);
        stage.batches_out = stage.batches_out.max(batches_out);
        stage.aqe_planned_reduce_tasks = planned_reduce_tasks;
        stage.aqe_adaptive_reduce_tasks = adaptive_reduce_tasks;
        stage.aqe_target_bytes = adaptive_target_bytes;
        stage.aqe_events = aqe_events;
        stage.aqe_layout_finalize_count = layout_finalize_count;
        stage.aqe_skew_split_tasks = skew_split_tasks;
        stage.streaming_first_chunk_ms = first_chunk_ms;
        stage.streaming_first_reduce_row_ms = first_reduce_row_ms;
        stage.streaming_lag_ms = stream_lag_ms;
        stage.streaming_buffered_bytes = stream_buffered_bytes;
        stage.streaming_active_streams = stream_active_count;
        stage.streaming_backpressure_events = backpressure_events;
        stage
            .partition_sizes_bytes
            .extend(partition_histogram_upper_bounds);
    }

    pub(crate) fn render_report(&self) -> Option<String> {
        let guard = self.inner.lock().ok()?;
        if guard.operators.is_empty() {
            return None;
        }
        let query_id = guard
            .query_id
            .clone()
            .unwrap_or_else(|| "unknown".to_string());
        let mut stage_ids = guard.stages.keys().copied().collect::<Vec<_>>();
        stage_ids.sort_unstable();

        let mut out = String::new();
        out.push_str(&format!("query_id={query_id}\n"));
        out.push_str("stages:\n");
        for sid in stage_ids {
            let s = guard.stages.get(&sid).expect("stage exists");
            let (part_min, part_max, part_avg, part_n) = if s.partition_sizes_bytes.is_empty() {
                (0_u64, 0_u64, 0.0_f64, 0_usize)
            } else {
                let min = *s.partition_sizes_bytes.iter().min().unwrap_or(&0);
                let max = *s.partition_sizes_bytes.iter().max().unwrap_or(&0);
                let sum = s.partition_sizes_bytes.iter().sum::<u64>() as f64;
                let n = s.partition_sizes_bytes.len();
                (min, max, sum / (n as f64), n)
            };
            out.push_str(&format!(
                "- stage={sid} ops={} tasks={} rows_in={} rows_out={} bytes_in={} bytes_out={} batches_in={} batches_out={} partition_sizes={{n:{part_n},min:{part_min},max:{part_max},avg:{part_avg:.1}}}\n",
                s.operator_count,
                s.task_count,
                s.rows_in,
                s.rows_out,
                s.bytes_in,
                s.bytes_out,
                s.batches_in,
                s.batches_out,
            ));
            out.push_str(&format!(
                "  aqe={{planned_reduce_tasks:{},adaptive_reduce_tasks:{},target_bytes:{},layout_finalize_count:{},skew_split_tasks:{}}}\n",
                s.aqe_planned_reduce_tasks,
                s.aqe_adaptive_reduce_tasks,
                s.aqe_target_bytes,
                s.aqe_layout_finalize_count,
                s.aqe_skew_split_tasks
            ));
            if !s.aqe_events.is_empty() {
                out.push_str(&format!("  aqe_events={}\n", s.aqe_events.join(" | ")));
            }
            out.push_str(&format!(
                "  streaming={{first_chunk_ms:{},first_reduce_row_ms:{},lag_ms:{},buffered_bytes:{},active_streams:{}}}\n",
                s.streaming_first_chunk_ms,
                s.streaming_first_reduce_row_ms,
                s.streaming_lag_ms,
                s.streaming_buffered_bytes,
                s.streaming_active_streams,
            ));
            if !s.streaming_backpressure_events.is_empty() {
                out.push_str(&format!(
                    "  backpressure_events={}\n",
                    s.streaming_backpressure_events.join(" | ")
                ));
            }
        }
        out.push_str("operators:\n");
        for op in &guard.operators {
            out.push_str(&format!(
                "- stage={} task={} op={} rows_in={} rows_out={} bytes_in={} bytes_out={} ms={:.3}\n",
                op.stage_id,
                op.task_id,
                op.operator,
                op.rows_in,
                op.rows_out,
                op.bytes_in,
                op.bytes_out,
                op.elapsed_ms
            ));
        }
        Some(out)
    }
}

/// Runtime = something that can execute a PhysicalPlan and return a stream of RecordBatches.
pub trait Runtime: Send + Sync + Debug {
    /// Execute one physical plan and return its output stream.
    ///
    /// Errors are surfaced as `FfqError::Planning`, `FfqError::Execution`,
    /// `FfqError::Io`, or `FfqError::InvalidConfig` depending on failure stage.
    fn execute(
        &self,
        plan: PhysicalPlan,
        ctx: QueryContext,
        catalog: Arc<Catalog>,
        physical_registry: Arc<PhysicalOperatorRegistry>,
    ) -> BoxFuture<'static, Result<SendableRecordBatchStream>>;

    fn shutdown(&self) -> BoxFuture<'static, Result<()>> {
        async { Ok(()) }.boxed()
    }
}

#[derive(Debug, Default)]
/// In-process runtime that executes all operators locally.
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
        physical_registry: Arc<PhysicalOperatorRegistry>,
    ) -> BoxFuture<'static, Result<SendableRecordBatchStream>> {
        async move {
            let requested = if ctx.mem_budget_bytes == usize::MAX {
                0
            } else {
                ctx.mem_budget_bytes
            };
            let manager = embedded_memory_manager(ctx.batch_size_rows);
            let reservation = manager.reserve(requested);
            let signal = reservation.signal();
            let mut exec_ctx = ctx;
            if requested > 0 {
                exec_ctx.mem_budget_bytes = signal.effective_mem_budget_bytes;
            }
            exec_ctx.batch_size_rows = signal.suggested_batch_size_rows;
            exec_ctx.spill_trigger_ratio_num = signal.spill_trigger_ratio_num;
            exec_ctx.spill_trigger_ratio_den = signal.spill_trigger_ratio_den;
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
            let exec = execute_plan(
                plan,
                exec_ctx,
                catalog,
                physical_registry,
                Arc::clone(&trace),
            )
            .await?;
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

#[derive(Clone)]
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

/// Recursively execute one physical-plan subtree and materialize output batches.
///
/// This is the central operator dispatcher for scan/filter/project/limit/top-k,
/// exchange, hash join, hash aggregate, and parquet sink paths.
///
/// Error taxonomy at call sites:
/// - `Execution`: operator evaluation, encode/decode, spill, or batch-shape failures
/// - `InvalidConfig`: missing table sink path/schema contracts discovered at runtime
/// - `Unsupported`: physical node or runtime feature path not supported in current build
/// - `Io`: filesystem failures surfaced through std io conversions
fn execute_plan(
    plan: PhysicalPlan,
    ctx: QueryContext,
    catalog: Arc<Catalog>,
    physical_registry: Arc<PhysicalOperatorRegistry>,
    trace: Arc<TraceIds>,
) -> BoxFuture<'static, Result<ExecOutput>> {
    let cte_cache = Arc::new(Mutex::new(HashMap::<String, ExecOutput>::new()));
    execute_plan_with_cache(plan, ctx, catalog, physical_registry, trace, cte_cache)
}

fn execute_plan_with_cache(
    plan: PhysicalPlan,
    ctx: QueryContext,
    catalog: Arc<Catalog>,
    physical_registry: Arc<PhysicalOperatorRegistry>,
    trace: Arc<TraceIds>,
    cte_cache: Arc<Mutex<HashMap<String, ExecOutput>>>,
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
        let stats_collector = ctx.stats_collector.clone();
        let eval = match plan {
            PhysicalPlan::ParquetScan(scan) => {
                let table = catalog.get(&scan.table)?.clone();
                #[cfg(feature = "s3")]
                let provider: Arc<dyn StorageProvider> =
                    if table.data_paths()?.iter().any(|p| is_object_store_uri(p)) {
                        Arc::new(ObjectStoreProvider::new())
                    } else {
                        Arc::new(ParquetProvider::new())
                    };
                #[cfg(not(feature = "s3"))]
                let provider: Arc<dyn StorageProvider> = Arc::new(ParquetProvider::new());
                let node = provider.scan(&table, scan.projection, scan.filters)?;
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
                let child = execute_plan_with_cache(
                    *write.input,
                    ctx,
                    catalog.clone(),
                    Arc::clone(&physical_registry),
                    Arc::clone(&trace),
                    Arc::clone(&cte_cache),
                )
                .await?;
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
                let child = execute_plan_with_cache(
                    *project.input,
                    ctx,
                    catalog,
                    Arc::clone(&physical_registry),
                    Arc::clone(&trace),
                    Arc::clone(&cte_cache),
                )
                .await?;
                let mut out_batches = Vec::with_capacity(child.batches.len());
                let schema = Arc::new(Schema::new(
                    project
                        .exprs
                        .iter()
                        .map(|(expr, name)| {
                            let dt = compile_expr(expr, &child.schema)?.data_type();
                            let nullable = infer_expr_nullable(expr, &child.schema)?;
                            Ok(Field::new(name, dt, nullable))
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
            PhysicalPlan::Window(window) => {
                let child = execute_plan_with_cache(
                    *window.input,
                    ctx.clone(),
                    catalog,
                    Arc::clone(&physical_registry),
                    Arc::clone(&trace),
                    Arc::clone(&cte_cache),
                )
                .await?;
                let (in_rows, in_batches, in_bytes) = batch_stats(&child.batches);
                let out =
                    run_window_exec_with_ctx(child, &window.exprs, &ctx, Some(trace.as_ref()))?;
                Ok(OpEval {
                    out,
                    in_rows,
                    in_batches,
                    in_bytes,
                })
            }
            PhysicalPlan::Filter(filter) => {
                let child = execute_plan_with_cache(
                    *filter.input,
                    ctx,
                    catalog,
                    Arc::clone(&physical_registry),
                    Arc::clone(&trace),
                    Arc::clone(&cte_cache),
                )
                .await?;
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
            PhysicalPlan::InSubqueryFilter(exec) => {
                let child = execute_plan_with_cache(
                    *exec.input,
                    ctx.clone(),
                    catalog.clone(),
                    Arc::clone(&physical_registry),
                    Arc::clone(&trace),
                    Arc::clone(&cte_cache),
                )
                .await?;
                let sub = execute_plan_with_cache(
                    *exec.subquery,
                    ctx,
                    catalog,
                    Arc::clone(&physical_registry),
                    Arc::clone(&trace),
                    Arc::clone(&cte_cache),
                )
                .await?;
                let (in_rows, in_batches, in_bytes) = batch_stats(&child.batches);
                Ok(OpEval {
                    out: run_in_subquery_filter(child, exec.expr, sub, exec.negated)?,
                    in_rows,
                    in_batches,
                    in_bytes,
                })
            }
            PhysicalPlan::ExistsSubqueryFilter(exec) => {
                let child = execute_plan_with_cache(
                    *exec.input,
                    ctx.clone(),
                    catalog.clone(),
                    Arc::clone(&physical_registry),
                    Arc::clone(&trace),
                    Arc::clone(&cte_cache),
                )
                .await?;
                let sub = execute_plan_with_cache(
                    *exec.subquery,
                    ctx,
                    catalog,
                    Arc::clone(&physical_registry),
                    Arc::clone(&trace),
                    Arc::clone(&cte_cache),
                )
                .await?;
                let (in_rows, in_batches, in_bytes) = batch_stats(&child.batches);
                Ok(OpEval {
                    out: run_exists_subquery_filter(child, sub, exec.negated),
                    in_rows,
                    in_batches,
                    in_bytes,
                })
            }
            PhysicalPlan::ScalarSubqueryFilter(exec) => {
                let child = execute_plan_with_cache(
                    *exec.input,
                    ctx.clone(),
                    catalog.clone(),
                    Arc::clone(&physical_registry),
                    Arc::clone(&trace),
                    Arc::clone(&cte_cache),
                )
                .await?;
                let sub = execute_plan_with_cache(
                    *exec.subquery,
                    ctx,
                    catalog,
                    Arc::clone(&physical_registry),
                    Arc::clone(&trace),
                    Arc::clone(&cte_cache),
                )
                .await?;
                let (in_rows, in_batches, in_bytes) = batch_stats(&child.batches);
                Ok(OpEval {
                    out: run_scalar_subquery_filter(child, exec.expr, exec.op, sub)?,
                    in_rows,
                    in_batches,
                    in_bytes,
                })
            }
            PhysicalPlan::Limit(limit) => {
                let child = execute_plan_with_cache(
                    *limit.input,
                    ctx,
                    catalog,
                    Arc::clone(&physical_registry),
                    Arc::clone(&trace),
                    Arc::clone(&cte_cache),
                )
                .await?;
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
                let child = execute_plan_with_cache(
                    *topk.input,
                    ctx,
                    catalog,
                    Arc::clone(&physical_registry),
                    Arc::clone(&trace),
                    Arc::clone(&cte_cache),
                )
                .await?;
                let (in_rows, in_batches, in_bytes) = batch_stats(&child.batches);
                Ok(OpEval {
                    out: run_topk_by_score(child, topk.score_expr, topk.k)?,
                    in_rows,
                    in_batches,
                    in_bytes,
                })
            }
            PhysicalPlan::UnionAll(union) => {
                let left = execute_plan_with_cache(
                    *union.left,
                    ctx.clone(),
                    Arc::clone(&catalog),
                    Arc::clone(&physical_registry),
                    Arc::clone(&trace),
                    Arc::clone(&cte_cache),
                )
                .await?;
                let right = execute_plan_with_cache(
                    *union.right,
                    ctx,
                    catalog,
                    Arc::clone(&physical_registry),
                    Arc::clone(&trace),
                    Arc::clone(&cte_cache),
                )
                .await?;
                if left.schema.fields().len() != right.schema.fields().len() {
                    return Err(FfqError::Execution(format!(
                        "UNION ALL schema mismatch: left has {} columns, right has {} columns",
                        left.schema.fields().len(),
                        right.schema.fields().len()
                    )));
                }
                let (l_rows, l_batches, l_bytes) = batch_stats(&left.batches);
                let (r_rows, r_batches, r_bytes) = batch_stats(&right.batches);
                let mut batches = left.batches;
                batches.extend(right.batches);
                Ok(OpEval {
                    out: ExecOutput {
                        schema: left.schema,
                        batches,
                    },
                    in_rows: l_rows + r_rows,
                    in_batches: l_batches + r_batches,
                    in_bytes: l_bytes + r_bytes,
                })
            }
            PhysicalPlan::CteRef(cte_ref) => {
                if let Some(cached) = cte_cache
                    .lock()
                    .ok()
                    .and_then(|m| m.get(&cte_ref.name).cloned())
                {
                    let (in_rows, in_batches, in_bytes) = batch_stats(&cached.batches);
                    Ok(OpEval {
                        out: cached,
                        in_rows,
                        in_batches,
                        in_bytes,
                    })
                } else {
                    let out = execute_plan_with_cache(
                        *cte_ref.plan,
                        ctx,
                        catalog,
                        Arc::clone(&physical_registry),
                        Arc::clone(&trace),
                        Arc::clone(&cte_cache),
                    )
                    .await?;
                    if let Ok(mut guard) = cte_cache.lock() {
                        guard.insert(cte_ref.name.clone(), out.clone());
                    }
                    let (in_rows, in_batches, in_bytes) = batch_stats(&out.batches);
                    Ok(OpEval {
                        out,
                        in_rows,
                        in_batches,
                        in_bytes,
                    })
                }
            }
            PhysicalPlan::VectorTopK(exec) => Ok(OpEval {
                out: execute_vector_topk(exec, catalog).await?,
                in_rows: 0,
                in_batches: 0,
                in_bytes: 0,
            }),
            PhysicalPlan::VectorKnn(exec) => Ok(OpEval {
                out: execute_vector_knn(exec, catalog).await?,
                in_rows: 0,
                in_batches: 0,
                in_bytes: 0,
            }),
            PhysicalPlan::Custom(custom) => {
                let child = execute_plan_with_cache(
                    *custom.input,
                    ctx,
                    catalog,
                    Arc::clone(&physical_registry),
                    Arc::clone(&trace),
                    Arc::clone(&cte_cache),
                )
                .await?;
                let factory = physical_registry.get(&custom.op_name).ok_or_else(|| {
                    FfqError::Unsupported(format!(
                        "custom physical operator '{}' is not registered",
                        custom.op_name
                    ))
                })?;
                let (schema, batches) =
                    factory.execute(child.schema.clone(), child.batches.clone(), &custom.config)?;
                let (in_rows, in_batches, in_bytes) = batch_stats(&child.batches);
                Ok(OpEval {
                    out: ExecOutput { schema, batches },
                    in_rows,
                    in_batches,
                    in_bytes,
                })
            }
            PhysicalPlan::Exchange(exchange) => match exchange {
                ExchangeExec::ShuffleWrite(x) => {
                    let child = execute_plan_with_cache(
                        *x.input,
                        ctx.clone(),
                        catalog,
                        Arc::clone(&physical_registry),
                        Arc::clone(&trace),
                        Arc::clone(&cte_cache),
                    )
                    .await?;
                    let (in_rows, in_batches, in_bytes) = batch_stats(&child.batches);
                    Ok(OpEval {
                        out: child,
                        in_rows,
                        in_batches,
                        in_bytes,
                    })
                }
                ExchangeExec::ShuffleRead(x) => {
                    let child = execute_plan_with_cache(
                        *x.input,
                        ctx.clone(),
                        catalog,
                        Arc::clone(&physical_registry),
                        Arc::clone(&trace),
                        Arc::clone(&cte_cache),
                    )
                    .await?;
                    let (in_rows, in_batches, in_bytes) = batch_stats(&child.batches);
                    if let Some(collector) = &ctx.stats_collector {
                        if let Ok(summary) =
                            embedded_adaptive_plan_for_partitioning(&child, &x.partitioning)
                        {
                            let (rows_out, _batches_out, bytes_out) = batch_stats(&child.batches);
                            collector.record_stage_summary(
                                &trace.query_id,
                                trace.stage_id,
                                summary.adaptive_reduce_tasks as u64,
                                rows_out,
                                bytes_out,
                                child.batches.len() as u64,
                                summary.planned_reduce_tasks,
                                summary.adaptive_reduce_tasks,
                                summary.target_bytes,
                                summary.aqe_events.clone(),
                                summary
                                    .partition_bytes_histogram
                                    .iter()
                                    .flat_map(|b| {
                                        std::iter::repeat_n(
                                            b.upper_bound_bytes,
                                            b.partition_count as usize,
                                        )
                                    })
                                    .collect(),
                                1,
                                summary.skew_split_tasks,
                                0,
                                0,
                                0,
                                0,
                                0,
                                Vec::new(),
                            );
                        }
                    }
                    Ok(OpEval {
                        out: child,
                        in_rows,
                        in_batches,
                        in_bytes,
                    })
                }
                ExchangeExec::Broadcast(x) => {
                    let child = execute_plan_with_cache(
                        *x.input,
                        ctx,
                        catalog,
                        Arc::clone(&physical_registry),
                        Arc::clone(&trace),
                        Arc::clone(&cte_cache),
                    )
                    .await?;
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
                let child = execute_plan_with_cache(
                    *agg.input,
                    ctx.clone(),
                    catalog,
                    Arc::clone(&physical_registry),
                    Arc::clone(&trace),
                    Arc::clone(&cte_cache),
                )
                .await?;
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
                let child = execute_plan_with_cache(
                    *agg.input,
                    ctx.clone(),
                    catalog,
                    Arc::clone(&physical_registry),
                    Arc::clone(&trace),
                    Arc::clone(&cte_cache),
                )
                .await?;
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
                    join_type,
                    strategy_hint,
                    build_side,
                    alternatives,
                    ..
                } = join;
                let (left_plan, right_plan, build_side, strategy_label) =
                    choose_adaptive_join_alternative(
                        &left_plan,
                        &right_plan,
                        build_side,
                        &alternatives,
                        &catalog,
                        &ctx,
                    );
                info!(
                    query_id = %trace.query_id,
                    stage_id = trace.stage_id,
                    task_id = trace.task_id,
                    strategy = strategy_label,
                    "hash join adaptive strategy selected"
                );
                let left = execute_plan_with_cache(
                    left_plan,
                    ctx.clone(),
                    catalog.clone(),
                    Arc::clone(&physical_registry),
                    Arc::clone(&trace),
                    Arc::clone(&cte_cache),
                )
                .await?;
                let right = execute_plan_with_cache(
                    right_plan,
                    ctx.clone(),
                    catalog,
                    Arc::clone(&physical_registry),
                    Arc::clone(&trace),
                    Arc::clone(&cte_cache),
                )
                .await?;
                let (l_rows, l_batches, l_bytes) = batch_stats(&left.batches);
                let (r_rows, r_batches, r_bytes) = batch_stats(&right.batches);
                let prefer_sort_merge =
                    matches!(strategy_hint, ffq_planner::JoinStrategyHint::SortMerge)
                        && alternatives.is_empty();
                Ok(OpEval {
                    out: if prefer_sort_merge && matches!(join_type, JoinType::Inner) {
                        run_sort_merge_join(left, right, on, build_side)?
                    } else {
                        run_hash_join(left, right, on, join_type, build_side, &ctx, &trace)?
                    },
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
        let elapsed_secs = started.elapsed().as_secs_f64();
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
            elapsed_secs,
        );
        if let Some(collector) = &stats_collector {
            collector.record_operator(
                &trace.query_id,
                OperatorExecutionStats {
                    stage_id: trace.stage_id,
                    task_id: trace.task_id,
                    operator,
                    rows_in: eval.in_rows,
                    rows_out: out_rows,
                    batches_in: eval.in_batches,
                    batches_out: out_batches,
                    bytes_in: eval.in_bytes,
                    bytes_out: out_bytes,
                    elapsed_ms: elapsed_secs * 1_000.0,
                    partition_sizes_bytes: eval
                        .out
                        .batches
                        .iter()
                        .map(|b| {
                            b.columns()
                                .iter()
                                .map(|a| a.get_array_memory_size() as u64)
                                .sum::<u64>()
                        })
                        .collect(),
                },
            );
        }
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

fn choose_adaptive_join_alternative(
    left: &Box<PhysicalPlan>,
    right: &Box<PhysicalPlan>,
    build_side: BuildSide,
    alternatives: &[ffq_planner::HashJoinAlternativeExec],
    catalog: &Arc<Catalog>,
    ctx: &QueryContext,
) -> (PhysicalPlan, PhysicalPlan, BuildSide, &'static str) {
    if alternatives.is_empty() {
        return ((**left).clone(), (**right).clone(), build_side, "fixed");
    }
    let threshold = ctx.broadcast_threshold_bytes;
    let mut best: Option<(u64, ffq_planner::HashJoinAlternativeExec)> = None;
    for alt in alternatives {
        let build_plan = match alt.build_side {
            BuildSide::Left => &alt.left,
            BuildSide::Right => &alt.right,
        };
        let est = estimate_plan_output_bytes(build_plan, catalog);
        if est <= threshold {
            match &best {
                Some((cur, _)) if *cur <= est => {}
                _ => best = Some((est, alt.clone())),
            }
        }
    }
    if let Some((_est, alt)) = best {
        let label = match alt.strategy_hint {
            ffq_planner::JoinStrategyHint::BroadcastLeft => "adaptive_broadcast_left",
            ffq_planner::JoinStrategyHint::BroadcastRight => "adaptive_broadcast_right",
            ffq_planner::JoinStrategyHint::Shuffle => "adaptive_shuffle",
            ffq_planner::JoinStrategyHint::Auto => "adaptive_auto",
            ffq_planner::JoinStrategyHint::SortMerge => "adaptive_sort_merge",
        };
        return (*alt.left, *alt.right, alt.build_side, label);
    }
    (
        (**left).clone(),
        (**right).clone(),
        build_side,
        "adaptive_fallback_shuffle",
    )
}

fn estimate_plan_output_bytes(plan: &PhysicalPlan, catalog: &Arc<Catalog>) -> u64 {
    match plan {
        PhysicalPlan::ParquetScan(scan) => catalog
            .get(&scan.table)
            .ok()
            .map(|t| {
                let uri_path = std::path::Path::new(&t.uri);
                if let Ok(meta) = std::fs::metadata(uri_path) {
                    return meta.len();
                }
                t.stats.bytes.unwrap_or(u64::MAX / 8)
            })
            .unwrap_or(u64::MAX / 8),
        PhysicalPlan::ParquetWrite(x) => estimate_plan_output_bytes(&x.input, catalog),
        PhysicalPlan::Filter(x) => estimate_plan_output_bytes(&x.input, catalog) / 2,
        PhysicalPlan::InSubqueryFilter(x) => estimate_plan_output_bytes(&x.input, catalog),
        PhysicalPlan::ExistsSubqueryFilter(x) => estimate_plan_output_bytes(&x.input, catalog),
        PhysicalPlan::ScalarSubqueryFilter(x) => estimate_plan_output_bytes(&x.input, catalog),
        PhysicalPlan::Project(x) => estimate_plan_output_bytes(&x.input, catalog),
        PhysicalPlan::Window(x) => estimate_plan_output_bytes(&x.input, catalog),
        PhysicalPlan::CoalesceBatches(x) => estimate_plan_output_bytes(&x.input, catalog),
        PhysicalPlan::PartialHashAggregate(x) => estimate_plan_output_bytes(&x.input, catalog),
        PhysicalPlan::FinalHashAggregate(x) => estimate_plan_output_bytes(&x.input, catalog),
        PhysicalPlan::HashJoin(x) => estimate_plan_output_bytes(&x.left, catalog)
            .saturating_add(estimate_plan_output_bytes(&x.right, catalog)),
        PhysicalPlan::Exchange(ExchangeExec::ShuffleWrite(x)) => {
            estimate_plan_output_bytes(&x.input, catalog)
        }
        PhysicalPlan::Exchange(ExchangeExec::ShuffleRead(x)) => {
            estimate_plan_output_bytes(&x.input, catalog)
        }
        PhysicalPlan::Exchange(ExchangeExec::Broadcast(x)) => {
            estimate_plan_output_bytes(&x.input, catalog)
        }
        PhysicalPlan::Limit(x) => estimate_plan_output_bytes(&x.input, catalog) / 2,
        PhysicalPlan::TopKByScore(x) => estimate_plan_output_bytes(&x.input, catalog) / 2,
        PhysicalPlan::UnionAll(x) => estimate_plan_output_bytes(&x.left, catalog)
            .saturating_add(estimate_plan_output_bytes(&x.right, catalog)),
        PhysicalPlan::CteRef(x) => estimate_plan_output_bytes(&x.plan, catalog),
        PhysicalPlan::VectorTopK(_) | PhysicalPlan::VectorKnn(_) => 64 * 1024,
        PhysicalPlan::Custom(x) => estimate_plan_output_bytes(&x.input, catalog),
    }
}

fn operator_name(plan: &PhysicalPlan) -> &'static str {
    match plan {
        PhysicalPlan::ParquetScan(_) => "ParquetScan",
        PhysicalPlan::ParquetWrite(_) => "ParquetWrite",
        PhysicalPlan::Filter(_) => "Filter",
        PhysicalPlan::InSubqueryFilter(_) => "InSubqueryFilter",
        PhysicalPlan::ExistsSubqueryFilter(_) => "ExistsSubqueryFilter",
        PhysicalPlan::ScalarSubqueryFilter(_) => "ScalarSubqueryFilter",
        PhysicalPlan::Project(_) => "Project",
        PhysicalPlan::Window(_) => "Window",
        PhysicalPlan::CoalesceBatches(_) => "CoalesceBatches",
        PhysicalPlan::PartialHashAggregate(_) => "PartialHashAggregate",
        PhysicalPlan::FinalHashAggregate(_) => "FinalHashAggregate",
        PhysicalPlan::HashJoin(_) => "HashJoin",
        PhysicalPlan::Exchange(ExchangeExec::ShuffleWrite(_)) => "ShuffleWrite",
        PhysicalPlan::Exchange(ExchangeExec::ShuffleRead(_)) => "ShuffleRead",
        PhysicalPlan::Exchange(ExchangeExec::Broadcast(_)) => "Broadcast",
        PhysicalPlan::Limit(_) => "Limit",
        PhysicalPlan::TopKByScore(_) => "TopKByScore",
        PhysicalPlan::UnionAll(_) => "UnionAll",
        PhysicalPlan::CteRef(_) => "CteRef",
        PhysicalPlan::VectorTopK(_) => "VectorTopK",
        PhysicalPlan::VectorKnn(_) => "VectorKnn",
        PhysicalPlan::Custom(_) => "Custom",
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
    Hll(HllSketch),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SpillRow {
    key: Vec<ScalarValue>,
    states: Vec<AggState>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct HllSketch {
    p: u8,
    registers: Vec<u8>,
}

impl HllSketch {
    fn new(p: u8) -> Self {
        let precision = p.clamp(4, 16);
        let m = 1usize << precision;
        Self {
            p: precision,
            registers: vec![0; m],
        }
    }

    fn add_scalar(&mut self, value: &ScalarValue) {
        if matches!(value, ScalarValue::Null) {
            return;
        }
        let mut h = DefaultHasher::new();
        value.hash(&mut h);
        self.add_hash(h.finish());
    }

    fn add_hash(&mut self, hash: u64) {
        let mask = (1_u64 << self.p) - 1;
        let idx = (hash & mask) as usize;
        let w = hash >> self.p;
        let max_rank = (64 - self.p) as u8 + 1;
        let rank = if w == 0 {
            max_rank
        } else {
            (w.trailing_zeros() as u8 + 1).min(max_rank)
        };
        if rank > self.registers[idx] {
            self.registers[idx] = rank;
        }
    }

    fn merge(&mut self, other: &Self) -> Result<()> {
        if self.p != other.p || self.registers.len() != other.registers.len() {
            return Err(FfqError::Execution(
                "incompatible HLL sketch precision".to_string(),
            ));
        }
        for (a, b) in self.registers.iter_mut().zip(other.registers.iter()) {
            *a = (*a).max(*b);
        }
        Ok(())
    }

    fn estimate(&self) -> f64 {
        let m = self.registers.len() as f64;
        let alpha = match self.registers.len() {
            16 => 0.673,
            32 => 0.697,
            64 => 0.709,
            _ => 0.7213 / (1.0 + 1.079 / m),
        };
        let z = self
            .registers
            .iter()
            .map(|r| 2_f64.powi(-(*r as i32)))
            .sum::<f64>();
        let raw = alpha * m * m / z;
        let zeros = self.registers.iter().filter(|r| **r == 0).count() as f64;
        if raw <= 2.5 * m && zeros > 0.0 {
            m * (m / zeros).ln()
        } else {
            raw
        }
    }
}

#[derive(Debug, Clone)]
struct GroupEntry {
    key: Vec<ScalarValue>,
    states: Vec<AggState>,
}

type GroupMap = HashMap<Vec<u8>, GroupEntry>;

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
/// Evaluate `TopKByScoreExec`.
///
/// Input: arbitrary batches + numeric score expression (`Float32`/`Float64`).
/// Output: one batch containing top-k rows in descending score order.
/// Errors: non-numeric score expression evaluation or batch concat failures.
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

fn execute_vector_knn(
    exec: ffq_planner::VectorKnnExec,
    catalog: Arc<Catalog>,
) -> BoxFuture<'static, Result<ExecOutput>> {
    async move {
        let as_topk = ffq_planner::VectorTopKExec {
            table: exec.source.clone(),
            query_vector: exec.query_vector.clone(),
            k: exec.k,
            filter: exec.prefilter.clone(),
        };
        let table = catalog.get(&as_topk.table)?.clone();
        if let Some(rows) = mock_vector_rows_from_table(&table, as_topk.k)? {
            return rows_to_vector_knn_output(rows);
        }
        if table.format != "qdrant" {
            return Err(FfqError::Unsupported(format!(
                "VectorKnnExec requires table format='qdrant', got '{}'",
                table.format
            )));
        }
        #[cfg(not(feature = "qdrant"))]
        {
            let _ = table;
            let _ = as_topk;
            return Err(FfqError::Unsupported(
                "qdrant feature is disabled; build ffq-client with --features qdrant".to_string(),
            ));
        }
        #[cfg(feature = "qdrant")]
        {
            let provider = QdrantProvider::from_table(&table)?;
            let rows = provider
                .topk(
                    as_topk.query_vector.clone(),
                    as_topk.k,
                    as_topk.filter.clone(),
                    VectorQueryOptions {
                        metric: Some(exec.metric.clone()),
                        ef_search: exec.ef_search,
                    },
                )
                .await?;
            rows_to_vector_knn_output(rows)
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
        .topk(
            exec.query_vector.clone(),
            exec.k,
            exec.filter.clone(),
            VectorQueryOptions::default(),
        )
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

fn rows_to_vector_knn_output(
    rows: Vec<ffq_storage::vector_index::VectorTopKRow>,
) -> Result<ExecOutput> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("_score", DataType::Float32, false),
        Field::new("score", DataType::Float32, false),
        Field::new("payload", DataType::Utf8, true),
    ]));
    let mut id_b = Int64Builder::with_capacity(rows.len());
    let mut score_alias_b = arrow::array::Float32Builder::with_capacity(rows.len());
    let mut score_b = arrow::array::Float32Builder::with_capacity(rows.len());
    let mut payload_b = StringBuilder::with_capacity(rows.len(), rows.len() * 16);
    for row in rows {
        id_b.append_value(row.id);
        score_alias_b.append_value(row.score);
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
            Arc::new(score_alias_b.finish()),
            Arc::new(score_b.finish()),
            Arc::new(payload_b.finish()),
        ],
    )
    .map_err(|e| FfqError::Execution(format!("build VectorKnn record batch failed: {e}")))?;
    Ok(ExecOutput {
        schema,
        batches: vec![batch],
    })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct JoinSpillRow {
    row_id: usize,
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

#[derive(Debug)]
struct JoinMatchOutput {
    rows: Vec<Vec<ScalarValue>>,
    matched_left: Vec<bool>,
    matched_right: Vec<bool>,
}

#[derive(Debug, Clone)]
struct JoinBloomFilter {
    bits: Vec<u64>,
    bit_mask: u64,
    hash_count: u8,
}

impl JoinBloomFilter {
    fn new(log2_bits: u8, hash_count: u8) -> Self {
        let eff_bits = log2_bits.clamp(8, 26);
        let bit_count = 1usize << eff_bits;
        let words = bit_count.div_ceil(64);
        Self {
            bits: vec![0_u64; words],
            bit_mask: (bit_count as u64) - 1,
            hash_count: hash_count.max(1),
        }
    }

    fn insert(&mut self, key: &[ScalarValue]) {
        let h1 = hash_key(key);
        let h2 = hash_key_with_seed(key, 0x9e37_79b9_7f4a_7c15);
        for i in 0..self.hash_count {
            let bit = h1.wrapping_add((i as u64).wrapping_mul(h2 | 1)) & self.bit_mask;
            let word = (bit / 64) as usize;
            let offset = (bit % 64) as u32;
            self.bits[word] |= 1_u64 << offset;
        }
    }

    fn may_contain(&self, key: &[ScalarValue]) -> bool {
        let h1 = hash_key(key);
        let h2 = hash_key_with_seed(key, 0x9e37_79b9_7f4a_7c15);
        for i in 0..self.hash_count {
            let bit = h1.wrapping_add((i as u64).wrapping_mul(h2 | 1)) & self.bit_mask;
            let word = (bit / 64) as usize;
            let offset = (bit % 64) as u32;
            if (self.bits[word] & (1_u64 << offset)) == 0 {
                return false;
            }
        }
        true
    }
}

#[cfg_attr(feature = "profiling", inline(never))]
/// Execute `HashJoinExec` with optional spill to grace-hash mode.
///
/// Input: fully materialized left/right child outputs and equi-join keys.
/// Output: one joined batch.
/// - `Inner/Left/Right/Full`: schema `left ++ right`
/// - `Semi/Anti`: schema `left`
/// Spill behavior: when estimated build-side bytes exceed
/// `ctx.mem_budget_bytes`, join partitions are spilled to JSONL and joined
/// partition-wise.
fn run_hash_join(
    left: ExecOutput,
    right: ExecOutput,
    on: Vec<(String, String)>,
    join_type: JoinType,
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

    let output_schema = match join_type {
        JoinType::Semi | JoinType::Anti => left.schema.clone(),
        _ => Arc::new(Schema::new(
            left.schema
                .fields()
                .iter()
                .map(|f| {
                    let nullable = match join_type {
                        JoinType::Right | JoinType::Full => true,
                        JoinType::Inner | JoinType::Left => f.is_nullable(),
                        JoinType::Semi | JoinType::Anti => f.is_nullable(),
                    };
                    f.as_ref().clone().with_nullable(nullable)
                })
                .chain(right.schema.fields().iter().map(|f| {
                    let nullable = match join_type {
                        JoinType::Left | JoinType::Full => true,
                        JoinType::Inner | JoinType::Right => f.is_nullable(),
                        JoinType::Semi | JoinType::Anti => f.is_nullable(),
                    };
                    f.as_ref().clone().with_nullable(nullable)
                }))
                .collect::<Vec<_>>(),
        )),
    };

    let probe_prefilter_storage =
        if matches!(join_type, JoinType::Inner) && ctx.join_bloom_enabled && !build_rows.is_empty()
        {
            let mut bloom = JoinBloomFilter::new(ctx.join_bloom_bits, 3);
            for row in build_rows.iter() {
                let key = join_key_from_row(row, &build_key_idx);
                if !join_key_has_null(&key) {
                    bloom.insert(&key);
                }
            }
            let filtered = probe_rows
                .iter()
                .filter(|row| {
                    let key = join_key_from_row(row, &probe_key_idx);
                    !join_key_has_null(&key) && bloom.may_contain(&key)
                })
                .cloned()
                .collect::<Vec<_>>();
            if filtered.len() < probe_rows.len() {
                let before_rows = probe_rows.len() as u64;
                let after_rows = filtered.len() as u64;
                let before_bytes = estimate_join_rows_bytes(probe_rows) as u64;
                let after_bytes = estimate_join_rows_bytes(&filtered) as u64;
                info!(
                    query_id = %trace.query_id,
                    stage_id = trace.stage_id,
                    task_id = trace.task_id,
                    probe_rows_before = before_rows,
                    probe_rows_after = after_rows,
                    probe_bytes_before = before_bytes,
                    probe_bytes_after = after_bytes,
                    "hash join bloom prefilter reduced probe side"
                );
            }
            Some(filtered)
        } else {
            None
        };
    let probe_rows = probe_prefilter_storage
        .as_ref()
        .map(|v| v.as_slice())
        .unwrap_or(probe_rows);

    let spill_signal = spill_signal_for_ctx(ctx);
    let mut match_output = if ctx.mem_budget_bytes > 0
        && spill_signal.should_spill(estimate_join_rows_bytes(build_rows))
    {
        grace_hash_join(
            build_rows,
            probe_rows,
            &build_key_idx,
            &probe_key_idx,
            build_input_side,
            left_rows.len(),
            right_rows.len(),
            ctx,
            trace,
        )?
    } else {
        if ctx.join_radix_bits > 0 {
            if let (Some(build_int_idx), Some(probe_int_idx)) = (
                single_int64_join_key_index(build_rows, &build_key_idx),
                single_int64_join_key_index(probe_rows, &probe_key_idx),
            ) {
                in_memory_radix_hash_join_i64(
                    build_rows,
                    probe_rows,
                    build_int_idx,
                    probe_int_idx,
                    build_input_side,
                    left_rows.len(),
                    right_rows.len(),
                    ctx.join_radix_bits,
                )
            } else {
                in_memory_radix_hash_join(
                    build_rows,
                    probe_rows,
                    &build_key_idx,
                    &probe_key_idx,
                    build_input_side,
                    left_rows.len(),
                    right_rows.len(),
                    ctx.join_radix_bits,
                )
            }
        } else {
            in_memory_hash_join(
                build_rows,
                probe_rows,
                &build_key_idx,
                &probe_key_idx,
                build_input_side,
                left_rows.len(),
                right_rows.len(),
            )
        }
    };

    if matches!(join_type, JoinType::Semi | JoinType::Anti) {
        match_output.rows = left_rows
            .iter()
            .enumerate()
            .filter_map(|(idx, row)| {
                let keep = match join_type {
                    JoinType::Semi => match_output.matched_left[idx],
                    JoinType::Anti => !match_output.matched_left[idx],
                    _ => false,
                };
                keep.then(|| row.clone())
            })
            .collect();
    } else {
        apply_outer_join_null_extension(
            &mut match_output.rows,
            &match_output.matched_left,
            &match_output.matched_right,
            &left_rows,
            &right_rows,
            join_type,
        );
    }

    let batch = rows_to_batch(&output_schema, &match_output.rows)?;
    Ok(ExecOutput {
        schema: output_schema,
        batches: vec![batch],
    })
}

fn run_sort_merge_join(
    left: ExecOutput,
    right: ExecOutput,
    on: Vec<(String, String)>,
    build_side: BuildSide,
) -> Result<ExecOutput> {
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

    let mut build_sorted = build_rows
        .iter()
        .enumerate()
        .filter_map(|(idx, row)| {
            let key = join_key_from_row(row, &build_key_idx);
            (!join_key_has_null(&key)).then_some((idx, key))
        })
        .collect::<Vec<_>>();
    let mut probe_sorted = probe_rows
        .iter()
        .enumerate()
        .filter_map(|(idx, row)| {
            let key = join_key_from_row(row, &probe_key_idx);
            (!join_key_has_null(&key)).then_some((idx, key))
        })
        .collect::<Vec<_>>();
    build_sorted.sort_by(|a, b| cmp_join_keys(&a.1, &b.1));
    probe_sorted.sort_by(|a, b| cmp_join_keys(&a.1, &b.1));

    let mut out_rows = Vec::new();
    let mut i = 0usize;
    let mut j = 0usize;
    while i < build_sorted.len() && j < probe_sorted.len() {
        let ord = cmp_join_keys(&build_sorted[i].1, &probe_sorted[j].1);
        if ord == Ordering::Less {
            i += 1;
            continue;
        }
        if ord == Ordering::Greater {
            j += 1;
            continue;
        }

        let i_start = i;
        let j_start = j;
        while i < build_sorted.len()
            && cmp_join_keys(&build_sorted[i_start].1, &build_sorted[i].1) == Ordering::Equal
        {
            i += 1;
        }
        while j < probe_sorted.len()
            && cmp_join_keys(&probe_sorted[j_start].1, &probe_sorted[j].1) == Ordering::Equal
        {
            j += 1;
        }

        for (build_row_idx, _) in &build_sorted[i_start..i] {
            for (probe_row_idx, _) in &probe_sorted[j_start..j] {
                out_rows.push(combine_join_rows(
                    &build_rows[*build_row_idx],
                    &probe_rows[*probe_row_idx],
                    build_input_side,
                ));
            }
        }
    }

    let output_schema = Arc::new(Schema::new(
        left.schema
            .fields()
            .iter()
            .chain(right.schema.fields().iter())
            .map(|f| (**f).clone())
            .collect::<Vec<_>>(),
    ));
    let batch = rows_to_batch(&output_schema, &out_rows)?;
    Ok(ExecOutput {
        schema: output_schema,
        batches: vec![batch],
    })
}

fn single_int64_join_key_index(rows: &[Vec<ScalarValue>], key_idx: &[usize]) -> Option<usize> {
    if key_idx.len() != 1 {
        return None;
    }
    let idx = key_idx[0];
    if rows.iter().all(|row| {
        matches!(
            row.get(idx),
            Some(ScalarValue::Int64(_) | ScalarValue::Null)
        )
    }) {
        Some(idx)
    } else {
        None
    }
}

fn apply_outer_join_null_extension(
    out_rows: &mut Vec<Vec<ScalarValue>>,
    matched_left: &[bool],
    matched_right: &[bool],
    left_rows: &[Vec<ScalarValue>],
    right_rows: &[Vec<ScalarValue>],
    join_type: JoinType,
) {
    let left_nulls = vec![ScalarValue::Null; left_rows.first().map_or(0, Vec::len)];
    let right_nulls = vec![ScalarValue::Null; right_rows.first().map_or(0, Vec::len)];
    match join_type {
        JoinType::Inner => {}
        JoinType::Semi | JoinType::Anti => {}
        JoinType::Left => {
            for (idx, left) in left_rows.iter().enumerate() {
                if !matched_left[idx] {
                    out_rows.push(
                        left.iter()
                            .cloned()
                            .chain(right_nulls.iter().cloned())
                            .collect(),
                    );
                }
            }
        }
        JoinType::Right => {
            for (idx, right) in right_rows.iter().enumerate() {
                if !matched_right[idx] {
                    out_rows.push(
                        left_nulls
                            .iter()
                            .cloned()
                            .chain(right.iter().cloned())
                            .collect(),
                    );
                }
            }
        }
        JoinType::Full => {
            for (idx, left) in left_rows.iter().enumerate() {
                if !matched_left[idx] {
                    out_rows.push(
                        left.iter()
                            .cloned()
                            .chain(right_nulls.iter().cloned())
                            .collect(),
                    );
                }
            }
            for (idx, right) in right_rows.iter().enumerate() {
                if !matched_right[idx] {
                    out_rows.push(
                        left_nulls
                            .iter()
                            .cloned()
                            .chain(right.iter().cloned())
                            .collect(),
                    );
                }
            }
        }
    }
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

#[cfg(test)]
fn run_window_exec(input: ExecOutput, exprs: &[WindowExpr]) -> Result<ExecOutput> {
    let default_ctx = QueryContext {
        batch_size_rows: 8192,
        mem_budget_bytes: usize::MAX,
        spill_trigger_ratio_num: 1,
        spill_trigger_ratio_den: 1,
        broadcast_threshold_bytes: u64::MAX,
        join_radix_bits: 8,
        join_bloom_enabled: true,
        join_bloom_bits: 20,
        spill_dir: "./ffq_spill".to_string(),
        stats_collector: None,
    };
    run_window_exec_with_ctx(input, exprs, &default_ctx, None)
}

fn run_window_exec_with_ctx(
    input: ExecOutput,
    exprs: &[WindowExpr],
    ctx: &QueryContext,
    trace: Option<&TraceIds>,
) -> Result<ExecOutput> {
    let row_count = input.batches.iter().map(|b| b.num_rows()).sum::<usize>();
    let mut eval_ctx_cache: HashMap<String, WindowEvalContext> = HashMap::new();
    let mut out_fields: Vec<Field> = input
        .schema
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    let mut out_columns: Vec<ArrayRef> = if input.batches.is_empty() {
        RecordBatch::new_empty(input.schema.clone())
            .columns()
            .to_vec()
    } else if input.batches.len() == 1 {
        input.batches[0].columns().to_vec()
    } else {
        concat_batches(&input.schema, &input.batches)
            .map_err(|e| FfqError::Execution(format!("window concat batches failed: {e}")))?
            .columns()
            .to_vec()
    };
    for (window_idx, w) in exprs.iter().enumerate() {
        let cache_key = window_compatibility_key(w);
        if !eval_ctx_cache.contains_key(&cache_key) {
            eval_ctx_cache.insert(cache_key.clone(), build_window_eval_context(&input, w)?);
        }
        let dt = window_output_type(&input.schema, w)?;
        let output = evaluate_window_expr_spill_aware(
            &input,
            w,
            eval_ctx_cache
                .get(&cache_key)
                .expect("window eval ctx must exist"),
            &dt,
            ctx,
            trace,
            window_idx,
        )?;
        if output.len() != row_count {
            return Err(FfqError::Execution(format!(
                "window output row count mismatch: expected {row_count}, got {}",
                output.len()
            )));
        }
        out_fields.push(Field::new(&w.output_name, dt, window_output_nullable(w)));
        out_columns.push(
            scalars_to_array(&output, out_fields.last().expect("field").data_type()).map_err(
                |e| {
                    FfqError::Execution(format!(
                        "window output column '{}' build failed: {e}",
                        w.output_name
                    ))
                },
            )?,
        );
    }
    let out_schema = Arc::new(Schema::new(out_fields));
    let batch = RecordBatch::try_new(out_schema.clone(), out_columns)
        .map_err(|e| FfqError::Execution(format!("window output batch failed: {e}")))?;
    Ok(ExecOutput {
        schema: out_schema,
        batches: vec![batch],
    })
}

fn evaluate_window_expr_spill_aware(
    input: &ExecOutput,
    w: &WindowExpr,
    eval_ctx: &WindowEvalContext,
    output_type: &DataType,
    ctx: &QueryContext,
    trace: Option<&TraceIds>,
    window_idx: usize,
) -> Result<Vec<ScalarValue>> {
    let row_count = input.batches.iter().map(|b| b.num_rows()).sum::<usize>();
    let estimated = estimate_window_eval_context_bytes(eval_ctx)
        + estimate_window_output_bytes(row_count, output_type);
    let spill_signal = spill_signal_for_ctx(ctx);
    if ctx.mem_budget_bytes == 0 || !spill_signal.should_spill(estimated) {
        return evaluate_window_expr_with_ctx(input, w, eval_ctx);
    }

    let spill_started = Instant::now();
    fs::create_dir_all(&ctx.spill_dir)?;
    let spill_path = window_spill_path(&ctx.spill_dir, trace, window_idx, &w.output_name);
    let output = evaluate_window_expr_with_ctx(input, w, eval_ctx)?;
    write_window_spill_file(&spill_path, &output)?;
    let spill_bytes = fs::metadata(&spill_path).map(|m| m.len()).unwrap_or(0);
    if let Some(t) = trace {
        global_metrics().record_spill(
            &t.query_id,
            t.stage_id,
            t.task_id,
            "window",
            spill_bytes,
            spill_started.elapsed().as_secs_f64(),
        );
    }
    let restored = read_window_spill_file(&spill_path)?;
    let _ = fs::remove_file(&spill_path);
    Ok(restored)
}

#[derive(Debug, Clone)]
struct WindowEvalContext {
    order_keys: Vec<Vec<ScalarValue>>,
    order_idx: Vec<usize>,
    partitions: Vec<(usize, usize)>,
}

fn window_compatibility_key(w: &WindowExpr) -> String {
    let partition_sig = w
        .partition_by
        .iter()
        .map(|e| format!("{e:?}"))
        .collect::<Vec<_>>()
        .join("|");
    let order_sig = w
        .order_by
        .iter()
        .map(|o| format!("{:?}:{}:{}", o.expr, o.asc, o.nulls_first))
        .collect::<Vec<_>>()
        .join("|");
    format!("P[{partition_sig}]O[{order_sig}]")
}

fn estimate_window_eval_context_bytes(eval_ctx: &WindowEvalContext) -> usize {
    let order_keys = eval_ctx
        .order_keys
        .iter()
        .map(|col| col.iter().map(scalar_estimate_bytes).sum::<usize>())
        .sum::<usize>();
    let order_idx = eval_ctx.order_idx.len() * std::mem::size_of::<usize>();
    let partitions = eval_ctx.partitions.len() * (std::mem::size_of::<usize>() * 2);
    order_keys + order_idx + partitions
}

fn estimate_window_output_bytes(row_count: usize, dt: &DataType) -> usize {
    let per_row = match dt {
        DataType::Int64 | DataType::Float64 => 8,
        DataType::Boolean => 1,
        DataType::Utf8 => 24,
        DataType::FixedSizeList(_, len) => (*len as usize) * 4,
        _ => 16,
    };
    row_count.saturating_mul(per_row)
}

fn sanitize_spill_component(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    if out.is_empty() { "_".to_string() } else { out }
}

fn window_spill_path(
    spill_dir: &str,
    trace: Option<&TraceIds>,
    window_idx: usize,
    output_name: &str,
) -> PathBuf {
    let (query_id, stage_id, task_id) = match trace {
        Some(t) => (t.query_id.as_str(), t.stage_id, t.task_id),
        None => ("local", 0, 0),
    };
    PathBuf::from(spill_dir).join(format!(
        "window_spill_q{}_s{}_t{}_w{:04}_{}.jsonl",
        sanitize_spill_component(query_id),
        stage_id,
        task_id,
        window_idx,
        sanitize_spill_component(output_name),
    ))
}

fn write_window_spill_file(path: &PathBuf, values: &[ScalarValue]) -> Result<()> {
    let file = File::create(path)?;
    let mut writer = BufWriter::new(file);
    for value in values {
        let line = serde_json::to_string(value)
            .map_err(|e| FfqError::Execution(format!("window spill serialize failed: {e}")))?;
        writer
            .write_all(line.as_bytes())
            .map_err(|e| FfqError::Execution(format!("window spill write failed: {e}")))?;
        writer
            .write_all(b"\n")
            .map_err(|e| FfqError::Execution(format!("window spill write failed: {e}")))?;
    }
    writer
        .flush()
        .map_err(|e| FfqError::Execution(format!("window spill flush failed: {e}")))?;
    Ok(())
}

fn read_window_spill_file(path: &PathBuf) -> Result<Vec<ScalarValue>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut out = Vec::new();
    for line in reader.lines() {
        let line =
            line.map_err(|e| FfqError::Execution(format!("window spill read failed: {e}")))?;
        let value = serde_json::from_str::<ScalarValue>(&line)
            .map_err(|e| FfqError::Execution(format!("window spill deserialize failed: {e}")))?;
        out.push(value);
    }
    Ok(out)
}

fn build_window_eval_context(input: &ExecOutput, w: &WindowExpr) -> Result<WindowEvalContext> {
    let row_count = input.batches.iter().map(|b| b.num_rows()).sum::<usize>();
    let partition_keys = w
        .partition_by
        .iter()
        .map(|e| evaluate_expr_rows(input, e))
        .collect::<Result<Vec<_>>>()?;
    let order_keys = w
        .order_by
        .iter()
        .map(|o| evaluate_expr_rows(input, &o.expr))
        .collect::<Result<Vec<_>>>()?;
    let fallback_keys = build_stable_row_fallback_keys(input)?;
    let mut order_idx: Vec<usize> = (0..row_count).collect();
    order_idx.sort_by(|a, b| {
        cmp_key_sets(&partition_keys, *a, *b)
            .then_with(|| cmp_order_key_sets(&order_keys, &w.order_by, *a, *b))
            .then_with(|| fallback_keys[*a].cmp(&fallback_keys[*b]))
            .then_with(|| a.cmp(b))
    });
    let partitions = partition_ranges(&order_idx, &partition_keys);
    Ok(WindowEvalContext {
        order_keys,
        order_idx,
        partitions,
    })
}

fn evaluate_window_expr_with_ctx(
    input: &ExecOutput,
    w: &WindowExpr,
    eval_ctx: &WindowEvalContext,
) -> Result<Vec<ScalarValue>> {
    let row_count = input.batches.iter().map(|b| b.num_rows()).sum::<usize>();
    let mut out = vec![ScalarValue::Null; row_count];
    let frame = effective_window_frame(w);
    match &w.func {
        WindowFunction::RowNumber => {
            for (start, end) in &eval_ctx.partitions {
                for (offset, pos) in eval_ctx.order_idx[*start..*end].iter().enumerate() {
                    out[*pos] = ScalarValue::Int64((offset + 1) as i64);
                }
            }
        }
        WindowFunction::Rank => {
            for (start, end) in &eval_ctx.partitions {
                let part = &eval_ctx.order_idx[*start..*end];
                let mut rank = 1_i64;
                let mut part_i = 0usize;
                while part_i < part.len() {
                    if part_i > 0
                        && cmp_order_key_sets(
                            &eval_ctx.order_keys,
                            &w.order_by,
                            part[part_i - 1],
                            part[part_i],
                        ) != Ordering::Equal
                    {
                        rank = (part_i as i64) + 1;
                    }
                    out[part[part_i]] = ScalarValue::Int64(rank);
                    part_i += 1;
                }
            }
        }
        WindowFunction::DenseRank => {
            for (start, end) in &eval_ctx.partitions {
                let part = &eval_ctx.order_idx[*start..*end];
                let mut rank = 1_i64;
                let mut part_i = 0usize;
                while part_i < part.len() {
                    if part_i > 0
                        && cmp_order_key_sets(
                            &eval_ctx.order_keys,
                            &w.order_by,
                            part[part_i - 1],
                            part[part_i],
                        ) != Ordering::Equal
                    {
                        rank += 1;
                    }
                    out[part[part_i]] = ScalarValue::Int64(rank);
                    part_i += 1;
                }
            }
        }
        WindowFunction::PercentRank => {
            for (start, end) in &eval_ctx.partitions {
                let part = &eval_ctx.order_idx[*start..*end];
                let n = part.len();
                if n <= 1 {
                    for pos in part {
                        out[*pos] = ScalarValue::Float64Bits(0.0_f64.to_bits());
                    }
                    continue;
                }
                let mut rank = 1_i64;
                for part_i in 0..part.len() {
                    if part_i > 0
                        && cmp_order_key_sets(
                            &eval_ctx.order_keys,
                            &w.order_by,
                            part[part_i - 1],
                            part[part_i],
                        ) != Ordering::Equal
                    {
                        rank = (part_i as i64) + 1;
                    }
                    let pct = (rank as f64 - 1.0_f64) / ((n as f64) - 1.0_f64);
                    out[part[part_i]] = ScalarValue::Float64Bits(pct.to_bits());
                }
            }
        }
        WindowFunction::CumeDist => {
            for (start, end) in &eval_ctx.partitions {
                let part = &eval_ctx.order_idx[*start..*end];
                let n = part.len() as f64;
                let mut i = 0usize;
                while i < part.len() {
                    let tie_start = i;
                    i += 1;
                    while i < part.len()
                        && cmp_order_key_sets(
                            &eval_ctx.order_keys,
                            &w.order_by,
                            part[tie_start],
                            part[i],
                        ) == Ordering::Equal
                    {
                        i += 1;
                    }
                    let cume = (i as f64) / n;
                    for pos in &part[tie_start..i] {
                        out[*pos] = ScalarValue::Float64Bits(cume.to_bits());
                    }
                }
            }
        }
        WindowFunction::Ntile(buckets) => {
            for (start, end) in &eval_ctx.partitions {
                let part = &eval_ctx.order_idx[*start..*end];
                let n_rows = part.len();
                let n_buckets = *buckets;
                for (i, pos) in part.iter().enumerate() {
                    let tile = ((i * n_buckets) / n_rows) + 1;
                    out[*pos] = ScalarValue::Int64(tile as i64);
                }
            }
        }
        WindowFunction::Count(arg) => {
            let values = evaluate_expr_rows(input, arg)?;
            for (start, end) in &eval_ctx.partitions {
                let part = &eval_ctx.order_idx[*start..*end];
                let part_ctx = build_partition_frame_ctx(part, &eval_ctx.order_keys, &w.order_by)?;
                for i in 0..part.len() {
                    let (fs, fe) = resolve_frame_range(&frame, i, part, &part_ctx)?;
                    let mut cnt = 0_i64;
                    for pos in filtered_frame_positions(&frame, &part_ctx, part, fs, fe, i) {
                        if !matches!(values[*pos], ScalarValue::Null) {
                            cnt += 1;
                        }
                    }
                    out[part[i]] = ScalarValue::Int64(cnt);
                }
            }
        }
        WindowFunction::Sum(arg) => {
            let values = evaluate_expr_rows(input, arg)?;
            for (start, end) in &eval_ctx.partitions {
                let part = &eval_ctx.order_idx[*start..*end];
                let part_ctx = build_partition_frame_ctx(part, &eval_ctx.order_keys, &w.order_by)?;
                for i in 0..part.len() {
                    let (fs, fe) = resolve_frame_range(&frame, i, part, &part_ctx)?;
                    let mut sum = 0.0_f64;
                    let mut seen = false;
                    for pos in filtered_frame_positions(&frame, &part_ctx, part, fs, fe, i) {
                        match &values[*pos] {
                            ScalarValue::Int64(v) => {
                                sum += *v as f64;
                                seen = true;
                            }
                            ScalarValue::Float64Bits(v) => {
                                sum += f64::from_bits(*v);
                                seen = true;
                            }
                            ScalarValue::Null => {}
                            other => {
                                return Err(FfqError::Execution(format!(
                                    "SUM() OVER encountered non-numeric value: {other:?}"
                                )));
                            }
                        }
                    }
                    out[part[i]] = if seen {
                        ScalarValue::Float64Bits(sum.to_bits())
                    } else {
                        ScalarValue::Null
                    };
                }
            }
        }
        WindowFunction::Avg(arg) => {
            let values = evaluate_expr_rows(input, arg)?;
            for (start, end) in &eval_ctx.partitions {
                let part = &eval_ctx.order_idx[*start..*end];
                let part_ctx = build_partition_frame_ctx(part, &eval_ctx.order_keys, &w.order_by)?;
                for i in 0..part.len() {
                    let (fs, fe) = resolve_frame_range(&frame, i, part, &part_ctx)?;
                    let mut sum = 0.0_f64;
                    let mut count = 0_i64;
                    for pos in filtered_frame_positions(&frame, &part_ctx, part, fs, fe, i) {
                        if let Some(v) = scalar_to_f64(&values[*pos]) {
                            sum += v;
                            count += 1;
                        } else if !matches!(values[*pos], ScalarValue::Null) {
                            return Err(FfqError::Execution(format!(
                                "AVG() OVER encountered non-numeric value: {:?}",
                                values[*pos]
                            )));
                        }
                    }
                    out[part[i]] = if count > 0 {
                        ScalarValue::Float64Bits((sum / count as f64).to_bits())
                    } else {
                        ScalarValue::Null
                    };
                }
            }
        }
        WindowFunction::Min(arg) => {
            let values = evaluate_expr_rows(input, arg)?;
            for (start, end) in &eval_ctx.partitions {
                let part = &eval_ctx.order_idx[*start..*end];
                let part_ctx = build_partition_frame_ctx(part, &eval_ctx.order_keys, &w.order_by)?;
                for i in 0..part.len() {
                    let (fs, fe) = resolve_frame_range(&frame, i, part, &part_ctx)?;
                    let mut current: Option<ScalarValue> = None;
                    for pos in filtered_frame_positions(&frame, &part_ctx, part, fs, fe, i) {
                        let v = values[*pos].clone();
                        if matches!(v, ScalarValue::Null) {
                            continue;
                        }
                        current = match current {
                            None => Some(v),
                            Some(existing) => {
                                if cmp_scalar_for_window(&v, &existing, false, false)
                                    == Ordering::Less
                                {
                                    Some(v)
                                } else {
                                    Some(existing)
                                }
                            }
                        };
                    }
                    out[part[i]] = current.unwrap_or(ScalarValue::Null);
                }
            }
        }
        WindowFunction::Max(arg) => {
            let values = evaluate_expr_rows(input, arg)?;
            for (start, end) in &eval_ctx.partitions {
                let part = &eval_ctx.order_idx[*start..*end];
                let part_ctx = build_partition_frame_ctx(part, &eval_ctx.order_keys, &w.order_by)?;
                for i in 0..part.len() {
                    let (fs, fe) = resolve_frame_range(&frame, i, part, &part_ctx)?;
                    let mut current: Option<ScalarValue> = None;
                    for pos in filtered_frame_positions(&frame, &part_ctx, part, fs, fe, i) {
                        let v = values[*pos].clone();
                        if matches!(v, ScalarValue::Null) {
                            continue;
                        }
                        current = match current {
                            None => Some(v),
                            Some(existing) => {
                                if cmp_scalar_for_window(&v, &existing, false, false)
                                    == Ordering::Greater
                                {
                                    Some(v)
                                } else {
                                    Some(existing)
                                }
                            }
                        };
                    }
                    out[part[i]] = current.unwrap_or(ScalarValue::Null);
                }
            }
        }
        WindowFunction::Lag {
            expr,
            offset,
            default,
        } => {
            let values = evaluate_expr_rows(input, expr)?;
            let defaults = default
                .as_ref()
                .map(|d| evaluate_expr_rows(input, d))
                .transpose()?;
            for (start, end) in &eval_ctx.partitions {
                let part = &eval_ctx.order_idx[*start..*end];
                for (i, pos) in part.iter().enumerate() {
                    out[*pos] = if i >= *offset {
                        values[part[i - *offset]].clone()
                    } else if let Some(d) = &defaults {
                        d[*pos].clone()
                    } else {
                        ScalarValue::Null
                    };
                }
            }
        }
        WindowFunction::Lead {
            expr,
            offset,
            default,
        } => {
            let values = evaluate_expr_rows(input, expr)?;
            let defaults = default
                .as_ref()
                .map(|d| evaluate_expr_rows(input, d))
                .transpose()?;
            for (start, end) in &eval_ctx.partitions {
                let part = &eval_ctx.order_idx[*start..*end];
                for (i, pos) in part.iter().enumerate() {
                    out[*pos] = if i + *offset < part.len() {
                        values[part[i + *offset]].clone()
                    } else if let Some(d) = &defaults {
                        d[*pos].clone()
                    } else {
                        ScalarValue::Null
                    };
                }
            }
        }
        WindowFunction::FirstValue(expr) => {
            let values = evaluate_expr_rows(input, expr)?;
            for (start, end) in &eval_ctx.partitions {
                let part = &eval_ctx.order_idx[*start..*end];
                let part_ctx = build_partition_frame_ctx(part, &eval_ctx.order_keys, &w.order_by)?;
                for i in 0..part.len() {
                    let (fs, fe) = resolve_frame_range(&frame, i, part, &part_ctx)?;
                    out[part[i]] = if fs < fe {
                        first_in_filtered_frame(&frame, &part_ctx, part, fs, fe, i)
                            .map(|p| values[p].clone())
                            .unwrap_or(ScalarValue::Null)
                    } else {
                        ScalarValue::Null
                    };
                }
            }
        }
        WindowFunction::LastValue(expr) => {
            let values = evaluate_expr_rows(input, expr)?;
            for (start, end) in &eval_ctx.partitions {
                let part = &eval_ctx.order_idx[*start..*end];
                let part_ctx = build_partition_frame_ctx(part, &eval_ctx.order_keys, &w.order_by)?;
                for i in 0..part.len() {
                    let (fs, fe) = resolve_frame_range(&frame, i, part, &part_ctx)?;
                    out[part[i]] = if fs < fe {
                        last_in_filtered_frame(&frame, &part_ctx, part, fs, fe, i)
                            .map(|p| values[p].clone())
                            .unwrap_or(ScalarValue::Null)
                    } else {
                        ScalarValue::Null
                    };
                }
            }
        }
        WindowFunction::NthValue { expr, n } => {
            let values = evaluate_expr_rows(input, expr)?;
            for (start, end) in &eval_ctx.partitions {
                let part = &eval_ctx.order_idx[*start..*end];
                let part_ctx = build_partition_frame_ctx(part, &eval_ctx.order_keys, &w.order_by)?;
                for i in 0..part.len() {
                    let (fs, fe) = resolve_frame_range(&frame, i, part, &part_ctx)?;
                    let filtered = filtered_frame_positions(&frame, &part_ctx, part, fs, fe, i);
                    out[part[i]] = if *n == 0 || *n > filtered.len() {
                        ScalarValue::Null
                    } else {
                        values[*filtered[*n - 1]].clone()
                    };
                }
            }
        }
    }
    Ok(out)
}

#[derive(Debug, Clone)]
struct PartitionFrameCtx {
    peer_groups: Vec<(usize, usize)>,
    row_group: Vec<usize>,
    normalized_first_key: Option<Vec<Option<f64>>>,
    order_key_count: usize,
}

fn build_partition_frame_ctx(
    part: &[usize],
    order_keys: &[Vec<ScalarValue>],
    order_exprs: &[WindowOrderExpr],
) -> Result<PartitionFrameCtx> {
    let (peer_groups, row_group) = build_peer_groups(part, order_keys, order_exprs);
    let normalized_first_key = if order_keys.is_empty() {
        None
    } else {
        Some(
            part.iter()
                .map(|row| {
                    scalar_to_f64(&order_keys[0][*row])
                        .map(|v| if order_exprs[0].asc { v } else { -v })
                })
                .collect(),
        )
    };
    Ok(PartitionFrameCtx {
        peer_groups,
        row_group,
        normalized_first_key,
        order_key_count: order_keys.len(),
    })
}

fn build_peer_groups(
    part: &[usize],
    order_keys: &[Vec<ScalarValue>],
    order_exprs: &[WindowOrderExpr],
) -> (Vec<(usize, usize)>, Vec<usize>) {
    if part.is_empty() {
        return (Vec::new(), Vec::new());
    }
    let mut groups = Vec::new();
    let mut row_group = vec![0usize; part.len()];
    let mut i = 0usize;
    while i < part.len() {
        let start = i;
        i += 1;
        while i < part.len()
            && cmp_order_key_sets(order_keys, order_exprs, part[start], part[i]) == Ordering::Equal
        {
            i += 1;
        }
        let gidx = groups.len();
        for rg in &mut row_group[start..i] {
            *rg = gidx;
        }
        groups.push((start, i));
    }
    (groups, row_group)
}

fn effective_window_frame(w: &WindowExpr) -> WindowFrameSpec {
    if let Some(f) = &w.frame {
        return f.clone();
    }
    if w.order_by.is_empty() {
        WindowFrameSpec {
            units: WindowFrameUnits::Rows,
            start_bound: WindowFrameBound::UnboundedPreceding,
            end_bound: WindowFrameBound::UnboundedFollowing,
            exclusion: WindowFrameExclusion::NoOthers,
        }
    } else {
        WindowFrameSpec {
            units: WindowFrameUnits::Range,
            start_bound: WindowFrameBound::UnboundedPreceding,
            end_bound: WindowFrameBound::CurrentRow,
            exclusion: WindowFrameExclusion::NoOthers,
        }
    }
}

fn resolve_frame_range(
    frame: &WindowFrameSpec,
    row_idx: usize,
    part: &[usize],
    ctx: &PartitionFrameCtx,
) -> Result<(usize, usize)> {
    match frame.units {
        WindowFrameUnits::Rows => resolve_rows_frame(frame, row_idx, part.len()),
        WindowFrameUnits::Groups => resolve_groups_frame(frame, row_idx, ctx),
        WindowFrameUnits::Range => resolve_range_frame(frame, row_idx, part.len(), ctx),
    }
}

fn resolve_rows_frame(
    frame: &WindowFrameSpec,
    row_idx: usize,
    part_len: usize,
) -> Result<(usize, usize)> {
    let start = rows_bound_to_raw_index(&frame.start_bound, row_idx, part_len, true)?;
    let end = rows_bound_to_raw_index(&frame.end_bound, row_idx, part_len, false)?;
    if end < start {
        return Ok((0, 0));
    }
    Ok((start as usize, (end as usize) + 1))
}

fn rows_bound_to_raw_index(
    bound: &WindowFrameBound,
    row_idx: usize,
    part_len: usize,
    is_start: bool,
) -> Result<i64> {
    let last = (part_len as i64) - 1;
    let raw = match bound {
        WindowFrameBound::UnboundedPreceding => 0,
        WindowFrameBound::Preceding(n) => row_idx as i64 - (*n as i64),
        WindowFrameBound::CurrentRow => row_idx as i64,
        WindowFrameBound::Following(n) => row_idx as i64 + (*n as i64),
        WindowFrameBound::UnboundedFollowing => last,
    };
    if is_start {
        Ok(raw.clamp(0, part_len as i64))
    } else {
        Ok(raw.clamp(-1, last))
    }
}

fn resolve_groups_frame(
    frame: &WindowFrameSpec,
    row_idx: usize,
    ctx: &PartitionFrameCtx,
) -> Result<(usize, usize)> {
    let gcur = ctx.row_group[row_idx] as i64;
    let glen = ctx.peer_groups.len() as i64;
    let start_g = match frame.start_bound {
        WindowFrameBound::UnboundedPreceding => 0,
        WindowFrameBound::Preceding(n) => (gcur - n as i64).clamp(0, glen),
        WindowFrameBound::CurrentRow => gcur,
        WindowFrameBound::Following(n) => (gcur + n as i64).clamp(0, glen),
        WindowFrameBound::UnboundedFollowing => glen,
    };
    let end_g = match frame.end_bound {
        WindowFrameBound::UnboundedPreceding => -1,
        WindowFrameBound::Preceding(n) => (gcur - n as i64).clamp(-1, glen - 1),
        WindowFrameBound::CurrentRow => gcur,
        WindowFrameBound::Following(n) => (gcur + n as i64).clamp(-1, glen - 1),
        WindowFrameBound::UnboundedFollowing => glen - 1,
    };
    if end_g < start_g {
        return Ok((0, 0));
    }
    let start = ctx.peer_groups[start_g as usize].0;
    let end = ctx.peer_groups[end_g as usize].1;
    Ok((start, end))
}

fn resolve_range_frame(
    frame: &WindowFrameSpec,
    row_idx: usize,
    part_len: usize,
    ctx: &PartitionFrameCtx,
) -> Result<(usize, usize)> {
    let uses_offset = matches!(
        frame.start_bound,
        WindowFrameBound::Preceding(_) | WindowFrameBound::Following(_)
    ) || matches!(
        frame.end_bound,
        WindowFrameBound::Preceding(_) | WindowFrameBound::Following(_)
    );

    if !uses_offset {
        let start = match frame.start_bound {
            WindowFrameBound::UnboundedPreceding => 0,
            WindowFrameBound::CurrentRow => {
                let g = ctx.row_group[row_idx];
                ctx.peer_groups[g].0
            }
            _ => {
                return Err(FfqError::Planning(
                    "unsupported RANGE frame start bound".to_string(),
                ));
            }
        };
        let end = match frame.end_bound {
            WindowFrameBound::CurrentRow => {
                let g = ctx.row_group[row_idx];
                ctx.peer_groups[g].1
            }
            WindowFrameBound::UnboundedFollowing => part_len,
            _ => {
                return Err(FfqError::Planning(
                    "unsupported RANGE frame end bound".to_string(),
                ));
            }
        };
        if end < start {
            return Ok((0, 0));
        }
        return Ok((start, end));
    }

    let keys = ctx.normalized_first_key.as_ref().ok_or_else(|| {
        FfqError::Planning("RANGE frame requires one numeric ORDER BY expression".to_string())
    })?;
    if ctx.order_key_count != 1 {
        return Err(FfqError::Planning(
            "RANGE frame with offset currently requires exactly one ORDER BY expression"
                .to_string(),
        ));
    }
    let cur = keys[row_idx].ok_or_else(|| {
        FfqError::Execution(
            "RANGE frame with offset requires non-null numeric ORDER BY value".to_string(),
        )
    })?;

    let lower = match frame.start_bound {
        WindowFrameBound::UnboundedPreceding => None,
        WindowFrameBound::Preceding(n) => Some(cur - (n as f64)),
        WindowFrameBound::CurrentRow => Some(cur),
        WindowFrameBound::Following(n) => Some(cur + (n as f64)),
        WindowFrameBound::UnboundedFollowing => Some(f64::INFINITY),
    };
    let upper = match frame.end_bound {
        WindowFrameBound::UnboundedFollowing => None,
        WindowFrameBound::Following(n) => Some(cur + (n as f64)),
        WindowFrameBound::CurrentRow => Some(cur),
        WindowFrameBound::Preceding(n) => Some(cur - (n as f64)),
        WindowFrameBound::UnboundedPreceding => Some(f64::NEG_INFINITY),
    };

    let mut start = part_len;
    let mut end = 0usize;
    for (i, kv) in keys.iter().enumerate() {
        let Some(v) = kv else {
            continue;
        };
        if lower.is_some_and(|l| *v < l) {
            continue;
        }
        if upper.is_some_and(|u| *v > u) {
            continue;
        }
        start = start.min(i);
        end = end.max(i + 1);
    }
    if start >= end {
        Ok((0, 0))
    } else {
        Ok((start, end))
    }
}

fn partition_ranges(
    order_idx: &[usize],
    partition_keys: &[Vec<ScalarValue>],
) -> Vec<(usize, usize)> {
    let mut out = Vec::new();
    let mut i = 0usize;
    while i < order_idx.len() {
        let start = i;
        let first = order_idx[i];
        i += 1;
        while i < order_idx.len()
            && cmp_key_sets(partition_keys, first, order_idx[i]) == Ordering::Equal
        {
            i += 1;
        }
        out.push((start, i));
    }
    out
}

fn window_output_type(input_schema: &SchemaRef, w: &WindowExpr) -> Result<DataType> {
    match &w.func {
        WindowFunction::RowNumber
        | WindowFunction::Rank
        | WindowFunction::DenseRank
        | WindowFunction::Ntile(_)
        | WindowFunction::Count(_) => Ok(DataType::Int64),
        WindowFunction::PercentRank
        | WindowFunction::CumeDist
        | WindowFunction::Sum(_)
        | WindowFunction::Avg(_) => Ok(DataType::Float64),
        WindowFunction::Min(expr) | WindowFunction::Max(expr) => {
            let compiled = compile_expr(expr, input_schema)?;
            Ok(compiled.data_type())
        }
        WindowFunction::Lag { expr, .. }
        | WindowFunction::Lead { expr, .. }
        | WindowFunction::FirstValue(expr)
        | WindowFunction::LastValue(expr)
        | WindowFunction::NthValue { expr, .. } => {
            let compiled = compile_expr(expr, input_schema)?;
            Ok(compiled.data_type())
        }
    }
}

fn window_output_nullable(w: &WindowExpr) -> bool {
    !matches!(
        w.func,
        WindowFunction::RowNumber
            | WindowFunction::Rank
            | WindowFunction::DenseRank
            | WindowFunction::Ntile(_)
            | WindowFunction::Count(_)
            | WindowFunction::PercentRank
            | WindowFunction::CumeDist
    )
}

fn infer_expr_nullable(expr: &Expr, schema: &SchemaRef) -> Result<bool> {
    match expr {
        Expr::ColumnRef { index, .. } => Ok(schema.field(*index).is_nullable()),
        Expr::Column(name) => {
            let idx = schema.index_of(name).map_err(|e| {
                FfqError::Execution(format!(
                    "projection column resolution failed for '{name}': {e}"
                ))
            })?;
            Ok(schema.field(idx).is_nullable())
        }
        Expr::Literal(v) => Ok(matches!(v, LiteralValue::Null)),
        Expr::Cast { expr, .. } => infer_expr_nullable(expr, schema),
        Expr::IsNull(_) | Expr::IsNotNull(_) => Ok(false),
        Expr::And(l, r)
        | Expr::Or(l, r)
        | Expr::BinaryOp {
            left: l, right: r, ..
        } => Ok(infer_expr_nullable(l, schema)? || infer_expr_nullable(r, schema)?),
        Expr::Not(inner) => infer_expr_nullable(inner, schema),
        Expr::CaseWhen {
            branches,
            else_expr,
        } => {
            let mut nullable = false;
            for (cond, value) in branches {
                nullable |= infer_expr_nullable(cond, schema)?;
                nullable |= infer_expr_nullable(value, schema)?;
            }
            nullable |= else_expr
                .as_ref()
                .map(|e| infer_expr_nullable(e, schema))
                .transpose()?
                .unwrap_or(true);
            Ok(nullable)
        }
        #[cfg(feature = "vector")]
        Expr::CosineSimilarity { .. } | Expr::L2Distance { .. } | Expr::DotProduct { .. } => {
            Ok(false)
        }
        Expr::ScalarUdf { .. } => Ok(true),
    }
}

fn scalar_to_f64(v: &ScalarValue) -> Option<f64> {
    match v {
        ScalarValue::Int64(x) => Some(*x as f64),
        ScalarValue::Float64Bits(x) => Some(f64::from_bits(*x)),
        ScalarValue::Null => None,
        _ => None,
    }
}

fn filtered_frame_positions<'a>(
    frame: &WindowFrameSpec,
    ctx: &'a PartitionFrameCtx,
    part: &'a [usize],
    fs: usize,
    fe: usize,
    row_idx: usize,
) -> Vec<&'a usize> {
    match frame.exclusion {
        WindowFrameExclusion::NoOthers => part[fs..fe].iter().collect(),
        WindowFrameExclusion::CurrentRow => part[fs..fe]
            .iter()
            .filter(|p| **p != part[row_idx])
            .collect(),
        WindowFrameExclusion::Group => {
            let g = ctx.row_group[row_idx];
            let (gs, ge) = ctx.peer_groups[g];
            part[fs..fe]
                .iter()
                .filter(|p| {
                    let abs = part.iter().position(|v| *v == **p).unwrap_or(usize::MAX);
                    abs < gs || abs >= ge
                })
                .collect()
        }
        WindowFrameExclusion::Ties => {
            let g = ctx.row_group[row_idx];
            let (gs, ge) = ctx.peer_groups[g];
            part[fs..fe]
                .iter()
                .filter(|p| {
                    if **p == part[row_idx] {
                        return true;
                    }
                    let abs = part.iter().position(|v| *v == **p).unwrap_or(usize::MAX);
                    abs < gs || abs >= ge
                })
                .collect()
        }
    }
}

fn first_in_filtered_frame(
    frame: &WindowFrameSpec,
    ctx: &PartitionFrameCtx,
    part: &[usize],
    fs: usize,
    fe: usize,
    row_idx: usize,
) -> Option<usize> {
    filtered_frame_positions(frame, ctx, part, fs, fe, row_idx)
        .first()
        .map(|p| **p)
}

fn last_in_filtered_frame(
    frame: &WindowFrameSpec,
    ctx: &PartitionFrameCtx,
    part: &[usize],
    fs: usize,
    fe: usize,
    row_idx: usize,
) -> Option<usize> {
    filtered_frame_positions(frame, ctx, part, fs, fe, row_idx)
        .last()
        .map(|p| **p)
}

fn evaluate_expr_rows(input: &ExecOutput, expr: &Expr) -> Result<Vec<ScalarValue>> {
    let compiled = compile_expr(expr, &input.schema)?;
    let mut out = Vec::with_capacity(input.batches.iter().map(|b| b.num_rows()).sum());
    for batch in &input.batches {
        let arr = compiled.evaluate(batch)?;
        for row in 0..batch.num_rows() {
            out.push(scalar_from_array(&arr, row)?);
        }
    }
    Ok(out)
}

fn cmp_key_sets(keys: &[Vec<ScalarValue>], a: usize, b: usize) -> Ordering {
    for col in keys {
        let ord = cmp_scalar_for_window(&col[a], &col[b], false, true);
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

fn cmp_order_key_sets(
    keys: &[Vec<ScalarValue>],
    order_exprs: &[WindowOrderExpr],
    a: usize,
    b: usize,
) -> Ordering {
    for (idx, col) in keys.iter().enumerate() {
        let ord = cmp_scalar_for_window(
            &col[a],
            &col[b],
            !order_exprs[idx].asc,
            order_exprs[idx].nulls_first,
        );
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

fn cmp_scalar_for_window(
    a: &ScalarValue,
    b: &ScalarValue,
    descending: bool,
    nulls_first: bool,
) -> Ordering {
    use ScalarValue::*;
    match (a, b) {
        (Null, Null) => return Ordering::Equal,
        (Null, _) => {
            return if nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            };
        }
        (_, Null) => {
            return if nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            };
        }
        _ => {}
    }
    let ord = match (a, b) {
        (Int64(x), Int64(y)) => x.cmp(y),
        (Float64Bits(x), Float64Bits(y)) => {
            cmp_f64_for_window(f64::from_bits(*x), f64::from_bits(*y))
        }
        (Int64(x), Float64Bits(y)) => cmp_f64_for_window(*x as f64, f64::from_bits(*y)),
        (Float64Bits(x), Int64(y)) => cmp_f64_for_window(f64::from_bits(*x), *y as f64),
        (Utf8(x), Utf8(y)) => x.cmp(y),
        (Boolean(x), Boolean(y)) => x.cmp(y),
        _ => format!("{a:?}").cmp(&format!("{b:?}")),
    };
    if descending { ord.reverse() } else { ord }
}

fn cmp_f64_for_window(a: f64, b: f64) -> Ordering {
    match (a.is_nan(), b.is_nan()) {
        // Treat all NaNs as peers for rank/tie semantics.
        (true, true) => Ordering::Equal,
        // SQL-style total ordering choice: NaN sorts above finite values (ascending).
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => a.total_cmp(&b),
    }
}

fn build_stable_row_fallback_keys(input: &ExecOutput) -> Result<Vec<u64>> {
    let rows = rows_from_batches(input)?;
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let mut hasher = DefaultHasher::new();
        for value in row {
            format!("{value:?}").hash(&mut hasher);
            "|".hash(&mut hasher);
        }
        out.push(hasher.finish());
    }
    Ok(out)
}

fn run_exists_subquery_filter(
    input: ExecOutput,
    subquery: ExecOutput,
    negated: bool,
) -> ExecOutput {
    let sub_rows = subquery.batches.iter().map(|b| b.num_rows()).sum::<usize>();
    let exists = sub_rows > 0;
    let keep = if negated { !exists } else { exists };
    if keep {
        input
    } else {
        ExecOutput {
            schema: input.schema.clone(),
            batches: vec![RecordBatch::new_empty(input.schema)],
        }
    }
}

fn run_in_subquery_filter(
    input: ExecOutput,
    expr: Expr,
    subquery: ExecOutput,
    negated: bool,
) -> Result<ExecOutput> {
    let sub_membership = subquery_membership_set(&subquery)?;
    let eval = compile_expr(&expr, &input.schema)?;
    let mut out_batches = Vec::with_capacity(input.batches.len());
    for batch in &input.batches {
        let values = eval.evaluate(batch)?;
        let mut mask_builder = BooleanBuilder::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            // SQL 3-valued semantics:
            // - keep row only when predicate is TRUE
            // - FALSE/NULL are filtered out by WHERE.
            let predicate = if values.is_null(row) {
                None
            } else {
                let value = scalar_from_array(&values, row)?;
                eval_in_predicate(value, &sub_membership, negated)
            };
            let keep = predicate == Some(true);
            mask_builder.append_value(keep);
        }
        let mask = mask_builder.finish();
        let filtered = arrow::compute::filter_record_batch(batch, &mask)
            .map_err(|e| FfqError::Execution(format!("in-subquery filter batch failed: {e}")))?;
        out_batches.push(filtered);
    }
    Ok(ExecOutput {
        schema: input.schema,
        batches: out_batches,
    })
}

fn run_scalar_subquery_filter(
    input: ExecOutput,
    expr: Expr,
    op: BinaryOp,
    subquery: ExecOutput,
) -> Result<ExecOutput> {
    let scalar = scalar_subquery_value(&subquery)?;
    let eval = compile_expr(&expr, &input.schema)?;
    let mut out_batches = Vec::with_capacity(input.batches.len());
    for batch in &input.batches {
        let values = eval.evaluate(batch)?;
        let mut mask_builder = BooleanBuilder::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let keep = if values.is_null(row) {
                false
            } else {
                let lhs = scalar_from_array(&values, row)?;
                compare_scalar_values(op, &lhs, &scalar).unwrap_or(false)
            };
            mask_builder.append_value(keep);
        }
        let mask = mask_builder.finish();
        let filtered = arrow::compute::filter_record_batch(batch, &mask).map_err(|e| {
            FfqError::Execution(format!("scalar-subquery filter batch failed: {e}"))
        })?;
        out_batches.push(filtered);
    }
    Ok(ExecOutput {
        schema: input.schema,
        batches: out_batches,
    })
}

fn scalar_subquery_value(subquery: &ExecOutput) -> Result<ScalarValue> {
    if subquery.schema.fields().len() != 1 {
        return Err(FfqError::Planning(format!(
            "{E_SUBQUERY_SCALAR_ROW_VIOLATION}: scalar subquery must produce exactly one column"
        )));
    }
    let mut seen: Option<ScalarValue> = None;
    let mut rows = 0usize;
    for batch in &subquery.batches {
        if batch.num_columns() != 1 {
            return Err(FfqError::Planning(format!(
                "{E_SUBQUERY_SCALAR_ROW_VIOLATION}: scalar subquery must produce exactly one column"
            )));
        }
        for row in 0..batch.num_rows() {
            rows += 1;
            if rows > 1 {
                return Err(FfqError::Execution(format!(
                    "{E_SUBQUERY_SCALAR_ROW_VIOLATION}: scalar subquery returned more than one row"
                )));
            }
            seen = Some(scalar_from_array(batch.column(0), row)?);
        }
    }
    Ok(seen.unwrap_or(ScalarValue::Null))
}

fn compare_scalar_values(op: BinaryOp, lhs: &ScalarValue, rhs: &ScalarValue) -> Option<bool> {
    use ScalarValue::*;
    if matches!(lhs, Null) || matches!(rhs, Null) {
        return None;
    }
    let numeric_cmp = |a: f64, b: f64| match op {
        BinaryOp::Eq => Some(a == b),
        BinaryOp::NotEq => Some(a != b),
        BinaryOp::Lt => Some(a < b),
        BinaryOp::LtEq => Some(a <= b),
        BinaryOp::Gt => Some(a > b),
        BinaryOp::GtEq => Some(a >= b),
        _ => None,
    };
    match (lhs, rhs) {
        (Int64(a), Int64(b)) => numeric_cmp(*a as f64, *b as f64),
        (Float64Bits(a), Float64Bits(b)) => numeric_cmp(f64::from_bits(*a), f64::from_bits(*b)),
        (Int64(a), Float64Bits(b)) => numeric_cmp(*a as f64, f64::from_bits(*b)),
        (Float64Bits(a), Int64(b)) => numeric_cmp(f64::from_bits(*a), *b as f64),
        (Utf8(a), Utf8(b)) => match op {
            BinaryOp::Eq => Some(a == b),
            BinaryOp::NotEq => Some(a != b),
            BinaryOp::Lt => Some(a < b),
            BinaryOp::LtEq => Some(a <= b),
            BinaryOp::Gt => Some(a > b),
            BinaryOp::GtEq => Some(a >= b),
            _ => None,
        },
        (Boolean(a), Boolean(b)) => match op {
            BinaryOp::Eq => Some(a == b),
            BinaryOp::NotEq => Some(a != b),
            _ => None,
        },
        _ => None,
    }
}

fn subquery_membership_set(subquery: &ExecOutput) -> Result<InSubqueryMembership> {
    if subquery.schema.fields().len() != 1 {
        return Err(FfqError::Planning(
            "IN subquery must produce exactly one column".to_string(),
        ));
    }
    let mut out = InSubqueryMembership::default();
    for batch in &subquery.batches {
        if batch.num_columns() != 1 {
            return Err(FfqError::Planning(
                "IN subquery must produce exactly one column".to_string(),
            ));
        }
        for row in 0..batch.num_rows() {
            let value = scalar_from_array(batch.column(0), row)?;
            if value != ScalarValue::Null {
                out.values.insert(value);
            } else {
                out.has_null = true;
            }
        }
    }
    Ok(out)
}

#[derive(Debug, Default)]
struct InSubqueryMembership {
    values: HashSet<ScalarValue>,
    has_null: bool,
}

fn eval_in_predicate(
    lhs: ScalarValue,
    membership: &InSubqueryMembership,
    negated: bool,
) -> Option<bool> {
    // NULL IN (...) and NULL NOT IN (...) are NULL.
    if lhs == ScalarValue::Null {
        return None;
    }
    // Match found.
    if membership.values.contains(&lhs) {
        return Some(!negated);
    }
    // No match, but NULL in rhs yields UNKNOWN for both IN and NOT IN.
    if membership.has_null {
        return None;
    }
    // No match and no NULL in rhs.
    Some(negated)
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
        .map(|(idx, col)| {
            let field = schema.field(idx);
            scalars_to_array(col, field.data_type()).map_err(|e| {
                FfqError::Execution(format!(
                    "join column '{}' build failed: {}",
                    field.name(),
                    e
                ))
            })
        })
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

fn embedded_adaptive_plan_for_partitioning(
    input: &ExecOutput,
    partitioning: &PartitioningSpec,
) -> Result<AdaptiveReducePlan> {
    let target_bytes = std::env::var("FFQ_ADAPTIVE_SHUFFLE_TARGET_BYTES")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(128 * 1024 * 1024);
    embedded_adaptive_plan_for_partitioning_with_target(input, partitioning, target_bytes)
}

fn embedded_adaptive_plan_for_partitioning_with_target(
    input: &ExecOutput,
    partitioning: &PartitioningSpec,
    target_bytes: u64,
) -> Result<AdaptiveReducePlan> {
    let mut bytes_by_partition = HashMap::<u32, u64>::new();
    let planned_partitions = match partitioning {
        PartitioningSpec::Single => {
            let total = input
                .batches
                .iter()
                .map(|b| {
                    b.columns()
                        .iter()
                        .map(|a| a.get_array_memory_size() as u64)
                        .sum::<u64>()
                })
                .sum::<u64>();
            bytes_by_partition.insert(0, total);
            1_u32
        }
        PartitioningSpec::HashKeys { keys, partitions } => {
            let partition_count = (*partitions).max(1) as u32;
            let rows = rows_from_batches(input)?;
            let key_idx = resolve_key_indexes(&input.schema, keys)?;
            for row in &rows {
                let key = join_key_from_row(row, &key_idx);
                let partition = (hash_key(&key) % partition_count as u64) as u32;
                let row_bytes = row
                    .iter()
                    .map(|v| scalar_estimate_bytes(v) as u64)
                    .sum::<u64>();
                bytes_by_partition
                    .entry(partition)
                    .and_modify(|b| *b = b.saturating_add(row_bytes))
                    .or_insert(row_bytes);
            }
            partition_count
        }
    };
    Ok(plan_adaptive_reduce_layout(
        planned_partitions,
        target_bytes,
        &bytes_by_partition,
        1,
        0,
        0,
    ))
}

fn strip_qual(name: &str) -> String {
    name.rsplit('.').next().unwrap_or(name).to_string()
}

fn join_key_from_row(row: &[ScalarValue], idxs: &[usize]) -> Vec<ScalarValue> {
    idxs.iter().map(|i| row[*i].clone()).collect()
}

fn join_key_has_null(key: &[ScalarValue]) -> bool {
    key.iter().any(|v| *v == ScalarValue::Null)
}

fn cmp_join_keys(a: &[ScalarValue], b: &[ScalarValue]) -> Ordering {
    for (av, bv) in a.iter().zip(b.iter()) {
        let ord = cmp_join_scalar(av, bv);
        if ord != Ordering::Equal {
            return ord;
        }
    }
    a.len().cmp(&b.len())
}

fn cmp_join_scalar(a: &ScalarValue, b: &ScalarValue) -> Ordering {
    use ScalarValue::*;
    match (a, b) {
        (Null, Null) => Ordering::Equal,
        (Null, _) => Ordering::Less,
        (_, Null) => Ordering::Greater,
        (Int64(x), Int64(y)) => x.cmp(y),
        (Float64Bits(x), Float64Bits(y)) => f64::from_bits(*x).total_cmp(&f64::from_bits(*y)),
        (Int64(x), Float64Bits(y)) => (*x as f64).total_cmp(&f64::from_bits(*y)),
        (Float64Bits(x), Int64(y)) => f64::from_bits(*x).total_cmp(&(*y as f64)),
        (Utf8(x), Utf8(y)) => x.cmp(y),
        (Boolean(x), Boolean(y)) => x.cmp(y),
        _ => format!("{a:?}").cmp(&format!("{b:?}")),
    }
}

fn in_memory_hash_join(
    build_rows: &[Vec<ScalarValue>],
    probe_rows: &[Vec<ScalarValue>],
    build_key_idx: &[usize],
    probe_key_idx: &[usize],
    build_side: JoinInputSide,
    left_len: usize,
    right_len: usize,
) -> JoinMatchOutput {
    let mut ht: HashMap<Vec<ScalarValue>, Vec<usize>> = HashMap::new();
    for (idx, row) in build_rows.iter().enumerate() {
        let key = join_key_from_row(row, build_key_idx);
        if join_key_has_null(&key) {
            continue;
        }
        ht.entry(key).or_default().push(idx);
    }

    let mut out = Vec::new();
    let mut matched_left = vec![false; left_len];
    let mut matched_right = vec![false; right_len];
    for (probe_idx, probe) in probe_rows.iter().enumerate() {
        let probe_key = join_key_from_row(probe, probe_key_idx);
        if join_key_has_null(&probe_key) {
            continue;
        }
        if let Some(build_matches) = ht.get(&probe_key) {
            for build_idx in build_matches {
                let build = &build_rows[*build_idx];
                out.push(combine_join_rows(build, probe, build_side));
                mark_join_match(
                    &mut matched_left,
                    &mut matched_right,
                    build_side,
                    *build_idx,
                    probe_idx,
                );
            }
        }
    }
    JoinMatchOutput {
        rows: out,
        matched_left,
        matched_right,
    }
}

fn in_memory_radix_hash_join(
    build_rows: &[Vec<ScalarValue>],
    probe_rows: &[Vec<ScalarValue>],
    build_key_idx: &[usize],
    probe_key_idx: &[usize],
    build_side: JoinInputSide,
    left_len: usize,
    right_len: usize,
    radix_bits: u8,
) -> JoinMatchOutput {
    // Keep partition fanout bounded so partition metadata stays cache-friendly.
    let bits = radix_bits.min(12);
    if bits == 0 {
        return in_memory_hash_join(
            build_rows,
            probe_rows,
            build_key_idx,
            probe_key_idx,
            build_side,
            left_len,
            right_len,
        );
    }

    let partitions = 1usize << bits;
    let mask = (partitions as u64) - 1;
    let mut build_parts = vec![Vec::<(usize, Vec<ScalarValue>, u64)>::new(); partitions];
    let mut probe_parts = vec![Vec::<(usize, Vec<ScalarValue>, u64)>::new(); partitions];

    for (idx, row) in build_rows.iter().enumerate() {
        let key = join_key_from_row(row, build_key_idx);
        if join_key_has_null(&key) {
            continue;
        }
        let key_hash = hash_key(&key);
        let part = (key_hash & mask) as usize;
        build_parts[part].push((idx, key, key_hash));
    }
    for (idx, row) in probe_rows.iter().enumerate() {
        let key = join_key_from_row(row, probe_key_idx);
        if join_key_has_null(&key) {
            continue;
        }
        let key_hash = hash_key(&key);
        let part = (key_hash & mask) as usize;
        probe_parts[part].push((idx, key, key_hash));
    }

    let mut out = Vec::new();
    let mut matched_left = vec![false; left_len];
    let mut matched_right = vec![false; right_len];
    for part in 0..partitions {
        if build_parts[part].is_empty() || probe_parts[part].is_empty() {
            continue;
        }
        let mut ht: HashMap<u64, Vec<(usize, Vec<ScalarValue>)>> = HashMap::new();
        for (build_idx, key, key_hash) in build_parts[part].drain(..) {
            ht.entry(key_hash).or_default().push((build_idx, key));
        }
        for (probe_idx, probe_key, probe_hash) in &probe_parts[part] {
            if let Some(build_matches) = ht.get(probe_hash) {
                for (build_idx, build_key) in build_matches {
                    if build_key == probe_key {
                        let build = &build_rows[*build_idx];
                        let probe = &probe_rows[*probe_idx];
                        out.push(combine_join_rows(build, probe, build_side));
                        mark_join_match(
                            &mut matched_left,
                            &mut matched_right,
                            build_side,
                            *build_idx,
                            *probe_idx,
                        );
                    }
                }
            }
        }
    }

    JoinMatchOutput {
        rows: out,
        matched_left,
        matched_right,
    }
}

fn in_memory_radix_hash_join_i64(
    build_rows: &[Vec<ScalarValue>],
    probe_rows: &[Vec<ScalarValue>],
    build_key_idx: usize,
    probe_key_idx: usize,
    build_side: JoinInputSide,
    left_len: usize,
    right_len: usize,
    radix_bits: u8,
) -> JoinMatchOutput {
    let bits = radix_bits.min(12);
    if bits == 0 {
        return in_memory_hash_join(
            build_rows,
            probe_rows,
            &[build_key_idx],
            &[probe_key_idx],
            build_side,
            left_len,
            right_len,
        );
    }
    let partitions = 1usize << bits;
    let mask = (partitions as u64) - 1;
    let mut build_parts = vec![Vec::<(usize, i64)>::new(); partitions];
    let mut probe_parts = vec![Vec::<(usize, i64)>::new(); partitions];

    for (idx, row) in build_rows.iter().enumerate() {
        let Some(ScalarValue::Int64(key)) = row.get(build_key_idx) else {
            continue;
        };
        let key_hash = hash_i64(*key);
        let part = (key_hash & mask) as usize;
        build_parts[part].push((idx, *key));
    }
    for (idx, row) in probe_rows.iter().enumerate() {
        let Some(ScalarValue::Int64(key)) = row.get(probe_key_idx) else {
            continue;
        };
        let key_hash = hash_i64(*key);
        let part = (key_hash & mask) as usize;
        probe_parts[part].push((idx, *key));
    }

    let mut out = Vec::new();
    let mut matched_left = vec![false; left_len];
    let mut matched_right = vec![false; right_len];
    for part in 0..partitions {
        if build_parts[part].is_empty() || probe_parts[part].is_empty() {
            continue;
        }
        let mut ht: HashMap<i64, Vec<usize>> = HashMap::new();
        for (build_idx, key) in &build_parts[part] {
            ht.entry(*key).or_default().push(*build_idx);
        }
        for (probe_idx, probe_key) in &probe_parts[part] {
            if let Some(build_matches) = ht.get(probe_key) {
                for build_idx in build_matches {
                    let build = &build_rows[*build_idx];
                    let probe = &probe_rows[*probe_idx];
                    out.push(combine_join_rows(build, probe, build_side));
                    mark_join_match(
                        &mut matched_left,
                        &mut matched_right,
                        build_side,
                        *build_idx,
                        *probe_idx,
                    );
                }
            }
        }
    }

    JoinMatchOutput {
        rows: out,
        matched_left,
        matched_right,
    }
}

fn mark_join_match(
    matched_left: &mut [bool],
    matched_right: &mut [bool],
    build_side: JoinInputSide,
    build_idx: usize,
    probe_idx: usize,
) {
    match build_side {
        JoinInputSide::Left => {
            matched_left[build_idx] = true;
            matched_right[probe_idx] = true;
        }
        JoinInputSide::Right => {
            matched_left[probe_idx] = true;
            matched_right[build_idx] = true;
        }
    }
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
/// Grace hash-join spill path.
///
/// Both build/probe rows are partitioned to disk by join key hash, then joined
/// partition-wise to keep peak in-memory state bounded.
fn grace_hash_join(
    build_rows: &[Vec<ScalarValue>],
    probe_rows: &[Vec<ScalarValue>],
    build_key_idx: &[usize],
    probe_key_idx: &[usize],
    build_side: JoinInputSide,
    left_len: usize,
    right_len: usize,
    ctx: &QueryContext,
    trace: &TraceIds,
) -> Result<JoinMatchOutput> {
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
    let mut matched_left = vec![false; left_len];
    let mut matched_right = vec![false; right_len];
    for p in 0..parts {
        let mut ht: HashMap<Vec<ScalarValue>, Vec<JoinSpillRow>> = HashMap::new();

        if let Ok(file) = File::open(&build_paths[p]) {
            let reader = BufReader::new(file);
            for line in reader.lines() {
                let line = line?;
                if line.trim().is_empty() {
                    continue;
                }
                let rec: JoinSpillRow = serde_json::from_str(&line)
                    .map_err(|e| FfqError::Execution(format!("join spill decode failed: {e}")))?;
                ht.entry(rec.key.clone()).or_default().push(rec);
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
                        out.push(combine_join_rows(&build.row, &rec.row, build_side));
                        mark_join_match(
                            &mut matched_left,
                            &mut matched_right,
                            build_side,
                            build.row_id,
                            rec.row_id,
                        );
                    }
                }
            }
        }

        let _ = fs::remove_file(&build_paths[p]);
        let _ = fs::remove_file(&probe_paths[p]);
    }

    Ok(JoinMatchOutput {
        rows: out,
        matched_left,
        matched_right,
    })
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

    for (row_id, row) in rows.iter().enumerate() {
        let key = join_key_from_row(row, key_idx);
        if join_key_has_null(&key) {
            continue;
        }
        let part = (hash_key(&key) as usize) % writers.len();
        let rec = JoinSpillRow {
            row_id,
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

fn hash_key_with_seed(key: &[ScalarValue], seed: u64) -> u64 {
    let mut h = DefaultHasher::new();
    seed.hash(&mut h);
    key.hash(&mut h);
    h.finish()
}

fn hash_i64(v: i64) -> u64 {
    let mut h = DefaultHasher::new();
    v.hash(&mut h);
    h.finish()
}

#[cfg_attr(feature = "profiling", inline(never))]
/// Execute two-phase hash aggregation (partial or final mode).
///
/// Input: child rows, group expressions, aggregate expressions.
/// Output: one batch with grouping columns followed by aggregate outputs.
/// Spill behavior: group state spills to JSONL files when estimated memory
/// usage exceeds `ctx.mem_budget_bytes`.
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

    let mut groups: GroupMap = HashMap::new();
    let mut spills = Vec::<PathBuf>::new();
    let mut spill_seq: u64 = 0;

    for batch in &child.batches {
        accumulate_batch(
            mode,
            &specs,
            &group_exprs,
            &input_schema,
            batch,
            &mut groups,
        )?;
        maybe_spill(&mut groups, &mut spills, &mut spill_seq, ctx, trace)?;
    }

    if group_exprs.is_empty() && groups.is_empty() {
        groups.insert(
            encode_group_key(&[]),
            GroupEntry {
                key: vec![],
                states: init_states(&specs),
            },
        );
    }

    if !groups.is_empty() {
        maybe_spill(&mut groups, &mut spills, &mut spill_seq, ctx, trace)?;
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
                AggExpr::CountDistinct(_) => {
                    return Err(FfqError::Execution(
                        "COUNT(DISTINCT ...) should be lowered before runtime aggregation"
                            .to_string(),
                    ));
                }
                AggExpr::ApproxCountDistinct(_) => DataType::Utf8,
                AggExpr::Sum(e) | AggExpr::Min(e) | AggExpr::Max(e) => {
                    expr_data_type(e, input_schema)?
                }
                AggExpr::Avg(_) => DataType::Float64,
            },
            AggregateMode::Final => match expr {
                AggExpr::ApproxCountDistinct(_) => DataType::Int64,
                _ => {
                    let col_idx = group_exprs.len() + idx;
                    input_schema.field(col_idx).data_type().clone()
                }
            },
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
            AggExpr::CountDistinct(_) => AggState::Count(0),
            AggExpr::ApproxCountDistinct(_) => AggState::Hll(HllSketch::new(12)),
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
    groups: &mut GroupMap,
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
                    | AggExpr::CountDistinct(e)
                    | AggExpr::ApproxCountDistinct(e)
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
        let encoded_key = encode_group_key(&key);
        let state_vec = &mut groups
            .entry(encoded_key)
            .or_insert_with(|| GroupEntry {
                key: key.clone(),
                states: init_states(specs),
            })
            .states;

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
        AggState::Hll(sketch) => match mode {
            AggregateMode::Partial => {
                sketch.add_scalar(&value);
            }
            AggregateMode::Final => {
                if value == ScalarValue::Null {
                    return Ok(());
                }
                let ScalarValue::Utf8(payload) = value else {
                    return Err(FfqError::Execution(
                        "invalid partial sketch state for APPROX_COUNT_DISTINCT".to_string(),
                    ));
                };
                let other = serde_json::from_str::<HllSketch>(&payload).map_err(|e| {
                    FfqError::Execution(format!(
                        "failed to deserialize APPROX_COUNT_DISTINCT sketch: {e}"
                    ))
                })?;
                sketch.merge(&other)?;
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
    groups: GroupMap,
    specs: &[AggSpec],
    group_exprs: &[Expr],
    input_schema: &SchemaRef,
    mode: AggregateMode,
) -> Result<ExecOutput> {
    let mut keys: Vec<Vec<ScalarValue>> = groups.values().map(|e| e.key.clone()).collect();
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
                .get(&encode_group_key(key))
                .map(|e| &e.states)
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
        .map(|(idx, col)| {
            let field = schema.field(idx);
            scalars_to_array(col, field.data_type()).map_err(|e| {
                FfqError::Execution(format!(
                    "aggregate column '{}' build failed: {}",
                    field.name(),
                    e
                ))
            })
        })
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
            if *index < input_schema.fields().len() && input_schema.field(*index).name() == name {
                input_schema.field(*index).data_type().clone()
            } else {
                // In final aggregate mode, group columns are physically laid out by position,
                // while ColumnRef indexes still refer to the pre-aggregate input schema.
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
        (AggState::Hll(sketch), AggExpr::ApproxCountDistinct(_)) => {
            if mode == AggregateMode::Partial {
                match serde_json::to_string(sketch) {
                    Ok(s) => ScalarValue::Utf8(s),
                    Err(_) => ScalarValue::Null,
                }
            } else {
                ScalarValue::Int64(sketch.estimate().round() as i64)
            }
        }
        _ => ScalarValue::Null,
    }
}

/// Spill aggregate state to disk when memory budget is exceeded.
fn maybe_spill(
    groups: &mut GroupMap,
    spills: &mut Vec<PathBuf>,
    spill_seq: &mut u64,
    ctx: &QueryContext,
    trace: &TraceIds,
) -> Result<()> {
    let spill_signal = spill_signal_for_ctx(ctx);
    if groups.is_empty() || ctx.mem_budget_bytes == 0 {
        return Ok(());
    }

    let estimated = estimate_groups_bytes(groups);
    if !spill_signal.should_spill(estimated) {
        return Ok(());
    }

    fs::create_dir_all(&ctx.spill_dir)?;
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| FfqError::Execution(format!("clock error: {e}")))?
        .as_nanos();
    let target_bytes = spill_signal.spill_target_bytes(3, 4);
    let target_bytes = target_bytes.max(1);
    let mut partition_cursor = 0_u8;
    let mut empty_partition_streak = 0_u8;
    const SPILL_PARTITIONS: u8 = 16;

    while !groups.is_empty() && estimate_groups_bytes(groups) > target_bytes {
        let spill_started = Instant::now();
        let path = PathBuf::from(&ctx.spill_dir).join(format!(
            "agg_spill_{suffix}_{:06}_p{:02}.jsonl",
            *spill_seq, partition_cursor
        ));
        *spill_seq += 1;

        let mut to_spill = groups
            .keys()
            .filter(|key| {
                (hash_encoded_key(key) % SPILL_PARTITIONS as u64) as u8 == partition_cursor
            })
            .cloned()
            .collect::<Vec<_>>();
        if to_spill.is_empty() {
            empty_partition_streak += 1;
            if empty_partition_streak >= SPILL_PARTITIONS {
                to_spill = groups.keys().cloned().collect::<Vec<_>>();
            } else {
                partition_cursor = (partition_cursor + 1) % SPILL_PARTITIONS;
                continue;
            }
        }
        empty_partition_streak = 0;

        let file = File::create(&path)?;
        let mut writer = BufWriter::new(file);
        for encoded in to_spill {
            if let Some(entry) = groups.remove(&encoded) {
                let row = SpillRow {
                    key: entry.key,
                    states: entry.states,
                };
                let line = serde_json::to_string(&row)
                    .map_err(|e| FfqError::Execution(format!("spill serialize failed: {e}")))?;
                writer.write_all(line.as_bytes()).map_err(FfqError::from)?;
                writer.write_all(b"\n").map_err(FfqError::from)?;
            }
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
        spills.push(path);
        partition_cursor = (partition_cursor + 1) % SPILL_PARTITIONS;
    }
    Ok(())
}

/// Merge one spilled aggregate state file back into in-memory groups.
fn merge_spill_file(path: &PathBuf, groups: &mut GroupMap) -> Result<()> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let row: SpillRow = serde_json::from_str(&line)
            .map_err(|e| FfqError::Execution(format!("spill deserialize failed: {e}")))?;
        let encoded = encode_group_key(&row.key);
        if let Some(existing) = groups.get_mut(&encoded) {
            merge_states(&mut existing.states, &row.states)?;
        } else {
            groups.insert(
                encoded,
                GroupEntry {
                    key: row.key,
                    states: row.states,
                },
            );
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
            (AggState::Hll(a), AggState::Hll(b)) => {
                a.merge(b)?;
            }
            _ => return Err(FfqError::Execution("spill state type mismatch".to_string())),
        }
    }

    Ok(())
}

fn estimate_groups_bytes(groups: &GroupMap) -> usize {
    let mut total = 0_usize;
    for (encoded, entry) in groups {
        total += 96;
        total += encoded.len();
        total += entry.key.iter().map(scalar_estimate_bytes).sum::<usize>();
        total += entry
            .states
            .iter()
            .map(agg_state_estimate_bytes)
            .sum::<usize>();
    }
    total
}

fn hash_encoded_key(key: &[u8]) -> u64 {
    let mut h = DefaultHasher::new();
    key.hash(&mut h);
    h.finish()
}

fn encode_group_key(values: &[ScalarValue]) -> Vec<u8> {
    let mut out = Vec::with_capacity(values.len() * 16);
    for value in values {
        match value {
            ScalarValue::Null => out.push(0),
            ScalarValue::Int64(v) => {
                out.push(1);
                out.extend_from_slice(&v.to_le_bytes());
            }
            ScalarValue::Float64Bits(v) => {
                out.push(2);
                out.extend_from_slice(&v.to_le_bytes());
            }
            ScalarValue::Boolean(v) => {
                out.push(3);
                out.push(u8::from(*v));
            }
            ScalarValue::Utf8(s) => {
                out.push(4);
                let len = s.len() as u32;
                out.extend_from_slice(&len.to_le_bytes());
                out.extend_from_slice(s.as_bytes());
            }
            ScalarValue::VectorF32Bits(v) => {
                out.push(5);
                let len = v.len() as u32;
                out.extend_from_slice(&len.to_le_bytes());
                for bits in v {
                    out.extend_from_slice(&bits.to_le_bytes());
                }
            }
        }
        out.push(0xff);
    }
    out
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
        AggState::Hll(sketch) => sketch.registers.len(),
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
                        return Err(FfqError::Execution(format!(
                            "type mismatch while building Int64 array: got {:?}",
                            v
                        )));
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

/// Execute parquet sink write with temp-file + atomic-commit semantics.
///
/// Side effect: writes/overwrites target parquet object and returns no rows.
fn write_parquet_sink(table: &ffq_storage::TableDef, child: &ExecOutput) -> Result<()> {
    ensure_sink_parent_layout(table)?;
    let out_path = resolve_sink_output_path(table)?;
    let staged_path = temp_sibling_path(&out_path, "staged");
    if let Some(parent) = staged_path.parent() {
        fs::create_dir_all(parent).map_err(|e| {
            FfqError::Execution(format!(
                "parquet sink create staged parent failed at '{}': {e}",
                parent.display()
            ))
        })?;
    }
    let file = File::create(&staged_path).map_err(|e| {
        FfqError::Execution(format!(
            "parquet sink create staged file failed at '{}': {e}",
            staged_path.display()
        ))
    })?;
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

fn ensure_sink_parent_layout(table: &ffq_storage::TableDef) -> Result<()> {
    let raw = if !table.uri.is_empty() {
        table.uri.clone()
    } else if let Some(first) = table.paths.first() {
        first.clone()
    } else {
        return Ok(());
    };
    let p = PathBuf::from(raw);
    let as_text = p.to_string_lossy();
    if !as_text.ends_with(".parquet") {
        fs::create_dir_all(&p).map_err(|e| {
            FfqError::Execution(format!(
                "parquet sink create output dir failed at '{}': {e}",
                p.display()
            ))
        })?;
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
        fs::create_dir_all(parent).map_err(|e| {
            FfqError::Execution(format!(
                "file commit create target parent failed at '{}': {e}",
                parent.display()
            ))
        })?;
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
use ffq_distributed::DistributedRuntime as InnerDistributedRuntime;
#[cfg(feature = "distributed")]
use ffq_distributed::grpc::ControlPlaneClient;
#[cfg(feature = "distributed")]
use ffq_distributed::grpc::v1::QueryState as DistQueryState;

#[cfg(feature = "distributed")]
#[derive(Debug)]
/// gRPC-backed runtime that submits and fetches query work/results from the
/// distributed coordinator.
pub struct DistributedRuntime {
    _inner: InnerDistributedRuntime,
    coordinator_endpoint: String,
}

#[cfg(feature = "distributed")]
impl DistributedRuntime {
    /// Create a distributed runtime targeting a coordinator endpoint.
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
        ctx: QueryContext,
        _catalog: Arc<Catalog>,
        _physical_registry: Arc<PhysicalOperatorRegistry>,
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
                        break (qstate, status.message, status.stage_metrics);
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
                        query_id: query_id.clone(),
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
                if let Some(collector) = &ctx.stats_collector {
                    for sm in &terminal.2 {
                        let tasks = (sm.queued_tasks as u64)
                            .saturating_add(sm.running_tasks as u64)
                            .saturating_add(sm.succeeded_tasks as u64)
                            .saturating_add(sm.failed_tasks as u64);
                        collector.record_stage_summary(
                            &query_id,
                            sm.stage_id,
                            tasks,
                            sm.map_output_rows,
                            sm.map_output_bytes,
                            sm.map_output_batches,
                            sm.planned_reduce_tasks,
                            sm.adaptive_reduce_tasks,
                            sm.adaptive_target_bytes,
                            sm.aqe_events.clone(),
                            sm.partition_bytes_histogram
                                .iter()
                                .map(|b| b.upper_bound_bytes)
                                .collect(),
                            sm.layout_finalize_count,
                            sm.skew_split_tasks,
                            sm.first_chunk_ms,
                            sm.first_reduce_row_ms,
                            sm.stream_lag_ms,
                            sm.stream_buffered_bytes,
                            sm.stream_active_count,
                            sm.backpressure_events.clone(),
                        );
                    }
                    let (rows_out, batches_out, bytes_out) = batch_stats(&batches);
                    collector.record_operator(
                        &query_id,
                        OperatorExecutionStats {
                            stage_id: 0,
                            task_id: 0,
                            operator: "DistributedRuntime",
                            rows_in: 0,
                            rows_out,
                            batches_in: 0,
                            batches_out,
                            bytes_in: 0,
                            bytes_out,
                            elapsed_ms: 0.0,
                            partition_sizes_bytes: batches
                                .iter()
                                .map(|b| {
                                    b.columns()
                                        .iter()
                                        .map(|a| a.get_array_memory_size() as u64)
                                        .sum::<u64>()
                                })
                                .collect(),
                        },
                    );
                }
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
#[path = "runtime_tests.rs"]
mod tests;
