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
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use crate::physical_registry::PhysicalOperatorRegistry;
use arrow::array::{
    Array, ArrayRef, BooleanBuilder, FixedSizeListBuilder, Float32Builder, Float64Builder,
    Int64Array, Int64Builder, StringBuilder,
};
use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use ffq_common::metrics::global_metrics;
use ffq_common::{FfqError, Result};
use ffq_execution::{SendableRecordBatchStream, StreamAdapter, TaskContext, compile_expr};
use ffq_planner::{
    AggExpr, BinaryOp, BuildSide, ExchangeExec, Expr, JoinType, LiteralValue, PhysicalPlan, WindowExpr,
    WindowFrameBound, WindowFrameExclusion, WindowFrameSpec, WindowFrameUnits, WindowFunction,
    WindowOrderExpr,
};
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
use tracing::{Instrument, info, info_span};
#[cfg(feature = "distributed")]
use tracing::{debug, error};

const E_SUBQUERY_SCALAR_ROW_VIOLATION: &str = "E_SUBQUERY_SCALAR_ROW_VIOLATION";

#[derive(Debug, Clone)]
/// Per-query runtime controls.
///
/// `mem_budget_bytes` is a soft threshold used by join/aggregate operators to
/// choose in-memory vs spill-enabled execution.
pub struct QueryContext {
    pub batch_size_rows: usize,
    pub mem_budget_bytes: usize,
    pub spill_dir: String,
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
            let exec =
                execute_plan(plan, ctx, catalog, physical_registry, Arc::clone(&trace)).await?;
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
                if let Some(cached) = cte_cache.lock().ok().and_then(|m| m.get(&cte_ref.name).cloned()) {
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
                ExchangeExec::ShuffleRead(x) => {
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
                    build_side,
                    ..
                } = join;
                let left = execute_plan_with_cache(
                    *left_plan,
                    ctx.clone(),
                    catalog.clone(),
                    Arc::clone(&physical_registry),
                    Arc::clone(&trace),
                    Arc::clone(&cte_cache),
                )
                .await?;
                let right = execute_plan_with_cache(
                    *right_plan,
                    ctx.clone(),
                    catalog,
                    Arc::clone(&physical_registry),
                    Arc::clone(&trace),
                    Arc::clone(&cte_cache),
                )
                .await?;
                let (l_rows, l_batches, l_bytes) = batch_stats(&left.batches);
                let (r_rows, r_batches, r_bytes) = batch_stats(&right.batches);
                Ok(OpEval {
                    out: run_hash_join(left, right, on, join_type, build_side, &ctx, &trace)?,
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

    let mut match_output = if ctx.mem_budget_bytes > 0
        && estimate_join_rows_bytes(build_rows) > ctx.mem_budget_bytes
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
        in_memory_hash_join(
            build_rows,
            probe_rows,
            &build_key_idx,
            &probe_key_idx,
            build_input_side,
            left_rows.len(),
            right_rows.len(),
        )
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
        spill_dir: "./ffq_spill".to_string(),
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
        RecordBatch::new_empty(input.schema.clone()).columns().to_vec()
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
        out_columns.push(scalars_to_array(&output, out_fields.last().expect("field").data_type()).map_err(
            |e| {
                FfqError::Execution(format!(
                    "window output column '{}' build failed: {e}",
                    w.output_name
                ))
            },
        )?);
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
    if ctx.mem_budget_bytes == 0 || estimated <= ctx.mem_budget_bytes {
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
        let line = line.map_err(|e| FfqError::Execution(format!("window spill read failed: {e}")))?;
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
                        )
                            != Ordering::Equal
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
                        )
                            != Ordering::Equal
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
                        )
                            != Ordering::Equal
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
                        )
                            == Ordering::Equal
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
                .map(|row| scalar_to_f64(&order_keys[0][*row]).map(|v| if order_exprs[0].asc { v } else { -v }))
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
                ))
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
                ))
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

fn partition_ranges(order_idx: &[usize], partition_keys: &[Vec<ScalarValue>]) -> Vec<(usize, usize)> {
    let mut out = Vec::new();
    let mut i = 0usize;
    while i < order_idx.len() {
        let start = i;
        let first = order_idx[i];
        i += 1;
        while i < order_idx.len() && cmp_key_sets(partition_keys, first, order_idx[i]) == Ordering::Equal
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
        | WindowFunction::Avg(_) => {
            Ok(DataType::Float64)
        }
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
                FfqError::Execution(format!("projection column resolution failed for '{name}': {e}"))
            })?;
            Ok(schema.field(idx).is_nullable())
        }
        Expr::Literal(v) => Ok(matches!(v, LiteralValue::Null)),
        Expr::Cast { expr, .. } => infer_expr_nullable(expr, schema),
        Expr::IsNull(_) | Expr::IsNotNull(_) => Ok(false),
        Expr::And(l, r) | Expr::Or(l, r) | Expr::BinaryOp { left: l, right: r, .. } => {
            Ok(infer_expr_nullable(l, schema)? || infer_expr_nullable(r, schema)?)
        }
        Expr::Not(inner) => infer_expr_nullable(inner, schema),
        Expr::CaseWhen { branches, else_expr } => {
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
        (Float64Bits(x), Float64Bits(y)) => cmp_f64_for_window(f64::from_bits(*x), f64::from_bits(*y)),
        (Int64(x), Float64Bits(y)) => cmp_f64_for_window(*x as f64, f64::from_bits(*y)),
        (Float64Bits(x), Int64(y)) => cmp_f64_for_window(f64::from_bits(*x), *y as f64),
        (Utf8(x), Utf8(y)) => x.cmp(y),
        (Boolean(x), Boolean(y)) => x.cmp(y),
        _ => format!("{a:?}").cmp(&format!("{b:?}")),
    };
    if descending {
        ord.reverse()
    } else {
        ord
    }
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

fn run_exists_subquery_filter(input: ExecOutput, subquery: ExecOutput, negated: bool) -> ExecOutput {
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
        let filtered = arrow::compute::filter_record_batch(batch, &mask)
            .map_err(|e| FfqError::Execution(format!("scalar-subquery filter batch failed: {e}")))?;
        out_batches.push(filtered);
    }
    Ok(ExecOutput {
        schema: input.schema,
        batches: out_batches,
    })
}

fn scalar_subquery_value(subquery: &ExecOutput) -> Result<ScalarValue> {
    if subquery.schema.fields().len() != 1 {
        return Err(FfqError::Planning(
            format!(
                "{E_SUBQUERY_SCALAR_ROW_VIOLATION}: scalar subquery must produce exactly one column"
            ),
        ));
    }
    let mut seen: Option<ScalarValue> = None;
    let mut rows = 0usize;
    for batch in &subquery.batches {
        if batch.num_columns() != 1 {
            return Err(FfqError::Planning(
                format!(
                    "{E_SUBQUERY_SCALAR_ROW_VIOLATION}: scalar subquery must produce exactly one column"
                ),
            ));
        }
        for row in 0..batch.num_rows() {
            rows += 1;
            if rows > 1 {
                return Err(FfqError::Execution(
                    format!(
                        "{E_SUBQUERY_SCALAR_ROW_VIOLATION}: scalar subquery returned more than one row"
                    ),
                ));
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

fn strip_qual(name: &str) -> String {
    name.rsplit('.').next().unwrap_or(name).to_string()
}

fn join_key_from_row(row: &[ScalarValue], idxs: &[usize]) -> Vec<ScalarValue> {
    idxs.iter().map(|i| row[*i].clone()).collect()
}

fn join_key_has_null(key: &[ScalarValue]) -> bool {
    key.iter().any(|v| *v == ScalarValue::Null)
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
        _ => ScalarValue::Null,
    }
}

/// Spill aggregate state to disk when memory budget is exceeded.
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

/// Merge one spilled aggregate state file back into in-memory groups.
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
        _ctx: QueryContext,
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
    use std::collections::HashMap;
    use std::fs::{self, File};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use arrow::array::Int64Array;
    use arrow::record_batch::RecordBatch;
    use arrow_schema::{DataType, Field, Schema};
    #[cfg(feature = "vector")]
    use arrow::array::{FixedSizeListBuilder, Float32Array, Float32Builder};
    use ffq_execution::PhysicalOperatorFactory;
    use ffq_planner::{
        CteRefExec, CustomExec, Expr, ParquetScanExec, PhysicalPlan, UnionAllExec, WindowExpr,
        WindowFrameBound, WindowFrameExclusion, WindowFrameSpec, WindowFrameUnits, WindowFunction,
        WindowOrderExpr,
    };
    use ffq_storage::{Catalog, TableDef, TableStats};
    use ffq_planner::VectorTopKExec;
    #[cfg(feature = "vector")]
    use ffq_planner::LiteralValue;
    use ffq_storage::vector_index::{VectorIndexProvider, VectorTopKRow};
    use futures::future::BoxFuture;
    use futures::TryStreamExt;
    use parquet::arrow::ArrowWriter;

    #[cfg(feature = "vector")]
    use super::run_topk_by_score;
    use super::{
        EmbeddedRuntime, ExecOutput, QueryContext, Runtime, TraceIds, rows_to_vector_topk_output,
        run_vector_topk_with_provider, run_window_exec, run_window_exec_with_ctx,
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
        assert_eq!(run(WindowFrameExclusion::CurrentRow), vec![30.0, 30.0, 20.0]);
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
                Arc::new(Int64Array::from_iter_values((1_i64..=n).map(|v| (v % 17) + 1))),
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
            spill_dir: spill_dir.to_string_lossy().into_owned(),
        };
        let trace = TraceIds {
            query_id: "window-spill-test".to_string(),
            stage_id: 7,
            task_id: 9,
        };
        let out =
            run_window_exec_with_ctx(input, &[w], &ctx, Some(&trace)).expect("window with spill");
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
                spill_dir: "./ffq_spill_test".to_string(),
            },
            Arc::clone(&catalog),
            Arc::clone(&registry),
        ))
        .expect("execute");
        let batches = futures::executor::block_on(stream.try_collect::<Vec<RecordBatch>>())
            .expect("collect");
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 6);
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "shared CTE subplan should execute exactly once"
        );
        let _ = std::fs::remove_file(tmp);
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
