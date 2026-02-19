//! Worker runtime and task execution loop.
//!
//! Responsibilities:
//! - pull task attempts from coordinator (`GetTask`);
//! - execute stage fragments with shared planner/runtime semantics;
//! - write/register shuffle outputs for map stages;
//! - publish final query results for sink stages;
//! - report task state transitions and heartbeat.
//!
//! Retry/attempt semantics:
//! - each assignment carries an explicit `attempt`;
//! - map outputs are keyed by `(query, stage, map_task, attempt)`;
//! - coordinator-side status updates use the same attempt key so stale
//!   attempts are not mistaken for current progress.

use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashMap, hash_map::DefaultHasher};
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
use ffq_execution::{TaskContext as ExecTaskContext, compile_expr};
use ffq_planner::{AggExpr, BuildSide, ExchangeExec, Expr, PartitioningSpec, PhysicalPlan};
use ffq_shuffle::{ShuffleReader, ShuffleWriter};
use ffq_storage::parquet_provider::ParquetProvider;
#[cfg(feature = "qdrant")]
use ffq_storage::qdrant_provider::QdrantProvider;
#[cfg(feature = "qdrant")]
use ffq_storage::vector_index::VectorIndexProvider;
use ffq_storage::{Catalog, StorageProvider};
use futures::TryStreamExt;
use parquet::arrow::ArrowWriter;
use tokio::sync::{Mutex, Semaphore};
use tonic::async_trait;
use tracing::{debug, error, info, info_span};

use crate::coordinator::{Coordinator, MapOutputPartitionMeta, TaskAssignment, TaskState};
use crate::grpc::v1;

#[derive(Debug, Clone)]
/// Worker resource/configuration controls.
pub struct WorkerConfig {
    /// Stable worker id used in scheduling and heartbeats.
    pub worker_id: String,
    /// Max concurrent task executions.
    pub cpu_slots: usize,
    /// Per-task soft memory budget.
    pub per_task_memory_budget_bytes: usize,
    /// Local spill directory for memory-pressure fallback paths.
    pub spill_dir: PathBuf,
    /// Root directory containing shuffle data.
    pub shuffle_root: PathBuf,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            worker_id: "worker-1".to_string(),
            cpu_slots: 2,
            per_task_memory_budget_bytes: 64 * 1024 * 1024,
            spill_dir: PathBuf::from(".ffq_spill"),
            shuffle_root: PathBuf::from("."),
        }
    }
}

#[derive(Debug, Clone)]
/// Task-scoped execution context provided to task executors.
pub struct TaskContext {
    /// Query id for this task attempt.
    pub query_id: String,
    /// Stage id for this task attempt.
    pub stage_id: u64,
    /// Task id within stage.
    pub task_id: u64,
    /// Attempt number for retries.
    pub attempt: u32,
    /// Per-task soft memory budget.
    pub per_task_memory_budget_bytes: usize,
    /// Local spill directory.
    pub spill_dir: PathBuf,
    /// Root directory containing shuffle data.
    pub shuffle_root: PathBuf,
}

#[derive(Debug, Clone, Default)]
/// Task execution outputs returned by [`TaskExecutor`].
pub struct TaskExecutionResult {
    /// Map output partition metadata emitted by map stages.
    pub map_output_partitions: Vec<MapOutputPartitionMeta>,
    /// Output batches emitted by sink/final stages.
    pub output_batches: Vec<RecordBatch>,
    /// Whether result batches should be published to coordinator.
    pub publish_results: bool,
    /// Human-readable completion message.
    pub message: String,
}

#[async_trait]
/// Control-plane contract used by worker runtime.
pub trait WorkerControlPlane: Send + Sync {
    /// Pull up to `capacity` task assignments for `worker_id`.
    async fn get_task(&self, worker_id: &str, capacity: u32) -> Result<Vec<TaskAssignment>>;
    /// Report a task state transition and status message.
    async fn report_task_status(
        &self,
        worker_id: &str,
        assignment: &TaskAssignment,
        state: TaskState,
        message: String,
    ) -> Result<()>;
    /// Register map output partition metadata for a completed map task.
    async fn register_map_output(
        &self,
        assignment: &TaskAssignment,
        partitions: Vec<MapOutputPartitionMeta>,
    ) -> Result<()>;
    /// Publish final query results payload for client fetching.
    async fn register_query_results(&self, query_id: &str, ipc_payload: Vec<u8>) -> Result<()>;
    /// Send periodic heartbeat with currently running task count.
    async fn heartbeat(&self, worker_id: &str, running_tasks: u32) -> Result<()>;
}

#[async_trait]
/// Task execution contract for worker-assigned plan fragments.
pub trait TaskExecutor: Send + Sync {
    /// Execute one task assignment and return map/sink outputs.
    async fn execute(
        &self,
        assignment: &TaskAssignment,
        ctx: &TaskContext,
    ) -> Result<TaskExecutionResult>;
}

#[derive(Clone, Default)]
/// Default task executor that evaluates physical plan fragments in-process.
pub struct DefaultTaskExecutor {
    catalog: Arc<Catalog>,
    sink_outputs: Arc<Mutex<HashMap<String, Vec<RecordBatch>>>>,
}

impl std::fmt::Debug for DefaultTaskExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DefaultTaskExecutor").finish()
    }
}

impl DefaultTaskExecutor {
    /// Construct executor backed by provided catalog.
    pub fn new(catalog: Arc<Catalog>) -> Self {
        Self {
            catalog,
            sink_outputs: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Take and clear sink output batches for a query (test helper).
    pub async fn take_query_output(&self, query_id: &str) -> Option<Vec<RecordBatch>> {
        self.sink_outputs.lock().await.remove(query_id)
    }
}

#[async_trait]
impl TaskExecutor for DefaultTaskExecutor {
    async fn execute(
        &self,
        assignment: &TaskAssignment,
        ctx: &TaskContext,
    ) -> Result<TaskExecutionResult> {
        info!(
            query_id = %ctx.query_id,
            stage_id = ctx.stage_id,
            task_id = ctx.task_id,
            attempt = ctx.attempt,
            operator = "TaskExecutor",
            "task execution started"
        );
        let plan: PhysicalPlan = serde_json::from_slice(&assignment.plan_fragment_json)
            .map_err(|e| FfqError::Execution(format!("task plan decode failed: {e}")))?;

        let dag = crate::stage::build_stage_dag(&plan);
        let stage = dag
            .stages
            .iter()
            .find(|s| s.id.0 as u64 == ctx.stage_id)
            .ok_or_else(|| FfqError::Execution(format!("unknown stage id {}", ctx.stage_id)))?;

        let mut state = EvalState {
            next_stage_id: 1,
            map_outputs: Vec::new(),
            query_numeric_id: ctx.query_id.parse::<u64>().map_err(|e| {
                FfqError::InvalidConfig(format!("query_id must be numeric for shuffle paths: {e}"))
            })?,
        };
        let output = eval_plan_for_stage(
            &plan,
            0,
            ctx.stage_id,
            &mut state,
            ctx,
            Arc::clone(&self.catalog),
        )?;

        let mut result = TaskExecutionResult {
            map_output_partitions: state.map_outputs,
            output_batches: Vec::new(),
            publish_results: false,
            message: String::new(),
        };
        if stage.children.is_empty() {
            result.message = format!("sink stage rows={}", count_rows(&output.batches));
            result.output_batches = output.batches.clone();
            result.publish_results = true;
            self.sink_outputs
                .lock()
                .await
                .insert(ctx.query_id.clone(), output.batches);
        } else {
            result.message = format!(
                "map stage wrote {} partitions",
                result.map_output_partitions.len()
            );
        }
        info!(
            query_id = %ctx.query_id,
            stage_id = ctx.stage_id,
            task_id = ctx.task_id,
            attempt = ctx.attempt,
            map_partitions = result.map_output_partitions.len(),
            output_batches = result.output_batches.len(),
            publish_results = result.publish_results,
            "task execution completed"
        );
        Ok(result)
    }
}

#[derive(Clone)]
/// Worker runtime that orchestrates pull scheduling and task execution.
pub struct Worker<C, E>
where
    C: WorkerControlPlane + 'static,
    E: TaskExecutor + 'static,
{
    config: WorkerConfig,
    control_plane: Arc<C>,
    task_executor: Arc<E>,
    cpu_slots: Arc<Semaphore>,
}

impl<C, E> Worker<C, E>
where
    C: WorkerControlPlane + 'static,
    E: TaskExecutor + 'static,
{
    /// Build worker runtime with control plane and task executor.
    pub fn new(config: WorkerConfig, control_plane: Arc<C>, task_executor: Arc<E>) -> Self {
        let slots = config.cpu_slots.max(1);
        Self {
            config,
            control_plane,
            task_executor,
            cpu_slots: Arc::new(Semaphore::new(slots)),
        }
    }

    /// Perform one poll cycle:
    /// - pull assignments
    /// - execute up to available CPU slots
    /// - report status/map outputs/results
    pub async fn poll_once(&self) -> Result<usize> {
        let capacity = self.cpu_slots.available_permits() as u32;
        if capacity == 0 {
            return Ok(0);
        }

        let tasks = self
            .control_plane
            .get_task(&self.config.worker_id, capacity)
            .await?;
        let task_count = tasks.len();
        if tasks.is_empty() {
            self.control_plane
                .heartbeat(&self.config.worker_id, 0)
                .await?;
            return Ok(0);
        }

        let mut handles = Vec::with_capacity(tasks.len());
        for assignment in tasks {
            debug!(
                worker_id = %self.config.worker_id,
                query_id = %assignment.query_id,
                stage_id = assignment.stage_id,
                task_id = assignment.task_id,
                attempt = assignment.attempt,
                "worker picked task assignment"
            );
            let permit = self
                .cpu_slots
                .clone()
                .acquire_owned()
                .await
                .map_err(|e| FfqError::Execution(format!("failed to acquire cpu slot: {e}")))?;
            let worker_id = self.config.worker_id.clone();
            let control_plane = Arc::clone(&self.control_plane);
            let task_executor = Arc::clone(&self.task_executor);
            let task_ctx = TaskContext {
                query_id: assignment.query_id.clone(),
                stage_id: assignment.stage_id,
                task_id: assignment.task_id,
                attempt: assignment.attempt,
                per_task_memory_budget_bytes: self.config.per_task_memory_budget_bytes,
                spill_dir: self.config.spill_dir.clone(),
                shuffle_root: self.config.shuffle_root.clone(),
            };
            handles.push(tokio::spawn(async move {
                let _permit = permit;
                let result = task_executor.execute(&assignment, &task_ctx).await;
                match result {
                    Ok(exec_result) => {
                        info!(
                            worker_id = %worker_id,
                            query_id = %assignment.query_id,
                            stage_id = assignment.stage_id,
                            task_id = assignment.task_id,
                            attempt = assignment.attempt,
                            "task execution succeeded"
                        );
                        if !exec_result.map_output_partitions.is_empty() {
                            control_plane
                                .register_map_output(
                                    &assignment,
                                    exec_result.map_output_partitions.clone(),
                                )
                                .await?;
                        }
                        if exec_result.publish_results {
                            let payload = encode_record_batches_ipc(&exec_result.output_batches)?;
                            control_plane
                                .register_query_results(&assignment.query_id, payload)
                                .await?;
                        }
                        control_plane
                            .report_task_status(
                                &worker_id,
                                &assignment,
                                TaskState::Succeeded,
                                exec_result.message,
                            )
                            .await
                    }
                    Err(e) => {
                        let msg = e.to_string();
                        error!(
                            worker_id = %worker_id,
                            query_id = %assignment.query_id,
                            stage_id = assignment.stage_id,
                            task_id = assignment.task_id,
                            attempt = assignment.attempt,
                            error = %msg,
                            "task execution failed"
                        );
                        let _ = control_plane
                            .report_task_status(&worker_id, &assignment, TaskState::Failed, msg)
                            .await;
                        Err(e)
                    }
                }
            }));
        }

        for handle in handles {
            handle
                .await
                .map_err(|e| FfqError::Execution(format!("task execution join error: {e}")))??;
        }

        Ok(task_count)
    }
}

#[derive(Clone)]
/// In-process control-plane adapter for embedded/distributed tests.
pub struct InProcessControlPlane {
    coordinator: Arc<Mutex<Coordinator>>,
}

impl InProcessControlPlane {
    /// Create adapter backed by shared in-memory coordinator.
    pub fn new(coordinator: Arc<Mutex<Coordinator>>) -> Self {
        Self { coordinator }
    }
}

#[derive(Debug)]
/// gRPC-based control-plane adapter for remote coordinator connectivity.
pub struct GrpcControlPlane {
    control: Mutex<crate::grpc::ControlPlaneClient<tonic::transport::Channel>>,
    shuffle: Mutex<crate::grpc::ShuffleServiceClient<tonic::transport::Channel>>,
    heartbeat: Mutex<crate::grpc::HeartbeatServiceClient<tonic::transport::Channel>>,
}

impl GrpcControlPlane {
    /// Connect gRPC control/shuffle/heartbeat clients to a coordinator endpoint.
    pub async fn connect(endpoint: &str) -> Result<Self> {
        let control = crate::grpc::ControlPlaneClient::connect(endpoint.to_string())
            .await
            .map_err(map_transport_err)?;
        let shuffle = crate::grpc::ShuffleServiceClient::connect(endpoint.to_string())
            .await
            .map_err(map_transport_err)?;
        let heartbeat = crate::grpc::HeartbeatServiceClient::connect(endpoint.to_string())
            .await
            .map_err(map_transport_err)?;
        Ok(Self {
            control: Mutex::new(control),
            shuffle: Mutex::new(shuffle),
            heartbeat: Mutex::new(heartbeat),
        })
    }
}

#[async_trait]
impl WorkerControlPlane for InProcessControlPlane {
    async fn get_task(&self, worker_id: &str, capacity: u32) -> Result<Vec<TaskAssignment>> {
        let mut c = self.coordinator.lock().await;
        c.get_task(worker_id, capacity)
    }

    async fn report_task_status(
        &self,
        worker_id: &str,
        assignment: &TaskAssignment,
        state: TaskState,
        message: String,
    ) -> Result<()> {
        let mut c = self.coordinator.lock().await;
        c.report_task_status(
            &assignment.query_id,
            assignment.stage_id,
            assignment.task_id,
            assignment.attempt,
            state,
            Some(worker_id),
            message,
        )
    }

    async fn register_map_output(
        &self,
        assignment: &TaskAssignment,
        partitions: Vec<MapOutputPartitionMeta>,
    ) -> Result<()> {
        let mut c = self.coordinator.lock().await;
        c.register_map_output(
            assignment.query_id.clone(),
            assignment.stage_id,
            assignment.task_id,
            assignment.attempt,
            partitions,
        )
    }

    async fn heartbeat(&self, worker_id: &str, running_tasks: u32) -> Result<()> {
        let mut c = self.coordinator.lock().await;
        c.heartbeat(worker_id, running_tasks)
    }

    async fn register_query_results(&self, query_id: &str, ipc_payload: Vec<u8>) -> Result<()> {
        let mut c = self.coordinator.lock().await;
        c.register_query_results(query_id.to_string(), ipc_payload)
    }
}

#[async_trait]
impl WorkerControlPlane for GrpcControlPlane {
    async fn get_task(&self, worker_id: &str, capacity: u32) -> Result<Vec<TaskAssignment>> {
        let mut client = self.control.lock().await;
        let response = client
            .get_task(v1::GetTaskRequest {
                worker_id: worker_id.to_string(),
                capacity,
            })
            .await
            .map_err(map_tonic_err)?;
        Ok(response
            .into_inner()
            .tasks
            .into_iter()
            .map(|t| TaskAssignment {
                query_id: t.query_id,
                stage_id: t.stage_id,
                task_id: t.task_id,
                attempt: t.attempt,
                plan_fragment_json: t.plan_fragment_json,
            })
            .collect())
    }

    async fn report_task_status(
        &self,
        _worker_id: &str,
        assignment: &TaskAssignment,
        state: TaskState,
        message: String,
    ) -> Result<()> {
        let mut client = self.control.lock().await;
        client
            .report_task_status(v1::ReportTaskStatusRequest {
                query_id: assignment.query_id.clone(),
                stage_id: assignment.stage_id,
                task_id: assignment.task_id,
                attempt: assignment.attempt,
                state: proto_task_state(state) as i32,
                message,
            })
            .await
            .map_err(map_tonic_err)?;
        Ok(())
    }

    async fn register_map_output(
        &self,
        assignment: &TaskAssignment,
        partitions: Vec<MapOutputPartitionMeta>,
    ) -> Result<()> {
        let mut client = self.shuffle.lock().await;
        client
            .register_map_output(v1::RegisterMapOutputRequest {
                query_id: assignment.query_id.clone(),
                stage_id: assignment.stage_id,
                map_task: assignment.task_id,
                attempt: assignment.attempt,
                partitions: partitions
                    .into_iter()
                    .map(|p| v1::MapOutputPartition {
                        reduce_partition: p.reduce_partition,
                        bytes: p.bytes,
                        rows: p.rows,
                        batches: p.batches,
                    })
                    .collect(),
            })
            .await
            .map_err(map_tonic_err)?;
        Ok(())
    }

    async fn heartbeat(&self, worker_id: &str, running_tasks: u32) -> Result<()> {
        let mut client = self.heartbeat.lock().await;
        client
            .heartbeat(v1::HeartbeatRequest {
                worker_id: worker_id.to_string(),
                at_ms: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_err(|e| FfqError::Execution(format!("clock error: {e}")))?
                    .as_millis() as u64,
                running_tasks,
            })
            .await
            .map_err(map_tonic_err)?;
        Ok(())
    }

    async fn register_query_results(&self, query_id: &str, ipc_payload: Vec<u8>) -> Result<()> {
        let mut client = self.control.lock().await;
        client
            .register_query_results(v1::RegisterQueryResultsRequest {
                query_id: query_id.to_string(),
                ipc_payload,
            })
            .await
            .map_err(map_tonic_err)?;
        Ok(())
    }
}

fn map_tonic_err(err: tonic::Status) -> FfqError {
    FfqError::Execution(format!("grpc call failed: {err}"))
}

fn map_transport_err(err: tonic::transport::Error) -> FfqError {
    FfqError::Execution(format!("grpc connect failed: {err}"))
}

fn proto_task_state(state: TaskState) -> v1::TaskState {
    match state {
        TaskState::Queued => v1::TaskState::Queued,
        TaskState::Running => v1::TaskState::Running,
        TaskState::Succeeded => v1::TaskState::Succeeded,
        TaskState::Failed => v1::TaskState::Failed,
    }
}

/// Encode a set of record batches as Arrow IPC stream bytes.
pub fn encode_record_batches_ipc(batches: &[RecordBatch]) -> Result<Vec<u8>> {
    if batches.is_empty() {
        return Ok(Vec::new());
    }
    let schema = batches[0].schema();
    let mut out = Vec::<u8>::new();
    {
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut out, schema.as_ref())
            .map_err(|e| FfqError::Execution(format!("ipc writer init failed: {e}")))?;
        for batch in batches {
            writer
                .write(batch)
                .map_err(|e| FfqError::Execution(format!("ipc write failed: {e}")))?;
        }
        writer
            .finish()
            .map_err(|e| FfqError::Execution(format!("ipc finish failed: {e}")))?;
    }
    Ok(out)
}

#[derive(Clone)]
struct ExecOutput {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

struct EvalState {
    next_stage_id: u64,
    map_outputs: Vec<MapOutputPartitionMeta>,
    query_numeric_id: u64,
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

fn eval_plan_for_stage(
    plan: &PhysicalPlan,
    current_stage: u64,
    target_stage: u64,
    state: &mut EvalState,
    ctx: &TaskContext,
    catalog: Arc<Catalog>,
) -> Result<ExecOutput> {
    let started = Instant::now();
    let _span = info_span!(
        "operator_execute",
        query_id = %ctx.query_id,
        stage_id = ctx.stage_id,
        task_id = ctx.task_id,
        operator = operator_name(plan)
    )
    .entered();
    let eval = match plan {
        PhysicalPlan::ParquetScan(scan) => {
            let mut table = catalog.get(&scan.table)?.clone();
            if let Some(schema) = &scan.schema {
                table.schema = Some(schema.clone());
            }
            let provider = ParquetProvider::new();
            let node = provider.scan(
                &table,
                scan.projection.clone(),
                scan.filters.iter().map(|f| format!("{f:?}")).collect(),
            )?;
            let stream = node.execute(Arc::new(ExecTaskContext {
                batch_size_rows: 8192,
                mem_budget_bytes: ctx.per_task_memory_budget_bytes,
            }))?;
            let schema = stream.schema();
            let batches = futures::executor::block_on(stream.try_collect::<Vec<RecordBatch>>())?;
            Ok(OpEval {
                out: ExecOutput { schema, batches },
                in_rows: 0,
                in_batches: 0,
                in_bytes: 0,
            })
        }
        PhysicalPlan::ParquetWrite(write) => {
            let child = eval_plan_for_stage(
                &write.input,
                current_stage,
                target_stage,
                state,
                ctx,
                catalog.clone(),
            )?;
            let table = catalog.get(&write.table)?.clone();
            let (in_rows, in_batches, in_bytes) = batch_stats(&child.batches);
            write_parquet_sink(&table, &child)?;
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
        PhysicalPlan::Exchange(exchange) => match exchange {
            ExchangeExec::Broadcast(x) => {
                let out = eval_plan_for_stage(
                    &x.input,
                    current_stage,
                    target_stage,
                    state,
                    ctx,
                    catalog,
                )?;
                let (in_rows, in_batches, in_bytes) = batch_stats(&out.batches);
                Ok(OpEval {
                    out,
                    in_rows,
                    in_batches,
                    in_bytes,
                })
            }
            ExchangeExec::ShuffleRead(read) => {
                let upstream_stage_id = state.next_stage_id;
                state.next_stage_id += 1;
                if current_stage == target_stage {
                    let out = read_stage_input_from_shuffle(
                        upstream_stage_id,
                        &read.partitioning,
                        state.query_numeric_id,
                        ctx,
                    )?;
                    Ok(OpEval {
                        out,
                        in_rows: 0,
                        in_batches: 0,
                        in_bytes: 0,
                    })
                } else {
                    let out = eval_plan_for_stage(
                        &read.input,
                        upstream_stage_id,
                        target_stage,
                        state,
                        ctx,
                        catalog,
                    )?;
                    let (in_rows, in_batches, in_bytes) = batch_stats(&out.batches);
                    Ok(OpEval {
                        out,
                        in_rows,
                        in_batches,
                        in_bytes,
                    })
                }
            }
            ExchangeExec::ShuffleWrite(write) => {
                let child = eval_plan_for_stage(
                    &write.input,
                    current_stage,
                    target_stage,
                    state,
                    ctx,
                    catalog,
                )?;
                if current_stage == target_stage {
                    let metas = write_stage_shuffle_outputs(
                        &child,
                        &write.partitioning,
                        state.query_numeric_id,
                        ctx,
                    )?;
                    state.map_outputs.extend(metas);
                }
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
                eval_plan_for_stage(&agg.input, current_stage, target_stage, state, ctx, catalog)?;
            let (in_rows, in_batches, in_bytes) = batch_stats(&child.batches);
            let out = run_hash_aggregate(
                child,
                agg.group_exprs.clone(),
                agg.aggr_exprs.clone(),
                AggregateMode::Partial,
                ctx,
            )?;
            Ok(OpEval {
                out,
                in_rows,
                in_batches,
                in_bytes,
            })
        }
        PhysicalPlan::FinalHashAggregate(agg) => {
            let child =
                eval_plan_for_stage(&agg.input, current_stage, target_stage, state, ctx, catalog)?;
            let (in_rows, in_batches, in_bytes) = batch_stats(&child.batches);
            let out = run_hash_aggregate(
                child,
                agg.group_exprs.clone(),
                agg.aggr_exprs.clone(),
                AggregateMode::Final,
                ctx,
            )?;
            Ok(OpEval {
                out,
                in_rows,
                in_batches,
                in_bytes,
            })
        }
        PhysicalPlan::HashJoin(join) => {
            let ffq_planner::HashJoinExec {
                left,
                right,
                on,
                build_side,
                ..
            } = join;
            let left = eval_plan_for_stage(
                left,
                current_stage,
                target_stage,
                state,
                ctx,
                Arc::clone(&catalog),
            )?;
            let right =
                eval_plan_for_stage(right, current_stage, target_stage, state, ctx, catalog)?;
            let (left_rows, left_batches, left_bytes) = batch_stats(&left.batches);
            let (right_rows, right_batches, right_bytes) = batch_stats(&right.batches);
            let out = run_hash_join(left, right, on.clone(), *build_side, ctx)?;
            Ok(OpEval {
                out,
                in_rows: left_rows + right_rows,
                in_batches: left_batches + right_batches,
                in_bytes: left_bytes + right_bytes,
            })
        }
        PhysicalPlan::Project(project) => {
            let child = eval_plan_for_stage(
                &project.input,
                current_stage,
                target_stage,
                state,
                ctx,
                catalog,
            )?;
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
            let child = eval_plan_for_stage(
                &filter.input,
                current_stage,
                target_stage,
                state,
                ctx,
                catalog,
            )?;
            let pred = compile_expr(&filter.predicate, &child.schema)?;
            let mut out = Vec::new();
            for batch in &child.batches {
                let mask = pred.evaluate(batch)?;
                let mask = mask
                    .as_any()
                    .downcast_ref::<arrow::array::BooleanArray>()
                    .ok_or_else(|| {
                        FfqError::Execution("filter predicate must evaluate to boolean".to_string())
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
            let child = eval_plan_for_stage(
                &limit.input,
                current_stage,
                target_stage,
                state,
                ctx,
                catalog,
            )?;
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
            let child = eval_plan_for_stage(
                &topk.input,
                current_stage,
                target_stage,
                state,
                ctx,
                catalog,
            )?;
            let (in_rows, in_batches, in_bytes) = batch_stats(&child.batches);
            let out = run_topk_by_score(child, topk.score_expr.clone(), topk.k)?;
            Ok(OpEval {
                out,
                in_rows,
                in_batches,
                in_bytes,
            })
        }
        PhysicalPlan::VectorTopK(exec) => Ok(OpEval {
            out: execute_vector_topk(exec, catalog)?,
            in_rows: 0,
            in_batches: 0,
            in_bytes: 0,
        }),
        PhysicalPlan::CoalesceBatches(_) => Err(FfqError::Unsupported(
            "CoalesceBatches execution is not implemented in distributed worker".to_string(),
        )),
    }?;
    let (out_rows, out_batches, out_bytes) = batch_stats(&eval.out.batches);
    global_metrics().record_operator(
        &ctx.query_id,
        ctx.stage_id,
        ctx.task_id,
        operator_name(plan),
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

struct OpEval {
    out: ExecOutput,
    in_rows: u64,
    in_batches: u64,
    in_bytes: u64,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct MockVectorRow {
    id: i64,
    score: f32,
    #[serde(default)]
    payload: Option<String>,
}

fn execute_vector_topk(
    exec: &ffq_planner::VectorTopKExec,
    catalog: Arc<Catalog>,
) -> Result<ExecOutput> {
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
        return Err(FfqError::Unsupported(
            "qdrant feature is disabled; build ffq-distributed with --features qdrant".to_string(),
        ));
    }
    #[cfg(feature = "qdrant")]
    {
        let provider = QdrantProvider::from_table(&table)?;
        let rows = futures::executor::block_on(provider.topk(
            exec.query_vector.clone(),
            exec.k,
            exec.filter.clone(),
        ))?;
        rows_to_vector_topk_output(rows)
    }
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

fn write_stage_shuffle_outputs(
    child: &ExecOutput,
    partitioning: &PartitioningSpec,
    query_numeric_id: u64,
    ctx: &TaskContext,
) -> Result<Vec<MapOutputPartitionMeta>> {
    let started = Instant::now();
    let writer = ShuffleWriter::new(&ctx.shuffle_root);
    let partitioned = partition_batches(child, partitioning)?;
    let mut metas = Vec::new();
    for (reduce, batches) in partitioned {
        if batches.is_empty() {
            continue;
        }
        let meta = writer.write_partition(
            query_numeric_id,
            ctx.stage_id,
            ctx.task_id,
            ctx.attempt,
            reduce,
            &batches,
        )?;
        metas.push(meta);
    }
    let index = writer.write_map_task_index(
        query_numeric_id,
        ctx.stage_id,
        ctx.task_id,
        ctx.attempt,
        metas.clone(),
    )?;
    let out = index
        .partitions
        .into_iter()
        .map(|m| MapOutputPartitionMeta {
            reduce_partition: m.reduce_partition,
            bytes: m.bytes,
            rows: m.rows,
            batches: m.batches,
        })
        .collect::<Vec<_>>();
    let written_bytes = out.iter().map(|m| m.bytes).sum::<u64>();
    global_metrics().record_shuffle_write(
        &ctx.query_id,
        ctx.stage_id,
        ctx.task_id,
        written_bytes,
        out.len() as u64,
        started.elapsed().as_secs_f64(),
    );
    Ok(out)
}

fn read_stage_input_from_shuffle(
    upstream_stage_id: u64,
    partitioning: &PartitioningSpec,
    query_numeric_id: u64,
    ctx: &TaskContext,
) -> Result<ExecOutput> {
    let started = Instant::now();
    let reader = ShuffleReader::new(&ctx.shuffle_root);
    let mut out_batches = Vec::new();
    let mut read_partitions = 0_u64;
    match partitioning {
        PartitioningSpec::Single => {
            if let Ok((_attempt, batches)) =
                reader.read_partition_latest(query_numeric_id, upstream_stage_id, 0, 0)
            {
                out_batches.extend(batches);
                read_partitions += 1;
            }
        }
        PartitioningSpec::HashKeys { partitions, .. } => {
            for reduce in 0..*partitions {
                if let Ok((_attempt, batches)) = reader.read_partition_latest(
                    query_numeric_id,
                    upstream_stage_id,
                    0,
                    reduce as u32,
                ) {
                    out_batches.extend(batches);
                    read_partitions += 1;
                }
            }
        }
    }
    let schema = out_batches
        .first()
        .map(|b| b.schema())
        .unwrap_or_else(|| Arc::new(Schema::empty()));
    let out = ExecOutput {
        schema,
        batches: out_batches,
    };
    let (_, _, read_bytes) = batch_stats(&out.batches);
    global_metrics().record_shuffle_read(
        &ctx.query_id,
        ctx.stage_id,
        ctx.task_id,
        read_bytes,
        read_partitions,
        started.elapsed().as_secs_f64(),
    );
    Ok(out)
}

fn partition_batches(
    child: &ExecOutput,
    partitioning: &PartitioningSpec,
) -> Result<HashMap<u32, Vec<RecordBatch>>> {
    match partitioning {
        PartitioningSpec::Single => Ok(HashMap::from([(0_u32, child.batches.clone())])),
        PartitioningSpec::HashKeys { keys, partitions } => {
            let key_idx = resolve_key_indexes(&child.schema, keys)?;
            let rows = rows_from_batches(child)?;
            let mut by_part = HashMap::<u32, Vec<Vec<ScalarValue>>>::new();
            for row in rows {
                let key = key_idx.iter().map(|i| row[*i].clone()).collect::<Vec<_>>();
                let part = (hash_key(&key) as usize % *partitions) as u32;
                by_part.entry(part).or_default().push(row);
            }

            let mut out = HashMap::<u32, Vec<RecordBatch>>::new();
            for (part, part_rows) in by_part {
                let batch = rows_to_batch(&child.schema, &part_rows)?;
                out.entry(part).or_default().push(batch);
            }
            Ok(out)
        }
    }
}

fn count_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
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

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
enum AggState {
    Count(i64),
    SumInt(i64),
    SumFloat(f64),
    Min(Option<ScalarValue>),
    Max(Option<ScalarValue>),
    Avg { sum: f64, count: i64 },
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct SpillRow {
    key: Vec<ScalarValue>,
    states: Vec<AggState>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
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
    ctx: &TaskContext,
) -> Result<ExecOutput> {
    #[cfg(feature = "profiling")]
    let _profile_span = info_span!(
        "profile_hash_join",
        query_id = %ctx.query_id,
        stage_id = ctx.stage_id,
        task_id = ctx.task_id
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

    let joined_rows = if ctx.per_task_memory_budget_bytes > 0
        && estimate_join_rows_bytes(build_rows) > ctx.per_task_memory_budget_bytes
    {
        grace_hash_join(
            build_rows,
            probe_rows,
            &build_key_idx,
            &probe_key_idx,
            build_input_side,
            ctx,
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
    ctx: &TaskContext,
) -> Result<Vec<Vec<ScalarValue>>> {
    #[cfg(feature = "profiling")]
    let _profile_span = info_span!(
        "profile_grace_hash_join",
        query_id = %ctx.query_id,
        stage_id = ctx.stage_id,
        task_id = ctx.task_id
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
        &ctx.query_id,
        ctx.stage_id,
        ctx.task_id,
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
    ctx: &TaskContext,
) -> Result<ExecOutput> {
    #[cfg(feature = "profiling")]
    let _profile_span = info_span!(
        "profile_hash_aggregate",
        query_id = %ctx.query_id,
        stage_id = ctx.stage_id,
        task_id = ctx.task_id,
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
        maybe_spill(&mut groups, &mut spills, ctx)?;
    }

    if group_exprs.is_empty() && groups.is_empty() {
        groups.insert(vec![], init_states(&specs));
    }

    if !groups.is_empty() {
        maybe_spill(&mut groups, &mut spills, ctx)?;
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
    ctx: &TaskContext,
) -> Result<()> {
    if groups.is_empty() || ctx.per_task_memory_budget_bytes == 0 {
        return Ok(());
    }
    let estimated = estimate_groups_bytes(groups);
    if estimated <= ctx.per_task_memory_budget_bytes {
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
        writer.write_all(line.as_bytes())?;
        writer.write_all(b"\n")?;
    }
    writer.flush()?;
    let spill_bytes = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
    global_metrics().record_spill(
        &ctx.query_id,
        ctx.stage_id,
        ctx.task_id,
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
            "scalar type not supported yet: {other:?}"
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
                    "output FixedSizeList item type not supported: {:?}",
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
            "output type not supported yet: {other:?}"
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordinator::CoordinatorConfig;
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
            &PhysicalPlannerConfig::default(),
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
                let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                assert!(rows > 0);
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
}
