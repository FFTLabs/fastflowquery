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
use std::collections::{BinaryHeap, HashMap, HashSet, hash_map::DefaultHasher};
use std::fs::{self, File};
use std::hash::{Hash, Hasher};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow::array::{
    Array, ArrayRef, BooleanBuilder, FixedSizeListBuilder, Float32Builder, Float64Builder,
    Int64Array, Int64Builder, StringBuilder,
};
use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use ffq_common::metrics::global_metrics;
use ffq_common::{FfqError, MemoryPressureSignal, MemorySpillManager, Result};
use ffq_execution::{
    PhysicalOperatorRegistry, TaskContext as ExecTaskContext, compile_expr,
    global_physical_operator_registry,
};
use ffq_planner::{
    AggExpr, BinaryOp, BuildSide, ExchangeExec, Expr, JoinType, PartitioningSpec, PhysicalPlan,
    WindowExpr, WindowFrameBound, WindowFrameExclusion, WindowFrameSpec, WindowFrameUnits,
    WindowFunction, WindowOrderExpr,
};
use ffq_shuffle::ShuffleCompressionCodec;
use ffq_shuffle::aggregate_partition_chunks;
use ffq_shuffle::{ShuffleReader, ShuffleWriter};
#[cfg(feature = "s3")]
use ffq_storage::object_store_provider::{ObjectStoreProvider, is_object_store_uri};
use ffq_storage::parquet_provider::ParquetProvider;
#[cfg(feature = "qdrant")]
use ffq_storage::qdrant_provider::QdrantProvider;
#[cfg(feature = "qdrant")]
use ffq_storage::vector_index::{VectorIndexProvider, VectorQueryOptions};
use ffq_storage::{Catalog, StorageProvider};
use futures::TryStreamExt;
use parquet::arrow::ArrowWriter;
use tokio::sync::{Mutex, Semaphore};
use tonic::async_trait;
use tracing::{debug, error, info, info_span};

use crate::coordinator::{Coordinator, MapOutputPartitionMeta, TaskAssignment, TaskState};
use crate::grpc::v1;

const E_SUBQUERY_SCALAR_ROW_VIOLATION: &str = "E_SUBQUERY_SCALAR_ROW_VIOLATION";
const MIN_TASK_BATCH_SIZE_ROWS: usize = 256;

#[derive(Debug, Clone)]
/// Worker resource/configuration controls.
pub struct WorkerConfig {
    /// Stable worker id used in scheduling and heartbeats.
    pub worker_id: String,
    /// Max concurrent task executions.
    pub cpu_slots: usize,
    /// Per-task soft memory budget.
    pub per_task_memory_budget_bytes: usize,
    /// Engine-level memory budget shared by all concurrent tasks on this worker.
    pub engine_memory_budget_bytes: usize,
    /// Number of radix bits for in-memory hash join partitioning.
    pub join_radix_bits: u8,
    /// Enables build-side bloom prefiltering on probe rows for join execution.
    pub join_bloom_enabled: bool,
    /// Bloom filter bit-width as log2(number_of_bits) for join prefiltering.
    pub join_bloom_bits: u8,
    /// Shuffle partition payload compression codec.
    pub shuffle_compression_codec: ShuffleCompressionCodec,
    /// Number of partition metadata entries to publish per register call.
    pub map_output_publish_window_partitions: u32,
    /// Number of assigned reduce partitions fetched per read window.
    pub reduce_fetch_window_partitions: u32,
    /// Base execution batch size used when pressure is normal.
    pub batch_size_rows: usize,
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
            engine_memory_budget_bytes: 128 * 1024 * 1024,
            join_radix_bits: 8,
            join_bloom_enabled: true,
            join_bloom_bits: 20,
            shuffle_compression_codec: ShuffleCompressionCodec::Lz4,
            map_output_publish_window_partitions: 1,
            reduce_fetch_window_partitions: 4,
            batch_size_rows: 8192,
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
    /// Runtime batch size hint for operator execution.
    pub batch_size_rows: usize,
    /// Spill trigger ratio numerator.
    pub spill_trigger_ratio_num: u32,
    /// Spill trigger ratio denominator.
    pub spill_trigger_ratio_den: u32,
    /// Number of radix bits for in-memory hash join partitioning.
    pub join_radix_bits: u8,
    /// Enables build-side bloom prefiltering on probe rows for join execution.
    pub join_bloom_enabled: bool,
    /// Bloom filter bit-width as log2(number_of_bits) for join prefiltering.
    pub join_bloom_bits: u8,
    /// Shuffle partition payload compression codec.
    pub shuffle_compression_codec: ShuffleCompressionCodec,
    /// Number of assigned reduce partitions fetched per read window.
    pub reduce_fetch_window_partitions: u32,
    /// Number of partition metadata entries to publish per register call.
    pub map_output_publish_window_partitions: u32,
    /// Local spill directory.
    pub spill_dir: PathBuf,
    /// Root directory containing shuffle data.
    pub shuffle_root: PathBuf,
    /// Reduce partitions assigned to this task (for shuffle-read stages).
    pub assigned_reduce_partitions: Vec<u32>,
    /// Hash-shard split index for assigned reduce partitions.
    pub assigned_reduce_split_index: u32,
    /// Hash-shard split count for assigned reduce partitions.
    pub assigned_reduce_split_count: u32,
}

fn spill_signal_for_task_ctx(ctx: &TaskContext) -> MemoryPressureSignal {
    MemoryPressureSignal {
        pressure: ffq_common::MemoryPressure::Normal,
        effective_mem_budget_bytes: ctx.per_task_memory_budget_bytes,
        suggested_batch_size_rows: ctx.batch_size_rows,
        spill_trigger_ratio_num: ctx.spill_trigger_ratio_num.max(1),
        spill_trigger_ratio_den: ctx.spill_trigger_ratio_den.max(1),
    }
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
    /// Observed reducer in-flight bytes for this task.
    pub reduce_fetch_inflight_bytes: u64,
    /// Observed reducer queue depth for this task.
    pub reduce_fetch_queue_depth: u32,
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
        reduce_fetch_inflight_bytes: u64,
        reduce_fetch_queue_depth: u32,
    ) -> Result<()>;
    /// Register map output partition metadata for a completed map task.
    async fn register_map_output(
        &self,
        assignment: &TaskAssignment,
        partitions: Vec<MapOutputPartitionMeta>,
    ) -> Result<()>;
    /// Publish final query results payload for client fetching.
    async fn register_query_results(&self, query_id: &str, ipc_payload: Vec<u8>) -> Result<()>;
    /// Send periodic heartbeat with currently running task count and worker capabilities.
    async fn heartbeat(
        &self,
        worker_id: &str,
        running_tasks: u32,
        custom_operator_capabilities: &[String],
    ) -> Result<()>;
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
    physical_registry: Arc<PhysicalOperatorRegistry>,
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
        Self::with_physical_registry(catalog, global_physical_operator_registry())
    }

    /// Construct executor with explicit physical operator registry.
    pub fn with_physical_registry(
        catalog: Arc<Catalog>,
        physical_registry: Arc<PhysicalOperatorRegistry>,
    ) -> Self {
        Self {
            catalog,
            physical_registry,
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
            cte_cache: HashMap::new(),
        };
        let output = eval_plan_for_stage(
            &plan,
            0,
            ctx.stage_id,
            &mut state,
            ctx,
            Arc::clone(&self.catalog),
            Arc::clone(&self.physical_registry),
        )?;

        let mut result = TaskExecutionResult {
            map_output_partitions: state.map_outputs,
            output_batches: Vec::new(),
            publish_results: false,
            message: String::new(),
            reduce_fetch_inflight_bytes: 0,
            reduce_fetch_queue_depth: 0,
        };
        if stage.children.is_empty() {
            result.message = format!("sink stage rows={}", count_rows(&output.batches));
            result.output_batches = output.batches.clone();
            result.publish_results = true;
            let mut sink = self.sink_outputs.lock().await;
            sink.entry(ctx.query_id.clone())
                .or_default()
                .extend(output.batches.clone());
        } else {
            result.message = format!(
                "map stage wrote {} partitions",
                result.map_output_partitions.len()
            );
        }
        if !ctx.assigned_reduce_partitions.is_empty() {
            let (_, _, bytes) = batch_stats(&output.batches);
            result.reduce_fetch_inflight_bytes = bytes;
            result.reduce_fetch_queue_depth = ctx
                .assigned_reduce_partitions
                .len()
                .try_into()
                .unwrap_or(u32::MAX);
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
    memory_manager: Arc<MemorySpillManager>,
}

impl<C, E> Worker<C, E>
where
    C: WorkerControlPlane + 'static,
    E: TaskExecutor + 'static,
{
    /// Build worker runtime with control plane and task executor.
    pub fn new(config: WorkerConfig, control_plane: Arc<C>, task_executor: Arc<E>) -> Self {
        let slots = config.cpu_slots.max(1);
        let memory_manager = MemorySpillManager::new(
            config.engine_memory_budget_bytes,
            config.batch_size_rows,
            MIN_TASK_BATCH_SIZE_ROWS,
        );
        Self {
            config,
            control_plane,
            task_executor,
            cpu_slots: Arc::new(Semaphore::new(slots)),
            memory_manager,
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
        let capabilities = global_physical_operator_registry().names();
        self.control_plane
            .heartbeat(&self.config.worker_id, 0, &capabilities)
            .await?;

        let tasks = self
            .control_plane
            .get_task(&self.config.worker_id, capacity)
            .await?;
        let task_count = tasks.len();
        if tasks.is_empty() {
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
            let requested = self.config.per_task_memory_budget_bytes;
            let reservation = self.memory_manager.reserve(requested);
            let signal = reservation.signal();
            let task_ctx = TaskContext {
                query_id: assignment.query_id.clone(),
                stage_id: assignment.stage_id,
                task_id: assignment.task_id,
                attempt: assignment.attempt,
                per_task_memory_budget_bytes: signal.effective_mem_budget_bytes,
                batch_size_rows: signal.suggested_batch_size_rows,
                spill_trigger_ratio_num: signal.spill_trigger_ratio_num,
                spill_trigger_ratio_den: signal.spill_trigger_ratio_den,
                join_radix_bits: self.config.join_radix_bits,
                join_bloom_enabled: self.config.join_bloom_enabled,
                join_bloom_bits: self.config.join_bloom_bits,
                shuffle_compression_codec: self.config.shuffle_compression_codec,
                reduce_fetch_window_partitions: assignment
                    .recommended_reduce_fetch_window_partitions
                    .max(1),
                map_output_publish_window_partitions: assignment
                    .recommended_map_output_publish_window_partitions
                    .max(1),
                spill_dir: self.config.spill_dir.clone(),
                shuffle_root: self.config.shuffle_root.clone(),
                assigned_reduce_partitions: assignment.assigned_reduce_partitions.clone(),
                assigned_reduce_split_index: assignment.assigned_reduce_split_index,
                assigned_reduce_split_count: assignment.assigned_reduce_split_count,
            };
            handles.push(tokio::spawn(async move {
                let _reservation = reservation;
                let _permit = permit;
                let _ = control_plane
                    .report_task_status(
                        &worker_id,
                        &assignment,
                        TaskState::Running,
                        "running".to_string(),
                        0,
                        assignment.recommended_reduce_fetch_window_partitions.max(1),
                    )
                    .await;
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
                            let publish_window =
                                task_ctx.map_output_publish_window_partitions.max(1) as usize;
                            for chunk in exec_result.map_output_partitions.chunks(publish_window) {
                                let commit_markers = chunk
                                    .iter()
                                    .cloned()
                                    .map(|mut p| {
                                        p.finalized = false;
                                        p
                                    })
                                    .collect::<Vec<_>>();
                                control_plane
                                    .register_map_output(&assignment, commit_markers)
                                    .await?;
                                tokio::task::yield_now().await;
                            }
                            for chunk in exec_result.map_output_partitions.chunks(publish_window) {
                                let finalize_markers = chunk
                                    .iter()
                                    .cloned()
                                    .map(|mut p| {
                                        p.finalized = true;
                                        p
                                    })
                                    .collect::<Vec<_>>();
                                control_plane
                                    .register_map_output(&assignment, finalize_markers)
                                    .await?;
                                tokio::task::yield_now().await;
                            }
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
                                exec_result.reduce_fetch_inflight_bytes,
                                exec_result.reduce_fetch_queue_depth,
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
                            .report_task_status(
                                &worker_id,
                                &assignment,
                                TaskState::Failed,
                                msg,
                                0,
                                0,
                            )
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
        reduce_fetch_inflight_bytes: u64,
        reduce_fetch_queue_depth: u32,
    ) -> Result<()> {
        let mut c = self.coordinator.lock().await;
        c.report_task_status_with_pressure(
            &assignment.query_id,
            assignment.stage_id,
            assignment.task_id,
            assignment.attempt,
            assignment.layout_version,
            assignment.layout_fingerprint,
            state,
            Some(worker_id),
            message,
            reduce_fetch_inflight_bytes,
            reduce_fetch_queue_depth,
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
            assignment.layout_version,
            assignment.layout_fingerprint,
            partitions,
        )
    }

    async fn heartbeat(
        &self,
        worker_id: &str,
        running_tasks: u32,
        custom_operator_capabilities: &[String],
    ) -> Result<()> {
        let mut c = self.coordinator.lock().await;
        c.heartbeat(worker_id, running_tasks, custom_operator_capabilities)
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
                assigned_reduce_partitions: t.assigned_reduce_partitions,
                assigned_reduce_split_index: t.assigned_reduce_split_index,
                assigned_reduce_split_count: t.assigned_reduce_split_count,
                layout_version: t.layout_version,
                layout_fingerprint: t.layout_fingerprint,
                recommended_map_output_publish_window_partitions: t
                    .recommended_map_output_publish_window_partitions,
                recommended_reduce_fetch_window_partitions: t
                    .recommended_reduce_fetch_window_partitions,
            })
            .collect())
    }

    async fn report_task_status(
        &self,
        _worker_id: &str,
        assignment: &TaskAssignment,
        state: TaskState,
        message: String,
        reduce_fetch_inflight_bytes: u64,
        reduce_fetch_queue_depth: u32,
    ) -> Result<()> {
        let mut client = self.control.lock().await;
        client
            .report_task_status(v1::ReportTaskStatusRequest {
                query_id: assignment.query_id.clone(),
                stage_id: assignment.stage_id,
                task_id: assignment.task_id,
                attempt: assignment.attempt,
                layout_version: assignment.layout_version,
                layout_fingerprint: assignment.layout_fingerprint,
                state: proto_task_state(state) as i32,
                message,
                reduce_fetch_inflight_bytes,
                reduce_fetch_queue_depth,
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
                layout_version: assignment.layout_version,
                layout_fingerprint: assignment.layout_fingerprint,
                partitions: partitions
                    .into_iter()
                    .map(|p| v1::MapOutputPartition {
                        reduce_partition: p.reduce_partition,
                        bytes: p.bytes,
                        rows: p.rows,
                        batches: p.batches,
                        stream_epoch: p.stream_epoch,
                        committed_offset: p.committed_offset,
                        finalized: p.finalized,
                    })
                    .collect(),
            })
            .await
            .map_err(map_tonic_err)?;
        Ok(())
    }

    async fn heartbeat(
        &self,
        worker_id: &str,
        running_tasks: u32,
        custom_operator_capabilities: &[String],
    ) -> Result<()> {
        let mut client = self.heartbeat.lock().await;
        client
            .heartbeat(v1::HeartbeatRequest {
                worker_id: worker_id.to_string(),
                at_ms: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_err(|e| FfqError::Execution(format!("clock error: {e}")))?
                    .as_millis() as u64,
                running_tasks,
                custom_operator_capabilities: custom_operator_capabilities.to_vec(),
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
    cte_cache: HashMap<String, ExecOutput>,
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

fn eval_plan_for_stage(
    plan: &PhysicalPlan,
    current_stage: u64,
    target_stage: u64,
    state: &mut EvalState,
    ctx: &TaskContext,
    catalog: Arc<Catalog>,
    physical_registry: Arc<PhysicalOperatorRegistry>,
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
            #[cfg(feature = "s3")]
            let provider: Arc<dyn StorageProvider> =
                if table.data_paths()?.iter().any(|p| is_object_store_uri(p)) {
                    Arc::new(ObjectStoreProvider::new())
                } else {
                    Arc::new(ParquetProvider::new())
                };
            #[cfg(not(feature = "s3"))]
            let provider: Arc<dyn StorageProvider> = Arc::new(ParquetProvider::new());
            let node = provider.scan(&table, scan.projection.clone(), scan.filters.clone())?;
            let stream = node.execute(Arc::new(ExecTaskContext {
                batch_size_rows: ctx.batch_size_rows,
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
                Arc::clone(&physical_registry),
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
                    Arc::clone(&physical_registry),
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
                        Arc::clone(&physical_registry),
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
                    Arc::clone(&physical_registry),
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
            let child = eval_plan_for_stage(
                &agg.input,
                current_stage,
                target_stage,
                state,
                ctx,
                catalog,
                Arc::clone(&physical_registry),
            )?;
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
            let child = eval_plan_for_stage(
                &agg.input,
                current_stage,
                target_stage,
                state,
                ctx,
                catalog,
                Arc::clone(&physical_registry),
            )?;
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
                join_type,
                strategy_hint,
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
                Arc::clone(&physical_registry),
            )?;
            let right = eval_plan_for_stage(
                right,
                current_stage,
                target_stage,
                state,
                ctx,
                catalog,
                Arc::clone(&physical_registry),
            )?;
            let (left_rows, left_batches, left_bytes) = batch_stats(&left.batches);
            let (right_rows, right_batches, right_bytes) = batch_stats(&right.batches);
            let out = if matches!(strategy_hint, ffq_planner::JoinStrategyHint::SortMerge) {
                run_sort_merge_join(left, right, on.clone(), *build_side)?
            } else {
                run_hash_join(left, right, on.clone(), *join_type, *build_side, ctx)?
            };
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
                Arc::clone(&physical_registry),
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
        PhysicalPlan::Window(window) => {
            let child = eval_plan_for_stage(
                &window.input,
                current_stage,
                target_stage,
                state,
                ctx,
                catalog,
                Arc::clone(&physical_registry),
            )?;
            let (in_rows, in_batches, in_bytes) = batch_stats(&child.batches);
            let out = run_window_exec(child, &window.exprs)?;
            Ok(OpEval {
                out,
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
                Arc::clone(&physical_registry),
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
        PhysicalPlan::InSubqueryFilter(exec) => {
            let child = eval_plan_for_stage(
                &exec.input,
                current_stage,
                target_stage,
                state,
                ctx,
                Arc::clone(&catalog),
                Arc::clone(&physical_registry),
            )?;
            let sub = eval_plan_for_stage(
                &exec.subquery,
                current_stage,
                target_stage,
                state,
                ctx,
                catalog,
                Arc::clone(&physical_registry),
            )?;
            let (in_rows, in_batches, in_bytes) = batch_stats(&child.batches);
            Ok(OpEval {
                out: run_in_subquery_filter(child, exec.expr.clone(), sub, exec.negated)?,
                in_rows,
                in_batches,
                in_bytes,
            })
        }
        PhysicalPlan::ExistsSubqueryFilter(exec) => {
            let child = eval_plan_for_stage(
                &exec.input,
                current_stage,
                target_stage,
                state,
                ctx,
                Arc::clone(&catalog),
                Arc::clone(&physical_registry),
            )?;
            let sub = eval_plan_for_stage(
                &exec.subquery,
                current_stage,
                target_stage,
                state,
                ctx,
                catalog,
                Arc::clone(&physical_registry),
            )?;
            let (in_rows, in_batches, in_bytes) = batch_stats(&child.batches);
            Ok(OpEval {
                out: run_exists_subquery_filter(child, sub, exec.negated),
                in_rows,
                in_batches,
                in_bytes,
            })
        }
        PhysicalPlan::ScalarSubqueryFilter(exec) => {
            let child = eval_plan_for_stage(
                &exec.input,
                current_stage,
                target_stage,
                state,
                ctx,
                Arc::clone(&catalog),
                Arc::clone(&physical_registry),
            )?;
            let sub = eval_plan_for_stage(
                &exec.subquery,
                current_stage,
                target_stage,
                state,
                ctx,
                catalog,
                Arc::clone(&physical_registry),
            )?;
            let (in_rows, in_batches, in_bytes) = batch_stats(&child.batches);
            Ok(OpEval {
                out: run_scalar_subquery_filter(child, exec.expr.clone(), exec.op, sub)?,
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
                Arc::clone(&physical_registry),
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
                Arc::clone(&physical_registry),
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
        PhysicalPlan::UnionAll(union) => {
            let left = eval_plan_for_stage(
                &union.left,
                current_stage,
                target_stage,
                state,
                ctx,
                Arc::clone(&catalog),
                Arc::clone(&physical_registry),
            )?;
            let right = eval_plan_for_stage(
                &union.right,
                current_stage,
                target_stage,
                state,
                ctx,
                catalog,
                Arc::clone(&physical_registry),
            )?;
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
            if let Some(cached) = state.cte_cache.get(&cte_ref.name).cloned() {
                let (in_rows, in_batches, in_bytes) = batch_stats(&cached.batches);
                Ok(OpEval {
                    out: cached,
                    in_rows,
                    in_batches,
                    in_bytes,
                })
            } else {
                let out = eval_plan_for_stage(
                    &cte_ref.plan,
                    current_stage,
                    target_stage,
                    state,
                    ctx,
                    catalog,
                    Arc::clone(&physical_registry),
                )?;
                state.cte_cache.insert(cte_ref.name.clone(), out.clone());
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
            out: execute_vector_topk(exec, catalog)?,
            in_rows: 0,
            in_batches: 0,
            in_bytes: 0,
        }),
        PhysicalPlan::VectorKnn(exec) => Ok(OpEval {
            out: execute_vector_knn(exec, catalog)?,
            in_rows: 0,
            in_batches: 0,
            in_bytes: 0,
        }),
        PhysicalPlan::Custom(custom) => {
            let child = eval_plan_for_stage(
                &custom.input,
                current_stage,
                target_stage,
                state,
                ctx,
                catalog,
                Arc::clone(&physical_registry),
            )?;
            let (in_rows, in_batches, in_bytes) = batch_stats(&child.batches);
            let factory = physical_registry.get(&custom.op_name).ok_or_else(|| {
                FfqError::Unsupported(format!(
                    "custom physical operator '{}' is not registered on worker",
                    custom.op_name
                ))
            })?;
            let (schema, batches) = factory.execute(child.schema, child.batches, &custom.config)?;
            Ok(OpEval {
                out: ExecOutput { schema, batches },
                in_rows,
                in_batches,
                in_bytes,
            })
        }
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
            VectorQueryOptions::default(),
        ))?;
        rows_to_vector_topk_output(rows)
    }
}

fn execute_vector_knn(
    exec: &ffq_planner::VectorKnnExec,
    catalog: Arc<Catalog>,
) -> Result<ExecOutput> {
    let topk = ffq_planner::VectorTopKExec {
        table: exec.source.clone(),
        query_vector: exec.query_vector.clone(),
        k: exec.k,
        filter: exec.prefilter.clone(),
    };
    let table = catalog.get(&topk.table)?.clone();
    if let Some(rows) = mock_vector_rows_from_table(&table, topk.k)? {
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
        return Err(FfqError::Unsupported(
            "qdrant feature is disabled; build ffq-distributed with --features qdrant".to_string(),
        ));
    }
    #[cfg(feature = "qdrant")]
    {
        let provider = QdrantProvider::from_table(&table)?;
        let rows = futures::executor::block_on(provider.topk(
            topk.query_vector.clone(),
            topk.k,
            topk.filter.clone(),
            VectorQueryOptions {
                metric: Some(exec.metric.clone()),
                ef_search: exec.ef_search,
            },
        ))?;
        rows_to_vector_knn_output(rows)
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

fn write_stage_shuffle_outputs(
    child: &ExecOutput,
    partitioning: &PartitioningSpec,
    query_numeric_id: u64,
    ctx: &TaskContext,
) -> Result<Vec<MapOutputPartitionMeta>> {
    let started = Instant::now();
    let writer =
        ShuffleWriter::new(&ctx.shuffle_root).with_compression_codec(ctx.shuffle_compression_codec);
    // Guardrail: periodically remove expired non-latest attempts to bound disk growth.
    let _ = writer.cleanup_expired_attempts(Duration::from_secs(10 * 60), SystemTime::now());
    let mut chunk_index = HashMap::<u32, Vec<ffq_shuffle::ShufflePartitionChunkMeta>>::new();
    for batch in &child.batches {
        let one = ExecOutput {
            schema: Arc::clone(&child.schema),
            batches: vec![batch.clone()],
        };
        let partitioned = partition_batches(&one, partitioning)?;
        for (reduce, batches) in partitioned {
            if batches.is_empty() {
                continue;
            }
            let chunk = writer.append_partition_chunk(
                query_numeric_id,
                ctx.stage_id,
                ctx.task_id,
                ctx.attempt,
                reduce,
                &batches,
                child.schema.as_ref(),
            )?;
            chunk_index.entry(reduce).or_default().push(chunk);
        }
    }
    let metas = aggregate_partition_chunks(
        query_numeric_id,
        ctx.stage_id,
        ctx.task_id,
        ctx.attempt,
        ctx.shuffle_compression_codec,
        chunk_index,
    );
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
            stream_epoch: ctx.attempt,
            committed_offset: m.bytes,
            finalized: true,
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
    let mut schema_hint: Option<SchemaRef> = None;
    let mut partition_read_cursors = HashMap::<u32, (u32, u64)>::new();
    let mut read_partitions = 0_u64;
    match partitioning {
        PartitioningSpec::Single => {
            if let Ok((_attempt, batches)) =
                reader.read_partition_latest(query_numeric_id, upstream_stage_id, 0, 0)
            {
                if schema_hint.is_none() && !batches.is_empty() {
                    schema_hint = Some(batches[0].schema());
                }
                out_batches.extend(batches);
                read_partitions += 1;
            }
        }
        PartitioningSpec::HashKeys { partitions, .. } => {
            if ctx.assigned_reduce_split_count == 0
                || ctx.assigned_reduce_split_index >= ctx.assigned_reduce_split_count
            {
                return Err(FfqError::Execution(format!(
                    "invalid reduce split assignment index={} count={} for stage={} task={}",
                    ctx.assigned_reduce_split_index,
                    ctx.assigned_reduce_split_count,
                    ctx.stage_id,
                    ctx.task_id
                )));
            }
            if ctx.assigned_reduce_partitions.is_empty() {
                return Err(FfqError::Execution(format!(
                    "missing assigned_reduce_partitions for shuffle-read hash stage={} task={}",
                    ctx.stage_id, ctx.task_id
                )));
            }
            let assigned = ctx
                .assigned_reduce_partitions
                .iter()
                .copied()
                .filter(|p| (*p as usize) < *partitions)
                .collect::<Vec<_>>();
            if assigned.is_empty() {
                return Err(FfqError::Execution(format!(
                    "assigned_reduce_partitions {:?} are out of range for {} partitions (stage={} task={})",
                    ctx.assigned_reduce_partitions, partitions, ctx.stage_id, ctx.task_id
                )));
            }
            let fetch_window = ctx.reduce_fetch_window_partitions.max(1) as usize;
            for chunk in assigned.chunks(fetch_window) {
                for reduce in chunk {
                    let (_attempt, batches) = read_partition_incremental_latest(
                        &reader,
                        query_numeric_id,
                        upstream_stage_id,
                        0,
                        *reduce,
                        &mut partition_read_cursors,
                    )?;
                    let batches = filter_partition_batches_for_assigned_shard(
                        batches,
                        partitioning,
                        ctx.assigned_reduce_split_index,
                        ctx.assigned_reduce_split_count,
                    )?;
                    if schema_hint.is_none() && !batches.is_empty() {
                        schema_hint = Some(batches[0].schema());
                    }
                    out_batches.extend(batches);
                    read_partitions += 1;
                }
            }
            if out_batches.is_empty() && schema_hint.is_none() {
                // Preserve schema for empty assigned partitions by probing
                // any available upstream partition.
                for reduce in 0..*partitions as u32 {
                    if let Ok((_attempt, batches)) =
                        reader.read_partition_latest(query_numeric_id, upstream_stage_id, 0, reduce)
                    {
                        if let Some(first) = batches.first() {
                            schema_hint = Some(first.schema());
                            break;
                        }
                    }
                }
            }
        }
    }
    let schema = out_batches
        .first()
        .map(|b| b.schema())
        .or(schema_hint)
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

fn read_partition_incremental_latest(
    reader: &ShuffleReader,
    query_numeric_id: u64,
    upstream_stage_id: u64,
    map_task: u64,
    reduce_partition: u32,
    read_cursors: &mut HashMap<u32, (u32, u64)>,
) -> Result<(u32, Vec<RecordBatch>)> {
    let attempt = reader
        .latest_attempt(query_numeric_id, upstream_stage_id, map_task)?
        .ok_or_else(|| FfqError::Execution("no shuffle attempts found for map task".to_string()))?;
    let index =
        reader.read_map_task_index(query_numeric_id, upstream_stage_id, map_task, attempt)?;
    let Some(meta) = index
        .partitions
        .into_iter()
        .find(|p| p.reduce_partition == reduce_partition)
    else {
        return Ok((attempt, Vec::new()));
    };
    let cursor = match read_cursors.get(&reduce_partition) {
        Some((cursor_attempt, cursor_offset)) if *cursor_attempt == attempt => *cursor_offset,
        _ => 0,
    };
    let watermark = meta.bytes;
    if cursor >= watermark {
        return Ok((attempt, Vec::new()));
    }

    let mut next_cursor = cursor;
    let mut out_batches = Vec::new();
    if meta.chunks.is_empty() {
        let fetched = reader.fetch_partition_chunks_range(
            query_numeric_id,
            upstream_stage_id,
            map_task,
            attempt,
            reduce_partition,
            cursor,
            watermark.saturating_sub(cursor),
        )?;
        if !fetched.is_empty() {
            let chunk_payloads = fetched.into_iter().map(|c| c.payload).collect::<Vec<_>>();
            if !chunk_payloads.is_empty() {
                let mut decoded = reader.read_partition_from_streamed_chunks(chunk_payloads)?;
                out_batches.append(&mut decoded);
            }
        }
        next_cursor = watermark;
    } else {
        let mut frame_chunks = meta.chunks;
        frame_chunks.sort_by_key(|c| c.offset_bytes);
        for frame in frame_chunks {
            let frame_start = frame.offset_bytes;
            let frame_end = frame.offset_bytes.saturating_add(frame.frame_bytes);
            if frame_end <= cursor {
                continue;
            }
            if frame_start < cursor {
                return Err(FfqError::Execution(format!(
                    "invalid incremental cursor {cursor} in middle of frame range [{frame_start}, {frame_end}) for reduce partition {reduce_partition}"
                )));
            }
            let fetched = reader.fetch_partition_chunks_range(
                query_numeric_id,
                upstream_stage_id,
                map_task,
                attempt,
                reduce_partition,
                frame_start,
                frame_end.saturating_sub(frame_start),
            )?;
            if fetched.is_empty() {
                break;
            }
            let chunk_payloads = fetched.into_iter().map(|c| c.payload).collect::<Vec<_>>();
            if chunk_payloads.is_empty() {
                break;
            }
            let mut decoded = reader.read_partition_from_streamed_chunks(chunk_payloads)?;
            out_batches.append(&mut decoded);
            next_cursor = frame_end;
        }
    }
    read_cursors.insert(reduce_partition, (attempt, next_cursor));
    Ok((attempt, out_batches))
}

fn filter_partition_batches_for_assigned_shard(
    batches: Vec<RecordBatch>,
    partitioning: &PartitioningSpec,
    split_index: u32,
    split_count: u32,
) -> Result<Vec<RecordBatch>> {
    if split_count <= 1 {
        return Ok(batches);
    }
    let PartitioningSpec::HashKeys { keys, .. } = partitioning else {
        return Ok(batches);
    };
    if batches.is_empty() {
        return Ok(batches);
    }
    let schema = batches[0].schema();
    let key_idx = resolve_key_indexes(&schema, keys)?;
    let input = ExecOutput {
        schema: Arc::clone(&schema),
        batches,
    };
    let rows = rows_from_batches(&input)?;
    let selected = rows
        .into_iter()
        .filter(|row| {
            let key = key_idx.iter().map(|i| row[*i].clone()).collect::<Vec<_>>();
            (hash_key(&key) % split_count as u64) == split_index as u64
        })
        .collect::<Vec<_>>();
    if selected.is_empty() {
        return Ok(Vec::new());
    }
    let batch = rows_to_batch(&schema, &selected)?;
    Ok(vec![batch])
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
    Hll(HllSketch),
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct SpillRow {
    key: Vec<ScalarValue>,
    states: Vec<AggState>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
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
fn run_hash_join(
    left: ExecOutput,
    right: ExecOutput,
    on: Vec<(String, String)>,
    join_type: JoinType,
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

    let output_schema = match join_type {
        JoinType::Semi | JoinType::Anti => left.schema.clone(),
        _ => Arc::new(Schema::new(
            left.schema
                .fields()
                .iter()
                .chain(right.schema.fields().iter())
                .map(|f| (**f).clone())
                .collect::<Vec<_>>(),
        )),
    };

    let probe_prefilter_storage = if ctx.join_bloom_enabled && !build_rows.is_empty() {
        let mut bloom = JoinBloomFilter::new(ctx.join_bloom_bits, 3);
        for row in build_rows.iter() {
            let key = join_key_from_row(row, &build_key_idx);
            if !key.iter().any(|v| *v == ScalarValue::Null) {
                bloom.insert(&key);
            }
        }
        let filtered = probe_rows
            .iter()
            .filter(|row| {
                let key = join_key_from_row(row, &probe_key_idx);
                !key.iter().any(|v| *v == ScalarValue::Null) && bloom.may_contain(&key)
            })
            .cloned()
            .collect::<Vec<_>>();
        if filtered.len() < probe_rows.len() {
            info!(
                query_id = %ctx.query_id,
                stage_id = ctx.stage_id,
                task_id = ctx.task_id,
                probe_rows_before = probe_rows.len(),
                probe_rows_after = filtered.len(),
                probe_bytes_before = estimate_join_rows_bytes(probe_rows),
                probe_bytes_after = estimate_join_rows_bytes(&filtered),
                "worker hash join bloom prefilter reduced probe side"
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

    let spill_signal = spill_signal_for_task_ctx(ctx);
    let mut match_output = if !matches!(join_type, JoinType::Semi | JoinType::Anti)
        && ctx.per_task_memory_budget_bytes > 0
        && spill_signal.should_spill(estimate_join_rows_bytes(build_rows))
    {
        let rows = grace_hash_join(
            build_rows,
            probe_rows,
            &build_key_idx,
            &probe_key_idx,
            build_input_side,
            ctx,
        )?;
        JoinMatchOutput {
            rows,
            matched_left: vec![false; left_rows.len()],
        }
    } else {
        if ctx.join_radix_bits > 0 {
            in_memory_radix_hash_join(
                build_rows,
                probe_rows,
                &build_key_idx,
                &probe_key_idx,
                build_input_side,
                left_rows.len(),
                ctx.join_radix_bits,
            )
        } else {
            in_memory_hash_join(
                build_rows,
                probe_rows,
                &build_key_idx,
                &probe_key_idx,
                build_input_side,
                left_rows.len(),
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
            (!key.iter().any(|v| *v == ScalarValue::Null)).then_some((idx, key))
        })
        .collect::<Vec<_>>();
    let mut probe_sorted = probe_rows
        .iter()
        .enumerate()
        .filter_map(|(idx, row)| {
            let key = join_key_from_row(row, &probe_key_idx);
            (!key.iter().any(|v| *v == ScalarValue::Null)).then_some((idx, key))
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

fn run_window_exec(input: ExecOutput, exprs: &[WindowExpr]) -> Result<ExecOutput> {
    let mut rows = rows_from_batches(&input)?;
    let row_count = rows.len();
    let mut eval_ctx_cache: HashMap<String, WindowEvalContext> = HashMap::new();
    let mut out_fields: Vec<Field> = input
        .schema
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    for w in exprs {
        let cache_key = window_compatibility_key(w);
        if !eval_ctx_cache.contains_key(&cache_key) {
            eval_ctx_cache.insert(cache_key.clone(), build_window_eval_context(&input, w)?);
        }
        let output = evaluate_window_expr_with_ctx(
            &input,
            w,
            eval_ctx_cache
                .get(&cache_key)
                .expect("window eval ctx must exist"),
        )?;
        if output.len() != row_count {
            return Err(FfqError::Execution(format!(
                "window output row count mismatch: expected {row_count}, got {}",
                output.len()
            )));
        }
        let dt = window_output_type(&input.schema, w)?;
        out_fields.push(Field::new(&w.output_name, dt, window_output_nullable(w)));
        for (idx, value) in output.into_iter().enumerate() {
            rows[idx].push(value);
        }
    }
    let out_schema = Arc::new(Schema::new(out_fields));
    let batch = rows_to_batch(&out_schema, &rows)?;
    Ok(ExecOutput {
        schema: out_schema,
        batches: vec![batch],
    })
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
                    let pct = (rank - 1) as f64 / (n as f64 - 1.0);
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
                    let cume = i as f64 / n;
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
                    for pos in &part[fs..fe] {
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
                    for pos in &part[fs..fe] {
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
                            _ => {
                                return Err(FfqError::Execution(
                                    "SUM window only supports numeric types".to_string(),
                                ));
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
                    for pos in &part[fs..fe] {
                        if let Some(v) = scalar_to_f64(&values[*pos]) {
                            sum += v;
                            count += 1;
                        } else if !matches!(values[*pos], ScalarValue::Null) {
                            return Err(FfqError::Execution(
                                "AVG window only supports numeric types".to_string(),
                            ));
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
                    for pos in &part[fs..fe] {
                        let v = values[*pos].clone();
                        if matches!(v, ScalarValue::Null) {
                            continue;
                        }
                        match &current {
                            None => current = Some(v),
                            Some(existing) => {
                                if scalar_lt(&v, existing)? {
                                    current = Some(v);
                                }
                            }
                        }
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
                    for pos in &part[fs..fe] {
                        let v = values[*pos].clone();
                        if matches!(v, ScalarValue::Null) {
                            continue;
                        }
                        match &current {
                            None => current = Some(v),
                            Some(existing) => {
                                if scalar_gt(&v, existing)? {
                                    current = Some(v);
                                }
                            }
                        }
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
            let default_values = default
                .as_ref()
                .map(|d| evaluate_expr_rows(input, d))
                .transpose()?;
            for (start, end) in &eval_ctx.partitions {
                let part = &eval_ctx.order_idx[*start..*end];
                for i in 0..part.len() {
                    out[part[i]] = if i >= *offset {
                        values[part[i - *offset]].clone()
                    } else if let Some(default_rows) = &default_values {
                        default_rows[part[i]].clone()
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
            let default_values = default
                .as_ref()
                .map(|d| evaluate_expr_rows(input, d))
                .transpose()?;
            for (start, end) in &eval_ctx.partitions {
                let part = &eval_ctx.order_idx[*start..*end];
                for i in 0..part.len() {
                    out[part[i]] = if i + *offset < part.len() {
                        values[part[i + *offset]].clone()
                    } else if let Some(default_rows) = &default_values {
                        default_rows[part[i]].clone()
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
                let first = values[part[0]].clone();
                for pos in part {
                    out[*pos] = first.clone();
                }
            }
        }
        WindowFunction::LastValue(expr) => {
            let values = evaluate_expr_rows(input, expr)?;
            for (start, end) in &eval_ctx.partitions {
                let part = &eval_ctx.order_idx[*start..*end];
                let last = values[*part.last().expect("partition non-empty")].clone();
                for pos in part {
                    out[*pos] = last.clone();
                }
            }
        }
        WindowFunction::NthValue { expr, n } => {
            let values = evaluate_expr_rows(input, expr)?;
            for (start, end) in &eval_ctx.partitions {
                let part = &eval_ctx.order_idx[*start..*end];
                let nth = if *n >= 1 && *n <= part.len() {
                    values[part[*n - 1]].clone()
                } else {
                    ScalarValue::Null
                };
                for pos in part {
                    out[*pos] = nth.clone();
                }
            }
        }
    }
    Ok(out)
}

fn window_output_type(input_schema: &SchemaRef, w: &WindowExpr) -> Result<DataType> {
    let dt = match &w.func {
        WindowFunction::RowNumber
        | WindowFunction::Rank
        | WindowFunction::DenseRank
        | WindowFunction::Ntile(_)
        | WindowFunction::Count(_) => DataType::Int64,
        WindowFunction::PercentRank | WindowFunction::CumeDist => DataType::Float64,
        WindowFunction::Sum(_) | WindowFunction::Avg(_) => DataType::Float64,
        WindowFunction::Min(expr)
        | WindowFunction::Max(expr)
        | WindowFunction::Lag { expr, .. }
        | WindowFunction::Lead { expr, .. }
        | WindowFunction::FirstValue(expr)
        | WindowFunction::LastValue(expr)
        | WindowFunction::NthValue { expr, .. } => compile_expr(expr, input_schema)?.data_type(),
    };
    Ok(dt)
}

fn window_output_nullable(w: &WindowExpr) -> bool {
    !matches!(
        w.func,
        WindowFunction::RowNumber
            | WindowFunction::Rank
            | WindowFunction::DenseRank
            | WindowFunction::Ntile(_)
            | WindowFunction::Count(_)
    )
}

fn effective_window_frame(w: &WindowExpr) -> WindowFrameSpec {
    if let Some(frame) = &w.frame {
        return frame.clone();
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

#[derive(Debug, Clone)]
struct FrameCtx {
    peer_groups: Vec<(usize, usize)>,
    row_group: Vec<usize>,
}

fn build_partition_frame_ctx(
    part: &[usize],
    order_keys: &[Vec<ScalarValue>],
    order_exprs: &[WindowOrderExpr],
) -> Result<FrameCtx> {
    let (peer_groups, row_group) = build_peer_groups(part, order_keys, order_exprs);
    Ok(FrameCtx {
        peer_groups,
        row_group,
    })
}

fn build_peer_groups(
    part: &[usize],
    order_keys: &[Vec<ScalarValue>],
    order_exprs: &[WindowOrderExpr],
) -> (Vec<(usize, usize)>, Vec<usize>) {
    let mut groups = Vec::new();
    let mut row_group = vec![0usize; part.len()];
    let mut start = 0usize;
    let mut i = 1usize;
    while i <= part.len() {
        let split = if i == part.len() {
            true
        } else {
            cmp_order_key_sets(order_keys, order_exprs, part[i - 1], part[i]) != Ordering::Equal
        };
        if split {
            let gidx = groups.len();
            for rg in &mut row_group[start..i] {
                *rg = gidx;
            }
            groups.push((start, i));
            start = i;
        }
        i += 1;
    }
    (groups, row_group)
}

fn resolve_frame_range(
    frame: &WindowFrameSpec,
    row_idx: usize,
    part: &[usize],
    ctx: &FrameCtx,
) -> Result<(usize, usize)> {
    if part.is_empty() {
        return Ok((0, 0));
    }
    let (mut start, mut end) = match frame.units {
        WindowFrameUnits::Rows => resolve_rows_frame(frame, row_idx, part.len()),
        WindowFrameUnits::Range => resolve_range_frame(frame, row_idx, ctx),
        WindowFrameUnits::Groups => resolve_groups_frame(frame, row_idx, ctx),
    }?;
    if start > end {
        return Ok((0, 0));
    }
    if start > part.len() {
        start = part.len();
    }
    if end > part.len() {
        end = part.len();
    }
    apply_exclusion(frame.exclusion, row_idx, start, end, ctx)
}

fn resolve_rows_frame(
    frame: &WindowFrameSpec,
    row_idx: usize,
    part_len: usize,
) -> Result<(usize, usize)> {
    let start = match frame.start_bound {
        WindowFrameBound::UnboundedPreceding => 0_i64,
        WindowFrameBound::Preceding(n) => {
            row_idx as i64 - window_bound_preceding_offset(n, "start")?
        }
        WindowFrameBound::CurrentRow => row_idx as i64,
        WindowFrameBound::Following(n) => {
            row_idx as i64 + window_bound_following_offset(n, "start")?
        }
        WindowFrameBound::UnboundedFollowing => part_len as i64,
    };
    let end_inclusive = match frame.end_bound {
        WindowFrameBound::UnboundedPreceding => -1_i64,
        WindowFrameBound::Preceding(n) => row_idx as i64 - window_bound_preceding_offset(n, "end")?,
        WindowFrameBound::CurrentRow => row_idx as i64,
        WindowFrameBound::Following(n) => row_idx as i64 + window_bound_following_offset(n, "end")?,
        WindowFrameBound::UnboundedFollowing => part_len as i64 - 1,
    };
    let start = start.clamp(0, part_len as i64);
    let end_exclusive = (end_inclusive + 1).clamp(0, part_len as i64);
    Ok((start as usize, end_exclusive as usize))
}

fn resolve_range_frame(
    frame: &WindowFrameSpec,
    row_idx: usize,
    ctx: &FrameCtx,
) -> Result<(usize, usize)> {
    let gcur = ctx.row_group[row_idx] as i64;
    let glen = ctx.peer_groups.len() as i64;
    let start_g = match frame.start_bound {
        WindowFrameBound::UnboundedPreceding => 0_i64,
        WindowFrameBound::Preceding(n) => gcur - window_bound_preceding_offset(n, "start")?,
        WindowFrameBound::CurrentRow => gcur,
        WindowFrameBound::Following(n) => gcur + window_bound_following_offset(n, "start")?,
        WindowFrameBound::UnboundedFollowing => glen,
    }
    .clamp(0, glen);
    let end_g_inclusive = match frame.end_bound {
        WindowFrameBound::UnboundedPreceding => -1_i64,
        WindowFrameBound::Preceding(n) => gcur - window_bound_preceding_offset(n, "end")?,
        WindowFrameBound::CurrentRow => gcur,
        WindowFrameBound::Following(n) => gcur + window_bound_following_offset(n, "end")?,
        WindowFrameBound::UnboundedFollowing => glen - 1,
    }
    .clamp(-1, glen - 1);
    if start_g > end_g_inclusive {
        return Ok((0, 0));
    }
    let start = ctx.peer_groups[start_g as usize].0;
    let end = ctx.peer_groups[end_g_inclusive as usize].1;
    Ok((start, end))
}

fn resolve_groups_frame(
    frame: &WindowFrameSpec,
    row_idx: usize,
    ctx: &FrameCtx,
) -> Result<(usize, usize)> {
    resolve_range_frame(frame, row_idx, ctx)
}

fn apply_exclusion(
    exclusion: WindowFrameExclusion,
    row_idx: usize,
    start: usize,
    end: usize,
    ctx: &FrameCtx,
) -> Result<(usize, usize)> {
    if start >= end {
        return Ok((0, 0));
    }
    let (s, e) = match exclusion {
        WindowFrameExclusion::NoOthers => (start, end),
        WindowFrameExclusion::CurrentRow => {
            if row_idx < start || row_idx >= end {
                (start, end)
            } else if row_idx == start {
                (start + 1, end)
            } else if row_idx + 1 == end {
                (start, end - 1)
            } else {
                return Ok((0, 0));
            }
        }
        WindowFrameExclusion::Group => {
            let g = ctx.row_group[row_idx];
            let (gs, ge) = ctx.peer_groups[g];
            if ge <= start || gs >= end {
                (start, end)
            } else if gs <= start && ge >= end {
                (0, 0)
            } else if gs <= start {
                (ge, end)
            } else if ge >= end {
                (start, gs)
            } else {
                return Ok((0, 0));
            }
        }
        WindowFrameExclusion::Ties => {
            let g = ctx.row_group[row_idx];
            let (gs, ge) = ctx.peer_groups[g];
            if ge <= start || gs >= end {
                (start, end)
            } else if gs <= start && ge >= end {
                (row_idx, row_idx + 1)
            } else if gs <= start {
                (ge, end)
            } else if ge >= end {
                (start, gs)
            } else {
                return Ok((row_idx, row_idx + 1));
            }
        }
    };
    Ok((s.min(e), e))
}

fn window_bound_preceding_offset(v: usize, where_: &str) -> Result<i64> {
    i64::try_from(v).map_err(|_| {
        FfqError::Execution(format!(
            "window frame {where_} bound PRECEDING value {v} overflows i64"
        ))
    })
}

fn window_bound_following_offset(v: usize, where_: &str) -> Result<i64> {
    i64::try_from(v).map_err(|_| {
        FfqError::Execution(format!(
            "window frame {where_} bound FOLLOWING value {v} overflows i64"
        ))
    })
}

fn evaluate_expr_rows(input: &ExecOutput, expr: &Expr) -> Result<Vec<ScalarValue>> {
    let eval = compile_expr(expr, &input.schema)?;
    let mut out = Vec::new();
    for batch in &input.batches {
        let arr = eval.evaluate(batch)?;
        for row in 0..batch.num_rows() {
            out.push(scalar_from_array(&arr, row)?);
        }
    }
    Ok(out)
}

fn cmp_key_sets(keys: &[Vec<ScalarValue>], a: usize, b: usize) -> Ordering {
    for k in keys {
        let ord = cmp_scalar_for_window(&k[a], &k[b], false, true);
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
    for (i, o) in order_exprs.iter().enumerate() {
        let ord = cmp_scalar_for_window(&keys[i][a], &keys[i][b], !o.asc, o.nulls_first);
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
        (true, true) => Ordering::Equal,
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

fn partition_ranges(
    order_idx: &[usize],
    partition_keys: &[Vec<ScalarValue>],
) -> Vec<(usize, usize)> {
    if order_idx.is_empty() {
        return Vec::new();
    }
    if partition_keys.is_empty() {
        return vec![(0, order_idx.len())];
    }
    let mut out = Vec::new();
    let mut start = 0usize;
    for i in 1..=order_idx.len() {
        let split = if i == order_idx.len() {
            true
        } else {
            cmp_key_sets(partition_keys, order_idx[i - 1], order_idx[i]) != Ordering::Equal
        };
        if split {
            out.push((start, i));
            start = i;
        }
    }
    out
}

fn scalar_to_f64(v: &ScalarValue) -> Option<f64> {
    match v {
        ScalarValue::Int64(x) => Some(*x as f64),
        ScalarValue::Float64Bits(x) => Some(f64::from_bits(*x)),
        ScalarValue::Null => None,
        _ => None,
    }
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
            let predicate = if values.is_null(row) {
                None
            } else {
                let value = scalar_from_array(&values, row)?;
                eval_in_predicate(value, &sub_membership, negated)
            };
            mask_builder.append_value(predicate == Some(true));
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
    if lhs == ScalarValue::Null {
        return None;
    }
    if membership.values.contains(&lhs) {
        return Some(!negated);
    }
    if membership.has_null {
        return None;
    }
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
    for (probe_idx, probe) in probe_rows.iter().enumerate() {
        let probe_key = join_key_from_row(probe, probe_key_idx);
        if join_key_has_null(&probe_key) {
            continue;
        }
        if let Some(build_matches) = ht.get(&probe_key) {
            for build_idx in build_matches {
                let build = &build_rows[*build_idx];
                out.push(combine_join_rows(build, probe, build_side));
                mark_join_match(&mut matched_left, build_side, *build_idx, probe_idx);
            }
        }
    }
    JoinMatchOutput {
        rows: out,
        matched_left,
    }
}

fn in_memory_radix_hash_join(
    build_rows: &[Vec<ScalarValue>],
    probe_rows: &[Vec<ScalarValue>],
    build_key_idx: &[usize],
    probe_key_idx: &[usize],
    build_side: JoinInputSide,
    left_len: usize,
    radix_bits: u8,
) -> JoinMatchOutput {
    let bits = radix_bits.min(12);
    if bits == 0 {
        return in_memory_hash_join(
            build_rows,
            probe_rows,
            build_key_idx,
            probe_key_idx,
            build_side,
            left_len,
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
                        mark_join_match(&mut matched_left, build_side, *build_idx, *probe_idx);
                    }
                }
            }
        }
    }
    JoinMatchOutput {
        rows: out,
        matched_left,
    }
}

struct JoinMatchOutput {
    rows: Vec<Vec<ScalarValue>>,
    matched_left: Vec<bool>,
}

fn mark_join_match(
    matched_left: &mut [bool],
    build_side: JoinInputSide,
    build_idx: usize,
    probe_idx: usize,
) {
    match build_side {
        JoinInputSide::Left => {
            matched_left[build_idx] = true;
        }
        JoinInputSide::Right => {
            matched_left[probe_idx] = true;
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

fn hash_key_with_seed(key: &[ScalarValue], seed: u64) -> u64 {
    let mut h = DefaultHasher::new();
    seed.hash(&mut h);
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
        maybe_spill(&mut groups, &mut spills, &mut spill_seq, ctx)?;
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
        maybe_spill(&mut groups, &mut spills, &mut spill_seq, ctx)?;
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

fn maybe_spill(
    groups: &mut GroupMap,
    spills: &mut Vec<PathBuf>,
    spill_seq: &mut u64,
    ctx: &TaskContext,
) -> Result<()> {
    let spill_signal = spill_signal_for_task_ctx(ctx);
    if groups.is_empty() || ctx.per_task_memory_budget_bytes == 0 {
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
                writer.write_all(line.as_bytes())?;
                writer.write_all(b"\n")?;
            }
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
        spills.push(path);
        partition_cursor = (partition_cursor + 1) % SPILL_PARTITIONS;
    }
    Ok(())
}

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
#[path = "worker_tests.rs"]
mod tests;
