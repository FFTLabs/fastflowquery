//! Coordinator state machine and scheduling logic.
//!
//! Responsibilities:
//! - accept submitted physical plans and cut stage DAGs;
//! - materialize task attempts and serve pull-based task assignment;
//! - track query/task status transitions and aggregate stage metrics;
//! - maintain map-output registry keyed by `(query, stage, map_task, attempt)`;
//! - enforce basic worker blacklisting/retry behavior.
//!
//! Retry semantics:
//! - attempts are explicit in task and map-output keys;
//! - fetches must request the intended attempt; stale attempts are not used
//!   unless caller asks for latest-attempt read path.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use ffq_common::adaptive::{
    PartitionBytesHistogramBucket, ReduceTaskAssignment, plan_adaptive_reduce_layout,
};
use ffq_common::metrics::global_metrics;
use ffq_common::{FfqError, Result, SchemaInferencePolicy};
use ffq_planner::{ExchangeExec, PartitioningSpec, PhysicalPlan};
use ffq_shuffle::{FetchedPartitionChunk, ShuffleReader};
use ffq_storage::Catalog;
use ffq_storage::parquet_provider::ParquetProvider;
use tracing::{debug, info, warn};

use crate::stage::{StageDag, build_stage_dag};

#[derive(Debug, Clone)]
/// Coordinator behavior/configuration knobs.
pub struct CoordinatorConfig {
    /// Consecutive task failures before a worker is blacklisted.
    pub blacklist_failure_threshold: u32,
    /// Root directory containing shuffle files and indexes.
    pub shuffle_root: PathBuf,
    /// Coordinator-side schema inference policy for schema-less parquet scans.
    pub schema_inference: SchemaInferencePolicy,
    /// Max runnable tasks a worker may own at once.
    pub max_concurrent_tasks_per_worker: u32,
    /// Max runnable tasks per query across all workers.
    pub max_concurrent_tasks_per_query: u32,
    /// Max attempts before a logical task is considered terminally failed.
    pub max_task_attempts: u32,
    /// Base retry backoff in milliseconds.
    pub retry_backoff_base_ms: u64,
    /// Liveness timeout after which worker-owned running tasks are requeued.
    pub worker_liveness_timeout_ms: u64,
    /// Target bytes used to derive adaptive downstream shuffle reduce-task counts.
    pub adaptive_shuffle_target_bytes: u64,
    /// Minimum reduce task count allowed for adaptive layouts (clamped to planned count).
    pub adaptive_shuffle_min_reduce_tasks: u32,
    /// Maximum reduce task count allowed for adaptive layouts (clamped to planned count).
    ///
    /// `0` means "no explicit max" (uses planned count as effective max).
    pub adaptive_shuffle_max_reduce_tasks: u32,
    /// Optional hard cap for number of reduce partitions per reduce task group.
    ///
    /// `0` disables this split rule.
    pub adaptive_shuffle_max_partitions_per_task: u32,
    /// Enables pipelined shuffle scheduling.
    ///
    /// When enabled, reduce tasks may be scheduled before all map tasks are
    /// finished if enough parent progress and partition outputs are available.
    pub pipelined_shuffle_enabled: bool,
    /// Minimum parent-stage completion ratio required before pipelined reduce
    /// scheduling starts.
    ///
    /// Range is clamped to `[0.0, 1.0]`.
    pub pipelined_shuffle_min_map_completion_ratio: f64,
    /// Minimum committed stream offset (bytes) required for a reduce partition
    /// to be considered readable in pipelined scheduling.
    pub pipelined_shuffle_min_committed_offset_bytes: u64,
    /// Target reducer in-flight bytes used by backpressure throttling.
    pub backpressure_target_inflight_bytes: u64,
    /// Target reducer queue depth used by backpressure throttling.
    pub backpressure_target_queue_depth: u32,
    /// Max map-output publish window used when system is unconstrained.
    pub backpressure_max_map_publish_window_partitions: u32,
    /// Max reduce-fetch window used when system is unconstrained.
    pub backpressure_max_reduce_fetch_window_partitions: u32,
    /// Enables speculative execution for detected stragglers.
    pub speculative_execution_enabled: bool,
    /// Minimum completed task samples required before p95 straggler baseline is used.
    pub speculative_min_completed_samples: u32,
    /// Runtime multiplier over p95 to classify a task as a straggler.
    pub speculative_p95_multiplier: f64,
    /// Minimum runtime threshold (ms) before straggler detection can trigger.
    pub speculative_min_runtime_ms: u64,
    /// Enables locality-aware task preference when worker locality tags are available.
    pub locality_preference_enabled: bool,
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            blacklist_failure_threshold: 3,
            shuffle_root: PathBuf::from("."),
            schema_inference: SchemaInferencePolicy::On,
            max_concurrent_tasks_per_worker: 8,
            max_concurrent_tasks_per_query: 32,
            max_task_attempts: 3,
            retry_backoff_base_ms: 250,
            worker_liveness_timeout_ms: 15_000,
            adaptive_shuffle_target_bytes: 128 * 1024 * 1024,
            adaptive_shuffle_min_reduce_tasks: 1,
            adaptive_shuffle_max_reduce_tasks: 0,
            adaptive_shuffle_max_partitions_per_task: 0,
            pipelined_shuffle_enabled: false,
            pipelined_shuffle_min_map_completion_ratio: 0.5,
            pipelined_shuffle_min_committed_offset_bytes: 1,
            backpressure_target_inflight_bytes: 64 * 1024 * 1024,
            backpressure_target_queue_depth: 32,
            backpressure_max_map_publish_window_partitions: 8,
            backpressure_max_reduce_fetch_window_partitions: 8,
            speculative_execution_enabled: true,
            speculative_min_completed_samples: 5,
            speculative_p95_multiplier: 1.5,
            speculative_min_runtime_ms: 250,
            locality_preference_enabled: true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Query lifecycle states tracked by the coordinator.
pub enum QueryState {
    /// Query is accepted but not yet running.
    Queued,
    /// At least one task attempt is currently running.
    Running,
    /// All tasks completed successfully.
    Succeeded,
    /// At least one task failed and query cannot recover.
    Failed,
    /// Query was canceled by user or system request.
    Canceled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Task lifecycle states tracked by the coordinator.
pub enum TaskState {
    /// Task is pending scheduling.
    Queued,
    /// Task is currently executing.
    Running,
    /// Task completed successfully.
    Succeeded,
    /// Task execution failed.
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StageBarrierState {
    NotApplicable,
    MapRunning,
    MapDone,
    LayoutFinalized,
    ReduceSchedulable,
}

#[derive(Debug, Clone)]
/// One schedulable task assignment returned to workers.
pub struct TaskAssignment {
    /// Stable query identifier.
    pub query_id: String,
    /// Stage identifier within query DAG.
    pub stage_id: u64,
    /// Task identifier within stage.
    pub task_id: u64,
    /// Attempt number for retries.
    pub attempt: u32,
    /// Serialized physical-plan fragment for this task.
    pub plan_fragment_json: Vec<u8>,
    /// Reduce partitions assigned to this task for shuffle-read stages.
    pub assigned_reduce_partitions: Vec<u32>,
    /// Hash-shard split index within assigned partition payloads.
    pub assigned_reduce_split_index: u32,
    /// Hash-shard split count within assigned partition payloads.
    pub assigned_reduce_split_count: u32,
    /// Stage adaptive-layout version this assignment was built from.
    pub layout_version: u32,
    /// Deterministic fingerprint of assignment layout for this stage version.
    pub layout_fingerprint: u64,
    /// Suggested map-output publish window for this task.
    pub recommended_map_output_publish_window_partitions: u32,
    /// Suggested reduce-fetch window for this task.
    pub recommended_reduce_fetch_window_partitions: u32,
}

#[derive(Debug, Clone, Default)]
/// Aggregated per-stage progress and map-output metrics.
pub struct StageMetrics {
    /// Number of queued tasks in the stage.
    pub queued_tasks: u32,
    /// Number of running tasks in the stage.
    pub running_tasks: u32,
    /// Number of succeeded tasks in the stage.
    pub succeeded_tasks: u32,
    /// Number of failed tasks in the stage.
    pub failed_tasks: u32,
    /// Total rows written by map outputs in this stage.
    pub map_output_rows: u64,
    /// Total bytes written by map outputs in this stage.
    pub map_output_bytes: u64,
    /// Total batches written by map outputs in this stage.
    pub map_output_batches: u64,
    /// Number of distinct reduce partitions present in latest map outputs.
    pub map_output_partitions: u64,
    /// Planned reduce-task count (before adaptive sizing).
    pub planned_reduce_tasks: u32,
    /// Adaptive reduce-task count derived from map output bytes and target size.
    pub adaptive_reduce_tasks: u32,
    /// Target bytes per reduce task used for adaptive sizing.
    pub adaptive_target_bytes: u64,
    /// AQE/layout events explaining why task fanout changed.
    pub aqe_events: Vec<String>,
    /// Histogram of map-output bytes by reduce partition.
    pub partition_bytes_histogram: Vec<PartitionBytesHistogramBucket>,
    /// Number of skew-induced split reduce tasks in the finalized layout.
    pub skew_split_tasks: u32,
    /// Number of times layout was finalized for the stage.
    pub layout_finalize_count: u32,
    /// Last observed reducer in-flight bytes for this stage.
    pub backpressure_inflight_bytes: u64,
    /// Last observed reducer queue depth for this stage.
    pub backpressure_queue_depth: u32,
    /// Current recommended map publish window.
    pub map_publish_window_partitions: u32,
    /// Current recommended reduce fetch window.
    pub reduce_fetch_window_partitions: u32,
    /// Milliseconds from query start until first readable map chunk was observed.
    pub first_chunk_ms: u64,
    /// Milliseconds from query start until first reduce-side row activity was observed.
    pub first_reduce_row_ms: u64,
    /// Current stream lag in milliseconds between first chunk and reduce activity/progress.
    pub stream_lag_ms: u64,
    /// Last observed buffered streaming bytes at reducers.
    pub stream_buffered_bytes: u64,
    /// Number of active (non-finalized) partition streams for this stage.
    pub stream_active_count: u32,
    /// Recent backpressure control-loop events for this stage.
    pub backpressure_events: Vec<String>,
    /// Number of speculative attempts launched for this stage.
    pub speculative_attempts_launched: u32,
    /// Number of speculative races won by an older attempt.
    pub speculative_older_attempt_wins: u32,
    /// Number of speculative races won by a newer attempt.
    pub speculative_newer_attempt_wins: u32,
}

#[derive(Debug, Clone)]
/// Map output metadata for one reduce partition.
pub struct MapOutputPartitionMeta {
    /// Reduce partition id this map output belongs to.
    pub reduce_partition: u32,
    /// Bytes produced for the partition.
    pub bytes: u64,
    /// Rows produced for the partition.
    pub rows: u64,
    /// Batches produced for the partition.
    pub batches: u64,
    /// Stream epoch for partition stream progress.
    pub stream_epoch: u32,
    /// Highest committed readable byte offset in the partition stream.
    pub committed_offset: u64,
    /// Whether the partition stream is finalized for this attempt.
    pub finalized: bool,
}

#[derive(Debug, Clone)]
/// One streamed shuffle chunk with readable-boundary metadata.
pub struct ShuffleFetchChunk {
    /// Payload bytes for this chunk.
    pub payload: Vec<u8>,
    /// Inclusive start byte offset in the partition payload.
    pub start_offset: u64,
    /// Exclusive end byte offset in the partition payload.
    pub end_offset: u64,
    /// Highest committed readable byte offset known for this partition.
    pub watermark_offset: u64,
    /// Whether this partition stream is finalized for the selected attempt.
    pub finalized: bool,
    /// Stream epoch of the partition metadata used for this chunk.
    pub stream_epoch: u32,
}

fn sanitize_map_output_partition_meta(mut p: MapOutputPartitionMeta) -> MapOutputPartitionMeta {
    if p.committed_offset > p.bytes {
        p.committed_offset = p.bytes;
    }
    if p.finalized {
        p.committed_offset = p.bytes;
    }
    p
}

#[derive(Debug, Clone)]
/// Public query status snapshot returned by control-plane APIs.
pub struct QueryStatus {
    /// Stable query identifier.
    pub query_id: String,
    /// Current query state.
    pub state: QueryState,
    /// Submission timestamp in unix milliseconds.
    pub submitted_at_ms: u64,
    /// First-start timestamp in unix milliseconds, or 0 if not started.
    pub started_at_ms: u64,
    /// Finish timestamp in unix milliseconds, or 0 if unfinished.
    pub finished_at_ms: u64,
    /// Human-readable status message.
    pub message: String,
    /// Total number of task attempts tracked for the query.
    pub total_tasks: u32,
    /// Number of queued tasks across all stages.
    pub queued_tasks: u32,
    /// Number of running tasks across all stages.
    pub running_tasks: u32,
    /// Number of succeeded tasks across all stages.
    pub succeeded_tasks: u32,
    /// Number of failed tasks across all stages.
    pub failed_tasks: u32,
    /// Per-stage metrics keyed by stage id.
    pub stage_metrics: HashMap<u64, StageMetrics>,
}

#[derive(Debug, Clone)]
struct StageRuntime {
    parents: Vec<u64>,
    children: Vec<u64>,
    layout_version: u32,
    barrier_state: StageBarrierState,
    layout_finalize_count: u32,
    metrics: StageMetrics,
    completed_runtime_ms_samples: Vec<u64>,
}

#[derive(Debug, Clone)]
struct TaskRuntime {
    query_id: String,
    stage_id: u64,
    task_id: u64,
    attempt: u32,
    state: TaskState,
    assigned_worker: Option<String>,
    ready_at_ms: u64,
    plan_fragment_json: Vec<u8>,
    assigned_reduce_partitions: Vec<u32>,
    assigned_reduce_split_index: u32,
    assigned_reduce_split_count: u32,
    layout_version: u32,
    layout_fingerprint: u64,
    required_custom_ops: Vec<String>,
    locality_hints: Vec<String>,
    running_since_ms: Option<u64>,
    is_speculative: bool,
    message: String,
}

#[derive(Debug, Clone, Default)]
struct WorkerHeartbeat {
    last_seen_ms: u64,
    custom_operator_capabilities: HashSet<String>,
    locality_tags: HashSet<String>,
}

#[derive(Debug, Clone, Default)]
struct ReduceBackpressureSample {
    inflight_bytes: u64,
    queue_depth: u32,
}

#[derive(Debug, Clone)]
struct QueryRuntime {
    state: QueryState,
    submitted_at_ms: u64,
    started_at_ms: u64,
    finished_at_ms: u64,
    message: String,
    stages: HashMap<u64, StageRuntime>,
    tasks: HashMap<(u64, u64, u32), TaskRuntime>,
}

#[derive(Debug, Default)]
/// In-memory coordinator runtime for query/task orchestration.
pub struct Coordinator {
    config: CoordinatorConfig,
    catalog: Catalog,
    queries: HashMap<String, QueryRuntime>,
    map_outputs: HashMap<(String, u64, u64, u32), Vec<MapOutputPartitionMeta>>,
    query_results: HashMap<String, Vec<u8>>,
    blacklisted_workers: HashSet<String>,
    worker_failures: HashMap<String, u32>,
    worker_heartbeats: HashMap<String, WorkerHeartbeat>,
    reduce_backpressure: HashMap<(String, u64, u64, u32), ReduceBackpressureSample>,
}

impl Coordinator {
    fn running_tasks_for_worker(&self, worker_id: &str) -> u32 {
        self.queries
            .values()
            .flat_map(|q| q.tasks.values())
            .filter(|t| {
                t.state == TaskState::Running && t.assigned_worker.as_deref() == Some(worker_id)
            })
            .count() as u32
    }

    fn touch_worker(&mut self, worker_id: &str, now: u64) {
        self.worker_heartbeats
            .entry(worker_id.to_string())
            .and_modify(|hb| hb.last_seen_ms = now)
            .or_insert_with(|| WorkerHeartbeat {
                last_seen_ms: now,
                custom_operator_capabilities: HashSet::new(),
                locality_tags: HashSet::new(),
            });
    }

    fn requeue_stale_workers(&mut self, now: u64) -> Result<()> {
        if self.config.worker_liveness_timeout_ms == 0 {
            return Ok(());
        }
        let stale_workers = self
            .worker_heartbeats
            .iter()
            .filter_map(|(worker, hb)| {
                let stale =
                    now.saturating_sub(hb.last_seen_ms) > self.config.worker_liveness_timeout_ms;
                if stale && !self.blacklisted_workers.contains(worker) {
                    Some(worker.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        for worker in stale_workers {
            warn!(
                worker_id = %worker,
                operator = "CoordinatorRequeue",
                "worker considered stale; requeueing running tasks"
            );
            self.requeue_worker_tasks(&worker, now)?;
            self.worker_heartbeats.remove(&worker);
        }
        Ok(())
    }

    fn requeue_worker_tasks(&mut self, worker_id: &str, now: u64) -> Result<()> {
        for (query_id, query) in self.queries.iter_mut() {
            if !matches!(query.state, QueryState::Queued | QueryState::Running) {
                continue;
            }
            let latest_attempts = latest_attempt_map(query);
            let mut to_retry = Vec::new();
            for t in query.tasks.values_mut() {
                if t.state == TaskState::Running
                    && t.assigned_worker.as_deref() == Some(worker_id)
                    && latest_attempts
                        .get(&(t.stage_id, t.task_id))
                        .is_some_and(|a| *a == t.attempt)
                {
                    let stage = query
                        .stages
                        .get_mut(&t.stage_id)
                        .ok_or_else(|| FfqError::Execution("task stage not found".to_string()))?;
                    stage.metrics.running_tasks = stage.metrics.running_tasks.saturating_sub(1);
                    stage.metrics.failed_tasks += 1;
                    update_scheduler_metrics(query_id, t.stage_id, &stage.metrics);
                    t.state = TaskState::Failed;
                    t.message = "worker lost heartbeat".to_string();
                    to_retry.push((
                        t.stage_id,
                        t.task_id,
                        t.attempt,
                        t.plan_fragment_json.clone(),
                        t.assigned_reduce_partitions.clone(),
                        t.assigned_reduce_split_index,
                        t.assigned_reduce_split_count,
                        t.layout_version,
                        t.layout_fingerprint,
                        t.required_custom_ops.clone(),
                        t.locality_hints.clone(),
                    ));
                }
            }

            for (
                stage_id,
                task_id,
                attempt,
                fragment,
                assigned_reduce_partitions,
                assigned_reduce_split_index,
                assigned_reduce_split_count,
                layout_version,
                layout_fingerprint,
                required_custom_ops,
                locality_hints,
            ) in to_retry
            {
                if attempt < self.config.max_task_attempts {
                    let next_attempt = attempt + 1;
                    let backoff_ms = self
                        .config
                        .retry_backoff_base_ms
                        .saturating_mul(1_u64 << (attempt.saturating_sub(1).min(10)));
                    query.tasks.insert(
                        (stage_id, task_id, next_attempt),
                        TaskRuntime {
                            query_id: query_id.clone(),
                            stage_id,
                            task_id,
                            attempt: next_attempt,
                            state: TaskState::Queued,
                            assigned_worker: None,
                            ready_at_ms: now.saturating_add(backoff_ms),
                            plan_fragment_json: fragment,
                            assigned_reduce_partitions,
                            assigned_reduce_split_index,
                            assigned_reduce_split_count,
                            layout_version,
                            layout_fingerprint,
                            required_custom_ops,
                            locality_hints,
                            running_since_ms: None,
                            is_speculative: false,
                            message: "retry scheduled after worker timeout".to_string(),
                        },
                    );
                    let stage = query
                        .stages
                        .get_mut(&stage_id)
                        .ok_or_else(|| FfqError::Execution("task stage not found".to_string()))?;
                    stage.metrics.queued_tasks += 1;
                    update_scheduler_metrics(query_id, stage_id, &stage.metrics);
                    global_metrics().inc_scheduler_retries(query_id, stage_id);
                } else {
                    query.state = QueryState::Failed;
                    query.finished_at_ms = now;
                    query.message = format!(
                        "task stage={stage_id} task={task_id} exhausted retries after worker timeout"
                    );
                }
            }
        }
        Ok(())
    }

    /// Construct coordinator with an empty catalog.
    pub fn new(config: CoordinatorConfig) -> Self {
        Self {
            config,
            catalog: Catalog::new(),
            ..Self::default()
        }
    }

    /// Construct coordinator with a preloaded catalog.
    pub fn with_catalog(config: CoordinatorConfig, catalog: Catalog) -> Self {
        Self {
            config,
            catalog,
            ..Self::default()
        }
    }

    /// Submit a physical plan and initialize query runtime/task attempts.
    pub fn submit_query(
        &mut self,
        query_id: String,
        physical_plan_json: &[u8],
    ) -> Result<QueryState> {
        if self.queries.contains_key(&query_id) {
            return Err(FfqError::Planning(format!(
                "query '{query_id}' already exists"
            )));
        }
        let mut plan: PhysicalPlan = serde_json::from_slice(physical_plan_json)
            .map_err(|e| FfqError::Planning(format!("invalid physical plan json: {e}")))?;
        self.resolve_parquet_scan_schemas(&mut plan)?;
        let resolved_plan_json = serde_json::to_vec(&plan).map_err(|e| {
            FfqError::Planning(format!("encode resolved physical plan failed: {e}"))
        })?;
        let dag = build_stage_dag(&plan);
        info!(
            query_id = %query_id,
            stages = dag.stages.len(),
            operator = "CoordinatorSubmit",
            "query submitted"
        );
        let qr = build_query_runtime(&query_id, dag, &resolved_plan_json)?;
        self.queries.insert(query_id, qr);
        Ok(QueryState::Queued)
    }

    fn resolve_parquet_scan_schemas(&mut self, plan: &mut PhysicalPlan) -> Result<()> {
        match plan {
            PhysicalPlan::ParquetScan(scan) => {
                if scan.schema.is_some() {
                    return Ok(());
                }
                let Ok(mut table) = self.catalog.get(&scan.table).cloned() else {
                    // Backward-compatible fallback: if coordinator does not have the table,
                    // leave scan schema unresolved and let worker-side catalog drive execution.
                    return Ok(());
                };
                if !table.format.eq_ignore_ascii_case("parquet") {
                    return Ok(());
                }
                if let Some(schema) = table.schema.clone() {
                    scan.schema = Some(schema);
                    return Ok(());
                }
                if !self.config.schema_inference.allows_inference() {
                    return Err(FfqError::InvalidConfig(format!(
                        "table '{}' has no schema and coordinator schema inference is disabled",
                        table.name
                    )));
                }
                let paths = table.data_paths()?;
                let inferred = ParquetProvider::infer_parquet_schema_with_policy(
                    &paths,
                    self.config.schema_inference.is_permissive_merge(),
                )
                .map_err(|e| {
                    FfqError::InvalidConfig(format!(
                        "schema inference failed for table '{}' on coordinator: {e}",
                        table.name
                    ))
                })?;
                scan.schema = Some(inferred.clone());
                table.schema = Some(inferred);
                self.catalog.register_table(table);
                Ok(())
            }
            PhysicalPlan::ParquetWrite(x) => self.resolve_parquet_scan_schemas(&mut x.input),
            PhysicalPlan::Filter(x) => self.resolve_parquet_scan_schemas(&mut x.input),
            PhysicalPlan::InSubqueryFilter(x) => {
                self.resolve_parquet_scan_schemas(&mut x.input)?;
                self.resolve_parquet_scan_schemas(&mut x.subquery)
            }
            PhysicalPlan::ExistsSubqueryFilter(x) => {
                self.resolve_parquet_scan_schemas(&mut x.input)?;
                self.resolve_parquet_scan_schemas(&mut x.subquery)
            }
            PhysicalPlan::ScalarSubqueryFilter(x) => {
                self.resolve_parquet_scan_schemas(&mut x.input)?;
                self.resolve_parquet_scan_schemas(&mut x.subquery)
            }
            PhysicalPlan::Project(x) => self.resolve_parquet_scan_schemas(&mut x.input),
            PhysicalPlan::Window(x) => self.resolve_parquet_scan_schemas(&mut x.input),
            PhysicalPlan::CoalesceBatches(x) => self.resolve_parquet_scan_schemas(&mut x.input),
            PhysicalPlan::PartialHashAggregate(x) => {
                self.resolve_parquet_scan_schemas(&mut x.input)
            }
            PhysicalPlan::FinalHashAggregate(x) => self.resolve_parquet_scan_schemas(&mut x.input),
            PhysicalPlan::HashJoin(x) => {
                self.resolve_parquet_scan_schemas(&mut x.left)?;
                self.resolve_parquet_scan_schemas(&mut x.right)?;
                for alt in &mut x.alternatives {
                    self.resolve_parquet_scan_schemas(&mut alt.left)?;
                    self.resolve_parquet_scan_schemas(&mut alt.right)?;
                }
                Ok(())
            }
            PhysicalPlan::Exchange(x) => match x {
                ExchangeExec::ShuffleWrite(e) => self.resolve_parquet_scan_schemas(&mut e.input),
                ExchangeExec::ShuffleRead(e) => self.resolve_parquet_scan_schemas(&mut e.input),
                ExchangeExec::Broadcast(e) => self.resolve_parquet_scan_schemas(&mut e.input),
            },
            PhysicalPlan::Limit(x) => self.resolve_parquet_scan_schemas(&mut x.input),
            PhysicalPlan::TopKByScore(x) => self.resolve_parquet_scan_schemas(&mut x.input),
            PhysicalPlan::UnionAll(x) => {
                self.resolve_parquet_scan_schemas(&mut x.left)?;
                self.resolve_parquet_scan_schemas(&mut x.right)
            }
            PhysicalPlan::CteRef(x) => self.resolve_parquet_scan_schemas(&mut x.plan),
            PhysicalPlan::VectorTopK(_) => Ok(()),
            PhysicalPlan::Custom(x) => self.resolve_parquet_scan_schemas(&mut x.input),
        }
    }

    /// Worker pull-scheduling API.
    ///
    /// Returns up to `capacity` runnable task attempts for the requesting
    /// worker, skipping blacklisted workers.
    pub fn get_task(&mut self, worker_id: &str, capacity: u32) -> Result<Vec<TaskAssignment>> {
        let now = now_ms()?;
        self.requeue_stale_workers(now)?;

        if self.blacklisted_workers.contains(worker_id) || capacity == 0 {
            debug!(
                worker_id = %worker_id,
                capacity,
                operator = "CoordinatorGetTask",
                "no tasks assigned (blacklisted or no capacity)"
            );
            return Ok(Vec::new());
        }
        let running_for_worker = self.running_tasks_for_worker(worker_id);
        let worker_budget = self
            .config
            .max_concurrent_tasks_per_worker
            .saturating_sub(running_for_worker);
        let mut remaining = capacity.min(worker_budget);
        let mut out = Vec::new();
        self.touch_worker(worker_id, now);
        let worker_hb = self.worker_heartbeats.get(worker_id).cloned();
        let worker_caps = worker_hb
            .as_ref()
            .map(|hb| hb.custom_operator_capabilities.clone());
        if remaining == 0 {
            return Ok(out);
        }

        let map_outputs_snapshot = self.map_outputs.clone();
        for (query_id, query) in self.queries.iter_mut() {
            if !matches!(query.state, QueryState::Queued | QueryState::Running) {
                continue;
            }

            if query.state == QueryState::Queued {
                query.state = QueryState::Running;
                query.started_at_ms = now_ms()?;
            }

            let running_for_query = running_tasks_for_query_latest(query);
            if running_for_query >= self.config.max_concurrent_tasks_per_query {
                continue;
            }
            let (observed_inflight, observed_queue_depth) =
                aggregate_reduce_backpressure(&self.reduce_backpressure, query_id);
            let (map_publish_window, reduce_fetch_window) = recommended_backpressure_windows(
                observed_inflight,
                observed_queue_depth,
                self.config.backpressure_target_inflight_bytes,
                self.config.backpressure_target_queue_depth,
                self.config.backpressure_max_map_publish_window_partitions,
                self.config.backpressure_max_reduce_fetch_window_partitions,
            );
            let mut query_budget = self
                .config
                .max_concurrent_tasks_per_query
                .saturating_sub(running_for_query);
            advance_stage_barriers_and_finalize_layout(
                query_id,
                query,
                &map_outputs_snapshot,
                self.config.adaptive_shuffle_target_bytes,
                self.config.adaptive_shuffle_min_reduce_tasks,
                self.config.adaptive_shuffle_max_reduce_tasks,
                self.config.adaptive_shuffle_max_partitions_per_task,
                now,
            );
            let latest_states = latest_task_states(query);
            if self.config.speculative_execution_enabled {
                enqueue_speculative_attempts(
                    query_id,
                    query,
                    now,
                    self.config.speculative_min_completed_samples,
                    self.config.speculative_p95_multiplier,
                    self.config.speculative_min_runtime_ms,
                    self.config.max_task_attempts,
                );
            }
            let latest_attempts = latest_attempt_map(query);
            for stage_id in runnable_stages_with_pipeline(
                query_id,
                query,
                &latest_states,
                &map_outputs_snapshot,
                self.config.pipelined_shuffle_enabled,
                self.config.pipelined_shuffle_min_map_completion_ratio,
                self.config.pipelined_shuffle_min_committed_offset_bytes,
            ) {
                let Some(stage_runtime) = query.stages.get(&stage_id) else {
                    continue;
                };
                let stage_parents_done =
                    all_parents_done_for_stage(query, stage_id, &latest_states);
                let pipeline_ready_partitions =
                    if self.config.pipelined_shuffle_enabled && !stage_parents_done {
                        Some(ready_reduce_partitions_for_stage(
                            query_id,
                            query,
                            stage_id,
                            &map_outputs_snapshot,
                            self.config.pipelined_shuffle_min_committed_offset_bytes,
                        ))
                    } else {
                        None
                    };
                if !matches!(
                    stage_runtime.barrier_state,
                    StageBarrierState::NotApplicable | StageBarrierState::ReduceSchedulable
                ) && !(self.config.pipelined_shuffle_enabled && !stage_parents_done)
                {
                    continue;
                }
                let running_logical_tasks_on_worker = query
                    .tasks
                    .values()
                    .filter(|t| {
                        t.state == TaskState::Running
                            && t.assigned_worker.as_deref() == Some(worker_id)
                    })
                    .map(|t| (t.stage_id, t.task_id))
                    .collect::<HashSet<_>>();
                for task in query.tasks.values_mut().filter(|t| {
                    t.stage_id == stage_id && t.state == TaskState::Queued && t.ready_at_ms <= now
                }) {
                    if remaining == 0 || query_budget == 0 {
                        return Ok(out);
                    }
                    if latest_attempts
                        .get(&(task.stage_id, task.task_id))
                        .is_some_and(|a| *a != task.attempt)
                    {
                        continue;
                    }
                    if task.is_speculative
                        && running_logical_tasks_on_worker.contains(&(task.stage_id, task.task_id))
                    {
                        continue;
                    }
                    if !worker_supports_task(worker_caps.as_ref(), &task.required_custom_ops) {
                        continue;
                    }
                    if self.config.locality_preference_enabled
                        && !task.locality_hints.is_empty()
                        && !worker_matches_locality(worker_hb.as_ref(), &task.locality_hints)
                        && has_any_live_worker_for_locality(
                            &self.worker_heartbeats,
                            &self.blacklisted_workers,
                            now,
                            self.config.worker_liveness_timeout_ms,
                            &task.locality_hints,
                        )
                    {
                        continue;
                    }
                    if let Some(ready) = &pipeline_ready_partitions {
                        if task.assigned_reduce_partitions.is_empty()
                            || !task
                                .assigned_reduce_partitions
                                .iter()
                                .all(|p| ready.contains(p))
                        {
                            continue;
                        }
                    }
                    task.state = TaskState::Running;
                    task.assigned_worker = Some(worker_id.to_string());
                    task.running_since_ms = Some(now);
                    let stage = query
                        .stages
                        .get_mut(&stage_id)
                        .expect("stage exists for task");
                    stage.metrics.queued_tasks = stage.metrics.queued_tasks.saturating_sub(1);
                    stage.metrics.running_tasks += 1;
                    update_scheduler_metrics(&task.query_id, stage_id, &stage.metrics);
                    if task.attempt > 0 {
                        global_metrics().inc_scheduler_retries(&task.query_id, stage_id);
                    }

                    out.push(TaskAssignment {
                        query_id: task.query_id.clone(),
                        stage_id: task.stage_id,
                        task_id: task.task_id,
                        attempt: task.attempt,
                        plan_fragment_json: task.plan_fragment_json.clone(),
                        assigned_reduce_partitions: task.assigned_reduce_partitions.clone(),
                        assigned_reduce_split_index: task.assigned_reduce_split_index,
                        assigned_reduce_split_count: task.assigned_reduce_split_count,
                        layout_version: task.layout_version,
                        layout_fingerprint: task.layout_fingerprint,
                        recommended_map_output_publish_window_partitions: map_publish_window,
                        recommended_reduce_fetch_window_partitions: reduce_fetch_window,
                    });
                    if let Some(stage) = query.stages.get_mut(&stage_id) {
                        if stage.metrics.map_publish_window_partitions != map_publish_window
                            || stage.metrics.reduce_fetch_window_partitions != reduce_fetch_window
                        {
                            push_stage_backpressure_event(
                                &mut stage.metrics,
                                format!(
                                    "window_update inflight={} queue_depth={} map_publish_window={} reduce_fetch_window={}",
                                    observed_inflight,
                                    observed_queue_depth,
                                    map_publish_window,
                                    reduce_fetch_window
                                ),
                            );
                        }
                        stage.metrics.backpressure_inflight_bytes = observed_inflight;
                        stage.metrics.backpressure_queue_depth = observed_queue_depth;
                        stage.metrics.stream_buffered_bytes = observed_inflight;
                        stage.metrics.map_publish_window_partitions = map_publish_window;
                        stage.metrics.reduce_fetch_window_partitions = reduce_fetch_window;
                    }
                    remaining = remaining.saturating_sub(1);
                    query_budget = query_budget.saturating_sub(1);
                    debug!(
                        worker_id = %worker_id,
                        query_id = %task.query_id,
                        stage_id = task.stage_id,
                        task_id = task.task_id,
                        attempt = task.attempt,
                        operator = "CoordinatorGetTask",
                        "assigned task"
                    );
                }
            }
        }

        Ok(out)
    }

    /// Record a task attempt status transition and update query/stage metrics.
    pub fn report_task_status(
        &mut self,
        query_id: &str,
        stage_id: u64,
        task_id: u64,
        attempt: u32,
        layout_version: u32,
        layout_fingerprint: u64,
        state: TaskState,
        worker_id: Option<&str>,
        message: String,
    ) -> Result<()> {
        self.report_task_status_with_pressure(
            query_id,
            stage_id,
            task_id,
            attempt,
            layout_version,
            layout_fingerprint,
            state,
            worker_id,
            message,
            0,
            0,
        )
    }

    /// Record a task attempt status transition and reducer backpressure sample.
    pub fn report_task_status_with_pressure(
        &mut self,
        query_id: &str,
        stage_id: u64,
        task_id: u64,
        attempt: u32,
        layout_version: u32,
        layout_fingerprint: u64,
        state: TaskState,
        worker_id: Option<&str>,
        message: String,
        reduce_fetch_inflight_bytes: u64,
        reduce_fetch_queue_depth: u32,
    ) -> Result<()> {
        let now = now_ms()?;
        self.requeue_stale_workers(now)?;
        let query = self
            .queries
            .get_mut(query_id)
            .ok_or_else(|| FfqError::Planning(format!("unknown query: {query_id}")))?;
        let mut latest_attempt = latest_attempt_map(query)
            .get(&(stage_id, task_id))
            .copied()
            .unwrap_or(attempt);
        if attempt < latest_attempt {
            if state == TaskState::Succeeded
                && adopt_older_attempt_success_from_speculation(
                    query,
                    stage_id,
                    task_id,
                    attempt,
                    latest_attempt,
                )
            {
                latest_attempt = attempt;
            } else {
                debug!(
                    query_id = %query_id,
                    stage_id,
                    task_id,
                    attempt,
                    operator = "CoordinatorReportTaskStatus",
                    "ignoring stale status report from old attempt"
                );
                return Ok(());
            }
        }
        if attempt < latest_attempt {
            debug!(
                query_id = %query_id,
                stage_id,
                task_id,
                attempt,
                operator = "CoordinatorReportTaskStatus",
                "ignoring stale status report from old attempt"
            );
            return Ok(());
        }
        let key = (stage_id, task_id, attempt);
        let Some(layout_identity) = query
            .tasks
            .get(&key)
            .map(|t| (t.layout_version, t.layout_fingerprint))
        else {
            debug!(
                query_id = %query_id,
                stage_id,
                task_id,
                attempt,
                operator = "CoordinatorReportTaskStatus",
                "ignoring status report for unknown task attempt"
            );
            return Ok(());
        };
        if layout_identity.0 != layout_version || layout_identity.1 != layout_fingerprint {
            debug!(
                query_id = %query_id,
                stage_id,
                task_id,
                attempt,
                expected_layout_version = layout_identity.0,
                reported_layout_version = layout_version,
                expected_layout_fingerprint = layout_identity.1,
                reported_layout_fingerprint = layout_fingerprint,
                operator = "CoordinatorReportTaskStatus",
                "ignoring stale status report from different adaptive layout"
            );
            return Ok(());
        }
        let prev_state = query
            .tasks
            .get(&key)
            .map(|t| t.state)
            .ok_or_else(|| FfqError::Planning("unknown task status report".to_string()))?;

        let bp_key = (query_id.to_string(), stage_id, task_id, attempt);
        if reduce_fetch_inflight_bytes > 0 || reduce_fetch_queue_depth > 0 {
            self.reduce_backpressure.insert(
                bp_key.clone(),
                ReduceBackpressureSample {
                    inflight_bytes: reduce_fetch_inflight_bytes,
                    queue_depth: reduce_fetch_queue_depth,
                },
            );
        }
        if matches!(state, TaskState::Failed) {
            self.reduce_backpressure.remove(&bp_key);
        }
        let elapsed_ms = query_elapsed_ms(query, now);
        if prev_state == state {
            if let Some(stage) = query.stages.get_mut(&stage_id) {
                stage.metrics.backpressure_inflight_bytes = reduce_fetch_inflight_bytes;
                stage.metrics.backpressure_queue_depth = reduce_fetch_queue_depth;
                stage.metrics.stream_buffered_bytes = reduce_fetch_inflight_bytes;
                if stage.metrics.first_reduce_row_ms == 0
                    && (reduce_fetch_inflight_bytes > 0 || reduce_fetch_queue_depth > 0)
                {
                    stage.metrics.first_reduce_row_ms = elapsed_ms;
                }
                update_stage_stream_lag(&mut stage.metrics, elapsed_ms);
            }
            return Ok(());
        }
        let stage = query
            .stages
            .get_mut(&stage_id)
            .ok_or_else(|| FfqError::Execution("task stage not found".to_string()))?;
        if prev_state == TaskState::Running {
            stage.metrics.running_tasks = stage.metrics.running_tasks.saturating_sub(1);
        }

        let task_plan_fragment = query
            .tasks
            .get(&key)
            .map(|t| t.plan_fragment_json.clone())
            .ok_or_else(|| FfqError::Planning("unknown task status report".to_string()))?;
        let task_assigned_reduce_partitions = query
            .tasks
            .get(&key)
            .map(|t| t.assigned_reduce_partitions.clone())
            .ok_or_else(|| FfqError::Planning("unknown task status report".to_string()))?;
        let task_assigned_reduce_split_index = query
            .tasks
            .get(&key)
            .map(|t| t.assigned_reduce_split_index)
            .ok_or_else(|| FfqError::Planning("unknown task status report".to_string()))?;
        let task_assigned_reduce_split_count = query
            .tasks
            .get(&key)
            .map(|t| t.assigned_reduce_split_count)
            .ok_or_else(|| FfqError::Planning("unknown task status report".to_string()))?;
        let task_required_custom_ops = query
            .tasks
            .get(&key)
            .map(|t| t.required_custom_ops.clone())
            .ok_or_else(|| FfqError::Planning("unknown task status report".to_string()))?;
        let task_locality_hints = query
            .tasks
            .get(&key)
            .map(|t| t.locality_hints.clone())
            .ok_or_else(|| FfqError::Planning("unknown task status report".to_string()))?;
        let assigned_worker_cached = query
            .tasks
            .get(&key)
            .and_then(|t| t.assigned_worker.clone());
        let task_running_since = query.tasks.get(&key).and_then(|t| t.running_since_ms);
        let task_is_speculative = query.tasks.get(&key).is_some_and(|t| t.is_speculative);
        if let Some(task) = query.tasks.get_mut(&key) {
            task.state = state;
            task.message = message.clone();
            match state {
                TaskState::Running => {
                    if task.running_since_ms.is_none() {
                        task.running_since_ms = Some(now);
                    }
                }
                TaskState::Queued => task.running_since_ms = None,
                TaskState::Succeeded | TaskState::Failed => task.running_since_ms = None,
            }
        }
        if prev_state == TaskState::Running
            && matches!(state, TaskState::Succeeded | TaskState::Failed)
            && let Some(start_ms) = task_running_since
        {
            let dur_ms = now.saturating_sub(start_ms);
            stage.completed_runtime_ms_samples.push(dur_ms);
            if stage.completed_runtime_ms_samples.len() > 128 {
                let keep_from = stage.completed_runtime_ms_samples.len().saturating_sub(128);
                stage.completed_runtime_ms_samples.drain(0..keep_from);
            }
        }
        match state {
            TaskState::Queued => {
                stage.metrics.queued_tasks += 1;
                if attempt > 0 {
                    global_metrics().inc_scheduler_retries(query_id, stage_id);
                }
            }
            TaskState::Running => stage.metrics.running_tasks += 1,
            TaskState::Succeeded => {
                stage.metrics.succeeded_tasks += 1;
                if let Some(worker) = worker_id.or(assigned_worker_cached.as_deref()) {
                    self.worker_failures.remove(worker);
                }
                if task_is_speculative {
                    stage.metrics.speculative_newer_attempt_wins =
                        stage.metrics.speculative_newer_attempt_wins.saturating_add(1);
                }
            }
            TaskState::Failed => {
                stage.metrics.failed_tasks += 1;
                if let Some(worker) = worker_id.or(assigned_worker_cached.as_deref()) {
                    let failures = self.worker_failures.entry(worker.to_string()).or_default();
                    *failures += 1;
                    if *failures >= self.config.blacklist_failure_threshold {
                        warn!(
                            worker_id = %worker,
                            failures = *failures,
                            threshold = self.config.blacklist_failure_threshold,
                            operator = "CoordinatorReportTaskStatus",
                            "worker blacklisted due to repeated failures"
                        );
                        self.blacklisted_workers.insert(worker.to_string());
                    }
                }
                if attempt < self.config.max_task_attempts {
                    let next_attempt = attempt + 1;
                    let backoff_ms = self
                        .config
                        .retry_backoff_base_ms
                        .saturating_mul(1_u64 << (attempt.saturating_sub(1).min(10)));
                    let retry_key = (stage_id, task_id, next_attempt);
                    query.tasks.insert(
                        retry_key,
                        TaskRuntime {
                            query_id: query_id.to_string(),
                            stage_id,
                            task_id,
                            attempt: next_attempt,
                            state: TaskState::Queued,
                            assigned_worker: None,
                            ready_at_ms: now.saturating_add(backoff_ms),
                            plan_fragment_json: task_plan_fragment,
                            assigned_reduce_partitions: task_assigned_reduce_partitions,
                            assigned_reduce_split_index: task_assigned_reduce_split_index,
                            assigned_reduce_split_count: task_assigned_reduce_split_count,
                            layout_version,
                            layout_fingerprint,
                            required_custom_ops: task_required_custom_ops,
                            locality_hints: task_locality_hints,
                            running_since_ms: None,
                            is_speculative: false,
                            message: format!("retry scheduled after failure: {message}"),
                        },
                    );
                    stage.metrics.queued_tasks += 1;
                    query.state = QueryState::Running;
                    query.message = format!("retrying failed task stage={stage_id} task={task_id}");
                } else {
                    query.state = QueryState::Failed;
                    query.finished_at_ms = now;
                    query.message = message;
                }
            }
        }
        stage.metrics.backpressure_inflight_bytes = reduce_fetch_inflight_bytes;
        stage.metrics.backpressure_queue_depth = reduce_fetch_queue_depth;
        stage.metrics.stream_buffered_bytes = reduce_fetch_inflight_bytes;
        if stage.metrics.first_reduce_row_ms == 0
            && (reduce_fetch_inflight_bytes > 0 || reduce_fetch_queue_depth > 0)
        {
            stage.metrics.first_reduce_row_ms = elapsed_ms;
        }
        update_stage_stream_lag(&mut stage.metrics, elapsed_ms);
        update_scheduler_metrics(query_id, stage_id, &stage.metrics);

        if query.state != QueryState::Failed && is_query_succeeded(query) {
            query.state = QueryState::Succeeded;
            query.finished_at_ms = now;
            info!(
                query_id = %query_id,
                operator = "CoordinatorReportTaskStatus",
                "query reached succeeded state"
            );
        }

        Ok(())
    }

    /// Record worker heartbeat and liveness metadata.
    pub fn heartbeat(
        &mut self,
        worker_id: &str,
        _running_tasks: u32,
        custom_operator_capabilities: &[String],
    ) -> Result<()> {
        let now = now_ms()?;
        self.worker_heartbeats.insert(
            worker_id.to_string(),
            WorkerHeartbeat {
                last_seen_ms: now,
                custom_operator_capabilities: custom_operator_capabilities
                    .iter()
                    .cloned()
                    .collect(),
                locality_tags: parse_locality_tags(custom_operator_capabilities),
            },
        );
        Ok(())
    }

    /// Cancel a running/queued query.
    pub fn cancel_query(&mut self, query_id: &str, reason: &str) -> Result<QueryState> {
        let query = self
            .queries
            .get_mut(query_id)
            .ok_or_else(|| FfqError::Planning(format!("unknown query: {query_id}")))?;
        query.state = QueryState::Canceled;
        query.finished_at_ms = now_ms()?;
        query.message = reason.to_string();
        Ok(QueryState::Canceled)
    }

    /// Read current query status snapshot.
    pub fn get_query_status(&self, query_id: &str) -> Result<QueryStatus> {
        let query = self
            .queries
            .get(query_id)
            .ok_or_else(|| FfqError::Planning(format!("unknown query: {query_id}")))?;
        Ok(build_query_status(query_id, query))
    }

    /// Register map output metadata for one `(query, stage, map_task, attempt)`.
    pub fn register_map_output(
        &mut self,
        query_id: String,
        stage_id: u64,
        map_task: u64,
        attempt: u32,
        layout_version: u32,
        layout_fingerprint: u64,
        partitions: Vec<MapOutputPartitionMeta>,
    ) -> Result<()> {
        let now = now_ms()?;
        let Some(query) = self.queries.get(&query_id) else {
            return Err(FfqError::Planning(format!("unknown query: {query_id}")));
        };
        let latest_attempt = latest_attempt_map(query)
            .get(&(stage_id, map_task))
            .copied()
            .unwrap_or(attempt);
        if attempt < latest_attempt {
            debug!(
                query_id = %query_id,
                stage_id,
                map_task,
                attempt,
                latest_attempt,
                operator = "CoordinatorRegisterMapOutput",
                "ignoring stale map-output registration from old attempt"
            );
            return Ok(());
        }
        let key = (stage_id, map_task, attempt);
        let Some(expected_layout) = query
            .tasks
            .get(&key)
            .map(|t| (t.layout_version, t.layout_fingerprint))
        else {
            debug!(
                query_id = %query_id,
                stage_id,
                map_task,
                attempt,
                operator = "CoordinatorRegisterMapOutput",
                "ignoring map-output registration for unknown task attempt"
            );
            return Ok(());
        };
        if expected_layout.0 != layout_version || expected_layout.1 != layout_fingerprint {
            debug!(
                query_id = %query_id,
                stage_id,
                map_task,
                attempt,
                expected_layout_version = expected_layout.0,
                reported_layout_version = layout_version,
                expected_layout_fingerprint = expected_layout.1,
                reported_layout_fingerprint = layout_fingerprint,
                operator = "CoordinatorRegisterMapOutput",
                "ignoring stale map-output registration from different adaptive layout"
            );
            return Ok(());
        }
        let registry_key = (query_id.clone(), stage_id, map_task, attempt);
        let partitions = partitions
            .into_iter()
            .map(sanitize_map_output_partition_meta)
            .collect::<Vec<_>>();
        self.map_outputs
            .entry(registry_key)
            .and_modify(|existing| merge_map_output_partitions(existing, &partitions))
            .or_insert(partitions);
        let latest = self.latest_map_partitions_for_stage(&query_id, stage_id);
        let mut rows = 0_u64;
        let mut bytes = 0_u64;
        let mut batches = 0_u64;
        let mut reduce_ids = HashSet::new();
        let mut bytes_by_partition = HashMap::<u32, u64>::new();
        let active_stream_count = latest
            .iter()
            .filter(|p| p.committed_offset > 0 && !p.finalized)
            .count() as u32;
        for p in &latest {
            rows = rows.saturating_add(p.rows);
            bytes = bytes.saturating_add(p.bytes);
            batches = batches.saturating_add(p.batches);
            reduce_ids.insert(p.reduce_partition);
            bytes_by_partition
                .entry(p.reduce_partition)
                .and_modify(|b| *b = b.saturating_add(p.bytes))
                .or_insert(p.bytes);
        }
        let planned_reduce_tasks = reduce_ids.len().max(1) as u32;
        let adaptive_reduce_tasks = adaptive_reduce_task_count(
            bytes,
            planned_reduce_tasks,
            self.config.adaptive_shuffle_target_bytes,
            self.config.adaptive_shuffle_min_reduce_tasks,
            self.config.adaptive_shuffle_max_reduce_tasks,
        );
        let query = self
            .queries
            .get_mut(&query_id)
            .ok_or_else(|| FfqError::Planning(format!("unknown query: {query_id}")))?;
        let elapsed_ms = query_elapsed_ms(query, now);
        let histogram = build_partition_bytes_histogram(&bytes_by_partition);
        let event = format!(
            "map_stage_observed bytes={} partitions={} planned={} adaptive_estimate={} target_bytes={}",
            bytes,
            reduce_ids.len(),
            planned_reduce_tasks,
            adaptive_reduce_tasks,
            self.config.adaptive_shuffle_target_bytes
        );
        let child_stage_ids = {
            let stage = query
                .stages
                .get_mut(&stage_id)
                .ok_or_else(|| FfqError::Planning(format!("unknown stage: {stage_id}")))?;
            stage.metrics.map_output_rows = rows;
            stage.metrics.map_output_bytes = bytes;
            stage.metrics.map_output_batches = batches;
            stage.metrics.map_output_partitions = reduce_ids.len() as u64;
            stage.metrics.planned_reduce_tasks = planned_reduce_tasks;
            stage.metrics.adaptive_reduce_tasks = adaptive_reduce_tasks;
            stage.metrics.adaptive_target_bytes = self.config.adaptive_shuffle_target_bytes;
            stage.metrics.partition_bytes_histogram = histogram.clone();
            stage.metrics.stream_active_count = active_stream_count;
            if stage.metrics.first_chunk_ms == 0 && bytes > 0 {
                stage.metrics.first_chunk_ms = elapsed_ms;
            }
            update_stage_stream_lag(&mut stage.metrics, elapsed_ms);
            push_stage_aqe_event(&mut stage.metrics, event.clone());
            stage.children.clone()
        };

        for child_stage_id in child_stage_ids {
            if let Some(child) = query.stages.get_mut(&child_stage_id) {
                child.metrics.planned_reduce_tasks = planned_reduce_tasks;
                child.metrics.adaptive_reduce_tasks = adaptive_reduce_tasks;
                child.metrics.adaptive_target_bytes = self.config.adaptive_shuffle_target_bytes;
                child.metrics.partition_bytes_histogram = histogram.clone();
                child.metrics.stream_active_count = active_stream_count;
                if child.metrics.first_chunk_ms == 0 && bytes > 0 {
                    child.metrics.first_chunk_ms = elapsed_ms;
                }
                update_stage_stream_lag(&mut child.metrics, elapsed_ms);
                push_stage_aqe_event(&mut child.metrics, event.clone());
            }
        }
        Ok(())
    }

    fn latest_map_partitions_for_stage(
        &self,
        query_id: &str,
        stage_id: u64,
    ) -> Vec<&MapOutputPartitionMeta> {
        let mut latest_attempt_by_task = HashMap::<u64, u32>::new();
        for ((qid, sid, map_task, attempt), _) in &self.map_outputs {
            if qid == query_id && *sid == stage_id {
                latest_attempt_by_task
                    .entry(*map_task)
                    .and_modify(|a| *a = (*a).max(*attempt))
                    .or_insert(*attempt);
            }
        }

        let mut out = Vec::new();
        for ((qid, sid, map_task, attempt), parts) in &self.map_outputs {
            if qid == query_id
                && *sid == stage_id
                && latest_attempt_by_task
                    .get(map_task)
                    .is_some_and(|latest| *latest == *attempt)
            {
                out.extend(parts.iter());
            }
        }
        out
    }

    /// Number of registered map-output entries.
    pub fn map_output_registry_size(&self) -> usize {
        self.map_outputs.len()
    }

    /// Return readable partition boundaries for one map task attempt.
    pub fn map_output_readable_boundaries(
        &self,
        query_id: &str,
        stage_id: u64,
        map_task: u64,
        attempt: u32,
    ) -> Result<Vec<MapOutputPartitionMeta>> {
        let key = (query_id.to_string(), stage_id, map_task, attempt);
        let mut parts = self
            .map_outputs
            .get(&key)
            .cloned()
            .ok_or_else(|| FfqError::Planning("map output not registered".to_string()))?;
        parts.sort_by_key(|p| p.reduce_partition);
        Ok(parts)
    }

    /// Store final query result payload (Arrow IPC bytes).
    pub fn register_query_results(&mut self, query_id: String, ipc_payload: Vec<u8>) -> Result<()> {
        if !self.queries.contains_key(&query_id) {
            return Err(FfqError::Planning(format!("unknown query: {query_id}")));
        }
        self.query_results.insert(query_id, ipc_payload);
        Ok(())
    }

    /// Fetch final query result payload.
    pub fn fetch_query_results(&self, query_id: &str) -> Result<Vec<u8>> {
        if !self.queries.contains_key(query_id) {
            return Err(FfqError::Planning(format!("unknown query: {query_id}")));
        }
        self.query_results
            .get(query_id)
            .cloned()
            .ok_or_else(|| FfqError::Execution("query results not ready".to_string()))
    }

    /// Returns whether worker is currently blacklisted.
    pub fn is_worker_blacklisted(&self, worker_id: &str) -> bool {
        self.blacklisted_workers.contains(worker_id)
    }

    /// Read shuffle partition bytes for the requested map attempt and byte range.
    pub fn fetch_shuffle_partition_chunks_range(
        &self,
        query_id: &str,
        stage_id: u64,
        map_task: u64,
        attempt: u32,
        layout_version: u32,
        reduce_partition: u32,
        min_stream_epoch: u32,
        start_offset: u64,
        max_bytes: u64,
    ) -> Result<Vec<ShuffleFetchChunk>> {
        if layout_version != 0 {
            let query = self
                .queries
                .get(query_id)
                .ok_or_else(|| FfqError::Planning(format!("unknown query: {query_id}")))?;
            let key = (stage_id, map_task, attempt);
            let expected = query.tasks.get(&key).ok_or_else(|| {
                FfqError::Planning("task attempt not found for fetch request".to_string())
            })?;
            if expected.layout_version != layout_version {
                return Err(FfqError::Planning(format!(
                    "stale fetch layout version: requested={} expected={}",
                    layout_version, expected.layout_version
                )));
            }
        }
        let key = (query_id.to_string(), stage_id, map_task, attempt);
        let parts = self.map_outputs.get(&key).ok_or_else(|| {
            FfqError::Planning("map output not registered for requested attempt".to_string())
        })?;
        let part_meta = parts
            .iter()
            .find(|p| p.reduce_partition == reduce_partition)
            .cloned()
            .unwrap_or(MapOutputPartitionMeta {
                reduce_partition,
                bytes: 0,
                rows: 0,
                batches: 0,
                stream_epoch: 0,
                committed_offset: 0,
                finalized: false,
            });
        if part_meta.stream_epoch < min_stream_epoch {
            return Err(FfqError::Planning(format!(
                "stale fetch stream epoch: requested>={} available={}",
                min_stream_epoch, part_meta.stream_epoch
            )));
        }

        let query_num = query_id.parse::<u64>().map_err(|e| {
            FfqError::InvalidConfig(format!(
                "query_id must be numeric for shuffle path layout in v1: {e}"
            ))
        })?;
        let reader = ShuffleReader::new(&self.config.shuffle_root);
        let readable_end = part_meta.committed_offset;
        let start = start_offset.min(readable_end);
        if start >= readable_end {
            return Ok(vec![ShuffleFetchChunk {
                payload: Vec::new(),
                start_offset: start,
                end_offset: start,
                watermark_offset: readable_end,
                finalized: part_meta.finalized,
                stream_epoch: part_meta.stream_epoch,
            }]);
        }
        let requested = if max_bytes == 0 {
            readable_end.saturating_sub(start)
        } else {
            max_bytes.min(readable_end.saturating_sub(start))
        };
        let chunks = reader.fetch_partition_chunks_range(
            query_num,
            stage_id,
            map_task,
            attempt,
            reduce_partition,
            start,
            requested,
        )?;
        let out = chunks
            .into_iter()
            .map(|c: FetchedPartitionChunk| ShuffleFetchChunk {
                end_offset: c.start_offset + c.payload.len() as u64,
                payload: c.payload,
                start_offset: c.start_offset,
                watermark_offset: part_meta.committed_offset,
                finalized: part_meta.finalized,
                stream_epoch: part_meta.stream_epoch,
            })
            .collect::<Vec<_>>();
        if out.is_empty() {
            return Ok(vec![ShuffleFetchChunk {
                payload: Vec::new(),
                start_offset: start,
                end_offset: start,
                watermark_offset: readable_end,
                finalized: part_meta.finalized,
                stream_epoch: part_meta.stream_epoch,
            }]);
        }
        Ok(out)
    }
}

fn build_query_runtime(
    query_id: &str,
    dag: StageDag,
    physical_plan_json: &[u8],
) -> Result<QueryRuntime> {
    let submitted_at_ms = now_ms()?;
    let mut stages = HashMap::<u64, StageRuntime>::new();
    let mut tasks = HashMap::<(u64, u64, u32), TaskRuntime>::new();
    let plan: PhysicalPlan = serde_json::from_slice(physical_plan_json)
        .map_err(|e| FfqError::Planning(format!("invalid physical plan json: {e}")))?;
    let mut required_custom_ops = HashSet::new();
    collect_custom_ops(&plan, &mut required_custom_ops);
    let mut required_custom_ops = required_custom_ops.into_iter().collect::<Vec<_>>();
    required_custom_ops.sort();
    let all_scan_locality_hints = collect_scan_locality_hints(&plan);
    let stage_reduce_task_counts = collect_stage_reduce_task_counts(&plan);

    for node in dag.stages {
        let sid = node.id.0 as u64;
        let task_count = stage_reduce_task_counts.get(&sid).copied().unwrap_or(1);
        let is_reduce_stage = stage_reduce_task_counts.contains_key(&sid);
        stages.insert(
            sid,
            StageRuntime {
                parents: node.parents.iter().map(|p| p.0 as u64).collect(),
                children: node.children.iter().map(|c| c.0 as u64).collect(),
                layout_version: 1,
                barrier_state: if is_reduce_stage {
                    StageBarrierState::MapRunning
                } else {
                    StageBarrierState::NotApplicable
                },
                layout_finalize_count: 0,
                metrics: StageMetrics {
                    queued_tasks: task_count,
                    planned_reduce_tasks: task_count,
                    adaptive_reduce_tasks: task_count,
                    ..StageMetrics::default()
                },
                completed_runtime_ms_samples: Vec::new(),
            },
        );
        // v1 simplification: each scheduled task carries the submitted physical plan bytes.
        // Stage boundaries are still respected by coordinator scheduling.
        let fragment = physical_plan_json.to_vec();
        for task_id in 0..task_count {
            let locality_hints = if node.parents.is_empty() {
                all_scan_locality_hints.clone()
            } else {
                Vec::new()
            };
            let assigned_reduce_partitions = if is_reduce_stage {
                vec![task_id]
            } else {
                Vec::new()
            };
            tasks.insert(
                (sid, task_id as u64, 1),
                TaskRuntime {
                    query_id: query_id.to_string(),
                    stage_id: sid,
                    task_id: task_id as u64,
                    attempt: 1,
                    state: TaskState::Queued,
                    assigned_worker: None,
                    ready_at_ms: submitted_at_ms,
                    plan_fragment_json: fragment.clone(),
                    assigned_reduce_partitions,
                    assigned_reduce_split_index: 0,
                    assigned_reduce_split_count: 1,
                    layout_version: 1,
                    layout_fingerprint: 0,
                    required_custom_ops: required_custom_ops.clone(),
                    locality_hints,
                    running_since_ms: None,
                    is_speculative: false,
                    message: String::new(),
                },
            );
        }
    }

    let mut runtime = QueryRuntime {
        state: QueryState::Queued,
        submitted_at_ms,
        started_at_ms: 0,
        finished_at_ms: 0,
        message: String::new(),
        stages,
        tasks,
    };
    initialize_stage_layout_identities(&mut runtime);
    Ok(runtime)
}

fn collect_stage_reduce_task_counts(plan: &PhysicalPlan) -> HashMap<u64, u32> {
    let mut out = HashMap::new();
    let mut next_stage_id = 1_u64;
    collect_stage_reduce_task_counts_visit(plan, 0, &mut next_stage_id, &mut out);
    out
}

fn collect_stage_reduce_task_counts_visit(
    plan: &PhysicalPlan,
    current_stage_id: u64,
    next_stage_id: &mut u64,
    out: &mut HashMap<u64, u32>,
) {
    match plan {
        PhysicalPlan::Exchange(ExchangeExec::ShuffleRead(read)) => {
            let partitions = match &read.partitioning {
                PartitioningSpec::HashKeys { partitions, .. } => (*partitions).max(1) as u32,
                PartitioningSpec::Single => 1,
            };
            out.entry(current_stage_id)
                .and_modify(|v| *v = (*v).max(partitions))
                .or_insert(partitions);
            let upstream = *next_stage_id;
            *next_stage_id += 1;
            collect_stage_reduce_task_counts_visit(&read.input, upstream, next_stage_id, out);
        }
        _ => {
            for child in plan.children() {
                collect_stage_reduce_task_counts_visit(child, current_stage_id, next_stage_id, out);
            }
        }
    }
}

fn advance_stage_barriers_and_finalize_layout(
    query_id: &str,
    query: &mut QueryRuntime,
    map_outputs: &HashMap<(String, u64, u64, u32), Vec<MapOutputPartitionMeta>>,
    target_bytes: u64,
    min_reduce_tasks: u32,
    max_reduce_tasks: u32,
    max_partitions_per_task: u32,
    ready_at_ms: u64,
) {
    let latest_states = latest_task_states(query);
    let mut stages_to_rewire = Vec::new();
    let mut stage_ids = query.stages.keys().copied().collect::<Vec<_>>();
    stage_ids.sort_unstable();
    for stage_id in stage_ids {
        let Some(stage) = query.stages.get_mut(&stage_id) else {
            continue;
        };
        if !matches!(
            stage.barrier_state,
            StageBarrierState::MapRunning | StageBarrierState::MapDone
        ) {
            continue;
        }
        let all_parents_done = stage.parents.iter().all(|pid| {
            latest_states
                .iter()
                .filter(|((stage_id, _), _)| stage_id == pid)
                .all(|(_, state)| *state == TaskState::Succeeded)
        });
        if !all_parents_done {
            stage.barrier_state = StageBarrierState::MapRunning;
            continue;
        }
        if stage.barrier_state == StageBarrierState::MapRunning {
            stage.barrier_state = StageBarrierState::MapDone;
        }
        let stage_tasks_queued = latest_states
            .iter()
            .filter(|((sid, _), _)| *sid == stage_id)
            .all(|(_, state)| *state == TaskState::Queued);
        if !stage_tasks_queued {
            continue;
        }
        let Some(parent_stage_id) = stage.parents.first().copied() else {
            continue;
        };
        let bytes_by_partition =
            latest_partition_bytes_for_stage(query_id, parent_stage_id, map_outputs);
        let adaptive_plan = plan_adaptive_reduce_layout(
            stage.metrics.planned_reduce_tasks.max(1),
            target_bytes,
            &bytes_by_partition,
            min_reduce_tasks,
            max_reduce_tasks,
            max_partitions_per_task,
        );
        let groups = adaptive_plan.assignments;
        let current_tasks = latest_states
            .iter()
            .filter(|((sid, _), _)| *sid == stage_id)
            .count() as u32;
        stages_to_rewire.push((stage_id, groups, current_tasks, adaptive_plan.aqe_events));
    }

    for (stage_id, groups, current_tasks, planner_events) in stages_to_rewire {
        let Some(template) = query
            .tasks
            .values()
            .find(|t| t.stage_id == stage_id && t.state == TaskState::Queued)
            .map(|t| {
                (
                    t.plan_fragment_json.clone(),
                    t.required_custom_ops.clone(),
                    t.locality_hints.clone(),
                    t.query_id.clone(),
                )
            })
        else {
            continue;
        };
        let layout_version = query
            .stages
            .get(&stage_id)
            .map(|s| s.layout_version.saturating_add(1))
            .unwrap_or(1);
        let layout_fingerprint = compute_layout_fingerprint_from_specs(stage_id, &groups);
        if (groups.len() as u32) != current_tasks {
            query.tasks.retain(|(sid, _, _), _| *sid != stage_id);
            for (task_id, assignment) in groups.into_iter().enumerate() {
                query.tasks.insert(
                    (stage_id, task_id as u64, 1),
                    TaskRuntime {
                        query_id: template.3.clone(),
                        stage_id,
                        task_id: task_id as u64,
                        attempt: 1,
                        state: TaskState::Queued,
                        assigned_worker: None,
                        ready_at_ms,
                        plan_fragment_json: template.0.clone(),
                        assigned_reduce_partitions: assignment.assigned_reduce_partitions,
                        assigned_reduce_split_index: assignment.assigned_reduce_split_index,
                        assigned_reduce_split_count: assignment.assigned_reduce_split_count,
                        layout_version,
                        layout_fingerprint,
                        required_custom_ops: template.1.clone(),
                        locality_hints: template.2.clone(),
                        running_since_ms: None,
                        is_speculative: false,
                        message: String::new(),
                    },
                );
            }
        } else {
            for task in query.tasks.values_mut().filter(|t| t.stage_id == stage_id) {
                task.layout_version = layout_version;
                task.layout_fingerprint = layout_fingerprint;
            }
        }
        if let Some(stage) = query.stages.get_mut(&stage_id) {
            stage.layout_version = layout_version;
            stage.barrier_state = StageBarrierState::LayoutFinalized;
            stage.layout_finalize_count = stage.layout_finalize_count.saturating_add(1);
            for event in planner_events {
                push_stage_aqe_event(&mut stage.metrics, event);
            }
            stage.metrics.queued_tasks = query
                .tasks
                .values()
                .filter(|t| t.stage_id == stage_id && t.state == TaskState::Queued)
                .count() as u32;
            stage.metrics.adaptive_reduce_tasks = stage.metrics.queued_tasks;
            stage.metrics.layout_finalize_count = stage.layout_finalize_count;
            stage.metrics.skew_split_tasks = query
                .tasks
                .values()
                .filter(|t| t.stage_id == stage_id && t.assigned_reduce_split_count > 1)
                .count() as u32;
            let planned = stage.metrics.planned_reduce_tasks;
            let adaptive = stage.metrics.adaptive_reduce_tasks;
            let skew_splits = stage.metrics.skew_split_tasks;
            let version = stage.layout_version;
            let reason = if adaptive > planned {
                "split"
            } else if adaptive < planned {
                "coalesce"
            } else {
                "unchanged"
            };
            push_stage_aqe_event(
                &mut stage.metrics,
                format!(
                    "layout_finalized version={} planned={} adaptive={} reason={} skew_splits={}",
                    version, planned, adaptive, reason, skew_splits
                ),
            );
            stage.barrier_state = StageBarrierState::ReduceSchedulable;
        }
    }
}

fn latest_partition_bytes_for_stage(
    query_id: &str,
    stage_id: u64,
    map_outputs: &HashMap<(String, u64, u64, u32), Vec<MapOutputPartitionMeta>>,
) -> HashMap<u32, u64> {
    let mut latest_attempt_by_task = HashMap::<u64, u32>::new();
    for ((qid, sid, map_task, attempt), _) in map_outputs {
        if qid == query_id && *sid == stage_id {
            latest_attempt_by_task
                .entry(*map_task)
                .and_modify(|a| *a = (*a).max(*attempt))
                .or_insert(*attempt);
        }
    }

    let mut out = HashMap::<u32, u64>::new();
    for ((qid, sid, map_task, attempt), partitions) in map_outputs {
        if qid == query_id
            && *sid == stage_id
            && latest_attempt_by_task
                .get(map_task)
                .is_some_and(|latest| *latest == *attempt)
        {
            for p in partitions {
                out.entry(p.reduce_partition)
                    .and_modify(|b| *b = b.saturating_add(p.bytes))
                    .or_insert(p.bytes);
            }
        }
    }
    out
}

fn build_partition_bytes_histogram(
    bytes_by_partition: &HashMap<u32, u64>,
) -> Vec<PartitionBytesHistogramBucket> {
    ffq_common::adaptive::build_partition_bytes_histogram(bytes_by_partition)
}

fn push_stage_aqe_event(metrics: &mut StageMetrics, event: String) {
    if metrics.aqe_events.iter().any(|e| e == &event) {
        return;
    }
    metrics.aqe_events.push(event);
    if metrics.aqe_events.len() > 16 {
        let keep_from = metrics.aqe_events.len().saturating_sub(16);
        metrics.aqe_events.drain(0..keep_from);
    }
}

fn push_stage_backpressure_event(metrics: &mut StageMetrics, event: String) {
    if metrics.backpressure_events.iter().any(|e| e == &event) {
        return;
    }
    metrics.backpressure_events.push(event);
    if metrics.backpressure_events.len() > 16 {
        let keep_from = metrics.backpressure_events.len().saturating_sub(16);
        metrics.backpressure_events.drain(0..keep_from);
    }
}

fn query_elapsed_ms(query: &QueryRuntime, now_ms: u64) -> u64 {
    let base = if query.started_at_ms > 0 {
        query.started_at_ms
    } else {
        query.submitted_at_ms
    };
    now_ms.saturating_sub(base)
}

fn update_stage_stream_lag(metrics: &mut StageMetrics, elapsed_ms: u64) {
    if metrics.first_chunk_ms == 0 {
        metrics.stream_lag_ms = 0;
        return;
    }
    let progress_ms = if metrics.first_reduce_row_ms > 0 {
        metrics.first_reduce_row_ms
    } else {
        elapsed_ms
    };
    metrics.stream_lag_ms = progress_ms.saturating_sub(metrics.first_chunk_ms);
}

type ReduceTaskAssignmentSpec = ReduceTaskAssignment;

fn deterministic_coalesce_split_groups(
    planned_partitions: u32,
    target_bytes: u64,
    bytes_by_partition: &HashMap<u32, u64>,
    min_reduce_tasks: u32,
    max_reduce_tasks: u32,
    max_partitions_per_task: u32,
) -> Vec<ReduceTaskAssignmentSpec> {
    plan_adaptive_reduce_layout(
        planned_partitions,
        target_bytes,
        bytes_by_partition,
        min_reduce_tasks,
        max_reduce_tasks,
        max_partitions_per_task,
    )
    .assignments
}

fn collect_custom_ops(plan: &PhysicalPlan, out: &mut HashSet<String>) {
    match plan {
        PhysicalPlan::ParquetScan(_) | PhysicalPlan::VectorTopK(_) => {}
        PhysicalPlan::ParquetWrite(x) => collect_custom_ops(&x.input, out),
        PhysicalPlan::Filter(x) => collect_custom_ops(&x.input, out),
        PhysicalPlan::InSubqueryFilter(x) => {
            collect_custom_ops(&x.input, out);
            collect_custom_ops(&x.subquery, out);
        }
        PhysicalPlan::ExistsSubqueryFilter(x) => {
            collect_custom_ops(&x.input, out);
            collect_custom_ops(&x.subquery, out);
        }
        PhysicalPlan::ScalarSubqueryFilter(x) => {
            collect_custom_ops(&x.input, out);
            collect_custom_ops(&x.subquery, out);
        }
        PhysicalPlan::Project(x) => collect_custom_ops(&x.input, out),
        PhysicalPlan::Window(x) => collect_custom_ops(&x.input, out),
        PhysicalPlan::CoalesceBatches(x) => collect_custom_ops(&x.input, out),
        PhysicalPlan::PartialHashAggregate(x) => collect_custom_ops(&x.input, out),
        PhysicalPlan::FinalHashAggregate(x) => collect_custom_ops(&x.input, out),
        PhysicalPlan::HashJoin(x) => {
            collect_custom_ops(&x.left, out);
            collect_custom_ops(&x.right, out);
            for alt in &x.alternatives {
                collect_custom_ops(&alt.left, out);
                collect_custom_ops(&alt.right, out);
            }
        }
        PhysicalPlan::Exchange(x) => match x {
            ExchangeExec::ShuffleWrite(e) => collect_custom_ops(&e.input, out),
            ExchangeExec::ShuffleRead(e) => collect_custom_ops(&e.input, out),
            ExchangeExec::Broadcast(e) => collect_custom_ops(&e.input, out),
        },
        PhysicalPlan::Limit(x) => collect_custom_ops(&x.input, out),
        PhysicalPlan::TopKByScore(x) => collect_custom_ops(&x.input, out),
        PhysicalPlan::UnionAll(x) => {
            collect_custom_ops(&x.left, out);
            collect_custom_ops(&x.right, out);
        }
        PhysicalPlan::CteRef(x) => collect_custom_ops(&x.plan, out),
        PhysicalPlan::Custom(x) => {
            out.insert(x.op_name.clone());
            collect_custom_ops(&x.input, out);
        }
    }
}

fn collect_scan_locality_hints(plan: &PhysicalPlan) -> Vec<String> {
    fn visit(plan: &PhysicalPlan, out: &mut HashSet<String>) {
        match plan {
            PhysicalPlan::ParquetScan(scan) => {
                out.insert(format!("table:{}", scan.table));
            }
            PhysicalPlan::ParquetWrite(x) => visit(&x.input, out),
            PhysicalPlan::Filter(x) => visit(&x.input, out),
            PhysicalPlan::InSubqueryFilter(x) => {
                visit(&x.input, out);
                visit(&x.subquery, out);
            }
            PhysicalPlan::ExistsSubqueryFilter(x) => {
                visit(&x.input, out);
                visit(&x.subquery, out);
            }
            PhysicalPlan::ScalarSubqueryFilter(x) => {
                visit(&x.input, out);
                visit(&x.subquery, out);
            }
            PhysicalPlan::Project(x) => visit(&x.input, out),
            PhysicalPlan::Window(x) => visit(&x.input, out),
            PhysicalPlan::CoalesceBatches(x) => visit(&x.input, out),
            PhysicalPlan::PartialHashAggregate(x) => visit(&x.input, out),
            PhysicalPlan::FinalHashAggregate(x) => visit(&x.input, out),
            PhysicalPlan::HashJoin(x) => {
                visit(&x.left, out);
                visit(&x.right, out);
                for alt in &x.alternatives {
                    visit(&alt.left, out);
                    visit(&alt.right, out);
                }
            }
            PhysicalPlan::Exchange(x) => match x {
                ExchangeExec::ShuffleWrite(e) => visit(&e.input, out),
                ExchangeExec::ShuffleRead(e) => visit(&e.input, out),
                ExchangeExec::Broadcast(e) => visit(&e.input, out),
            },
            PhysicalPlan::Limit(x) => visit(&x.input, out),
            PhysicalPlan::TopKByScore(x) => visit(&x.input, out),
            PhysicalPlan::UnionAll(x) => {
                visit(&x.left, out);
                visit(&x.right, out);
            }
            PhysicalPlan::CteRef(x) => visit(&x.plan, out),
            PhysicalPlan::VectorTopK(_) => {}
            PhysicalPlan::Custom(x) => visit(&x.input, out),
        }
    }
    let mut hints = HashSet::new();
    visit(plan, &mut hints);
    let mut out = hints.into_iter().collect::<Vec<_>>();
    out.sort();
    out
}

fn parse_locality_tags(caps: &[String]) -> HashSet<String> {
    caps.iter()
        .filter_map(|c| c.strip_prefix("locality:").map(|s| s.to_string()))
        .collect()
}

fn worker_matches_locality(worker: Option<&WorkerHeartbeat>, locality_hints: &[String]) -> bool {
    if locality_hints.is_empty() {
        return true;
    }
    let Some(worker) = worker else {
        return false;
    };
    locality_hints.iter().any(|hint| worker.locality_tags.contains(hint))
}

fn has_any_live_worker_for_locality(
    heartbeats: &HashMap<String, WorkerHeartbeat>,
    blacklisted_workers: &HashSet<String>,
    now_ms: u64,
    liveness_timeout_ms: u64,
    locality_hints: &[String],
) -> bool {
    heartbeats.iter().any(|(worker, hb)| {
        if blacklisted_workers.contains(worker) {
            return false;
        }
        if liveness_timeout_ms > 0 && now_ms.saturating_sub(hb.last_seen_ms) > liveness_timeout_ms {
            return false;
        }
        locality_hints.iter().any(|hint| hb.locality_tags.contains(hint))
    })
}

fn stage_p95_runtime_ms(samples: &[u64]) -> Option<u64> {
    if samples.is_empty() {
        return None;
    }
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let idx = ((sorted.len().saturating_sub(1) as f64) * 0.95).round() as usize;
    sorted.get(idx).copied()
}

fn enqueue_speculative_attempts(
    query_id: &str,
    query: &mut QueryRuntime,
    now_ms: u64,
    min_completed_samples: u32,
    p95_multiplier: f64,
    min_runtime_ms: u64,
    max_task_attempts: u32,
) {
    let latest_attempts = latest_attempt_map(query);
    let mut launches = Vec::new();
    for task in query.tasks.values() {
        if task.state != TaskState::Running {
            continue;
        }
        if latest_attempts
            .get(&(task.stage_id, task.task_id))
            .is_some_and(|a| *a != task.attempt)
        {
            continue;
        }
        if task.attempt >= max_task_attempts {
            continue;
        }
        let Some(start_ms) = task.running_since_ms else {
            continue;
        };
        let observed_runtime = now_ms.saturating_sub(start_ms);
        let Some(stage_rt) = query.stages.get(&task.stage_id) else {
            continue;
        };
        if stage_rt.completed_runtime_ms_samples.len() < min_completed_samples as usize {
            continue;
        }
        let Some(p95_ms) = stage_p95_runtime_ms(&stage_rt.completed_runtime_ms_samples) else {
            continue;
        };
        let threshold = ((p95_ms as f64) * p95_multiplier.max(1.0))
            .round()
            .max(min_runtime_ms as f64) as u64;
        if observed_runtime < threshold {
            continue;
        }
        launches.push((
            task.stage_id,
            task.task_id,
            task.attempt,
            task.plan_fragment_json.clone(),
            task.assigned_reduce_partitions.clone(),
            task.assigned_reduce_split_index,
            task.assigned_reduce_split_count,
            task.layout_version,
            task.layout_fingerprint,
            task.required_custom_ops.clone(),
            task.locality_hints.clone(),
            threshold,
            observed_runtime,
        ));
    }

    for (
        stage_id,
        task_id,
        attempt,
        plan_fragment_json,
        assigned_reduce_partitions,
        assigned_reduce_split_index,
        assigned_reduce_split_count,
        layout_version,
        layout_fingerprint,
        required_custom_ops,
        locality_hints,
        threshold,
        observed_runtime,
    ) in launches
    {
        let next_attempt = attempt.saturating_add(1);
        let key = (stage_id, task_id, next_attempt);
        if query.tasks.contains_key(&key) {
            continue;
        }
        query.tasks.insert(
            key,
            TaskRuntime {
                query_id: query_id.to_string(),
                stage_id,
                task_id,
                attempt: next_attempt,
                state: TaskState::Queued,
                assigned_worker: None,
                ready_at_ms: now_ms,
                plan_fragment_json,
                assigned_reduce_partitions,
                assigned_reduce_split_index,
                assigned_reduce_split_count,
                layout_version,
                layout_fingerprint,
                required_custom_ops,
                locality_hints,
                running_since_ms: None,
                is_speculative: true,
                message: format!(
                    "speculative attempt scheduled (runtime_ms={} threshold_ms={})",
                    observed_runtime, threshold
                ),
            },
        );
        if let Some(stage) = query.stages.get_mut(&stage_id) {
            stage.metrics.queued_tasks = stage.metrics.queued_tasks.saturating_add(1);
            stage.metrics.speculative_attempts_launched =
                stage.metrics.speculative_attempts_launched.saturating_add(1);
            push_stage_aqe_event(
                &mut stage.metrics,
                format!(
                    "speculative_launch stage={} task={} old_attempt={} new_attempt={} runtime_ms={} threshold_ms={}",
                    stage_id, task_id, attempt, next_attempt, observed_runtime, threshold
                ),
            );
        }
    }
}

fn adopt_older_attempt_success_from_speculation(
    query: &mut QueryRuntime,
    stage_id: u64,
    task_id: u64,
    attempt: u32,
    latest_attempt: u32,
) -> bool {
    if latest_attempt <= attempt {
        return false;
    }
    let newer_attempts = query
        .tasks
        .values()
        .filter(|t| t.stage_id == stage_id && t.task_id == task_id && t.attempt > attempt)
        .cloned()
        .collect::<Vec<_>>();
    if newer_attempts.is_empty() {
        return false;
    }
    if newer_attempts.iter().any(|t| t.state == TaskState::Succeeded) {
        return false;
    }
    if !newer_attempts.iter().any(|t| t.is_speculative) {
        return false;
    }

    let keys_to_remove = newer_attempts
        .iter()
        .map(|t| (t.stage_id, t.task_id, t.attempt))
        .collect::<Vec<_>>();
    let mut removed_queued = 0_u32;
    let mut removed_running = 0_u32;
    for key in keys_to_remove {
        if let Some(removed) = query.tasks.remove(&key) {
            match removed.state {
                TaskState::Queued => removed_queued = removed_queued.saturating_add(1),
                TaskState::Running => removed_running = removed_running.saturating_add(1),
                TaskState::Succeeded | TaskState::Failed => {}
            }
        }
    }
    if let Some(stage) = query.stages.get_mut(&stage_id) {
        stage.metrics.queued_tasks = stage.metrics.queued_tasks.saturating_sub(removed_queued);
        stage.metrics.running_tasks = stage.metrics.running_tasks.saturating_sub(removed_running);
        stage.metrics.failed_tasks = stage
            .metrics
            .failed_tasks
            .saturating_add(removed_queued.saturating_add(removed_running));
        stage.metrics.speculative_older_attempt_wins =
            stage.metrics.speculative_older_attempt_wins.saturating_add(1);
    }
    true
}

fn worker_supports_task(caps: Option<&HashSet<String>>, required_custom_ops: &[String]) -> bool {
    if required_custom_ops.is_empty() {
        return true;
    }
    let Some(caps) = caps else {
        return false;
    };
    required_custom_ops.iter().all(|op| caps.contains(op))
}

fn runnable_stages_with_pipeline(
    query_id: &str,
    query: &QueryRuntime,
    latest_states: &HashMap<(u64, u64), TaskState>,
    map_outputs: &HashMap<(String, u64, u64, u32), Vec<MapOutputPartitionMeta>>,
    pipelined_shuffle_enabled: bool,
    min_completion_ratio: f64,
    min_committed_offset_bytes: u64,
) -> Vec<u64> {
    let mut out = Vec::new();
    let min_ratio = min_completion_ratio.clamp(0.0, 1.0);
    for (sid, stage) in &query.stages {
        let parents_done = all_parents_done_for_stage(query, *sid, latest_states);
        if parents_done {
            out.push(*sid);
            continue;
        }
        if !pipelined_shuffle_enabled || stage.parents.is_empty() {
            continue;
        }
        let parent_ready = stage.parents.iter().all(|pid| {
            let ratio = stage_completion_ratio(*pid, latest_states);
            ratio >= min_ratio && has_any_map_output_for_stage(query_id, *pid, map_outputs)
        });
        if !parent_ready {
            continue;
        }
        let ready = ready_reduce_partitions_for_stage(
            query_id,
            query,
            *sid,
            map_outputs,
            min_committed_offset_bytes,
        );
        if !ready.is_empty() {
            out.push(*sid);
        }
    }
    out
}

fn stage_completion_ratio(stage_id: u64, latest_states: &HashMap<(u64, u64), TaskState>) -> f64 {
    let mut total = 0_u64;
    let mut succeeded = 0_u64;
    for ((sid, _), state) in latest_states {
        if *sid != stage_id {
            continue;
        }
        total += 1;
        if *state == TaskState::Succeeded {
            succeeded += 1;
        }
    }
    if total == 0 {
        0.0
    } else {
        succeeded as f64 / total as f64
    }
}

fn all_parents_done_for_stage(
    query: &QueryRuntime,
    stage_id: u64,
    latest_states: &HashMap<(u64, u64), TaskState>,
) -> bool {
    let Some(stage) = query.stages.get(&stage_id) else {
        return false;
    };
    stage.parents.iter().all(|pid| {
        latest_states
            .iter()
            .filter(|((sid, _), _)| sid == pid)
            .all(|(_, state)| *state == TaskState::Succeeded)
    })
}

fn has_any_map_output_for_stage(
    query_id: &str,
    stage_id: u64,
    map_outputs: &HashMap<(String, u64, u64, u32), Vec<MapOutputPartitionMeta>>,
) -> bool {
    map_outputs
        .iter()
        .any(|((qid, sid, _, _), parts)| qid == query_id && *sid == stage_id && !parts.is_empty())
}

fn ready_reduce_partitions_for_stage(
    query_id: &str,
    query: &QueryRuntime,
    reduce_stage_id: u64,
    map_outputs: &HashMap<(String, u64, u64, u32), Vec<MapOutputPartitionMeta>>,
    min_committed_offset_bytes: u64,
) -> HashSet<u32> {
    let Some(stage) = query.stages.get(&reduce_stage_id) else {
        return HashSet::new();
    };
    let Some(parent_stage_id) = stage.parents.first().copied() else {
        return HashSet::new();
    };
    let latest = latest_partition_stream_progress_for_stage(query_id, parent_stage_id, map_outputs);
    let mut out = HashSet::new();
    for (partition, (_, committed_offset, finalized)) in latest {
        if finalized || committed_offset >= min_committed_offset_bytes {
            out.insert(partition);
        }
    }
    out
}

fn latest_partition_stream_progress_for_stage(
    query_id: &str,
    stage_id: u64,
    map_outputs: &HashMap<(String, u64, u64, u32), Vec<MapOutputPartitionMeta>>,
) -> HashMap<u32, (u32, u64, bool)> {
    let mut latest_attempt_by_task = HashMap::<u64, u32>::new();
    for ((qid, sid, map_task, attempt), _) in map_outputs {
        if qid == query_id && *sid == stage_id {
            latest_attempt_by_task
                .entry(*map_task)
                .and_modify(|a| *a = (*a).max(*attempt))
                .or_insert(*attempt);
        }
    }

    let mut out = HashMap::<u32, (u32, u64, bool)>::new();
    for ((qid, sid, map_task, attempt), partitions) in map_outputs {
        if qid != query_id || *sid != stage_id {
            continue;
        }
        if !latest_attempt_by_task
            .get(map_task)
            .is_some_and(|latest| *latest == *attempt)
        {
            continue;
        }
        for p in partitions {
            out.entry(p.reduce_partition)
                .and_modify(|cur| {
                    if p.stream_epoch > cur.0 {
                        *cur = (p.stream_epoch, p.committed_offset, p.finalized);
                    } else if p.stream_epoch == cur.0 {
                        cur.1 = cur.1.max(p.committed_offset);
                        cur.2 = cur.2 || p.finalized;
                    }
                })
                .or_insert((p.stream_epoch, p.committed_offset, p.finalized));
        }
    }
    out
}

fn merge_map_output_partitions(
    existing: &mut Vec<MapOutputPartitionMeta>,
    incoming: &[MapOutputPartitionMeta],
) {
    let mut by_partition = existing
        .iter()
        .cloned()
        .map(|p| (p.reduce_partition, p))
        .collect::<HashMap<_, _>>();
    for p in incoming {
        by_partition
            .entry(p.reduce_partition)
            .and_modify(|cur| {
                if p.stream_epoch > cur.stream_epoch {
                    *cur = p.clone();
                } else if p.stream_epoch == cur.stream_epoch {
                    cur.bytes = cur.bytes.max(p.bytes);
                    cur.rows = cur.rows.max(p.rows);
                    cur.batches = cur.batches.max(p.batches);
                    cur.committed_offset = cur.committed_offset.max(p.committed_offset);
                    cur.finalized = cur.finalized || p.finalized;
                }
            })
            .or_insert_with(|| p.clone());
    }
    let mut merged = by_partition.into_values().collect::<Vec<_>>();
    merged.sort_by_key(|p| p.reduce_partition);
    *existing = merged;
}

fn aggregate_reduce_backpressure(
    samples: &HashMap<(String, u64, u64, u32), ReduceBackpressureSample>,
    query_id: &str,
) -> (u64, u32) {
    samples
        .iter()
        .filter(|((qid, _, _, _), _)| qid == query_id)
        .fold((0_u64, 0_u32), |acc, (_, s)| {
            (
                acc.0.saturating_add(s.inflight_bytes),
                acc.1.saturating_add(s.queue_depth),
            )
        })
}

fn recommended_backpressure_windows(
    inflight_bytes: u64,
    queue_depth: u32,
    target_inflight_bytes: u64,
    target_queue_depth: u32,
    max_map_window: u32,
    max_reduce_window: u32,
) -> (u32, u32) {
    let max_map = max_map_window.max(1);
    let max_reduce = max_reduce_window.max(1);
    let bytes_ratio = if target_inflight_bytes == 0 {
        1.0
    } else {
        inflight_bytes as f64 / target_inflight_bytes as f64
    };
    let queue_ratio = if target_queue_depth == 0 {
        1.0
    } else {
        queue_depth as f64 / target_queue_depth as f64
    };
    let pressure = bytes_ratio.max(queue_ratio).max(1.0);
    let divisor = pressure.ceil() as u32;
    ((max_map / divisor).max(1), (max_reduce / divisor).max(1))
}

fn is_query_succeeded(query: &QueryRuntime) -> bool {
    latest_task_states(query)
        .values()
        .all(|s| *s == TaskState::Succeeded)
}

fn latest_task_states(query: &QueryRuntime) -> HashMap<(u64, u64), TaskState> {
    let mut out = HashMap::<(u64, u64), (u32, TaskState)>::new();
    for t in query.tasks.values() {
        let key = (t.stage_id, t.task_id);
        match out.get(&key) {
            Some((existing_attempt, _)) if *existing_attempt >= t.attempt => {}
            _ => {
                out.insert(key, (t.attempt, t.state));
            }
        }
    }
    out.into_iter().map(|(k, (_, s))| (k, s)).collect()
}

fn initialize_stage_layout_identities(query: &mut QueryRuntime) {
    let stage_ids = query.stages.keys().copied().collect::<Vec<_>>();
    for stage_id in stage_ids {
        let layout_fingerprint = compute_layout_fingerprint_from_tasks(query, stage_id, 1);
        if let Some(stage) = query.stages.get_mut(&stage_id) {
            stage.layout_version = 1;
        }
        for task in query
            .tasks
            .values_mut()
            .filter(|t| t.stage_id == stage_id && t.attempt == 1)
        {
            task.layout_version = 1;
            task.layout_fingerprint = layout_fingerprint;
        }
    }
}

fn compute_layout_fingerprint_from_tasks(query: &QueryRuntime, stage_id: u64, attempt: u32) -> u64 {
    let mut assignments = query
        .tasks
        .values()
        .filter(|t| t.stage_id == stage_id && t.attempt == attempt)
        .map(|t| {
            (
                t.task_id,
                t.assigned_reduce_partitions.clone(),
                t.assigned_reduce_split_index,
                t.assigned_reduce_split_count,
            )
        })
        .collect::<Vec<_>>();
    assignments.sort_by_key(|(task_id, _, _, _)| *task_id);
    compute_layout_fingerprint(stage_id, &assignments)
}

fn compute_layout_fingerprint_from_specs(stage_id: u64, specs: &[ReduceTaskAssignmentSpec]) -> u64 {
    let assignments = specs
        .iter()
        .enumerate()
        .map(|(task_id, s)| {
            (
                task_id as u64,
                s.assigned_reduce_partitions.clone(),
                s.assigned_reduce_split_index,
                s.assigned_reduce_split_count,
            )
        })
        .collect::<Vec<_>>();
    compute_layout_fingerprint(stage_id, &assignments)
}

fn compute_layout_fingerprint(stage_id: u64, assignments: &[(u64, Vec<u32>, u32, u32)]) -> u64 {
    let mut h = 1469598103934665603_u64;
    fn mix(h: &mut u64, v: u64) {
        *h ^= v;
        *h = h.wrapping_mul(1099511628211_u64);
    }
    mix(&mut h, stage_id);
    for (task_id, partitions, split_idx, split_count) in assignments {
        mix(&mut h, *task_id);
        mix(&mut h, partitions.len() as u64);
        for p in partitions {
            mix(&mut h, *p as u64);
        }
        mix(&mut h, *split_idx as u64);
        mix(&mut h, *split_count as u64);
    }
    h
}

fn latest_attempt_map(query: &QueryRuntime) -> HashMap<(u64, u64), u32> {
    let mut out = HashMap::<(u64, u64), u32>::new();
    for t in query.tasks.values() {
        out.entry((t.stage_id, t.task_id))
            .and_modify(|a| {
                if *a < t.attempt {
                    *a = t.attempt;
                }
            })
            .or_insert(t.attempt);
    }
    out
}

fn running_tasks_for_query_latest(query: &QueryRuntime) -> u32 {
    latest_task_states(query)
        .values()
        .filter(|s| **s == TaskState::Running)
        .count() as u32
}

fn build_query_status(query_id: &str, q: &QueryRuntime) -> QueryStatus {
    let total = q.tasks.len() as u32;
    let mut queued = 0_u32;
    let mut running = 0_u32;
    let mut succeeded = 0_u32;
    let mut failed = 0_u32;

    for t in q.tasks.values() {
        match t.state {
            TaskState::Queued => queued += 1,
            TaskState::Running => running += 1,
            TaskState::Succeeded => succeeded += 1,
            TaskState::Failed => failed += 1,
        }
    }

    let stage_metrics = q
        .stages
        .iter()
        .map(|(sid, s)| (*sid, s.metrics.clone()))
        .collect::<HashMap<_, _>>();

    QueryStatus {
        query_id: query_id.to_string(),
        state: q.state,
        submitted_at_ms: q.submitted_at_ms,
        started_at_ms: q.started_at_ms,
        finished_at_ms: q.finished_at_ms,
        message: q.message.clone(),
        total_tasks: total,
        queued_tasks: queued,
        running_tasks: running,
        succeeded_tasks: succeeded,
        failed_tasks: failed,
        stage_metrics,
    }
}

fn update_scheduler_metrics(query_id: &str, stage_id: u64, m: &StageMetrics) {
    global_metrics().set_scheduler_queued_tasks(query_id, stage_id, m.queued_tasks as u64);
    global_metrics().set_scheduler_running_tasks(query_id, stage_id, m.running_tasks as u64);
}

fn adaptive_reduce_task_count(
    total_bytes: u64,
    planned_tasks: u32,
    target_bytes: u64,
    min_reduce_tasks: u32,
    max_reduce_tasks: u32,
) -> u32 {
    if planned_tasks == 0 {
        return 1;
    }
    let min_eff = min_reduce_tasks.max(1).min(planned_tasks);
    let max_eff = if max_reduce_tasks == 0 {
        planned_tasks
    } else {
        max_reduce_tasks
    }
    .max(min_eff)
    .min(planned_tasks);
    if target_bytes == 0 {
        return planned_tasks.clamp(min_eff, max_eff);
    }
    let needed = ((total_bytes.saturating_add(target_bytes - 1)) / target_bytes)
        .max(1)
        .min(planned_tasks as u64);
    (needed as u32).clamp(min_eff, max_eff)
}

fn now_ms() -> Result<u64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| FfqError::Execution(format!("clock error: {e}")))?
        .as_millis() as u64)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::thread;
    use std::time::Duration;

    use super::*;
    use arrow_schema::Schema;
    use ffq_planner::{
        ExchangeExec, ParquetScanExec, PartitioningSpec, PhysicalPlan, ShuffleReadExchange,
        ShuffleWriteExchange,
    };

    fn hash_shuffle_plan(partitions: usize) -> PhysicalPlan {
        PhysicalPlan::Exchange(ExchangeExec::ShuffleRead(ShuffleReadExchange {
            input: Box::new(PhysicalPlan::Exchange(ExchangeExec::ShuffleWrite(
                ShuffleWriteExchange {
                    input: Box::new(PhysicalPlan::ParquetScan(ParquetScanExec {
                        table: "t".to_string(),
                        schema: Some(Schema::empty()),
                        projection: None,
                        filters: vec![],
                    })),
                    partitioning: PartitioningSpec::HashKeys {
                        keys: vec!["k".to_string()],
                        partitions,
                    },
                },
            ))),
            partitioning: PartitioningSpec::HashKeys {
                keys: vec!["k".to_string()],
                partitions,
            },
        }))
    }

    fn single_scan_plan(table: &str) -> PhysicalPlan {
        PhysicalPlan::ParquetScan(ParquetScanExec {
            table: table.to_string(),
            schema: Some(Schema::empty()),
            projection: None,
            filters: vec![],
        })
    }

    #[test]
    fn coordinator_schedules_and_tracks_query_state() {
        let mut c = Coordinator::new(CoordinatorConfig::default());
        let plan = serde_json::to_vec(&PhysicalPlan::ParquetScan(ParquetScanExec {
            table: "t".to_string(),
            schema: Some(Schema::empty()),
            projection: None,
            filters: vec![],
        }))
        .expect("plan");
        c.submit_query("q1".to_string(), &plan).expect("submit");

        let assignments = c.get_task("w1", 10).expect("get task");
        assert_eq!(assignments.len(), 1);

        let a = &assignments[0];
        c.report_task_status(
            &a.query_id,
            a.stage_id,
            a.task_id,
            a.attempt,
            a.layout_version,
            a.layout_fingerprint,
            TaskState::Succeeded,
            Some("w1"),
            String::new(),
        )
        .expect("report");

        let st = c.get_query_status("q1").expect("status");
        assert_eq!(st.state, QueryState::Succeeded);
        assert_eq!(st.succeeded_tasks, st.total_tasks);
    }

    #[test]
    fn coordinator_blacklists_failing_worker() {
        let mut c = Coordinator::new(CoordinatorConfig {
            blacklist_failure_threshold: 2,
            shuffle_root: PathBuf::from("."),
            ..CoordinatorConfig::default()
        });
        let plan = serde_json::to_vec(&PhysicalPlan::ParquetScan(ParquetScanExec {
            table: "t".to_string(),
            schema: Some(Schema::empty()),
            projection: None,
            filters: vec![],
        }))
        .expect("plan");
        c.submit_query("q2".to_string(), &plan).expect("submit");
        let a = c.get_task("wbad", 1).expect("task").remove(0);

        c.report_task_status(
            &a.query_id,
            a.stage_id,
            a.task_id,
            a.attempt,
            a.layout_version,
            a.layout_fingerprint,
            TaskState::Failed,
            Some("wbad"),
            "boom".to_string(),
        )
        .expect("fail1");
        c.submit_query("q3".to_string(), &plan).expect("submit2");
        let a2 = c.get_task("wbad", 1).expect("task2").remove(0);
        c.report_task_status(
            &a2.query_id,
            a2.stage_id,
            a2.task_id,
            a2.attempt,
            a2.layout_version,
            a2.layout_fingerprint,
            TaskState::Failed,
            Some("wbad"),
            "boom".to_string(),
        )
        .expect("fail2");

        assert!(c.is_worker_blacklisted("wbad"));
        assert!(c.get_task("wbad", 10).expect("blocked").is_empty());
    }

    #[test]
    fn coordinator_requeues_tasks_from_stale_worker() {
        let mut c = Coordinator::new(CoordinatorConfig {
            worker_liveness_timeout_ms: 5,
            retry_backoff_base_ms: 0,
            ..CoordinatorConfig::default()
        });
        let plan = serde_json::to_vec(&PhysicalPlan::ParquetScan(ParquetScanExec {
            table: "t".to_string(),
            schema: Some(Schema::empty()),
            projection: None,
            filters: vec![],
        }))
        .expect("plan");
        c.submit_query("10".to_string(), &plan).expect("submit");
        c.heartbeat("w1", 0, &[]).expect("heartbeat");

        let assigned = c.get_task("w1", 1).expect("assign");
        assert_eq!(assigned.len(), 1);
        let first = assigned[0].clone();
        assert_eq!(first.attempt, 1);

        thread::sleep(Duration::from_millis(10));
        let reassigned = c.get_task("w2", 1).expect("reassign");
        assert_eq!(reassigned.len(), 1);
        assert_eq!(reassigned[0].query_id, "10");
        assert_eq!(reassigned[0].stage_id, first.stage_id);
        assert_eq!(reassigned[0].task_id, first.task_id);
        assert_eq!(reassigned[0].attempt, 2);
    }

    #[test]
    fn coordinator_enforces_worker_and_query_concurrency_limits() {
        let mut c = Coordinator::new(CoordinatorConfig {
            max_concurrent_tasks_per_worker: 1,
            max_concurrent_tasks_per_query: 1,
            ..CoordinatorConfig::default()
        });
        let plan = serde_json::to_vec(&PhysicalPlan::ParquetScan(ParquetScanExec {
            table: "t".to_string(),
            schema: Some(Schema::empty()),
            projection: None,
            filters: vec![],
        }))
        .expect("plan");
        c.submit_query("20".to_string(), &plan).expect("submit q20");
        c.submit_query("21".to_string(), &plan).expect("submit q21");

        let first_pull = c.get_task("w1", 10).expect("first pull");
        assert_eq!(first_pull.len(), 1);

        let second_pull = c.get_task("w1", 10).expect("second pull");
        assert!(second_pull.is_empty());

        let t = &first_pull[0];
        c.report_task_status(
            &t.query_id,
            t.stage_id,
            t.task_id,
            t.attempt,
            t.layout_version,
            t.layout_fingerprint,
            TaskState::Succeeded,
            Some("w1"),
            "ok".to_string(),
        )
        .expect("mark success");

        let third_pull = c.get_task("w1", 10).expect("third pull");
        assert_eq!(third_pull.len(), 1);
    }

    #[test]
    fn coordinator_assigns_custom_operator_tasks_only_to_capable_workers() {
        let mut c = Coordinator::new(CoordinatorConfig::default());
        let plan = serde_json::to_vec(&PhysicalPlan::Custom(ffq_planner::CustomExec {
            op_name: "my_custom_op".to_string(),
            config: HashMap::new(),
            input: Box::new(PhysicalPlan::ParquetScan(ParquetScanExec {
                table: "t".to_string(),
                schema: Some(Schema::empty()),
                projection: None,
                filters: vec![],
            })),
        }))
        .expect("plan");
        c.submit_query("q_custom".to_string(), &plan)
            .expect("submit");

        c.heartbeat("w_plain", 0, &[]).expect("heartbeat plain");
        let plain_assignments = c.get_task("w_plain", 10).expect("plain assignments");
        assert!(plain_assignments.is_empty());

        c.heartbeat("w_custom", 0, &["my_custom_op".to_string()])
            .expect("heartbeat custom");
        let custom_assignments = c.get_task("w_custom", 10).expect("custom assignments");
        assert_eq!(custom_assignments.len(), 1);
    }

    #[test]
    fn coordinator_launches_speculative_attempt_for_straggler_and_accepts_older_success() {
        let mut c = Coordinator::new(CoordinatorConfig {
            speculative_execution_enabled: true,
            speculative_min_completed_samples: 1,
            speculative_p95_multiplier: 1.0,
            speculative_min_runtime_ms: 1,
            retry_backoff_base_ms: 0,
            ..CoordinatorConfig::default()
        });
        let plan = serde_json::to_vec(&single_scan_plan("t")).expect("plan");
        c.submit_query("qspec".to_string(), &plan).expect("submit");

        let first = c.get_task("wslow", 1).expect("first task");
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].attempt, 1);
        std::thread::sleep(std::time::Duration::from_millis(5));
        {
            let q = c.queries.get_mut("qspec").expect("query");
            let st = q.stages.get_mut(&0).expect("stage");
            st.completed_runtime_ms_samples.push(1);
        }
        let speculative = c.get_task("wfast", 1).expect("speculative task");
        assert_eq!(speculative.len(), 1);
        assert_eq!(speculative[0].attempt, 2);

        c.report_task_status(
            "qspec",
            first[0].stage_id,
            first[0].task_id,
            first[0].attempt,
            first[0].layout_version,
            first[0].layout_fingerprint,
            TaskState::Succeeded,
            Some("wslow"),
            "older attempt won".to_string(),
        )
        .expect("report success");
        let st = c.get_query_status("qspec").expect("status");
        assert_eq!(st.state, QueryState::Succeeded);
        let stage = st.stage_metrics.get(&0).expect("stage metrics");
        assert!(stage.speculative_older_attempt_wins >= 1);
    }

    #[test]
    fn coordinator_prefers_locality_matching_worker_for_scan_tasks() {
        let mut c = Coordinator::new(CoordinatorConfig {
            locality_preference_enabled: true,
            ..CoordinatorConfig::default()
        });
        let plan = serde_json::to_vec(&single_scan_plan("lineitem")).expect("plan");
        c.submit_query("qlocal".to_string(), &plan).expect("submit");

        c.heartbeat("w_remote", 0, &["locality:table:orders".to_string()])
            .expect("remote heartbeat");
        c.heartbeat("w_local", 0, &["locality:table:lineitem".to_string()])
            .expect("local heartbeat");

        let remote = c.get_task("w_remote", 1).expect("remote task");
        assert!(remote.is_empty());
        let local = c.get_task("w_local", 1).expect("local task");
        assert_eq!(local.len(), 1);
    }

    #[test]
    fn coordinator_fans_out_reduce_stage_tasks_from_shuffle_layout() {
        let mut c = Coordinator::new(CoordinatorConfig::default());
        let plan = serde_json::to_vec(&PhysicalPlan::Exchange(ExchangeExec::ShuffleRead(
            ffq_planner::ShuffleReadExchange {
                input: Box::new(PhysicalPlan::Exchange(ExchangeExec::ShuffleWrite(
                    ffq_planner::ShuffleWriteExchange {
                        input: Box::new(PhysicalPlan::ParquetScan(ParquetScanExec {
                            table: "t".to_string(),
                            schema: Some(Schema::empty()),
                            projection: None,
                            filters: vec![],
                        })),
                        partitioning: ffq_planner::PartitioningSpec::HashKeys {
                            keys: vec!["k".to_string()],
                            partitions: 4,
                        },
                    },
                ))),
                partitioning: ffq_planner::PartitioningSpec::HashKeys {
                    keys: vec!["k".to_string()],
                    partitions: 4,
                },
            },
        )))
        .expect("plan");
        c.submit_query("qfanout".to_string(), &plan)
            .expect("submit");

        let map_assignments = c.get_task("w1", 10).expect("get map task");
        assert_eq!(map_assignments.len(), 1);
        let map = &map_assignments[0];
        assert!(map.assigned_reduce_partitions.is_empty());
        c.report_task_status(
            &map.query_id,
            map.stage_id,
            map.task_id,
            map.attempt,
            map.layout_version,
            map.layout_fingerprint,
            TaskState::Succeeded,
            Some("w1"),
            "map done".to_string(),
        )
        .expect("mark map success");

        let assignments = c.get_task("w1", 10).expect("get reduce tasks");
        assert_eq!(assignments.len(), 4);
        let mut task_ids = assignments.iter().map(|t| t.task_id).collect::<Vec<_>>();
        task_ids.sort_unstable();
        assert_eq!(task_ids, vec![0, 1, 2, 3]);
        for a in &assignments {
            assert_eq!(a.assigned_reduce_partitions, vec![a.task_id as u32]);
            assert_eq!(a.assigned_reduce_split_index, 0);
            assert_eq!(a.assigned_reduce_split_count, 1);
        }

        let status = c.get_query_status("qfanout").expect("status");
        let root = status.stage_metrics.get(&0).expect("root stage metrics");
        assert_eq!(root.planned_reduce_tasks, 4);
        assert_eq!(root.queued_tasks, 0);
        assert_eq!(root.running_tasks, 4);
    }

    #[test]
    fn coordinator_updates_adaptive_shuffle_reduce_metrics_from_map_outputs() {
        let mut c = Coordinator::new(CoordinatorConfig {
            adaptive_shuffle_target_bytes: 50,
            ..CoordinatorConfig::default()
        });
        let plan = PhysicalPlan::Exchange(ExchangeExec::ShuffleRead(ShuffleReadExchange {
            input: Box::new(PhysicalPlan::Exchange(ExchangeExec::ShuffleWrite(
                ShuffleWriteExchange {
                    input: Box::new(PhysicalPlan::ParquetScan(ParquetScanExec {
                        table: "t".to_string(),
                        schema: Some(Schema::empty()),
                        projection: None,
                        filters: vec![],
                    })),
                    partitioning: PartitioningSpec::HashKeys {
                        keys: vec!["k".to_string()],
                        partitions: 4,
                    },
                },
            ))),
            partitioning: PartitioningSpec::HashKeys {
                keys: vec!["k".to_string()],
                partitions: 4,
            },
        }));
        let bytes = serde_json::to_vec(&plan).expect("plan");
        c.submit_query("300".to_string(), &bytes).expect("submit");
        let map_task = c.get_task("w1", 10).expect("map").remove(0);
        c.register_map_output(
            "300".to_string(),
            map_task.stage_id,
            map_task.task_id,
            map_task.attempt,
            map_task.layout_version,
            map_task.layout_fingerprint,
            vec![
                MapOutputPartitionMeta {
                    reduce_partition: 0,
                    bytes: 10,
                    rows: 1,
                    batches: 1,
                    stream_epoch: 1,
                    committed_offset: 0,
                    finalized: true,
                },
                MapOutputPartitionMeta {
                    reduce_partition: 1,
                    bytes: 20,
                    rows: 1,
                    batches: 1,
                    stream_epoch: 1,
                    committed_offset: 0,
                    finalized: true,
                },
                MapOutputPartitionMeta {
                    reduce_partition: 2,
                    bytes: 30,
                    rows: 1,
                    batches: 1,
                    stream_epoch: 1,
                    committed_offset: 0,
                    finalized: true,
                },
                MapOutputPartitionMeta {
                    reduce_partition: 3,
                    bytes: 40,
                    rows: 1,
                    batches: 1,
                    stream_epoch: 1,
                    committed_offset: 0,
                    finalized: true,
                },
            ],
        )
        .expect("register");

        let status = c.get_query_status("300").expect("status");
        let root = status.stage_metrics.get(&0).expect("root stage metrics");
        assert_eq!(root.planned_reduce_tasks, 4);
        assert_eq!(root.adaptive_reduce_tasks, 2);
        assert_eq!(root.adaptive_target_bytes, 50);
    }

    #[test]
    fn coordinator_applies_barrier_time_adaptive_partition_coalescing() {
        let mut c = Coordinator::new(CoordinatorConfig {
            adaptive_shuffle_target_bytes: 30,
            ..CoordinatorConfig::default()
        });
        let plan = PhysicalPlan::Exchange(ExchangeExec::ShuffleRead(ShuffleReadExchange {
            input: Box::new(PhysicalPlan::Exchange(ExchangeExec::ShuffleWrite(
                ShuffleWriteExchange {
                    input: Box::new(PhysicalPlan::ParquetScan(ParquetScanExec {
                        table: "t".to_string(),
                        schema: Some(Schema::empty()),
                        projection: None,
                        filters: vec![],
                    })),
                    partitioning: PartitioningSpec::HashKeys {
                        keys: vec!["k".to_string()],
                        partitions: 4,
                    },
                },
            ))),
            partitioning: PartitioningSpec::HashKeys {
                keys: vec!["k".to_string()],
                partitions: 4,
            },
        }));
        let bytes = serde_json::to_vec(&plan).expect("plan");
        c.submit_query("301".to_string(), &bytes).expect("submit");

        let map_task = c.get_task("w1", 10).expect("map").remove(0);
        c.register_map_output(
            "301".to_string(),
            map_task.stage_id,
            map_task.task_id,
            map_task.attempt,
            map_task.layout_version,
            map_task.layout_fingerprint,
            vec![
                MapOutputPartitionMeta {
                    reduce_partition: 0,
                    bytes: 5,
                    rows: 1,
                    batches: 1,
                    stream_epoch: 1,
                    committed_offset: 0,
                    finalized: true,
                },
                MapOutputPartitionMeta {
                    reduce_partition: 1,
                    bytes: 5,
                    rows: 1,
                    batches: 1,
                    stream_epoch: 1,
                    committed_offset: 0,
                    finalized: true,
                },
                MapOutputPartitionMeta {
                    reduce_partition: 2,
                    bytes: 5,
                    rows: 1,
                    batches: 1,
                    stream_epoch: 1,
                    committed_offset: 0,
                    finalized: true,
                },
                MapOutputPartitionMeta {
                    reduce_partition: 3,
                    bytes: 5,
                    rows: 1,
                    batches: 1,
                    stream_epoch: 1,
                    committed_offset: 0,
                    finalized: true,
                },
            ],
        )
        .expect("register map output");
        c.report_task_status(
            &map_task.query_id,
            map_task.stage_id,
            map_task.task_id,
            map_task.attempt,
            map_task.layout_version,
            map_task.layout_fingerprint,
            TaskState::Succeeded,
            Some("w1"),
            "map done".to_string(),
        )
        .expect("map success");

        let reduce_tasks = c.get_task("w1", 10).expect("reduce tasks");
        assert_eq!(reduce_tasks.len(), 1);
        assert_eq!(reduce_tasks[0].assigned_reduce_partitions, vec![0, 1, 2, 3]);
        assert_eq!(reduce_tasks[0].assigned_reduce_split_count, 1);
        let status = c.get_query_status("301").expect("status");
        let root = status.stage_metrics.get(&0).expect("root stage");
        assert_eq!(root.planned_reduce_tasks, 4);
        assert_eq!(root.adaptive_reduce_tasks, 1);
        assert_eq!(root.adaptive_target_bytes, 30);
        assert!(!root.partition_bytes_histogram.is_empty());
        assert!(!root.aqe_events.is_empty());
        assert!(root.layout_finalize_count >= 1);
    }

    #[test]
    fn coordinator_ignores_stale_reports_from_old_adaptive_layout() {
        let mut c = Coordinator::new(CoordinatorConfig {
            adaptive_shuffle_target_bytes: 30,
            ..CoordinatorConfig::default()
        });
        let plan = PhysicalPlan::Exchange(ExchangeExec::ShuffleRead(ShuffleReadExchange {
            input: Box::new(PhysicalPlan::Exchange(ExchangeExec::ShuffleWrite(
                ShuffleWriteExchange {
                    input: Box::new(PhysicalPlan::ParquetScan(ParquetScanExec {
                        table: "t".to_string(),
                        schema: Some(Schema::empty()),
                        projection: None,
                        filters: vec![],
                    })),
                    partitioning: PartitioningSpec::HashKeys {
                        keys: vec!["k".to_string()],
                        partitions: 4,
                    },
                },
            ))),
            partitioning: PartitioningSpec::HashKeys {
                keys: vec!["k".to_string()],
                partitions: 4,
            },
        }));
        let bytes = serde_json::to_vec(&plan).expect("plan");
        c.submit_query("303".to_string(), &bytes).expect("submit");

        let map_task = c.get_task("w1", 10).expect("map").remove(0);
        c.register_map_output(
            "303".to_string(),
            map_task.stage_id,
            map_task.task_id,
            map_task.attempt,
            map_task.layout_version.saturating_sub(1),
            map_task.layout_fingerprint ^ 0xDEADBEEF_u64,
            vec![MapOutputPartitionMeta {
                reduce_partition: 0,
                bytes: 5,
                rows: 1,
                batches: 1,
                stream_epoch: 1,
                committed_offset: 0,
                finalized: true,
            }],
        )
        .expect("stale map output ignored");
        assert_eq!(c.map_output_registry_size(), 0);

        c.register_map_output(
            "303".to_string(),
            map_task.stage_id,
            map_task.task_id,
            map_task.attempt,
            map_task.layout_version,
            map_task.layout_fingerprint,
            vec![
                MapOutputPartitionMeta {
                    reduce_partition: 0,
                    bytes: 5,
                    rows: 1,
                    batches: 1,
                    stream_epoch: 1,
                    committed_offset: 0,
                    finalized: true,
                },
                MapOutputPartitionMeta {
                    reduce_partition: 1,
                    bytes: 5,
                    rows: 1,
                    batches: 1,
                    stream_epoch: 1,
                    committed_offset: 0,
                    finalized: true,
                },
                MapOutputPartitionMeta {
                    reduce_partition: 2,
                    bytes: 5,
                    rows: 1,
                    batches: 1,
                    stream_epoch: 1,
                    committed_offset: 0,
                    finalized: true,
                },
                MapOutputPartitionMeta {
                    reduce_partition: 3,
                    bytes: 5,
                    rows: 1,
                    batches: 1,
                    stream_epoch: 1,
                    committed_offset: 0,
                    finalized: true,
                },
            ],
        )
        .expect("register map output");
        c.report_task_status(
            &map_task.query_id,
            map_task.stage_id,
            map_task.task_id,
            map_task.attempt,
            map_task.layout_version,
            map_task.layout_fingerprint,
            TaskState::Succeeded,
            Some("w1"),
            "map done".to_string(),
        )
        .expect("map success");

        let reduce_task = c.get_task("w1", 10).expect("reduce tasks").remove(0);
        assert!(reduce_task.layout_version > 1);
        c.report_task_status(
            &reduce_task.query_id,
            reduce_task.stage_id,
            reduce_task.task_id,
            reduce_task.attempt,
            reduce_task.layout_version.saturating_sub(1),
            reduce_task.layout_fingerprint ^ 0xABCD_u64,
            TaskState::Succeeded,
            Some("w1"),
            "stale success".to_string(),
        )
        .expect("stale status ignored");
        let status_after_stale = c.get_query_status("303").expect("status");
        let root = status_after_stale
            .stage_metrics
            .get(&reduce_task.stage_id)
            .expect("reduce stage metrics");
        assert_eq!(root.succeeded_tasks, 0);
        assert_eq!(status_after_stale.state, QueryState::Running);

        c.report_task_status(
            &reduce_task.query_id,
            reduce_task.stage_id,
            reduce_task.task_id,
            reduce_task.attempt,
            reduce_task.layout_version,
            reduce_task.layout_fingerprint,
            TaskState::Succeeded,
            Some("w1"),
            "reduce done".to_string(),
        )
        .expect("reduce success");
        let final_status = c.get_query_status("303").expect("final");
        assert_eq!(final_status.state, QueryState::Succeeded);
    }

    #[test]
    fn coordinator_adaptive_shuffle_retries_failed_map_attempt_and_completes() {
        let mut c = Coordinator::new(CoordinatorConfig {
            retry_backoff_base_ms: 0,
            adaptive_shuffle_target_bytes: 30,
            ..CoordinatorConfig::default()
        });
        let bytes = serde_json::to_vec(&hash_shuffle_plan(4)).expect("plan");
        c.submit_query("305".to_string(), &bytes).expect("submit");

        let map1 = c.get_task("w1", 10).expect("map1").remove(0);
        assert_eq!(map1.attempt, 1);
        c.report_task_status(
            &map1.query_id,
            map1.stage_id,
            map1.task_id,
            map1.attempt,
            map1.layout_version,
            map1.layout_fingerprint,
            TaskState::Failed,
            Some("w1"),
            "synthetic map failure".to_string(),
        )
        .expect("map1 failed");

        let map2 = c.get_task("w2", 10).expect("map2").remove(0);
        assert_eq!(map2.stage_id, map1.stage_id);
        assert_eq!(map2.task_id, map1.task_id);
        assert_eq!(map2.attempt, 2);
        c.register_map_output(
            "305".to_string(),
            map2.stage_id,
            map2.task_id,
            map2.attempt,
            map2.layout_version,
            map2.layout_fingerprint,
            vec![
                MapOutputPartitionMeta {
                    reduce_partition: 0,
                    bytes: 5,
                    rows: 1,
                    batches: 1,
                    stream_epoch: 1,
                    committed_offset: 0,
                    finalized: true,
                },
                MapOutputPartitionMeta {
                    reduce_partition: 1,
                    bytes: 5,
                    rows: 1,
                    batches: 1,
                    stream_epoch: 1,
                    committed_offset: 0,
                    finalized: true,
                },
                MapOutputPartitionMeta {
                    reduce_partition: 2,
                    bytes: 5,
                    rows: 1,
                    batches: 1,
                    stream_epoch: 1,
                    committed_offset: 0,
                    finalized: true,
                },
                MapOutputPartitionMeta {
                    reduce_partition: 3,
                    bytes: 5,
                    rows: 1,
                    batches: 1,
                    stream_epoch: 1,
                    committed_offset: 0,
                    finalized: true,
                },
            ],
        )
        .expect("register map2");
        c.report_task_status(
            &map2.query_id,
            map2.stage_id,
            map2.task_id,
            map2.attempt,
            map2.layout_version,
            map2.layout_fingerprint,
            TaskState::Succeeded,
            Some("w2"),
            "map2 done".to_string(),
        )
        .expect("map2 success");

        let reduce = c.get_task("w2", 10).expect("reduce");
        assert!(!reduce.is_empty());
        for t in reduce {
            c.report_task_status(
                &t.query_id,
                t.stage_id,
                t.task_id,
                t.attempt,
                t.layout_version,
                t.layout_fingerprint,
                TaskState::Succeeded,
                Some("w2"),
                "reduce done".to_string(),
            )
            .expect("reduce success");
        }

        let st = c.get_query_status("305").expect("final status");
        assert_eq!(st.state, QueryState::Succeeded);
        assert_eq!(st.running_tasks, 0);
        assert_eq!(st.queued_tasks, 0);
    }

    #[test]
    fn coordinator_adaptive_shuffle_recovers_from_worker_death_during_map_and_reduce() {
        let mut c = Coordinator::new(CoordinatorConfig {
            worker_liveness_timeout_ms: 5,
            retry_backoff_base_ms: 0,
            adaptive_shuffle_target_bytes: 30,
            ..CoordinatorConfig::default()
        });
        let bytes = serde_json::to_vec(&hash_shuffle_plan(4)).expect("plan");
        c.submit_query("306".to_string(), &bytes).expect("submit");
        c.heartbeat("w1", 0, &[]).expect("hb w1");

        let map1 = c.get_task("w1", 10).expect("map1").remove(0);
        assert_eq!(map1.attempt, 1);

        thread::sleep(Duration::from_millis(10));
        c.heartbeat("w2", 0, &[]).expect("hb w2");
        let map2 = c.get_task("w2", 10).expect("map2").remove(0);
        assert_eq!(map2.stage_id, map1.stage_id);
        assert_eq!(map2.task_id, map1.task_id);
        assert_eq!(map2.attempt, 2);

        c.register_map_output(
            "306".to_string(),
            map2.stage_id,
            map2.task_id,
            map2.attempt,
            map2.layout_version,
            map2.layout_fingerprint,
            vec![
                MapOutputPartitionMeta {
                    reduce_partition: 0,
                    bytes: 5,
                    rows: 1,
                    batches: 1,
                    stream_epoch: 1,
                    committed_offset: 0,
                    finalized: true,
                },
                MapOutputPartitionMeta {
                    reduce_partition: 1,
                    bytes: 5,
                    rows: 1,
                    batches: 1,
                    stream_epoch: 1,
                    committed_offset: 0,
                    finalized: true,
                },
                MapOutputPartitionMeta {
                    reduce_partition: 2,
                    bytes: 5,
                    rows: 1,
                    batches: 1,
                    stream_epoch: 1,
                    committed_offset: 0,
                    finalized: true,
                },
                MapOutputPartitionMeta {
                    reduce_partition: 3,
                    bytes: 5,
                    rows: 1,
                    batches: 1,
                    stream_epoch: 1,
                    committed_offset: 0,
                    finalized: true,
                },
            ],
        )
        .expect("register map2");
        c.report_task_status(
            &map2.query_id,
            map2.stage_id,
            map2.task_id,
            map2.attempt,
            map2.layout_version,
            map2.layout_fingerprint,
            TaskState::Succeeded,
            Some("w2"),
            "map2 done".to_string(),
        )
        .expect("map2 success");

        c.heartbeat("w2", 0, &[]).expect("hb w2 pre-reduce");
        let reduce1 = c.get_task("w2", 10).expect("reduce1").remove(0);
        assert_eq!(reduce1.attempt, 1);
        thread::sleep(Duration::from_millis(10));

        c.heartbeat("w3", 0, &[]).expect("hb w3");
        let reduce2 = c.get_task("w3", 10).expect("reduce2").remove(0);
        assert_eq!(reduce2.stage_id, reduce1.stage_id);
        assert_eq!(reduce2.task_id, reduce1.task_id);
        assert_eq!(reduce2.attempt, 2);
        c.report_task_status(
            &reduce2.query_id,
            reduce2.stage_id,
            reduce2.task_id,
            reduce2.attempt,
            reduce2.layout_version,
            reduce2.layout_fingerprint,
            TaskState::Succeeded,
            Some("w3"),
            "reduce2 done".to_string(),
        )
        .expect("reduce2 success");

        let st = c.get_query_status("306").expect("final status");
        assert_eq!(st.state, QueryState::Succeeded);
        assert_eq!(st.running_tasks, 0);
        assert_eq!(st.queued_tasks, 0);
    }

    #[test]
    fn deterministic_coalesce_split_groups_is_stable_across_input_map_order() {
        let mut a = HashMap::new();
        a.insert(0_u32, 10_u64);
        a.insert(1_u32, 15_u64);
        a.insert(2_u32, 5_u64);
        a.insert(3_u32, 20_u64);
        let mut b = HashMap::new();
        b.insert(3_u32, 20_u64);
        b.insert(1_u32, 15_u64);
        b.insert(0_u32, 10_u64);
        b.insert(2_u32, 5_u64);

        let g1 = deterministic_coalesce_split_groups(4, 25, &a, 1, 0, 0);
        let g2 = deterministic_coalesce_split_groups(4, 25, &b, 1, 0, 0);
        assert_eq!(g1, g2);
        assert_eq!(g1.len(), 2);
        assert_eq!(g1[0].assigned_reduce_partitions, vec![0, 1]);
        assert_eq!(g1[1].assigned_reduce_partitions, vec![2, 3]);
    }

    #[test]
    fn deterministic_coalesce_split_groups_applies_optional_group_split_cap() {
        let mut bytes = HashMap::new();
        bytes.insert(0_u32, 5_u64);
        bytes.insert(1_u32, 5_u64);
        bytes.insert(2_u32, 5_u64);
        bytes.insert(3_u32, 5_u64);

        let groups = deterministic_coalesce_split_groups(4, 1_000, &bytes, 1, 0, 2);
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].assigned_reduce_partitions, vec![0, 1]);
        assert_eq!(groups[1].assigned_reduce_partitions, vec![2, 3]);
    }

    #[test]
    fn deterministic_coalesce_split_groups_respects_min_max_reduce_task_bounds() {
        let mut bytes = HashMap::new();
        bytes.insert(0_u32, 10_u64);
        bytes.insert(1_u32, 10_u64);
        bytes.insert(2_u32, 10_u64);
        bytes.insert(3_u32, 10_u64);

        // Natural grouping with high target would be 1 group; min=2 forces deterministic split.
        let min_groups = deterministic_coalesce_split_groups(4, 1_000, &bytes, 2, 0, 0);
        assert_eq!(min_groups.len(), 2);
        assert_eq!(min_groups[0].assigned_reduce_partitions, vec![0, 1]);
        assert_eq!(min_groups[1].assigned_reduce_partitions, vec![2, 3]);

        // Natural grouping with low target would be 4 groups; max=2 forces deterministic merge.
        let max_groups = deterministic_coalesce_split_groups(4, 1, &bytes, 1, 2, 0);
        assert_eq!(max_groups.len(), 2);
        assert_eq!(max_groups[0].assigned_reduce_partitions, vec![0]);
        assert_eq!(max_groups[1].assigned_reduce_partitions, vec![1, 2, 3]);
    }

    #[test]
    fn deterministic_coalesce_split_groups_splits_hot_singleton_partition() {
        let mut bytes = HashMap::new();
        bytes.insert(0_u32, 8_u64);
        bytes.insert(1_u32, 120_u64);
        bytes.insert(2_u32, 8_u64);
        bytes.insert(3_u32, 8_u64);

        let groups = deterministic_coalesce_split_groups(4, 32, &bytes, 1, 8, 0);
        let hot = groups
            .iter()
            .filter(|g| {
                g.assigned_reduce_partitions == vec![1] && g.assigned_reduce_split_count > 1
            })
            .collect::<Vec<_>>();
        assert_eq!(hot.len(), 4);
        for (i, g) in hot.into_iter().enumerate() {
            assert_eq!(g.assigned_reduce_split_index, i as u32);
            assert_eq!(g.assigned_reduce_split_count, 4);
        }
    }

    #[test]
    fn coordinator_barrier_time_hot_partition_splitting_increases_reduce_tasks() {
        let mut c = Coordinator::new(CoordinatorConfig {
            adaptive_shuffle_target_bytes: 32,
            adaptive_shuffle_max_reduce_tasks: 8,
            ..CoordinatorConfig::default()
        });
        let plan = PhysicalPlan::Exchange(ExchangeExec::ShuffleRead(ShuffleReadExchange {
            input: Box::new(PhysicalPlan::Exchange(ExchangeExec::ShuffleWrite(
                ShuffleWriteExchange {
                    input: Box::new(PhysicalPlan::ParquetScan(ParquetScanExec {
                        table: "t".to_string(),
                        schema: Some(Schema::empty()),
                        projection: None,
                        filters: vec![],
                    })),
                    partitioning: PartitioningSpec::HashKeys {
                        keys: vec!["k".to_string()],
                        partitions: 4,
                    },
                },
            ))),
            partitioning: PartitioningSpec::HashKeys {
                keys: vec!["k".to_string()],
                partitions: 4,
            },
        }));
        let bytes = serde_json::to_vec(&plan).expect("plan");
        c.submit_query("302".to_string(), &bytes).expect("submit");

        let map_task = c.get_task("w1", 10).expect("map").remove(0);
        c.register_map_output(
            "302".to_string(),
            map_task.stage_id,
            map_task.task_id,
            map_task.attempt,
            map_task.layout_version,
            map_task.layout_fingerprint,
            vec![
                MapOutputPartitionMeta {
                    reduce_partition: 0,
                    bytes: 8,
                    rows: 1,
                    batches: 1,
                    stream_epoch: 1,
                    committed_offset: 0,
                    finalized: true,
                },
                MapOutputPartitionMeta {
                    reduce_partition: 1,
                    bytes: 120,
                    rows: 1,
                    batches: 1,
                    stream_epoch: 1,
                    committed_offset: 0,
                    finalized: true,
                },
                MapOutputPartitionMeta {
                    reduce_partition: 2,
                    bytes: 8,
                    rows: 1,
                    batches: 1,
                    stream_epoch: 1,
                    committed_offset: 0,
                    finalized: true,
                },
                MapOutputPartitionMeta {
                    reduce_partition: 3,
                    bytes: 8,
                    rows: 1,
                    batches: 1,
                    stream_epoch: 1,
                    committed_offset: 0,
                    finalized: true,
                },
            ],
        )
        .expect("register");
        c.report_task_status(
            &map_task.query_id,
            map_task.stage_id,
            map_task.task_id,
            map_task.attempt,
            map_task.layout_version,
            map_task.layout_fingerprint,
            TaskState::Succeeded,
            Some("w1"),
            "map done".to_string(),
        )
        .expect("map success");

        let reduce_tasks = c.get_task("w1", 20).expect("reduce tasks");
        assert!(reduce_tasks.len() > 4);
        let hot_splits = reduce_tasks
            .iter()
            .filter(|t| {
                t.assigned_reduce_partitions == vec![1] && t.assigned_reduce_split_count > 1
            })
            .count();
        assert_eq!(hot_splits, 4);
        let st = c.get_query_status("302").expect("status");
        let root = st.stage_metrics.get(&0).expect("root stage");
        assert!(
            root.aqe_events
                .iter()
                .any(|e| e.contains("skew_p95_bytes=") && e.contains("skew_p99_bytes=")),
            "expected skew percentile diagnostics in AQE events: {:?}",
            root.aqe_events
        );
    }

    #[test]
    fn coordinator_finalizes_adaptive_layout_once_before_reduce_scheduling() {
        let mut c = Coordinator::new(CoordinatorConfig {
            adaptive_shuffle_target_bytes: 30,
            ..CoordinatorConfig::default()
        });
        let plan = PhysicalPlan::Exchange(ExchangeExec::ShuffleRead(ShuffleReadExchange {
            input: Box::new(PhysicalPlan::Exchange(ExchangeExec::ShuffleWrite(
                ShuffleWriteExchange {
                    input: Box::new(PhysicalPlan::ParquetScan(ParquetScanExec {
                        table: "t".to_string(),
                        schema: Some(Schema::empty()),
                        projection: None,
                        filters: vec![],
                    })),
                    partitioning: PartitioningSpec::HashKeys {
                        keys: vec!["k".to_string()],
                        partitions: 4,
                    },
                },
            ))),
            partitioning: PartitioningSpec::HashKeys {
                keys: vec!["k".to_string()],
                partitions: 4,
            },
        }));
        let bytes = serde_json::to_vec(&plan).expect("plan");
        c.submit_query("304".to_string(), &bytes).expect("submit");

        let map_task = c.get_task("w1", 10).expect("map").remove(0);
        let while_map_running = c.get_task("w2", 10).expect("no reduce before barrier");
        assert!(while_map_running.is_empty());

        c.register_map_output(
            "304".to_string(),
            map_task.stage_id,
            map_task.task_id,
            map_task.attempt,
            map_task.layout_version,
            map_task.layout_fingerprint,
            vec![
                MapOutputPartitionMeta {
                    reduce_partition: 0,
                    bytes: 5,
                    rows: 1,
                    batches: 1,
                    stream_epoch: 1,
                    committed_offset: 0,
                    finalized: true,
                },
                MapOutputPartitionMeta {
                    reduce_partition: 1,
                    bytes: 5,
                    rows: 1,
                    batches: 1,
                    stream_epoch: 1,
                    committed_offset: 0,
                    finalized: true,
                },
                MapOutputPartitionMeta {
                    reduce_partition: 2,
                    bytes: 5,
                    rows: 1,
                    batches: 1,
                    stream_epoch: 1,
                    committed_offset: 0,
                    finalized: true,
                },
                MapOutputPartitionMeta {
                    reduce_partition: 3,
                    bytes: 5,
                    rows: 1,
                    batches: 1,
                    stream_epoch: 1,
                    committed_offset: 0,
                    finalized: true,
                },
            ],
        )
        .expect("register");
        c.report_task_status(
            &map_task.query_id,
            map_task.stage_id,
            map_task.task_id,
            map_task.attempt,
            map_task.layout_version,
            map_task.layout_fingerprint,
            TaskState::Succeeded,
            Some("w1"),
            "map done".to_string(),
        )
        .expect("map success");

        let reduce_tasks = c.get_task("w2", 10).expect("reduce tasks");
        assert!(!reduce_tasks.is_empty());
        let query = c.queries.get("304").expect("query runtime");
        let reduce_stage = query.stages.get(&0).expect("reduce stage");
        assert_eq!(reduce_stage.layout_finalize_count, 1);
        assert_eq!(
            reduce_stage.barrier_state,
            StageBarrierState::ReduceSchedulable
        );

        let _ = c.get_task("w3", 10).expect("subsequent poll");
        let query = c.queries.get("304").expect("query runtime");
        let reduce_stage = query.stages.get(&0).expect("reduce stage");
        assert_eq!(reduce_stage.layout_finalize_count, 1);
    }

    #[test]
    fn coordinator_allows_pipelined_reduce_assignment_when_partition_ready() {
        let mut c = Coordinator::new(CoordinatorConfig {
            pipelined_shuffle_enabled: true,
            pipelined_shuffle_min_map_completion_ratio: 0.0,
            ..CoordinatorConfig::default()
        });
        let plan = PhysicalPlan::Exchange(ExchangeExec::ShuffleRead(ShuffleReadExchange {
            input: Box::new(PhysicalPlan::Exchange(ExchangeExec::ShuffleWrite(
                ShuffleWriteExchange {
                    input: Box::new(PhysicalPlan::ParquetScan(ParquetScanExec {
                        table: "t".to_string(),
                        schema: Some(Schema::empty()),
                        projection: None,
                        filters: vec![],
                    })),
                    partitioning: PartitioningSpec::HashKeys {
                        keys: vec!["k".to_string()],
                        partitions: 4,
                    },
                },
            ))),
            partitioning: PartitioningSpec::HashKeys {
                keys: vec!["k".to_string()],
                partitions: 4,
            },
        }));
        let bytes = serde_json::to_vec(&plan).expect("plan");
        c.submit_query("305".to_string(), &bytes).expect("submit");

        let map_task = c.get_task("w1", 10).expect("map").remove(0);
        c.register_map_output(
            "305".to_string(),
            map_task.stage_id,
            map_task.task_id,
            map_task.attempt,
            map_task.layout_version,
            map_task.layout_fingerprint,
            vec![MapOutputPartitionMeta {
                reduce_partition: 0,
                bytes: 10,
                rows: 2,
                batches: 1,
                stream_epoch: 1,
                committed_offset: 10,
                finalized: false,
            }],
        )
        .expect("register partial");

        let reduce_tasks = c.get_task("w2", 10).expect("pipelined reduce task");
        assert!(
            !reduce_tasks.is_empty(),
            "expected at least one pipelined reduce task assignment"
        );
        assert!(
            reduce_tasks
                .iter()
                .all(|t| t.assigned_reduce_partitions == vec![0]),
            "only ready partition should be schedulable before map completion"
        );
    }

    #[test]
    fn coordinator_pipeline_requires_committed_offset_threshold_before_scheduling() {
        let mut c = Coordinator::new(CoordinatorConfig {
            pipelined_shuffle_enabled: true,
            pipelined_shuffle_min_map_completion_ratio: 0.0,
            pipelined_shuffle_min_committed_offset_bytes: 64,
            ..CoordinatorConfig::default()
        });
        let plan = PhysicalPlan::Exchange(ExchangeExec::ShuffleRead(ShuffleReadExchange {
            input: Box::new(PhysicalPlan::Exchange(ExchangeExec::ShuffleWrite(
                ShuffleWriteExchange {
                    input: Box::new(PhysicalPlan::ParquetScan(ParquetScanExec {
                        table: "t".to_string(),
                        schema: Some(Schema::empty()),
                        projection: None,
                        filters: vec![],
                    })),
                    partitioning: PartitioningSpec::HashKeys {
                        keys: vec!["k".to_string()],
                        partitions: 4,
                    },
                },
            ))),
            partitioning: PartitioningSpec::HashKeys {
                keys: vec!["k".to_string()],
                partitions: 4,
            },
        }));
        let bytes = serde_json::to_vec(&plan).expect("plan");
        c.submit_query("307".to_string(), &bytes).expect("submit");

        let map_task = c.get_task("w1", 10).expect("map").remove(0);
        c.register_map_output(
            "307".to_string(),
            map_task.stage_id,
            map_task.task_id,
            map_task.attempt,
            map_task.layout_version,
            map_task.layout_fingerprint,
            vec![MapOutputPartitionMeta {
                reduce_partition: 0,
                bytes: 32,
                rows: 1,
                batches: 1,
                stream_epoch: 1,
                committed_offset: 32,
                finalized: false,
            }],
        )
        .expect("register partial under threshold");
        assert!(
            c.get_task("w2", 10)
                .expect("no reduce before threshold")
                .is_empty()
        );

        c.register_map_output(
            "307".to_string(),
            map_task.stage_id,
            map_task.task_id,
            map_task.attempt,
            map_task.layout_version,
            map_task.layout_fingerprint,
            vec![MapOutputPartitionMeta {
                reduce_partition: 0,
                bytes: 96,
                rows: 2,
                batches: 2,
                stream_epoch: 1,
                committed_offset: 96,
                finalized: false,
            }],
        )
        .expect("register partial over threshold");
        let reduce_tasks = c.get_task("w2", 10).expect("reduce after threshold");
        assert!(!reduce_tasks.is_empty());
        assert!(
            reduce_tasks
                .iter()
                .all(|t| t.assigned_reduce_partitions == vec![0])
        );
    }

    #[test]
    fn coordinator_backpressure_throttles_assignment_windows() {
        let mut c = Coordinator::new(CoordinatorConfig {
            backpressure_target_inflight_bytes: 10,
            backpressure_target_queue_depth: 2,
            backpressure_max_map_publish_window_partitions: 8,
            backpressure_max_reduce_fetch_window_partitions: 8,
            ..CoordinatorConfig::default()
        });
        let plan = PhysicalPlan::Exchange(ExchangeExec::ShuffleRead(ShuffleReadExchange {
            input: Box::new(PhysicalPlan::Exchange(ExchangeExec::ShuffleWrite(
                ShuffleWriteExchange {
                    input: Box::new(PhysicalPlan::ParquetScan(ParquetScanExec {
                        table: "t".to_string(),
                        schema: Some(Schema::empty()),
                        projection: None,
                        filters: vec![],
                    })),
                    partitioning: PartitioningSpec::HashKeys {
                        keys: vec!["k".to_string()],
                        partitions: 4,
                    },
                },
            ))),
            partitioning: PartitioningSpec::HashKeys {
                keys: vec!["k".to_string()],
                partitions: 4,
            },
        }));
        let bytes = serde_json::to_vec(&plan).expect("plan");
        c.submit_query("308".to_string(), &bytes).expect("submit");

        let map_task = c.get_task("w1", 10).expect("map").remove(0);
        c.report_task_status_with_pressure(
            &map_task.query_id,
            map_task.stage_id,
            map_task.task_id,
            map_task.attempt,
            map_task.layout_version,
            map_task.layout_fingerprint,
            TaskState::Running,
            Some("w1"),
            "running".to_string(),
            40,
            8,
        )
        .expect("running pressure");

        c.register_map_output(
            map_task.query_id.clone(),
            map_task.stage_id,
            map_task.task_id,
            map_task.attempt,
            map_task.layout_version,
            map_task.layout_fingerprint,
            vec![MapOutputPartitionMeta {
                reduce_partition: 0,
                bytes: 100,
                rows: 10,
                batches: 1,
                stream_epoch: 1,
                committed_offset: 100,
                finalized: false,
            }],
        )
        .expect("register map");
        c.report_task_status(
            &map_task.query_id,
            map_task.stage_id,
            map_task.task_id,
            map_task.attempt,
            map_task.layout_version,
            map_task.layout_fingerprint,
            TaskState::Succeeded,
            Some("w1"),
            "map done".to_string(),
        )
        .expect("map success");

        let reduce_tasks = c.get_task("w2", 10).expect("reduce");
        assert!(!reduce_tasks.is_empty());
        assert!(
            reduce_tasks
                .iter()
                .all(|t| t.recommended_map_output_publish_window_partitions <= 2)
        );
        assert!(
            reduce_tasks
                .iter()
                .all(|t| t.recommended_reduce_fetch_window_partitions <= 2)
        );

        let reduce = reduce_tasks[0].clone();
        c.report_task_status_with_pressure(
            &reduce.query_id,
            reduce.stage_id,
            reduce.task_id,
            reduce.attempt,
            reduce.layout_version,
            reduce.layout_fingerprint,
            TaskState::Running,
            Some("w2"),
            "reduce running".to_string(),
            24,
            5,
        )
        .expect("reduce running pressure");

        let st = c
            .get_query_status(&map_task.query_id)
            .expect("query status with streaming metrics");
        let map_stage = st
            .stage_metrics
            .get(&map_task.stage_id)
            .expect("map stage metrics");
        assert_eq!(map_stage.map_output_bytes, 100);
        assert!(map_stage.stream_active_count >= 1);
        assert!(map_stage.backpressure_events.iter().any(|e| e.contains("window_update")));

        let reduce_stage = st
            .stage_metrics
            .get(&reduce.stage_id)
            .expect("reduce stage metrics");
        assert!(reduce_stage.first_chunk_ms <= reduce_stage.first_reduce_row_ms);
        assert!(reduce_stage.first_reduce_row_ms >= reduce_stage.first_chunk_ms);
        assert_eq!(reduce_stage.stream_buffered_bytes, 24);
        assert!(reduce_stage.stream_lag_ms <= reduce_stage.first_reduce_row_ms);
    }

    #[test]
    fn coordinator_reports_partition_readable_boundaries_per_attempt() {
        let mut c = Coordinator::new(CoordinatorConfig::default());
        let plan = PhysicalPlan::Exchange(ExchangeExec::ShuffleRead(ShuffleReadExchange {
            input: Box::new(PhysicalPlan::Exchange(ExchangeExec::ShuffleWrite(
                ShuffleWriteExchange {
                    input: Box::new(PhysicalPlan::ParquetScan(ParquetScanExec {
                        table: "t".to_string(),
                        schema: Some(Schema::empty()),
                        projection: None,
                        filters: vec![],
                    })),
                    partitioning: PartitioningSpec::HashKeys {
                        keys: vec!["k".to_string()],
                        partitions: 4,
                    },
                },
            ))),
            partitioning: PartitioningSpec::HashKeys {
                keys: vec!["k".to_string()],
                partitions: 4,
            },
        }));
        let bytes = serde_json::to_vec(&plan).expect("plan");
        c.submit_query("306".to_string(), &bytes).expect("submit");
        let map_task = c.get_task("w1", 10).expect("map").remove(0);
        c.register_map_output(
            "306".to_string(),
            map_task.stage_id,
            map_task.task_id,
            map_task.attempt,
            map_task.layout_version,
            map_task.layout_fingerprint,
            vec![
                MapOutputPartitionMeta {
                    reduce_partition: 2,
                    bytes: 33,
                    rows: 3,
                    batches: 2,
                    stream_epoch: 4,
                    committed_offset: 33,
                    finalized: false,
                },
                MapOutputPartitionMeta {
                    reduce_partition: 1,
                    bytes: 55,
                    rows: 5,
                    batches: 3,
                    stream_epoch: 4,
                    committed_offset: 55,
                    finalized: true,
                },
            ],
        )
        .expect("register");
        let boundaries = c
            .map_output_readable_boundaries(
                "306",
                map_task.stage_id,
                map_task.task_id,
                map_task.attempt,
            )
            .expect("boundaries");
        assert_eq!(boundaries.len(), 2);
        assert_eq!(boundaries[0].reduce_partition, 1);
        assert_eq!(boundaries[0].committed_offset, 55);
        assert!(boundaries[0].finalized);
        assert_eq!(boundaries[1].reduce_partition, 2);
        assert_eq!(boundaries[1].stream_epoch, 4);
        assert_eq!(boundaries[1].committed_offset, 33);
        assert!(!boundaries[1].finalized);
    }
}
