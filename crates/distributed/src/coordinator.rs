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

use ffq_common::metrics::global_metrics;
use ffq_common::{FfqError, Result, SchemaInferencePolicy};
use ffq_planner::{ExchangeExec, PartitioningSpec, PhysicalPlan};
use ffq_shuffle::ShuffleReader;
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
    metrics: StageMetrics,
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
    message: String,
}

#[derive(Debug, Clone, Default)]
struct WorkerHeartbeat {
    last_seen_ms: u64,
    custom_operator_capabilities: HashSet<String>,
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
        let worker_caps = self
            .worker_heartbeats
            .get(worker_id)
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
            let mut query_budget = self
                .config
                .max_concurrent_tasks_per_query
                .saturating_sub(running_for_query);
            maybe_apply_adaptive_partition_layout(
                query_id,
                query,
                &map_outputs_snapshot,
                self.config.adaptive_shuffle_target_bytes,
                self.config.adaptive_shuffle_min_reduce_tasks,
                self.config.adaptive_shuffle_max_reduce_tasks,
                self.config.adaptive_shuffle_max_partitions_per_task,
                now,
            );
            let latest_attempts = latest_attempt_map(query);
            for stage_id in runnable_stages(query) {
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
                    if !worker_supports_task(worker_caps.as_ref(), &task.required_custom_ops) {
                        continue;
                    }
                    task.state = TaskState::Running;
                    task.assigned_worker = Some(worker_id.to_string());
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
                    });
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
        let now = now_ms()?;
        self.requeue_stale_workers(now)?;
        let query = self
            .queries
            .get_mut(query_id)
            .ok_or_else(|| FfqError::Planning(format!("unknown query: {query_id}")))?;
        let latest_attempt = latest_attempt_map(query)
            .get(&(stage_id, task_id))
            .copied()
            .unwrap_or(attempt);
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

        if prev_state == state {
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
        let assigned_worker_cached = query
            .tasks
            .get(&key)
            .and_then(|t| t.assigned_worker.clone());
        if let Some(task) = query.tasks.get_mut(&key) {
            task.state = state;
            task.message = message.clone();
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
        self.map_outputs
            .insert((query_id.clone(), stage_id, map_task, attempt), partitions);
        let latest = self.latest_map_partitions_for_stage(&query_id, stage_id);
        let mut rows = 0_u64;
        let mut bytes = 0_u64;
        let mut batches = 0_u64;
        let mut reduce_ids = HashSet::new();
        for p in latest {
            rows = rows.saturating_add(p.rows);
            bytes = bytes.saturating_add(p.bytes);
            batches = batches.saturating_add(p.batches);
            reduce_ids.insert(p.reduce_partition);
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

        for child_stage_id in stage.children.clone() {
            if let Some(child) = query.stages.get_mut(&child_stage_id) {
                child.metrics.planned_reduce_tasks = planned_reduce_tasks;
                child.metrics.adaptive_reduce_tasks = adaptive_reduce_tasks;
                child.metrics.adaptive_target_bytes = self.config.adaptive_shuffle_target_bytes;
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

    /// Read shuffle partition bytes for the requested map attempt.
    pub fn fetch_shuffle_partition_chunks(
        &self,
        query_id: &str,
        stage_id: u64,
        map_task: u64,
        attempt: u32,
        reduce_partition: u32,
    ) -> Result<Vec<Vec<u8>>> {
        let key = (query_id.to_string(), stage_id, map_task, attempt);
        if !self.map_outputs.contains_key(&key) {
            return Err(FfqError::Planning(
                "map output not registered for requested attempt".to_string(),
            ));
        }

        let query_num = query_id.parse::<u64>().map_err(|e| {
            FfqError::InvalidConfig(format!(
                "query_id must be numeric for shuffle path layout in v1: {e}"
            ))
        })?;
        let reader = ShuffleReader::new(&self.config.shuffle_root);
        reader.fetch_partition_chunks(query_num, stage_id, map_task, attempt, reduce_partition)
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
                metrics: StageMetrics {
                    queued_tasks: task_count,
                    planned_reduce_tasks: task_count,
                    adaptive_reduce_tasks: task_count,
                    ..StageMetrics::default()
                },
            },
        );
        // v1 simplification: each scheduled task carries the submitted physical plan bytes.
        // Stage boundaries are still respected by coordinator scheduling.
        let fragment = physical_plan_json.to_vec();
        for task_id in 0..task_count {
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

fn maybe_apply_adaptive_partition_layout(
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
    for stage_id in runnable_stages(query) {
        let Some(stage) = query.stages.get(&stage_id) else {
            continue;
        };
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
        if bytes_by_partition.is_empty() {
            continue;
        }
        let groups = deterministic_coalesce_split_groups(
            stage.metrics.planned_reduce_tasks,
            target_bytes,
            &bytes_by_partition,
            min_reduce_tasks,
            max_reduce_tasks,
            max_partitions_per_task,
        );
        let current_tasks = latest_states
            .iter()
            .filter(|((sid, _), _)| *sid == stage_id)
            .count() as u32;
        if (groups.len() as u32) != current_tasks {
            stages_to_rewire.push((stage_id, groups));
        }
    }

    for (stage_id, groups) in stages_to_rewire {
        let Some(template) = query
            .tasks
            .values()
            .find(|t| t.stage_id == stage_id && t.state == TaskState::Queued)
            .map(|t| {
                (
                    t.plan_fragment_json.clone(),
                    t.required_custom_ops.clone(),
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
        query.tasks.retain(|(sid, _, _), _| *sid != stage_id);
        for (task_id, assignment) in groups.into_iter().enumerate() {
            query.tasks.insert(
                (stage_id, task_id as u64, 1),
                TaskRuntime {
                    query_id: template.2.clone(),
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
                    message: String::new(),
                },
            );
        }
        if let Some(stage) = query.stages.get_mut(&stage_id) {
            stage.layout_version = layout_version;
            stage.metrics.queued_tasks = query
                .tasks
                .values()
                .filter(|t| t.stage_id == stage_id && t.state == TaskState::Queued)
                .count() as u32;
            stage.metrics.adaptive_reduce_tasks = stage.metrics.queued_tasks;
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReduceTaskAssignmentSpec {
    assigned_reduce_partitions: Vec<u32>,
    assigned_reduce_split_index: u32,
    assigned_reduce_split_count: u32,
}

fn deterministic_coalesce_split_groups(
    planned_partitions: u32,
    target_bytes: u64,
    bytes_by_partition: &HashMap<u32, u64>,
    min_reduce_tasks: u32,
    max_reduce_tasks: u32,
    max_partitions_per_task: u32,
) -> Vec<ReduceTaskAssignmentSpec> {
    if planned_partitions <= 1 {
        return vec![ReduceTaskAssignmentSpec {
            assigned_reduce_partitions: vec![0],
            assigned_reduce_split_index: 0,
            assigned_reduce_split_count: 1,
        }];
    }
    if target_bytes == 0 {
        return (0..planned_partitions)
            .map(|p| ReduceTaskAssignmentSpec {
                assigned_reduce_partitions: vec![p],
                assigned_reduce_split_index: 0,
                assigned_reduce_split_count: 1,
            })
            .collect();
    }
    let mut groups = Vec::new();
    let mut current = Vec::new();
    let mut current_bytes = 0_u64;
    for p in 0..planned_partitions {
        let bytes = *bytes_by_partition.get(&p).unwrap_or(&0);
        if !current.is_empty() && current_bytes.saturating_add(bytes) > target_bytes {
            groups.push(current);
            current = Vec::new();
            current_bytes = 0;
        }
        current.push(p);
        current_bytes = current_bytes.saturating_add(bytes);
    }
    if !current.is_empty() {
        groups.push(current);
    }
    let groups = split_groups_by_max_partitions(groups, max_partitions_per_task);
    let groups = clamp_group_count_to_bounds(
        groups,
        planned_partitions,
        min_reduce_tasks,
        max_reduce_tasks,
    );
    apply_hot_partition_splitting(
        groups,
        bytes_by_partition,
        target_bytes,
        min_reduce_tasks,
        max_reduce_tasks,
    )
}

fn split_groups_by_max_partitions(
    groups: Vec<Vec<u32>>,
    max_partitions_per_task: u32,
) -> Vec<Vec<u32>> {
    if max_partitions_per_task == 0 {
        return groups;
    }
    let cap = max_partitions_per_task as usize;
    let mut out = Vec::new();
    for g in groups {
        if g.len() <= cap {
            out.push(g);
            continue;
        }
        let mut i = 0usize;
        while i < g.len() {
            let end = (i + cap).min(g.len());
            out.push(g[i..end].to_vec());
            i = end;
        }
    }
    out
}

fn clamp_group_count_to_bounds(
    mut groups: Vec<Vec<u32>>,
    planned_partitions: u32,
    min_reduce_tasks: u32,
    max_reduce_tasks: u32,
) -> Vec<Vec<u32>> {
    let min_eff = min_reduce_tasks.max(1).min(planned_partitions) as usize;
    let mut max_eff = if max_reduce_tasks == 0 {
        planned_partitions
    } else {
        max_reduce_tasks
    }
    .max(min_eff as u32)
    .min(planned_partitions) as usize;
    if max_eff == 0 {
        max_eff = 1;
    }

    // Deterministic split (left-to-right): keep splitting the first splittable group.
    while groups.len() < min_eff {
        let Some(idx) = groups.iter().position(|g| g.len() > 1) else {
            break;
        };
        let g = groups.remove(idx);
        let split_at = g.len() / 2;
        groups.insert(idx, g[split_at..].to_vec());
        groups.insert(idx, g[..split_at].to_vec());
    }

    // Deterministic merge (right-to-left): merge last two groups until within max.
    while groups.len() > max_eff && groups.len() >= 2 {
        let right = groups.pop().expect("has right group");
        if let Some(prev) = groups.last_mut() {
            prev.extend(right);
        }
    }
    groups
}

fn apply_hot_partition_splitting(
    groups: Vec<Vec<u32>>,
    bytes_by_partition: &HashMap<u32, u64>,
    target_bytes: u64,
    min_reduce_tasks: u32,
    max_reduce_tasks: u32,
) -> Vec<ReduceTaskAssignmentSpec> {
    let mut layouts = groups
        .into_iter()
        .map(|g| ReduceTaskAssignmentSpec {
            assigned_reduce_partitions: g,
            assigned_reduce_split_index: 0,
            assigned_reduce_split_count: 1,
        })
        .collect::<Vec<_>>();
    if target_bytes == 0 {
        return layouts;
    }
    let min_eff = min_reduce_tasks.max(1);
    let max_eff = if max_reduce_tasks == 0 {
        u32::MAX
    } else {
        max_reduce_tasks.max(min_eff)
    };
    let mut hot = bytes_by_partition
        .iter()
        .map(|(p, b)| (*p, *b))
        .collect::<Vec<_>>();
    hot.sort_by_key(|(p, _)| *p);
    for (partition, bytes) in hot {
        if bytes <= target_bytes {
            continue;
        }
        let Some(idx) = layouts.iter().position(|l| {
            l.assigned_reduce_split_count == 1
                && l.assigned_reduce_partitions.len() == 1
                && l.assigned_reduce_partitions[0] == partition
        }) else {
            continue;
        };
        let desired = bytes.div_ceil(target_bytes).max(2) as u32;
        let current_tasks = layouts.len() as u32;
        let max_for_this = 1 + max_eff.saturating_sub(current_tasks);
        let split_count = desired.min(max_for_this);
        if split_count <= 1 {
            continue;
        }
        layouts.remove(idx);
        for split_index in (0..split_count).rev() {
            layouts.insert(
                idx,
                ReduceTaskAssignmentSpec {
                    assigned_reduce_partitions: vec![partition],
                    assigned_reduce_split_index: split_index,
                    assigned_reduce_split_count: split_count,
                },
            );
        }
    }
    layouts
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

fn worker_supports_task(caps: Option<&HashSet<String>>, required_custom_ops: &[String]) -> bool {
    if required_custom_ops.is_empty() {
        return true;
    }
    let Some(caps) = caps else {
        return false;
    };
    required_custom_ops.iter().all(|op| caps.contains(op))
}

fn runnable_stages(query: &QueryRuntime) -> Vec<u64> {
    let mut out = Vec::new();
    for (sid, stage) in &query.stages {
        let all_parents_done = stage.parents.iter().all(|pid| {
            latest_task_states(query)
                .into_iter()
                .filter(|((stage_id, _), _)| stage_id == pid)
                .all(|(_, state)| state == TaskState::Succeeded)
        });
        if all_parents_done {
            out.push(*sid);
        }
    }
    out
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
                },
                MapOutputPartitionMeta {
                    reduce_partition: 1,
                    bytes: 20,
                    rows: 1,
                    batches: 1,
                },
                MapOutputPartitionMeta {
                    reduce_partition: 2,
                    bytes: 30,
                    rows: 1,
                    batches: 1,
                },
                MapOutputPartitionMeta {
                    reduce_partition: 3,
                    bytes: 40,
                    rows: 1,
                    batches: 1,
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
                },
                MapOutputPartitionMeta {
                    reduce_partition: 1,
                    bytes: 5,
                    rows: 1,
                    batches: 1,
                },
                MapOutputPartitionMeta {
                    reduce_partition: 2,
                    bytes: 5,
                    rows: 1,
                    batches: 1,
                },
                MapOutputPartitionMeta {
                    reduce_partition: 3,
                    bytes: 5,
                    rows: 1,
                    batches: 1,
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
                },
                MapOutputPartitionMeta {
                    reduce_partition: 1,
                    bytes: 5,
                    rows: 1,
                    batches: 1,
                },
                MapOutputPartitionMeta {
                    reduce_partition: 2,
                    bytes: 5,
                    rows: 1,
                    batches: 1,
                },
                MapOutputPartitionMeta {
                    reduce_partition: 3,
                    bytes: 5,
                    rows: 1,
                    batches: 1,
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
                },
                MapOutputPartitionMeta {
                    reduce_partition: 1,
                    bytes: 120,
                    rows: 1,
                    batches: 1,
                },
                MapOutputPartitionMeta {
                    reduce_partition: 2,
                    bytes: 8,
                    rows: 1,
                    batches: 1,
                },
                MapOutputPartitionMeta {
                    reduce_partition: 3,
                    bytes: 8,
                    rows: 1,
                    batches: 1,
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
    }
}
