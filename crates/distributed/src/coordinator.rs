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
use ffq_planner::{ExchangeExec, PhysicalPlan};
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
    message: String,
}

#[derive(Debug, Clone, Copy, Default)]
struct WorkerHeartbeat {
    last_seen_ms: u64,
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
            .insert(worker_id.to_string(), WorkerHeartbeat { last_seen_ms: now });
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
                    ));
                }
            }

            for (stage_id, task_id, attempt, fragment) in to_retry {
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
            PhysicalPlan::Project(x) => self.resolve_parquet_scan_schemas(&mut x.input),
            PhysicalPlan::CoalesceBatches(x) => self.resolve_parquet_scan_schemas(&mut x.input),
            PhysicalPlan::PartialHashAggregate(x) => {
                self.resolve_parquet_scan_schemas(&mut x.input)
            }
            PhysicalPlan::FinalHashAggregate(x) => self.resolve_parquet_scan_schemas(&mut x.input),
            PhysicalPlan::HashJoin(x) => {
                self.resolve_parquet_scan_schemas(&mut x.left)?;
                self.resolve_parquet_scan_schemas(&mut x.right)
            }
            PhysicalPlan::Exchange(x) => match x {
                ExchangeExec::ShuffleWrite(e) => self.resolve_parquet_scan_schemas(&mut e.input),
                ExchangeExec::ShuffleRead(e) => self.resolve_parquet_scan_schemas(&mut e.input),
                ExchangeExec::Broadcast(e) => self.resolve_parquet_scan_schemas(&mut e.input),
            },
            PhysicalPlan::Limit(x) => self.resolve_parquet_scan_schemas(&mut x.input),
            PhysicalPlan::TopKByScore(x) => self.resolve_parquet_scan_schemas(&mut x.input),
            PhysicalPlan::VectorTopK(_) => Ok(()),
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
        if remaining == 0 {
            return Ok(out);
        }

        for query in self.queries.values_mut() {
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
    pub fn heartbeat(&mut self, worker_id: &str, _running_tasks: u32) -> Result<()> {
        let now = now_ms()?;
        self.worker_heartbeats
            .insert(worker_id.to_string(), WorkerHeartbeat { last_seen_ms: now });
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
        partitions: Vec<MapOutputPartitionMeta>,
    ) -> Result<()> {
        let query = self
            .queries
            .get_mut(&query_id)
            .ok_or_else(|| FfqError::Planning(format!("unknown query: {query_id}")))?;
        let stage = query
            .stages
            .get_mut(&stage_id)
            .ok_or_else(|| FfqError::Planning(format!("unknown stage: {stage_id}")))?;

        for p in &partitions {
            stage.metrics.map_output_rows = stage.metrics.map_output_rows.saturating_add(p.rows);
            stage.metrics.map_output_bytes = stage.metrics.map_output_bytes.saturating_add(p.bytes);
            stage.metrics.map_output_batches =
                stage.metrics.map_output_batches.saturating_add(p.batches);
        }

        self.map_outputs
            .insert((query_id, stage_id, map_task, attempt), partitions);
        Ok(())
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

    for node in dag.stages {
        let sid = node.id.0 as u64;
        stages.insert(
            sid,
            StageRuntime {
                parents: node.parents.iter().map(|p| p.0 as u64).collect(),
                metrics: StageMetrics {
                    queued_tasks: 1,
                    ..StageMetrics::default()
                },
            },
        );
        // v1 simplification: each scheduled task carries the submitted physical plan bytes.
        // Stage boundaries are still respected by coordinator scheduling.
        let fragment = physical_plan_json.to_vec();
        tasks.insert(
            (sid, 0, 1),
            TaskRuntime {
                query_id: query_id.to_string(),
                stage_id: sid,
                task_id: 0,
                attempt: 1,
                state: TaskState::Queued,
                assigned_worker: None,
                ready_at_ms: submitted_at_ms,
                plan_fragment_json: fragment,
                message: String::new(),
            },
        );
    }

    Ok(QueryRuntime {
        state: QueryState::Queued,
        submitted_at_ms,
        started_at_ms: 0,
        finished_at_ms: 0,
        message: String::new(),
        stages,
        tasks,
    })
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

fn now_ms() -> Result<u64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| FfqError::Execution(format!("clock error: {e}")))?
        .as_millis() as u64)
}

#[cfg(test)]
mod tests {
    use std::thread;
    use std::time::Duration;

    use super::*;
    use arrow_schema::Schema;
    use ffq_planner::{ParquetScanExec, PhysicalPlan};

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
        c.heartbeat("w1", 0).expect("heartbeat");

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
            TaskState::Succeeded,
            Some("w1"),
            "ok".to_string(),
        )
        .expect("mark success");

        let third_pull = c.get_task("w1", 10).expect("third pull");
        assert_eq!(third_pull.len(), 1);
    }
}
