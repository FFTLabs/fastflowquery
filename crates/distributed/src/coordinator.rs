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
pub struct CoordinatorConfig {
    pub blacklist_failure_threshold: u32,
    pub shuffle_root: PathBuf,
    pub schema_inference: SchemaInferencePolicy,
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            blacklist_failure_threshold: 3,
            shuffle_root: PathBuf::from("."),
            schema_inference: SchemaInferencePolicy::On,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryState {
    Queued,
    Running,
    Succeeded,
    Failed,
    Canceled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskState {
    Queued,
    Running,
    Succeeded,
    Failed,
}

#[derive(Debug, Clone)]
pub struct TaskAssignment {
    pub query_id: String,
    pub stage_id: u64,
    pub task_id: u64,
    pub attempt: u32,
    pub plan_fragment_json: Vec<u8>,
}

#[derive(Debug, Clone, Default)]
pub struct StageMetrics {
    pub queued_tasks: u32,
    pub running_tasks: u32,
    pub succeeded_tasks: u32,
    pub failed_tasks: u32,
    pub map_output_rows: u64,
    pub map_output_bytes: u64,
    pub map_output_batches: u64,
}

#[derive(Debug, Clone)]
pub struct MapOutputPartitionMeta {
    pub reduce_partition: u32,
    pub bytes: u64,
    pub rows: u64,
    pub batches: u64,
}

#[derive(Debug, Clone)]
pub struct QueryStatus {
    pub query_id: String,
    pub state: QueryState,
    pub submitted_at_ms: u64,
    pub started_at_ms: u64,
    pub finished_at_ms: u64,
    pub message: String,
    pub total_tasks: u32,
    pub queued_tasks: u32,
    pub running_tasks: u32,
    pub succeeded_tasks: u32,
    pub failed_tasks: u32,
    pub stage_metrics: HashMap<u64, StageMetrics>,
}

#[derive(Debug, Clone)]
struct StageRuntime {
    parents: Vec<u64>,
    children: Vec<u64>,
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
    plan_fragment_json: Vec<u8>,
    message: String,
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
pub struct Coordinator {
    config: CoordinatorConfig,
    catalog: Catalog,
    queries: HashMap<String, QueryRuntime>,
    map_outputs: HashMap<(String, u64, u64, u32), Vec<MapOutputPartitionMeta>>,
    query_results: HashMap<String, Vec<u8>>,
    blacklisted_workers: HashSet<String>,
    worker_failures: HashMap<String, u32>,
}

impl Coordinator {
    pub fn new(config: CoordinatorConfig) -> Self {
        Self {
            config,
            catalog: Catalog::new(),
            ..Self::default()
        }
    }

    pub fn with_catalog(config: CoordinatorConfig, catalog: Catalog) -> Self {
        Self {
            config,
            catalog,
            ..Self::default()
        }
    }

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

    pub fn get_task(&mut self, worker_id: &str, capacity: u32) -> Result<Vec<TaskAssignment>> {
        if self.blacklisted_workers.contains(worker_id) || capacity == 0 {
            debug!(
                worker_id = %worker_id,
                capacity,
                operator = "CoordinatorGetTask",
                "no tasks assigned (blacklisted or no capacity)"
            );
            return Ok(Vec::new());
        }
        let mut out = Vec::new();

        for query in self.queries.values_mut() {
            if !matches!(query.state, QueryState::Queued | QueryState::Running) {
                continue;
            }

            if query.state == QueryState::Queued {
                query.state = QueryState::Running;
                query.started_at_ms = now_ms()?;
            }

            for stage_id in runnable_stages(query) {
                for task in query
                    .tasks
                    .values_mut()
                    .filter(|t| t.stage_id == stage_id && t.state == TaskState::Queued)
                {
                    if out.len() as u32 >= capacity {
                        return Ok(out);
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
        let query = self
            .queries
            .get_mut(query_id)
            .ok_or_else(|| FfqError::Planning(format!("unknown query: {query_id}")))?;
        let key = (stage_id, task_id, attempt);
        let task = query
            .tasks
            .get_mut(&key)
            .ok_or_else(|| FfqError::Planning("unknown task status report".to_string()))?;

        if task.state == state {
            return Ok(());
        }
        let stage = query
            .stages
            .get_mut(&stage_id)
            .ok_or_else(|| FfqError::Execution("task stage not found".to_string()))?;
        if task.state == TaskState::Running {
            stage.metrics.running_tasks = stage.metrics.running_tasks.saturating_sub(1);
        }

        task.state = state;
        task.message = message.clone();
        match state {
            TaskState::Queued => {
                stage.metrics.queued_tasks += 1;
                if attempt > 0 {
                    global_metrics().inc_scheduler_retries(query_id, stage_id);
                }
            }
            TaskState::Running => stage.metrics.running_tasks += 1,
            TaskState::Succeeded => stage.metrics.succeeded_tasks += 1,
            TaskState::Failed => {
                stage.metrics.failed_tasks += 1;
                if let Some(worker) = worker_id.or(task.assigned_worker.as_deref()) {
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
                query.state = QueryState::Failed;
                query.finished_at_ms = now_ms()?;
                query.message = message;
            }
        }
        update_scheduler_metrics(query_id, stage_id, &stage.metrics);

        if query.state != QueryState::Failed && is_query_succeeded(query) {
            query.state = QueryState::Succeeded;
            query.finished_at_ms = now_ms()?;
            info!(
                query_id = %query_id,
                operator = "CoordinatorReportTaskStatus",
                "query reached succeeded state"
            );
        }

        Ok(())
    }

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

    pub fn get_query_status(&self, query_id: &str) -> Result<QueryStatus> {
        let query = self
            .queries
            .get(query_id)
            .ok_or_else(|| FfqError::Planning(format!("unknown query: {query_id}")))?;
        Ok(build_query_status(query_id, query))
    }

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

    pub fn map_output_registry_size(&self) -> usize {
        self.map_outputs.len()
    }

    pub fn register_query_results(&mut self, query_id: String, ipc_payload: Vec<u8>) -> Result<()> {
        if !self.queries.contains_key(&query_id) {
            return Err(FfqError::Planning(format!("unknown query: {query_id}")));
        }
        self.query_results.insert(query_id, ipc_payload);
        Ok(())
    }

    pub fn fetch_query_results(&self, query_id: &str) -> Result<Vec<u8>> {
        if !self.queries.contains_key(query_id) {
            return Err(FfqError::Planning(format!("unknown query: {query_id}")));
        }
        self.query_results
            .get(query_id)
            .cloned()
            .ok_or_else(|| FfqError::Execution("query results not ready".to_string()))
    }

    pub fn is_worker_blacklisted(&self, worker_id: &str) -> bool {
        self.blacklisted_workers.contains(worker_id)
    }

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
                children: node.children.iter().map(|c| c.0 as u64).collect(),
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
            query
                .tasks
                .values()
                .filter(|t| t.stage_id == *pid)
                .all(|t| t.state == TaskState::Succeeded)
        });
        if all_parents_done {
            out.push(*sid);
        }
    }
    out
}

fn is_query_succeeded(query: &QueryRuntime) -> bool {
    query
        .tasks
        .values()
        .all(|t| t.state == TaskState::Succeeded)
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
}
