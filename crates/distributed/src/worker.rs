use std::path::PathBuf;
use std::sync::Arc;

use ffq_common::{FfqError, Result};
use ffq_planner::PhysicalPlan;
use tokio::sync::{Mutex, Semaphore};
use tonic::async_trait;

use crate::coordinator::{Coordinator, MapOutputPartitionMeta, TaskAssignment, TaskState};
use crate::grpc::v1;

#[derive(Debug, Clone)]
pub struct WorkerConfig {
    pub worker_id: String,
    pub cpu_slots: usize,
    pub per_task_memory_budget_bytes: usize,
    pub spill_dir: PathBuf,
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
pub struct TaskContext {
    pub per_task_memory_budget_bytes: usize,
    pub spill_dir: PathBuf,
    pub shuffle_root: PathBuf,
}

#[derive(Debug, Clone, Default)]
pub struct TaskExecutionResult {
    pub map_output_partitions: Vec<MapOutputPartitionMeta>,
    pub message: String,
}

#[async_trait]
pub trait WorkerControlPlane: Send + Sync {
    async fn get_task(&self, worker_id: &str, capacity: u32) -> Result<Vec<TaskAssignment>>;
    async fn report_task_status(
        &self,
        worker_id: &str,
        assignment: &TaskAssignment,
        state: TaskState,
        message: String,
    ) -> Result<()>;
    async fn register_map_output(
        &self,
        assignment: &TaskAssignment,
        partitions: Vec<MapOutputPartitionMeta>,
    ) -> Result<()>;
    async fn heartbeat(&self, worker_id: &str, running_tasks: u32) -> Result<()>;
}

#[async_trait]
pub trait TaskExecutor: Send + Sync {
    async fn execute(
        &self,
        assignment: &TaskAssignment,
        ctx: &TaskContext,
    ) -> Result<TaskExecutionResult>;
}

#[derive(Debug, Default)]
pub struct DefaultTaskExecutor;

#[async_trait]
impl TaskExecutor for DefaultTaskExecutor {
    async fn execute(
        &self,
        assignment: &TaskAssignment,
        _ctx: &TaskContext,
    ) -> Result<TaskExecutionResult> {
        let _plan: PhysicalPlan = serde_json::from_slice(&assignment.plan_fragment_json)
            .map_err(|e| FfqError::Execution(format!("task plan decode failed: {e}")))?;
        Ok(TaskExecutionResult::default())
    }
}

#[derive(Clone)]
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
    pub fn new(config: WorkerConfig, control_plane: Arc<C>, task_executor: Arc<E>) -> Self {
        let slots = config.cpu_slots.max(1);
        Self {
            config,
            control_plane,
            task_executor,
            cpu_slots: Arc::new(Semaphore::new(slots)),
        }
    }

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
                per_task_memory_budget_bytes: self.config.per_task_memory_budget_bytes,
                spill_dir: self.config.spill_dir.clone(),
                shuffle_root: self.config.shuffle_root.clone(),
            };
            handles.push(tokio::spawn(async move {
                let _permit = permit;
                let result = task_executor.execute(&assignment, &task_ctx).await;
                match result {
                    Ok(exec_result) => {
                        if !exec_result.map_output_partitions.is_empty() {
                            control_plane
                                .register_map_output(
                                    &assignment,
                                    exec_result.map_output_partitions.clone(),
                                )
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
pub struct InProcessControlPlane {
    coordinator: Arc<Mutex<Coordinator>>,
}

impl InProcessControlPlane {
    pub fn new(coordinator: Arc<Mutex<Coordinator>>) -> Self {
        Self { coordinator }
    }
}

#[derive(Debug)]
pub struct GrpcControlPlane {
    control: Mutex<crate::grpc::ControlPlaneClient<tonic::transport::Channel>>,
    shuffle: Mutex<crate::grpc::ShuffleServiceClient<tonic::transport::Channel>>,
    heartbeat: Mutex<crate::grpc::HeartbeatServiceClient<tonic::transport::Channel>>,
}

impl GrpcControlPlane {
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

    async fn heartbeat(&self, _worker_id: &str, _running_tasks: u32) -> Result<()> {
        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use ffq_planner::{
        create_physical_plan, AggExpr, Expr, JoinStrategyHint, JoinType, LogicalPlan,
        PhysicalPlannerConfig,
    };

    #[tokio::test]
    async fn coordinator_with_two_workers_runs_join_and_agg_query() {
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
                    strategy_hint: JoinStrategyHint::Auto,
                }),
            },
            &PhysicalPlannerConfig::default(),
        )
        .expect("physical plan");
        let physical_json = serde_json::to_vec(&physical).expect("physical json");

        let coordinator = Arc::new(Mutex::new(Coordinator::default()));
        {
            let mut c = coordinator.lock().await;
            c.submit_query("1001".to_string(), &physical_json)
                .expect("submit");
        }

        let control = Arc::new(InProcessControlPlane::new(Arc::clone(&coordinator)));
        let worker1 = Worker::new(
            WorkerConfig {
                worker_id: "w1".to_string(),
                cpu_slots: 1,
                ..WorkerConfig::default()
            },
            Arc::clone(&control),
            Arc::new(DefaultTaskExecutor),
        );
        let worker2 = Worker::new(
            WorkerConfig {
                worker_id: "w2".to_string(),
                cpu_slots: 1,
                ..WorkerConfig::default()
            },
            control,
            Arc::new(DefaultTaskExecutor),
        );

        for _ in 0..16 {
            let _ = worker1.poll_once().await.expect("worker1 poll");
            let _ = worker2.poll_once().await.expect("worker2 poll");
            let state = {
                let c = coordinator.lock().await;
                c.get_query_status("1001").expect("status").state
            };
            if state == crate::coordinator::QueryState::Succeeded {
                return;
            }
            assert_ne!(state, crate::coordinator::QueryState::Failed);
        }

        panic!("query did not finish in allotted polls");
    }
}
