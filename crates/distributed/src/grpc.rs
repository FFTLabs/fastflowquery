//! gRPC service/client glue for coordinator and worker shuffle services.
//!
//! RPC schema source: `proto/ffq_distributed.proto`.
//!
//! Key control-plane RPCs (generated under [`v1`]):
//! - `SubmitQuery`, `GetTask`, `ReportTaskStatus`
//! - `GetQueryStatus`, `CancelQuery`
//! - `RegisterQueryResults`, `FetchQueryResults`
//!
//! Key shuffle/data RPCs:
//! - `RegisterMapOutput`
//! - `FetchShufflePartition` (stream)
//! - `Heartbeat`
//!
//! Useful generated request/response types:
//! [`v1::SubmitQueryRequest`], [`v1::GetTaskRequest`],
//! [`v1::ReportTaskStatusRequest`], [`v1::GetQueryStatusRequest`],
//! [`v1::RegisterMapOutputRequest`], [`v1::FetchShufflePartitionRequest`],
//! [`v1::FetchQueryResultsRequest`].

use std::sync::Arc;
use std::{collections::HashMap, path::PathBuf};

use ffq_shuffle::ShuffleReader;
use tokio::sync::Mutex;
use tokio_stream::{self as stream, Stream};
use tonic::{Request, Response, Status};

use crate::coordinator::{
    Coordinator, MapOutputPartitionMeta, QueryState as CoreQueryState,
    QueryStatus as CoreQueryStatus, TaskAssignment as CoreTaskAssignment,
    TaskState as CoreTaskState,
};

#[allow(missing_docs)]
pub mod v1 {
    tonic::include_proto!("ffq.distributed.v1");
}

pub use v1::control_plane_client::ControlPlaneClient;
pub use v1::control_plane_server::{ControlPlane, ControlPlaneServer};
pub use v1::heartbeat_service_client::HeartbeatServiceClient;
pub use v1::heartbeat_service_server::{HeartbeatService, HeartbeatServiceServer};
pub use v1::shuffle_service_client::ShuffleServiceClient;
pub use v1::shuffle_service_server::{ShuffleService, ShuffleServiceServer};

#[derive(Clone)]
/// Combined gRPC service implementation backed by shared [`Coordinator`].
pub struct CoordinatorServices {
    coordinator: Arc<Mutex<Coordinator>>,
}

impl CoordinatorServices {
    /// Build services from an owned coordinator instance.
    pub fn new(coordinator: Coordinator) -> Self {
        Self {
            coordinator: Arc::new(Mutex::new(coordinator)),
        }
    }

    /// Build services from shared coordinator state.
    pub fn from_shared(coordinator: Arc<Mutex<Coordinator>>) -> Self {
        Self { coordinator }
    }

    /// Access shared coordinator state.
    pub fn coordinator(&self) -> Arc<Mutex<Coordinator>> {
        Arc::clone(&self.coordinator)
    }
}

#[tonic::async_trait]
impl ControlPlane for CoordinatorServices {
    async fn submit_query(
        &self,
        request: Request<v1::SubmitQueryRequest>,
    ) -> Result<Response<v1::SubmitQueryResponse>, Status> {
        let req = request.into_inner();
        let mut coordinator = self.coordinator.lock().await;
        let state = coordinator
            .submit_query(req.query_id.clone(), &req.physical_plan_json)
            .map_err(to_status)?;
        Ok(Response::new(v1::SubmitQueryResponse {
            query_id: req.query_id,
            state: proto_query_state(state) as i32,
        }))
    }

    async fn get_task(
        &self,
        request: Request<v1::GetTaskRequest>,
    ) -> Result<Response<v1::GetTaskResponse>, Status> {
        let req = request.into_inner();
        let mut coordinator = self.coordinator.lock().await;
        let tasks = coordinator
            .get_task(&req.worker_id, req.capacity)
            .map_err(to_status)?;
        Ok(Response::new(v1::GetTaskResponse {
            tasks: tasks.into_iter().map(proto_task_assignment).collect(),
        }))
    }

    async fn report_task_status(
        &self,
        request: Request<v1::ReportTaskStatusRequest>,
    ) -> Result<Response<v1::ReportTaskStatusResponse>, Status> {
        let req = request.into_inner();
        let mut coordinator = self.coordinator.lock().await;
        coordinator
            .report_task_status(
                &req.query_id,
                req.stage_id,
                req.task_id,
                req.attempt,
                req.layout_version,
                req.layout_fingerprint,
                core_task_state(req.state)?,
                None,
                req.message,
            )
            .map_err(to_status)?;
        Ok(Response::new(v1::ReportTaskStatusResponse {}))
    }

    async fn get_query_status(
        &self,
        request: Request<v1::GetQueryStatusRequest>,
    ) -> Result<Response<v1::GetQueryStatusResponse>, Status> {
        let req = request.into_inner();
        let coordinator = self.coordinator.lock().await;
        let status = coordinator
            .get_query_status(&req.query_id)
            .map_err(to_status)?;
        Ok(Response::new(v1::GetQueryStatusResponse {
            status: Some(proto_query_status(status)),
        }))
    }

    async fn cancel_query(
        &self,
        request: Request<v1::CancelQueryRequest>,
    ) -> Result<Response<v1::CancelQueryResponse>, Status> {
        let req = request.into_inner();
        let mut coordinator = self.coordinator.lock().await;
        let state = coordinator
            .cancel_query(&req.query_id, &req.reason)
            .map_err(to_status)?;
        Ok(Response::new(v1::CancelQueryResponse {
            query_id: req.query_id,
            state: proto_query_state(state) as i32,
        }))
    }

    async fn register_query_results(
        &self,
        request: Request<v1::RegisterQueryResultsRequest>,
    ) -> Result<Response<v1::RegisterQueryResultsResponse>, Status> {
        let req = request.into_inner();
        let mut coordinator = self.coordinator.lock().await;
        coordinator
            .register_query_results(req.query_id, req.ipc_payload)
            .map_err(to_status)?;
        Ok(Response::new(v1::RegisterQueryResultsResponse {}))
    }

    type FetchQueryResultsStream =
        std::pin::Pin<Box<dyn Stream<Item = Result<v1::QueryResultsChunk, Status>> + Send>>;

    async fn fetch_query_results(
        &self,
        request: Request<v1::FetchQueryResultsRequest>,
    ) -> Result<Response<Self::FetchQueryResultsStream>, Status> {
        let req = request.into_inner();
        let coordinator = self.coordinator.lock().await;
        let payload = coordinator
            .fetch_query_results(&req.query_id)
            .map_err(to_status)?;
        drop(coordinator);

        let mut out = Vec::new();
        let mut offset = 0_usize;
        let chunk_size = 64 * 1024;
        while offset < payload.len() {
            let end = (offset + chunk_size).min(payload.len());
            out.push(Ok(v1::QueryResultsChunk {
                payload: payload[offset..end].to_vec(),
            }));
            offset = end;
        }
        Ok(Response::new(Box::pin(stream::iter(out))))
    }
}

#[tonic::async_trait]
impl ShuffleService for CoordinatorServices {
    async fn register_map_output(
        &self,
        request: Request<v1::RegisterMapOutputRequest>,
    ) -> Result<Response<v1::RegisterMapOutputResponse>, Status> {
        let req = request.into_inner();
        let partitions = req
            .partitions
            .into_iter()
            .map(|p| MapOutputPartitionMeta {
                reduce_partition: p.reduce_partition,
                bytes: p.bytes,
                rows: p.rows,
                batches: p.batches,
            })
            .collect();
        let mut coordinator = self.coordinator.lock().await;
        coordinator
            .register_map_output(
                req.query_id,
                req.stage_id,
                req.map_task,
                req.attempt,
                req.layout_version,
                req.layout_fingerprint,
                partitions,
            )
            .map_err(to_status)?;
        Ok(Response::new(v1::RegisterMapOutputResponse {}))
    }

    type FetchShufflePartitionStream =
        std::pin::Pin<Box<dyn Stream<Item = Result<v1::ShufflePartitionChunk, Status>> + Send>>;

    async fn fetch_shuffle_partition(
        &self,
        request: Request<v1::FetchShufflePartitionRequest>,
    ) -> Result<Response<Self::FetchShufflePartitionStream>, Status> {
        let req = request.into_inner();
        let coordinator = self.coordinator.lock().await;
        let chunks = coordinator
            .fetch_shuffle_partition_chunks(
                &req.query_id,
                req.stage_id,
                req.map_task,
                req.attempt,
                req.reduce_partition,
            )
            .map_err(to_status)?;
        drop(coordinator);

        let out = chunks
            .into_iter()
            .map(|payload| Ok(v1::ShufflePartitionChunk { payload }));
        Ok(Response::new(Box::pin(stream::iter(out))))
    }
}

#[tonic::async_trait]
impl HeartbeatService for CoordinatorServices {
    async fn heartbeat(
        &self,
        request: Request<v1::HeartbeatRequest>,
    ) -> Result<Response<v1::HeartbeatResponse>, Status> {
        let req = request.into_inner();
        let mut coordinator = self.coordinator.lock().await;
        coordinator
            .heartbeat(
                &req.worker_id,
                req.running_tasks,
                &req.custom_operator_capabilities,
            )
            .map_err(to_status)?;
        Ok(Response::new(v1::HeartbeatResponse { accepted: true }))
    }
}

fn proto_query_state(state: CoreQueryState) -> v1::QueryState {
    match state {
        CoreQueryState::Queued => v1::QueryState::Queued,
        CoreQueryState::Running => v1::QueryState::Running,
        CoreQueryState::Succeeded => v1::QueryState::Succeeded,
        CoreQueryState::Failed => v1::QueryState::Failed,
        CoreQueryState::Canceled => v1::QueryState::Canceled,
    }
}

fn core_task_state(state: i32) -> Result<CoreTaskState, Status> {
    let parsed = v1::TaskState::try_from(state)
        .map_err(|_| Status::invalid_argument(format!("invalid task state value: {state}")))?;
    match parsed {
        v1::TaskState::Queued => Ok(CoreTaskState::Queued),
        v1::TaskState::Running => Ok(CoreTaskState::Running),
        v1::TaskState::Succeeded => Ok(CoreTaskState::Succeeded),
        v1::TaskState::Failed => Ok(CoreTaskState::Failed),
        v1::TaskState::Unspecified => Err(Status::invalid_argument("task state unspecified")),
    }
}

fn proto_task_assignment(task: CoreTaskAssignment) -> v1::TaskAssignment {
    v1::TaskAssignment {
        query_id: task.query_id,
        stage_id: task.stage_id,
        task_id: task.task_id,
        attempt: task.attempt,
        plan_fragment_json: task.plan_fragment_json,
        assigned_reduce_partitions: task.assigned_reduce_partitions,
        assigned_reduce_split_index: task.assigned_reduce_split_index,
        assigned_reduce_split_count: task.assigned_reduce_split_count,
        layout_version: task.layout_version,
        layout_fingerprint: task.layout_fingerprint,
    }
}

fn proto_query_status(status: CoreQueryStatus) -> v1::QueryStatus {
    let mut stage_metrics = status
        .stage_metrics
        .into_iter()
        .map(|(stage_id, m)| v1::StageMetrics {
            stage_id,
            queued_tasks: m.queued_tasks,
            running_tasks: m.running_tasks,
            succeeded_tasks: m.succeeded_tasks,
            failed_tasks: m.failed_tasks,
            map_output_rows: m.map_output_rows,
            map_output_bytes: m.map_output_bytes,
            map_output_batches: m.map_output_batches,
            map_output_partitions: m.map_output_partitions,
            planned_reduce_tasks: m.planned_reduce_tasks,
            adaptive_reduce_tasks: m.adaptive_reduce_tasks,
            adaptive_target_bytes: m.adaptive_target_bytes,
            aqe_events: m.aqe_events,
            partition_bytes_histogram: m
                .partition_bytes_histogram
                .into_iter()
                .map(|b| v1::PartitionBytesHistogramBucket {
                    upper_bound_bytes: b.upper_bound_bytes,
                    partition_count: b.partition_count,
                })
                .collect(),
            skew_split_tasks: m.skew_split_tasks,
            layout_finalize_count: m.layout_finalize_count,
        })
        .collect::<Vec<_>>();
    stage_metrics.sort_by_key(|m| m.stage_id);
    v1::QueryStatus {
        query_id: status.query_id,
        state: proto_query_state(status.state) as i32,
        submitted_at_ms: status.submitted_at_ms,
        started_at_ms: status.started_at_ms,
        finished_at_ms: status.finished_at_ms,
        message: status.message,
        stage_metrics,
    }
}

fn to_status(err: ffq_common::FfqError) -> Status {
    match err {
        ffq_common::FfqError::InvalidConfig(msg) => Status::invalid_argument(msg),
        ffq_common::FfqError::Planning(msg) => Status::failed_precondition(msg),
        ffq_common::FfqError::Execution(msg) => Status::internal(msg),
        ffq_common::FfqError::Io(e) => Status::internal(e.to_string()),
        ffq_common::FfqError::Unsupported(msg) => Status::unimplemented(msg),
    }
}

#[derive(Clone)]
/// Worker-local shuffle service that reads shuffle data from local filesystem.
pub struct WorkerShuffleService {
    shuffle_root: PathBuf,
    map_outputs: Arc<Mutex<HashMap<(String, u64, u64, u32), Vec<MapOutputPartitionMeta>>>>,
}

impl WorkerShuffleService {
    /// Create service bound to a shuffle root directory.
    pub fn new(shuffle_root: impl Into<PathBuf>) -> Self {
        Self {
            shuffle_root: shuffle_root.into(),
            map_outputs: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[tonic::async_trait]
impl ShuffleService for WorkerShuffleService {
    async fn register_map_output(
        &self,
        request: Request<v1::RegisterMapOutputRequest>,
    ) -> Result<Response<v1::RegisterMapOutputResponse>, Status> {
        let req = request.into_inner();
        let partitions = req
            .partitions
            .into_iter()
            .map(|p| MapOutputPartitionMeta {
                reduce_partition: p.reduce_partition,
                bytes: p.bytes,
                rows: p.rows,
                batches: p.batches,
            })
            .collect::<Vec<_>>();
        let key = (req.query_id, req.stage_id, req.map_task, req.attempt);
        self.map_outputs.lock().await.insert(key, partitions);
        Ok(Response::new(v1::RegisterMapOutputResponse {}))
    }

    type FetchShufflePartitionStream =
        std::pin::Pin<Box<dyn Stream<Item = Result<v1::ShufflePartitionChunk, Status>> + Send>>;

    async fn fetch_shuffle_partition(
        &self,
        request: Request<v1::FetchShufflePartitionRequest>,
    ) -> Result<Response<Self::FetchShufflePartitionStream>, Status> {
        let req = request.into_inner();
        let query_num = req
            .query_id
            .parse::<u64>()
            .map_err(|e| Status::invalid_argument(format!("query_id must be numeric: {e}")))?;
        let reader = ShuffleReader::new(&self.shuffle_root);
        let chunks = if req.attempt == 0 {
            let (_attempt, chunks) = reader
                .fetch_partition_chunks_latest(
                    query_num,
                    req.stage_id,
                    req.map_task,
                    req.reduce_partition,
                )
                .map_err(to_status)?;
            chunks
        } else {
            reader
                .fetch_partition_chunks(
                    query_num,
                    req.stage_id,
                    req.map_task,
                    req.attempt,
                    req.reduce_partition,
                )
                .map_err(to_status)?
        };

        let out = chunks
            .into_iter()
            .map(|payload| Ok(v1::ShufflePartitionChunk { payload }));
        Ok(Response::new(Box::pin(stream::iter(out))))
    }
}
