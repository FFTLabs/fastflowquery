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
use std::time::{SystemTime, UNIX_EPOCH};
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
            .report_task_status_with_pressure(
                &req.query_id,
                req.stage_id,
                req.task_id,
                req.attempt,
                req.layout_version,
                req.layout_fingerprint,
                core_task_state(req.state)?,
                None,
                req.message,
                req.reduce_fetch_inflight_bytes,
                req.reduce_fetch_queue_depth,
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
                stream_epoch: p.stream_epoch,
                committed_offset: p.committed_offset,
                finalized: p.finalized,
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
            .fetch_shuffle_partition_chunks_range(
                &req.query_id,
                req.stage_id,
                req.map_task,
                req.attempt,
                req.layout_version,
                req.reduce_partition,
                req.min_stream_epoch,
                req.start_offset,
                req.max_bytes,
            )
            .map_err(to_status)?;
        drop(coordinator);

        let out = chunks.into_iter().map(|c| {
            Ok(v1::ShufflePartitionChunk {
                payload: c.payload,
                start_offset: c.start_offset,
                end_offset: c.end_offset,
                watermark_offset: c.watermark_offset,
                finalized: c.finalized,
                stream_epoch: c.stream_epoch,
            })
        });
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
        recommended_map_output_publish_window_partitions: task
            .recommended_map_output_publish_window_partitions,
        recommended_reduce_fetch_window_partitions: task.recommended_reduce_fetch_window_partitions,
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
            backpressure_inflight_bytes: m.backpressure_inflight_bytes,
            backpressure_queue_depth: m.backpressure_queue_depth,
            map_publish_window_partitions: m.map_publish_window_partitions,
            reduce_fetch_window_partitions: m.reduce_fetch_window_partitions,
            first_chunk_ms: m.first_chunk_ms,
            first_reduce_row_ms: m.first_reduce_row_ms,
            stream_lag_ms: m.stream_lag_ms,
            stream_buffered_bytes: m.stream_buffered_bytes,
            stream_active_count: m.stream_active_count,
            backpressure_events: m.backpressure_events,
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
    layout_versions: Arc<Mutex<HashMap<(String, u64, u64, u32), u32>>>,
    last_touched_ms: Arc<Mutex<HashMap<(String, u64, u64, u32), u64>>>,
    max_active_streams: usize,
    max_partitions_per_stream: usize,
    max_chunks_per_response: usize,
    inactive_stream_ttl_ms: u64,
}

impl WorkerShuffleService {
    /// Create service bound to a shuffle root directory.
    pub fn new(shuffle_root: impl Into<PathBuf>) -> Self {
        Self::with_limits(
            shuffle_root,
            4096,
            65536,
            1024,
            10 * 60 * 1000, // 10 minutes
        )
    }

    /// Create service with explicit guardrail limits.
    pub fn with_limits(
        shuffle_root: impl Into<PathBuf>,
        max_active_streams: usize,
        max_partitions_per_stream: usize,
        max_chunks_per_response: usize,
        inactive_stream_ttl_ms: u64,
    ) -> Self {
        Self {
            shuffle_root: shuffle_root.into(),
            map_outputs: Arc::new(Mutex::new(HashMap::new())),
            layout_versions: Arc::new(Mutex::new(HashMap::new())),
            last_touched_ms: Arc::new(Mutex::new(HashMap::new())),
            max_active_streams: max_active_streams.max(1),
            max_partitions_per_stream: max_partitions_per_stream.max(1),
            max_chunks_per_response: max_chunks_per_response.max(1),
            inactive_stream_ttl_ms,
        }
    }
}

#[tonic::async_trait]
impl ShuffleService for WorkerShuffleService {
    async fn register_map_output(
        &self,
        request: Request<v1::RegisterMapOutputRequest>,
    ) -> Result<Response<v1::RegisterMapOutputResponse>, Status> {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| Status::internal(format!("clock error: {e}")))?
            .as_millis() as u64;
        let req = request.into_inner();
        let partitions = req
            .partitions
            .into_iter()
            .map(|p| MapOutputPartitionMeta {
                reduce_partition: p.reduce_partition,
                bytes: p.bytes,
                rows: p.rows,
                batches: p.batches,
                stream_epoch: p.stream_epoch,
                committed_offset: p.committed_offset,
                finalized: p.finalized,
            })
            .collect::<Vec<_>>();
        if partitions.len() > self.max_partitions_per_stream {
            return Err(Status::resource_exhausted(format!(
                "stream metadata exceeds max_partitions_per_stream={} (got {})",
                self.max_partitions_per_stream,
                partitions.len()
            )));
        }
        let key = (req.query_id, req.stage_id, req.map_task, req.attempt);
        let mut touched = self.last_touched_ms.lock().await;
        if self.inactive_stream_ttl_ms > 0 {
            let stale_before = now_ms.saturating_sub(self.inactive_stream_ttl_ms);
            let stale_keys = touched
                .iter()
                .filter_map(|(k, ts)| (*ts <= stale_before).then_some(k.clone()))
                .collect::<Vec<_>>();
            if !stale_keys.is_empty() {
                let mut outputs = self.map_outputs.lock().await;
                let mut versions = self.layout_versions.lock().await;
                for k in stale_keys {
                    outputs.remove(&k);
                    versions.remove(&k);
                    touched.remove(&k);
                }
            }
        }
        if !touched.contains_key(&key) && touched.len() >= self.max_active_streams {
            let mut entries = touched
                .iter()
                .map(|(k, ts)| (k.clone(), *ts))
                .collect::<Vec<_>>();
            entries.sort_by_key(|(_, ts)| *ts);
            let evict_count = touched.len().saturating_sub(self.max_active_streams) + 1;
            let mut outputs = self.map_outputs.lock().await;
            let mut versions = self.layout_versions.lock().await;
            for (evict_key, _) in entries.into_iter().take(evict_count) {
                outputs.remove(&evict_key);
                versions.remove(&evict_key);
                touched.remove(&evict_key);
            }
        }
        let mut versions = self.layout_versions.lock().await;
        if let Some(existing) = versions.get(&key)
            && req.layout_version < *existing
        {
            return Ok(Response::new(v1::RegisterMapOutputResponse {}));
        }
        versions.insert(key.clone(), req.layout_version);
        drop(versions);
        self.map_outputs.lock().await.insert(key.clone(), partitions);
        touched.insert(key, now_ms);
        Ok(Response::new(v1::RegisterMapOutputResponse {}))
    }

    type FetchShufflePartitionStream =
        std::pin::Pin<Box<dyn Stream<Item = Result<v1::ShufflePartitionChunk, Status>> + Send>>;

    async fn fetch_shuffle_partition(
        &self,
        request: Request<v1::FetchShufflePartitionRequest>,
    ) -> Result<Response<Self::FetchShufflePartitionStream>, Status> {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| Status::internal(format!("clock error: {e}")))?
            .as_millis() as u64;
        let req = request.into_inner();
        let query_num = req
            .query_id
            .parse::<u64>()
            .map_err(|e| Status::invalid_argument(format!("query_id must be numeric: {e}")))?;
        let meta_key = (
            req.query_id.clone(),
            req.stage_id,
            req.map_task,
            req.attempt,
        );
        if req.layout_version != 0 {
            let versions = self.layout_versions.lock().await;
            if let Some(stored) = versions.get(&meta_key)
                && *stored != req.layout_version
            {
                return Err(Status::failed_precondition(format!(
                    "stale fetch layout version: requested={} stored={}",
                    req.layout_version, stored
                )));
            }
        }
        let reader = ShuffleReader::new(&self.shuffle_root);
        let (attempt, chunks) = if req.attempt == 0 {
            let attempt = reader
                .latest_attempt(query_num, req.stage_id, req.map_task)
                .map_err(to_status)?
                .ok_or_else(|| {
                    Status::failed_precondition("no shuffle attempts found for map task")
                })?;
            let chunks = reader
                .fetch_partition_chunks_range(
                    query_num,
                    req.stage_id,
                    req.map_task,
                    attempt,
                    req.reduce_partition,
                    req.start_offset,
                    req.max_bytes,
                )
                .map_err(to_status)?;
            (attempt, chunks)
        } else {
            let chunks = reader
                .fetch_partition_chunks_range(
                    query_num,
                    req.stage_id,
                    req.map_task,
                    req.attempt,
                    req.reduce_partition,
                    req.start_offset,
                    req.max_bytes,
                )
                .map_err(to_status)?;
            (req.attempt, chunks)
        };

        let meta_key = (meta_key.0, meta_key.1, meta_key.2, attempt);
        self.last_touched_ms
            .lock()
            .await
            .insert(meta_key.clone(), now_ms);
        let part_meta = self
            .map_outputs
            .lock()
            .await
            .get(&meta_key)
            .and_then(|parts| {
                parts
                    .iter()
                    .find(|p| p.reduce_partition == req.reduce_partition)
                    .cloned()
            });
        let (watermark_offset, finalized, stream_epoch) = part_meta
            .as_ref()
            .map(|m| (m.committed_offset, m.finalized, m.stream_epoch))
            .unwrap_or((0, false, 0));
        if stream_epoch < req.min_stream_epoch {
            return Err(Status::failed_precondition(format!(
                "stale fetch stream epoch: requested>={} available={}",
                req.min_stream_epoch, stream_epoch
            )));
        }
        let readable_end = watermark_offset;
        let start = req.start_offset.min(readable_end);
        let requested = if start >= readable_end {
            0
        } else if req.max_bytes == 0 {
            readable_end.saturating_sub(start)
        } else {
            req.max_bytes.min(readable_end.saturating_sub(start))
        };

        let out = if requested == 0 {
            vec![Ok(v1::ShufflePartitionChunk {
                start_offset: start,
                end_offset: start,
                payload: Vec::new(),
                watermark_offset,
                finalized,
                stream_epoch,
            })]
        } else {
            let end_limit = start.saturating_add(requested);
            let mut filtered = chunks
                .into_iter()
                .filter_map(|c| {
                    let chunk_start = c.start_offset.max(start);
                    let chunk_end = (c.start_offset + c.payload.len() as u64).min(end_limit);
                    if chunk_end <= chunk_start {
                        return None;
                    }
                    let trim_start = (chunk_start - c.start_offset) as usize;
                    let trim_end = (chunk_end - c.start_offset) as usize;
                    let payload = c.payload[trim_start..trim_end].to_vec();
                    Some(Ok(v1::ShufflePartitionChunk {
                        start_offset: chunk_start,
                        end_offset: chunk_end,
                        payload,
                        watermark_offset,
                        finalized,
                        stream_epoch,
                    }))
                })
                .collect::<Vec<_>>();
            if filtered.len() > self.max_chunks_per_response {
                filtered.truncate(self.max_chunks_per_response);
            }
            if filtered.is_empty() {
                vec![Ok(v1::ShufflePartitionChunk {
                    start_offset: start,
                    end_offset: start,
                    payload: Vec::new(),
                    watermark_offset,
                    finalized,
                    stream_epoch,
                })]
            } else {
                filtered
            }
        };
        Ok(Response::new(Box::pin(stream::iter(out))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    use arrow_schema::Schema;
    use ffq_planner::{
        ExchangeExec, ParquetScanExec, PartitioningSpec, PhysicalPlan, ShuffleReadExchange,
        ShuffleWriteExchange,
    };
    use ffq_shuffle::layout::shuffle_path;
    use tokio_stream::StreamExt;

    fn shuffle_plan(partitions: usize) -> PhysicalPlan {
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

    #[tokio::test]
    async fn grpc_control_plane_matches_coordinator_adaptive_assignment_and_stats() {
        let coordinator = Arc::new(Mutex::new(Coordinator::default()));
        let services = CoordinatorServices::from_shared(Arc::clone(&coordinator));

        let plan = serde_json::to_vec(&shuffle_plan(4)).expect("plan bytes");
        {
            let mut c = coordinator.lock().await;
            c.submit_query("9001".to_string(), &plan).expect("submit");
        }

        let map_task = services
            .get_task(Request::new(v1::GetTaskRequest {
                worker_id: "w1".to_string(),
                capacity: 10,
            }))
            .await
            .expect("grpc get map task")
            .into_inner()
            .tasks
            .into_iter()
            .next()
            .expect("map task exists");
        assert!(map_task.assigned_reduce_partitions.is_empty());
        assert_eq!(map_task.assigned_reduce_split_count, 1);
        assert_eq!(map_task.layout_version, 1);

        services
            .register_map_output(Request::new(v1::RegisterMapOutputRequest {
                query_id: map_task.query_id.clone(),
                stage_id: map_task.stage_id,
                map_task: map_task.task_id,
                attempt: map_task.attempt,
                layout_version: map_task.layout_version,
                layout_fingerprint: map_task.layout_fingerprint,
                partitions: vec![
                    v1::MapOutputPartition {
                        reduce_partition: 0,
                        bytes: 8,
                        rows: 1,
                        batches: 1,
                        stream_epoch: 1,
                        committed_offset: 0,
                        finalized: true,
                    },
                    v1::MapOutputPartition {
                        reduce_partition: 1,
                        bytes: 120,
                        rows: 1,
                        batches: 1,
                        stream_epoch: 1,
                        committed_offset: 0,
                        finalized: true,
                    },
                    v1::MapOutputPartition {
                        reduce_partition: 2,
                        bytes: 8,
                        rows: 1,
                        batches: 1,
                        stream_epoch: 1,
                        committed_offset: 0,
                        finalized: true,
                    },
                    v1::MapOutputPartition {
                        reduce_partition: 3,
                        bytes: 8,
                        rows: 1,
                        batches: 1,
                        stream_epoch: 1,
                        committed_offset: 0,
                        finalized: true,
                    },
                ],
            }))
            .await
            .expect("grpc register map output");
        services
            .report_task_status(Request::new(v1::ReportTaskStatusRequest {
                query_id: map_task.query_id.clone(),
                stage_id: map_task.stage_id,
                task_id: map_task.task_id,
                attempt: map_task.attempt,
                layout_version: map_task.layout_version,
                layout_fingerprint: map_task.layout_fingerprint,
                state: v1::TaskState::Succeeded as i32,
                message: "map done".to_string(),
                reduce_fetch_inflight_bytes: 0,
                reduce_fetch_queue_depth: 0,
            }))
            .await
            .expect("grpc report map success");

        let reduce_tasks = services
            .get_task(Request::new(v1::GetTaskRequest {
                worker_id: "w2".to_string(),
                capacity: 20,
            }))
            .await
            .expect("grpc get reduce tasks")
            .into_inner()
            .tasks;
        assert!(!reduce_tasks.is_empty());
        assert!(
            reduce_tasks
                .iter()
                .all(|t| !t.assigned_reduce_partitions.is_empty())
        );
        assert!(
            reduce_tasks
                .iter()
                .all(|t| t.recommended_map_output_publish_window_partitions >= 1)
        );
        assert!(
            reduce_tasks
                .iter()
                .all(|t| t.recommended_reduce_fetch_window_partitions >= 1)
        );

        let grpc_status = services
            .get_query_status(Request::new(v1::GetQueryStatusRequest {
                query_id: "9001".to_string(),
            }))
            .await
            .expect("grpc query status")
            .into_inner()
            .status
            .expect("status payload");
        let direct_status = {
            let c = coordinator.lock().await;
            c.get_query_status("9001").expect("direct status")
        };
        let grpc_stage0 = grpc_status
            .stage_metrics
            .iter()
            .find(|m| m.stage_id == 0)
            .expect("grpc stage0");
        let direct_stage0 = direct_status.stage_metrics.get(&0).expect("direct stage0");

        assert_eq!(
            grpc_stage0.planned_reduce_tasks,
            direct_stage0.planned_reduce_tasks
        );
        assert_eq!(
            grpc_stage0.adaptive_reduce_tasks,
            direct_stage0.adaptive_reduce_tasks
        );
        assert_eq!(
            grpc_stage0.adaptive_target_bytes,
            direct_stage0.adaptive_target_bytes
        );
        assert_eq!(grpc_stage0.skew_split_tasks, direct_stage0.skew_split_tasks);
        assert_eq!(
            grpc_stage0.layout_finalize_count,
            direct_stage0.layout_finalize_count
        );
        assert_eq!(grpc_stage0.aqe_events, direct_stage0.aqe_events);
        assert_eq!(grpc_stage0.first_chunk_ms, direct_stage0.first_chunk_ms);
        assert_eq!(
            grpc_stage0.first_reduce_row_ms,
            direct_stage0.first_reduce_row_ms
        );
        assert_eq!(grpc_stage0.stream_lag_ms, direct_stage0.stream_lag_ms);
        assert_eq!(
            grpc_stage0.stream_buffered_bytes,
            direct_stage0.stream_buffered_bytes
        );
        assert_eq!(
            grpc_stage0.stream_active_count,
            direct_stage0.stream_active_count
        );
        assert_eq!(
            grpc_stage0.backpressure_events,
            direct_stage0.backpressure_events
        );
        let grpc_hist = grpc_stage0
            .partition_bytes_histogram
            .iter()
            .map(|b| (b.upper_bound_bytes, b.partition_count))
            .collect::<Vec<_>>();
        let direct_hist = direct_stage0
            .partition_bytes_histogram
            .iter()
            .map(|b| (b.upper_bound_bytes, b.partition_count))
            .collect::<Vec<_>>();
        assert_eq!(grpc_hist, direct_hist);
    }

    #[tokio::test]
    async fn worker_shuffle_fetch_supports_range_and_returns_chunk_offsets_and_watermark() {
        let base = std::env::temp_dir().join(format!(
            "ffq-grpc-fetch-range-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        fs::create_dir_all(&base).expect("create temp root");
        let svc = WorkerShuffleService::new(&base);

        let query_id = "9010".to_string();
        let stage_id = 1_u64;
        let map_task = 0_u64;
        let attempt = 1_u32;
        let reduce_partition = 3_u32;

        let rel = shuffle_path(
            query_id.parse().expect("numeric query"),
            stage_id,
            map_task,
            attempt,
            reduce_partition,
        );
        let payload = (0_u8..32).collect::<Vec<_>>();
        let full = base.join(rel);
        if let Some(parent) = full.parent() {
            fs::create_dir_all(parent).expect("mkdirs");
        }
        fs::write(&full, &payload).expect("write payload");

        svc.register_map_output(Request::new(v1::RegisterMapOutputRequest {
            query_id: query_id.clone(),
            stage_id,
            map_task,
            attempt,
            layout_version: 1,
            layout_fingerprint: 7,
            partitions: vec![v1::MapOutputPartition {
                reduce_partition,
                bytes: payload.len() as u64,
                rows: 2,
                batches: 1,
                stream_epoch: 1,
                committed_offset: 24,
                finalized: false,
            }],
        }))
        .await
        .expect("register");

        let response = svc
            .fetch_shuffle_partition(Request::new(v1::FetchShufflePartitionRequest {
                query_id,
                stage_id,
                map_task,
                attempt,
                reduce_partition,
                start_offset: 8,
                max_bytes: 10,
                layout_version: 1,
                min_stream_epoch: 1,
            }))
            .await
            .expect("fetch");
        let mut stream = response.into_inner();
        let mut chunks = Vec::new();
        while let Some(next) = stream.next().await {
            chunks.push(next.expect("chunk"));
        }

        assert!(!chunks.is_empty(), "expected at least one streamed chunk");
        let stitched = chunks
            .iter()
            .flat_map(|c| c.payload.iter().copied())
            .collect::<Vec<_>>();
        assert_eq!(stitched, payload[8..18].to_vec());
        assert_eq!(chunks[0].start_offset, 8);
        assert_eq!(
            chunks.last().expect("last").end_offset,
            8 + stitched.len() as u64
        );
        assert!(chunks.iter().all(|c| c.watermark_offset == 24));
        assert!(chunks.iter().all(|c| !c.finalized));

        let _ = fs::remove_dir_all(&base);
    }

    #[tokio::test]
    async fn worker_shuffle_fetch_respects_committed_watermark_and_emits_eof_marker() {
        let base = std::env::temp_dir().join(format!(
            "ffq-grpc-fetch-watermark-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        fs::create_dir_all(&base).expect("create temp root");
        let svc = WorkerShuffleService::new(&base);

        let query_id = "9011".to_string();
        let stage_id = 1_u64;
        let map_task = 0_u64;
        let attempt = 1_u32;
        let reduce_partition = 4_u32;
        let payload = (0_u8..32).collect::<Vec<_>>();
        let rel = shuffle_path(
            query_id.parse().expect("numeric query"),
            stage_id,
            map_task,
            attempt,
            reduce_partition,
        );
        let full = base.join(rel);
        if let Some(parent) = full.parent() {
            fs::create_dir_all(parent).expect("mkdirs");
        }
        fs::write(&full, &payload).expect("write payload");

        svc.register_map_output(Request::new(v1::RegisterMapOutputRequest {
            query_id: query_id.clone(),
            stage_id,
            map_task,
            attempt,
            layout_version: 1,
            layout_fingerprint: 9,
            partitions: vec![v1::MapOutputPartition {
                reduce_partition,
                bytes: payload.len() as u64,
                rows: 2,
                batches: 1,
                stream_epoch: 1,
                committed_offset: 8,
                finalized: false,
            }],
        }))
        .await
        .expect("register partial");

        let mut s1 = svc
            .fetch_shuffle_partition(Request::new(v1::FetchShufflePartitionRequest {
                query_id: query_id.clone(),
                stage_id,
                map_task,
                attempt,
                reduce_partition,
                start_offset: 0,
                max_bytes: 0,
                layout_version: 1,
                min_stream_epoch: 1,
            }))
            .await
            .expect("fetch partial bytes")
            .into_inner();
        let mut c1 = Vec::new();
        while let Some(next) = s1.next().await {
            c1.push(next.expect("chunk"));
        }
        let stitched = c1
            .iter()
            .flat_map(|c| c.payload.iter().copied())
            .collect::<Vec<_>>();
        assert_eq!(stitched, payload[0..8].to_vec());
        assert!(c1.iter().all(|c| c.watermark_offset == 8));
        assert!(c1.iter().all(|c| !c.finalized));

        let mut s2 = svc
            .fetch_shuffle_partition(Request::new(v1::FetchShufflePartitionRequest {
                query_id: query_id.clone(),
                stage_id,
                map_task,
                attempt,
                reduce_partition,
                start_offset: 8,
                max_bytes: 0,
                layout_version: 1,
                min_stream_epoch: 1,
            }))
            .await
            .expect("fetch eof marker")
            .into_inner();
        let mut c2 = Vec::new();
        while let Some(next) = s2.next().await {
            c2.push(next.expect("chunk"));
        }
        assert_eq!(c2.len(), 1);
        assert!(c2[0].payload.is_empty());
        assert_eq!(c2[0].start_offset, 8);
        assert_eq!(c2[0].end_offset, 8);
        assert_eq!(c2[0].watermark_offset, 8);
        assert!(!c2[0].finalized);

        svc.register_map_output(Request::new(v1::RegisterMapOutputRequest {
            query_id: query_id.clone(),
            stage_id,
            map_task,
            attempt,
            layout_version: 1,
            layout_fingerprint: 9,
            partitions: vec![v1::MapOutputPartition {
                reduce_partition,
                bytes: payload.len() as u64,
                rows: 2,
                batches: 1,
                stream_epoch: 1,
                committed_offset: payload.len() as u64,
                finalized: true,
            }],
        }))
        .await
        .expect("register finalize");

        let mut s3 = svc
            .fetch_shuffle_partition(Request::new(v1::FetchShufflePartitionRequest {
                query_id: query_id.clone(),
                stage_id,
                map_task,
                attempt,
                reduce_partition,
                start_offset: 32,
                max_bytes: 0,
                layout_version: 1,
                min_stream_epoch: 1,
            }))
            .await
            .expect("fetch final eof marker")
            .into_inner();
        let mut c3 = Vec::new();
        while let Some(next) = s3.next().await {
            c3.push(next.expect("chunk"));
        }
        assert_eq!(c3.len(), 1);
        assert!(c3[0].payload.is_empty());
        assert_eq!(c3[0].start_offset, 32);
        assert_eq!(c3[0].end_offset, 32);
        assert_eq!(c3[0].watermark_offset, 32);
        assert!(c3[0].finalized);

        let stale_epoch_err = svc
            .fetch_shuffle_partition(Request::new(v1::FetchShufflePartitionRequest {
                query_id: query_id.clone(),
                stage_id,
                map_task,
                attempt,
                reduce_partition,
                start_offset: 0,
                max_bytes: 1,
                layout_version: 1,
                min_stream_epoch: 2,
            }))
            .await
            .err()
            .expect("stale epoch fetch should fail");
        assert_eq!(stale_epoch_err.code(), tonic::Code::FailedPrecondition);

        let stale_layout_err = svc
            .fetch_shuffle_partition(Request::new(v1::FetchShufflePartitionRequest {
                query_id: query_id.clone(),
                stage_id,
                map_task,
                attempt,
                reduce_partition,
                start_offset: 0,
                max_bytes: 1,
                layout_version: 999,
                min_stream_epoch: 1,
            }))
            .await
            .err()
            .expect("stale layout fetch should fail");
        assert_eq!(stale_layout_err.code(), tonic::Code::FailedPrecondition);

        let _ = fs::remove_dir_all(&base);
    }

    #[tokio::test]
    async fn worker_shuffle_service_enforces_stream_guardrails() {
        let base = std::env::temp_dir().join(format!(
            "ffq-grpc-guardrails-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        fs::create_dir_all(&base).expect("create temp root");
        let svc = WorkerShuffleService::with_limits(&base, 2, 1, 2, 1);

        let query_id = "9020".to_string();
        let stage_id = 1_u64;
        let reduce_partition = 0_u32;
        let payload = vec![7_u8; 200_000];
        for map_task in 0_u64..3_u64 {
            let rel = shuffle_path(
                query_id.parse().expect("numeric query"),
                stage_id,
                map_task,
                1,
                reduce_partition,
            );
            let full = base.join(rel);
            if let Some(parent) = full.parent() {
                fs::create_dir_all(parent).expect("mkdirs");
            }
            fs::write(&full, &payload).expect("write payload");
            let res = svc
                .register_map_output(Request::new(v1::RegisterMapOutputRequest {
                    query_id: query_id.clone(),
                    stage_id,
                    map_task,
                    attempt: 1,
                    layout_version: 1,
                    layout_fingerprint: 1,
                    partitions: vec![v1::MapOutputPartition {
                        reduce_partition,
                        bytes: payload.len() as u64,
                        rows: 1,
                        batches: 1,
                        stream_epoch: 1,
                        committed_offset: payload.len() as u64,
                        finalized: true,
                    }],
                }))
                .await;
            if map_task == 2 {
                assert!(
                    res.is_ok(),
                    "oldest stream should be evicted to admit new one"
                );
            }
        }

        // Oldest stream (map_task=0) should have been evicted.
        let evicted = svc
            .fetch_shuffle_partition(Request::new(v1::FetchShufflePartitionRequest {
                query_id: query_id.clone(),
                stage_id,
                map_task: 0,
                attempt: 1,
                reduce_partition,
                start_offset: 0,
                max_bytes: 100,
                layout_version: 1,
                min_stream_epoch: 1,
            }))
            .await
            .err()
            .expect("evicted stream should fail");
        assert_eq!(evicted.code(), tonic::Code::FailedPrecondition);

        // Surviving stream should fetch and honor max_chunks_per_response=2.
        let mut stream = svc
            .fetch_shuffle_partition(Request::new(v1::FetchShufflePartitionRequest {
                query_id: query_id.clone(),
                stage_id,
                map_task: 2,
                attempt: 1,
                reduce_partition,
                start_offset: 0,
                max_bytes: payload.len() as u64,
                layout_version: 1,
                min_stream_epoch: 1,
            }))
            .await
            .expect("fetch surviving stream")
            .into_inner();
        let mut chunks = Vec::new();
        while let Some(next) = stream.next().await {
            chunks.push(next.expect("chunk"));
        }
        assert!(
            chunks.len() <= 2,
            "expected capped chunk response, got {}",
            chunks.len()
        );

        // Per-stream partition metadata cap.
        let over = svc
            .register_map_output(Request::new(v1::RegisterMapOutputRequest {
                query_id,
                stage_id,
                map_task: 99,
                attempt: 1,
                layout_version: 1,
                layout_fingerprint: 1,
                partitions: vec![
                    v1::MapOutputPartition {
                        reduce_partition: 0,
                        bytes: 1,
                        rows: 1,
                        batches: 1,
                        stream_epoch: 1,
                        committed_offset: 1,
                        finalized: true,
                    },
                    v1::MapOutputPartition {
                        reduce_partition: 1,
                        bytes: 1,
                        rows: 1,
                        batches: 1,
                        stream_epoch: 1,
                        committed_offset: 1,
                        finalized: true,
                    },
                ],
            }))
            .await
            .err()
            .expect("partition cap should fail");
        assert_eq!(over.code(), tonic::Code::ResourceExhausted);

        let _ = fs::remove_dir_all(&base);
    }

    #[tokio::test]
    async fn worker_shuffle_fetch_out_of_order_range_requests_reconstruct_without_loss() {
        let base = std::env::temp_dir().join(format!(
            "ffq-grpc-out-of-order-range-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        fs::create_dir_all(&base).expect("create temp root");
        let svc = WorkerShuffleService::new(&base);

        let query_id = "9030".to_string();
        let stage_id = 1_u64;
        let map_task = 0_u64;
        let attempt = 1_u32;
        let reduce_partition = 0_u32;
        let payload = (0_u8..64).collect::<Vec<_>>();
        let rel = shuffle_path(
            query_id.parse().expect("numeric query"),
            stage_id,
            map_task,
            attempt,
            reduce_partition,
        );
        let full = base.join(rel);
        if let Some(parent) = full.parent() {
            fs::create_dir_all(parent).expect("mkdirs");
        }
        fs::write(&full, &payload).expect("write payload");

        svc.register_map_output(Request::new(v1::RegisterMapOutputRequest {
            query_id: query_id.clone(),
            stage_id,
            map_task,
            attempt,
            layout_version: 1,
            layout_fingerprint: 1,
            partitions: vec![v1::MapOutputPartition {
                reduce_partition,
                bytes: payload.len() as u64,
                rows: 4,
                batches: 2,
                stream_epoch: 1,
                committed_offset: payload.len() as u64,
                finalized: true,
            }],
        }))
        .await
        .expect("register");

        let mut high = svc
            .fetch_shuffle_partition(Request::new(v1::FetchShufflePartitionRequest {
                query_id: query_id.clone(),
                stage_id,
                map_task,
                attempt,
                reduce_partition,
                start_offset: 32,
                max_bytes: 32,
                layout_version: 1,
                min_stream_epoch: 1,
            }))
            .await
            .expect("fetch high range")
            .into_inner();
        let mut high_chunks = Vec::new();
        while let Some(next) = high.next().await {
            high_chunks.push(next.expect("chunk"));
        }

        let mut low = svc
            .fetch_shuffle_partition(Request::new(v1::FetchShufflePartitionRequest {
                query_id: query_id.clone(),
                stage_id,
                map_task,
                attempt,
                reduce_partition,
                start_offset: 0,
                max_bytes: 32,
                layout_version: 1,
                min_stream_epoch: 1,
            }))
            .await
            .expect("fetch low range")
            .into_inner();
        let mut low_chunks = Vec::new();
        while let Some(next) = low.next().await {
            low_chunks.push(next.expect("chunk"));
        }

        let mut all = Vec::new();
        all.extend(high_chunks.into_iter());
        all.extend(low_chunks.into_iter());
        all.sort_by_key(|c| c.start_offset);
        let reconstructed = all
            .into_iter()
            .flat_map(|c| c.payload.into_iter())
            .collect::<Vec<_>>();
        assert_eq!(reconstructed, payload);

        let _ = fs::remove_dir_all(&base);
    }

    #[tokio::test]
    async fn worker_shuffle_service_restart_requires_reregistration_then_reads_deterministically() {
        let base = std::env::temp_dir().join(format!(
            "ffq-grpc-restart-reregister-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        fs::create_dir_all(&base).expect("create temp root");

        let query_id = "9031".to_string();
        let stage_id = 1_u64;
        let map_task = 0_u64;
        let attempt = 1_u32;
        let reduce_partition = 0_u32;
        let payload = (0_u8..24).collect::<Vec<_>>();
        let rel = shuffle_path(
            query_id.parse().expect("numeric query"),
            stage_id,
            map_task,
            attempt,
            reduce_partition,
        );
        let full = base.join(rel);
        if let Some(parent) = full.parent() {
            fs::create_dir_all(parent).expect("mkdirs");
        }
        fs::write(&full, &payload).expect("write payload");

        let svc1 = WorkerShuffleService::new(&base);
        svc1.register_map_output(Request::new(v1::RegisterMapOutputRequest {
            query_id: query_id.clone(),
            stage_id,
            map_task,
            attempt,
            layout_version: 1,
            layout_fingerprint: 1,
            partitions: vec![v1::MapOutputPartition {
                reduce_partition,
                bytes: payload.len() as u64,
                rows: 3,
                batches: 1,
                stream_epoch: 1,
                committed_offset: payload.len() as u64,
                finalized: true,
            }],
        }))
        .await
        .expect("register on first service");

        let svc2 = WorkerShuffleService::new(&base);
        let err = svc2
            .fetch_shuffle_partition(Request::new(v1::FetchShufflePartitionRequest {
                query_id: query_id.clone(),
                stage_id,
                map_task,
                attempt,
                reduce_partition,
                start_offset: 0,
                max_bytes: 0,
                layout_version: 1,
                min_stream_epoch: 1,
            }))
            .await
            .err()
            .expect("restart without re-register should fail");
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);

        svc2.register_map_output(Request::new(v1::RegisterMapOutputRequest {
            query_id: query_id.clone(),
            stage_id,
            map_task,
            attempt,
            layout_version: 1,
            layout_fingerprint: 1,
            partitions: vec![v1::MapOutputPartition {
                reduce_partition,
                bytes: payload.len() as u64,
                rows: 3,
                batches: 1,
                stream_epoch: 1,
                committed_offset: payload.len() as u64,
                finalized: true,
            }],
        }))
        .await
        .expect("re-register on restarted service");
        let mut s = svc2
            .fetch_shuffle_partition(Request::new(v1::FetchShufflePartitionRequest {
                query_id: query_id.clone(),
                stage_id,
                map_task,
                attempt,
                reduce_partition,
                start_offset: 0,
                max_bytes: 0,
                layout_version: 1,
                min_stream_epoch: 1,
            }))
            .await
            .expect("fetch after reregister")
            .into_inner();
        let mut chunks = Vec::new();
        while let Some(next) = s.next().await {
            chunks.push(next.expect("chunk"));
        }
        let stitched = chunks
            .into_iter()
            .flat_map(|c| c.payload.into_iter())
            .collect::<Vec<_>>();
        assert_eq!(stitched, payload);

        let _ = fs::remove_dir_all(&base);
    }
}
