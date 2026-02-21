use std::env;
use std::net::SocketAddr;
use std::sync::Arc;

use ffq_distributed::grpc::{
    ControlPlaneServer, CoordinatorServices, HeartbeatServiceServer, ShuffleServiceServer,
};
use ffq_distributed::{Coordinator, CoordinatorConfig};
use ffq_storage::Catalog;
use tokio::sync::Mutex;
use tonic::transport::Server;

fn env_or_default(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_u32_or_default(key: &str, default: u32) -> u32 {
    env::var(key)
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(default)
}

fn env_u64_or_default(key: &str, default: u64) -> u64 {
    env::var(key)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(default)
}

fn env_f64_or_default(key: &str, default: f64) -> f64 {
    env::var(key)
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(default)
}

fn env_bool_or_default(key: &str, default: bool) -> bool {
    env::var(key)
        .ok()
        .and_then(|v| match v.to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Some(true),
            "0" | "false" | "no" | "off" => Some(false),
            _ => None,
        })
        .unwrap_or(default)
}

fn load_catalog(path: Option<String>) -> Result<Catalog, Box<dyn std::error::Error>> {
    match path {
        Some(p) => Ok(Catalog::load(&p)?),
        None => Ok(Catalog::new()),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bind = env_or_default("FFQ_COORDINATOR_BIND", "0.0.0.0:50051");
    let addr: SocketAddr = bind.parse()?;
    let shuffle_root = env_or_default("FFQ_SHUFFLE_ROOT", "/var/lib/ffq/shuffle");
    let blacklist_failure_threshold = env_u32_or_default("FFQ_BLACKLIST_FAILURE_THRESHOLD", 3);
    let max_concurrent_tasks_per_worker =
        env_u32_or_default("FFQ_MAX_CONCURRENT_TASKS_PER_WORKER", 8);
    let max_concurrent_tasks_per_query =
        env_u32_or_default("FFQ_MAX_CONCURRENT_TASKS_PER_QUERY", 32);
    let max_task_attempts = env_u32_or_default("FFQ_MAX_TASK_ATTEMPTS", 3);
    let retry_backoff_base_ms = env_u64_or_default("FFQ_RETRY_BACKOFF_BASE_MS", 250);
    let worker_liveness_timeout_ms = env_u64_or_default("FFQ_WORKER_LIVENESS_TIMEOUT_MS", 15000);
    let adaptive_shuffle_target_bytes =
        env_u64_or_default("FFQ_ADAPTIVE_SHUFFLE_TARGET_BYTES", 128 * 1024 * 1024);
    let adaptive_shuffle_min_reduce_tasks =
        env_u32_or_default("FFQ_ADAPTIVE_SHUFFLE_MIN_REDUCE_TASKS", 1);
    let adaptive_shuffle_max_reduce_tasks =
        env_u32_or_default("FFQ_ADAPTIVE_SHUFFLE_MAX_REDUCE_TASKS", 0);
    let adaptive_shuffle_max_partitions_per_task =
        env_u32_or_default("FFQ_ADAPTIVE_SHUFFLE_MAX_PARTITIONS_PER_TASK", 0);
    let pipelined_shuffle_enabled = env_bool_or_default("FFQ_PIPELINED_SHUFFLE_ENABLED", false);
    let pipelined_shuffle_min_map_completion_ratio =
        env_f64_or_default("FFQ_PIPELINED_SHUFFLE_MIN_MAP_COMPLETION_RATIO", 0.5);
    let pipelined_shuffle_min_committed_offset_bytes =
        env_u64_or_default("FFQ_PIPELINED_SHUFFLE_MIN_COMMITTED_OFFSET_BYTES", 1);
    let speculative_execution_enabled =
        env_bool_or_default("FFQ_SPECULATIVE_EXECUTION_ENABLED", true);
    let speculative_min_completed_samples =
        env_u32_or_default("FFQ_SPECULATIVE_MIN_COMPLETED_SAMPLES", 5);
    let speculative_p95_multiplier =
        env_f64_or_default("FFQ_SPECULATIVE_P95_MULTIPLIER", 1.5);
    let speculative_min_runtime_ms =
        env_u64_or_default("FFQ_SPECULATIVE_MIN_RUNTIME_MS", 250);
    let locality_preference_enabled =
        env_bool_or_default("FFQ_LOCALITY_PREFERENCE_ENABLED", true);
    let catalog_path = env::var("FFQ_COORDINATOR_CATALOG_PATH").ok();
    std::fs::create_dir_all(&shuffle_root)?;
    let catalog = load_catalog(catalog_path.clone())?;

    let coordinator = Arc::new(Mutex::new(Coordinator::with_catalog(
        CoordinatorConfig {
            blacklist_failure_threshold,
            shuffle_root: shuffle_root.clone().into(),
            max_concurrent_tasks_per_worker,
            max_concurrent_tasks_per_query,
            max_task_attempts,
            retry_backoff_base_ms,
            worker_liveness_timeout_ms,
            adaptive_shuffle_target_bytes,
            adaptive_shuffle_min_reduce_tasks,
            adaptive_shuffle_max_reduce_tasks,
            adaptive_shuffle_max_partitions_per_task,
            pipelined_shuffle_enabled,
            pipelined_shuffle_min_map_completion_ratio,
            pipelined_shuffle_min_committed_offset_bytes,
            speculative_execution_enabled,
            speculative_min_completed_samples,
            speculative_p95_multiplier,
            speculative_min_runtime_ms,
            locality_preference_enabled,
            ..CoordinatorConfig::default()
        },
        catalog,
    )));
    let services = CoordinatorServices::from_shared(Arc::clone(&coordinator));

    println!(
        "ffq-coordinator listening on {addr} (shuffle_root={shuffle_root}, blacklist_threshold={blacklist_failure_threshold}, worker_limit={max_concurrent_tasks_per_worker}, query_limit={max_concurrent_tasks_per_query}, max_attempts={max_task_attempts}, retry_backoff_ms={retry_backoff_base_ms}, liveness_timeout_ms={worker_liveness_timeout_ms}, adaptive_shuffle_target_bytes={adaptive_shuffle_target_bytes}, adaptive_shuffle_min_reduce_tasks={adaptive_shuffle_min_reduce_tasks}, adaptive_shuffle_max_reduce_tasks={adaptive_shuffle_max_reduce_tasks}, adaptive_shuffle_max_partitions_per_task={adaptive_shuffle_max_partitions_per_task}, pipelined_shuffle_enabled={pipelined_shuffle_enabled}, pipelined_shuffle_min_map_completion_ratio={pipelined_shuffle_min_map_completion_ratio}, pipelined_shuffle_min_committed_offset_bytes={pipelined_shuffle_min_committed_offset_bytes}, speculative_execution_enabled={speculative_execution_enabled}, speculative_min_completed_samples={speculative_min_completed_samples}, speculative_p95_multiplier={speculative_p95_multiplier}, speculative_min_runtime_ms={speculative_min_runtime_ms}, locality_preference_enabled={locality_preference_enabled}, catalog_path={})",
        catalog_path.unwrap_or_else(|| "<none>".to_string())
    );

    Server::builder()
        .add_service(ControlPlaneServer::new(services.clone()))
        .add_service(ShuffleServiceServer::new(services.clone()))
        .add_service(HeartbeatServiceServer::new(services))
        .serve(addr)
        .await?;

    Ok(())
}
