use std::env;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use ffq_distributed::grpc::{ShuffleServiceServer, WorkerShuffleService};
use ffq_distributed::{DefaultTaskExecutor, GrpcControlPlane, Worker, WorkerConfig};
use ffq_shuffle::ShuffleCompressionCodec;
use ffq_storage::Catalog;
use tonic::transport::Server;

fn env_or_default(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_usize_or_default(key: &str, default: usize) -> usize {
    env::var(key)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(default)
}

fn env_u64_or_default(key: &str, default: u64) -> u64 {
    env::var(key)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(default)
}

fn parse_shuffle_codec(raw: &str) -> ShuffleCompressionCodec {
    match raw.trim().to_ascii_lowercase().as_str() {
        "none" | "off" => ShuffleCompressionCodec::None,
        "lz4" => ShuffleCompressionCodec::Lz4,
        "zstd" => ShuffleCompressionCodec::Zstd,
        _ => ShuffleCompressionCodec::Lz4,
    }
}

fn load_catalog(path: Option<String>) -> Result<Catalog, Box<dyn std::error::Error>> {
    match path {
        Some(p) => Ok(Catalog::load(&p)?),
        None => Ok(Catalog::new()),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let worker_id = env_or_default("FFQ_WORKER_ID", "worker-1");
    let coordinator_endpoint =
        env_or_default("FFQ_COORDINATOR_ENDPOINT", "http://coordinator:50051");
    let shuffle_bind = env_or_default("FFQ_WORKER_SHUFFLE_BIND", "0.0.0.0:50061");
    let shuffle_addr: SocketAddr = shuffle_bind.parse()?;
    let shuffle_root = env_or_default("FFQ_SHUFFLE_ROOT", "/var/lib/ffq/shuffle");
    let spill_dir = env_or_default(
        "FFQ_WORKER_SPILL_DIR",
        &format!("/var/lib/ffq/spill/{worker_id}"),
    );
    let cpu_slots = env_usize_or_default("FFQ_WORKER_CPU_SLOTS", 2);
    let per_task_memory_budget_bytes =
        env_usize_or_default("FFQ_WORKER_MEM_BUDGET_BYTES", 64 * 1024 * 1024);
    let map_output_publish_window_partitions =
        env_u64_or_default("FFQ_MAP_OUTPUT_PUBLISH_WINDOW_PARTITIONS", 1) as u32;
    let reduce_fetch_window_partitions =
        env_u64_or_default("FFQ_REDUCE_FETCH_WINDOW_PARTITIONS", 4) as u32;
    let poll_ms = env_u64_or_default("FFQ_WORKER_POLL_MS", 20);
    let shuffle_codec = parse_shuffle_codec(&env_or_default("FFQ_SHUFFLE_COMPRESSION", "lz4"));
    let max_active_streams = env_usize_or_default("FFQ_STREAM_MAX_ACTIVE_STREAMS", 4096);
    let max_partitions_per_stream =
        env_usize_or_default("FFQ_STREAM_MAX_PARTITIONS_PER_STREAM", 65536);
    let max_chunks_per_response = env_usize_or_default("FFQ_STREAM_MAX_CHUNKS_PER_RESPONSE", 1024);
    let inactive_stream_ttl_ms = env_u64_or_default("FFQ_STREAM_INACTIVE_TTL_MS", 10 * 60 * 1000);
    let catalog_path = env::var("FFQ_WORKER_CATALOG_PATH").ok();

    std::fs::create_dir_all(&shuffle_root)?;
    std::fs::create_dir_all(&spill_dir)?;

    let catalog = Arc::new(load_catalog(catalog_path)?);
    let task_executor = Arc::new(DefaultTaskExecutor::new(catalog));
    let control_plane = Arc::new(GrpcControlPlane::connect(&coordinator_endpoint).await?);
    let worker = Arc::new(Worker::new(
        WorkerConfig {
            worker_id: worker_id.clone(),
            cpu_slots,
            per_task_memory_budget_bytes,
            shuffle_compression_codec: shuffle_codec,
            map_output_publish_window_partitions,
            reduce_fetch_window_partitions,
            spill_dir: spill_dir.clone().into(),
            shuffle_root: shuffle_root.clone().into(),
            ..WorkerConfig::default()
        },
        control_plane,
        task_executor,
    ));

    let worker_for_poll = Arc::clone(&worker);
    let poll_handle = tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(poll_ms)).await;
            let _ = worker_for_poll.poll_once().await;
        }
    });

    let shuffle_service = WorkerShuffleService::with_limits(
        shuffle_root,
        max_active_streams,
        max_partitions_per_stream,
        max_chunks_per_response,
        inactive_stream_ttl_ms,
    );
    println!(
        "ffq-worker {worker_id} started (coordinator={coordinator_endpoint}, shuffle_bind={shuffle_addr}, spill_dir={spill_dir})"
    );
    Server::builder()
        .add_service(ShuffleServiceServer::new(shuffle_service))
        .serve(shuffle_addr)
        .await?;

    poll_handle.abort();
    Ok(())
}
