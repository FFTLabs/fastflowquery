use std::env;
use std::net::SocketAddr;
use std::sync::Arc;

use ffq_distributed::grpc::{
    ControlPlaneServer, CoordinatorServices, HeartbeatServiceServer, ShuffleServiceServer,
};
use ffq_distributed::{Coordinator, CoordinatorConfig};
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bind = env_or_default("FFQ_COORDINATOR_BIND", "0.0.0.0:50051");
    let addr: SocketAddr = bind.parse()?;
    let shuffle_root = env_or_default("FFQ_SHUFFLE_ROOT", "/var/lib/ffq/shuffle");
    let blacklist_failure_threshold =
        env_u32_or_default("FFQ_BLACKLIST_FAILURE_THRESHOLD", 3);
    std::fs::create_dir_all(&shuffle_root)?;

    let coordinator = Arc::new(Mutex::new(Coordinator::new(CoordinatorConfig {
        blacklist_failure_threshold,
        shuffle_root: shuffle_root.clone().into(),
    })));
    let services = CoordinatorServices::from_shared(Arc::clone(&coordinator));

    println!(
        "ffq-coordinator listening on {addr} (shuffle_root={shuffle_root}, blacklist_threshold={blacklist_failure_threshold})"
    );

    Server::builder()
        .add_service(ControlPlaneServer::new(services.clone()))
        .add_service(ShuffleServiceServer::new(services.clone()))
        .add_service(HeartbeatServiceServer::new(services))
        .serve(addr)
        .await?;

    Ok(())
}
