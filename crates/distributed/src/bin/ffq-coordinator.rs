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
    let catalog_path = env::var("FFQ_COORDINATOR_CATALOG_PATH").ok();
    std::fs::create_dir_all(&shuffle_root)?;
    let catalog = load_catalog(catalog_path.clone())?;

    let coordinator = Arc::new(Mutex::new(Coordinator::with_catalog(
        CoordinatorConfig {
            blacklist_failure_threshold,
            shuffle_root: shuffle_root.clone().into(),
            ..CoordinatorConfig::default()
        },
        catalog,
    )));
    let services = CoordinatorServices::from_shared(Arc::clone(&coordinator));

    println!(
        "ffq-coordinator listening on {addr} (shuffle_root={shuffle_root}, blacklist_threshold={blacklist_failure_threshold}, catalog_path={})",
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
