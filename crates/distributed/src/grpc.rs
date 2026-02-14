pub mod v1 {
    tonic::include_proto!("ffq.distributed.v1");
}

pub use v1::control_plane_client::ControlPlaneClient;
pub use v1::control_plane_server::{ControlPlane, ControlPlaneServer};
pub use v1::heartbeat_service_client::HeartbeatServiceClient;
pub use v1::heartbeat_service_server::{HeartbeatService, HeartbeatServiceServer};
pub use v1::shuffle_service_client::ShuffleServiceClient;
pub use v1::shuffle_service_server::{ShuffleService, ShuffleServiceServer};
