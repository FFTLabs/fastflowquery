use std::io;
use std::net::SocketAddr;

use axum::{routing::get, Router};
use tokio::net::TcpListener;

use crate::metrics::global_metrics;

pub async fn run_metrics_exporter(addr: SocketAddr) -> io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    serve_metrics_listener(listener).await
}

pub async fn serve_metrics_listener(listener: TcpListener) -> io::Result<()> {
    let app = Router::new().route("/metrics", get(metrics_handler));
    axum::serve(listener, app).await.map_err(io::Error::other)
}

async fn metrics_handler() -> String {
    global_metrics().render_prometheus()
}

#[cfg(test)]
mod tests {
    use super::metrics_handler;
    use crate::metrics::global_metrics;

    #[tokio::test]
    async fn metrics_handler_returns_prometheus_text() {
        global_metrics().record_operator("qprof", 0, 0, "Project", 5, 5, 1, 1, 64, 64, 0.001);
        let body = metrics_handler().await;
        assert!(body.contains("ffq_operator_rows_in_total"));
        assert!(body.contains("qprof"));
    }
}
