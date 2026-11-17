use crate::config::OcrConfig;
use crate::consumer::OcrConsumer;
use actix_web::{web, App, HttpResponse, HttpServer};
use metrics_exporter_prometheus::PrometheusBuilder;
use tracing_subscriber::EnvFilter;

pub mod config;
pub mod consumer;
pub mod pipeline;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cfg = OcrConfig::from_env();
    tracing::info!("ocr-worker iniciando con stream {}", cfg.redis_ocr_stream);
    metrics::counter!("worker_ocr_started_total").increment(1);

    // Prometheus metrics recorder
    let _ = PrometheusBuilder::new()
        .install_recorder()
        .map_err(|e| format!("prometheus recorder error: {e}"));

    let metrics_addr = std::env::var("METRICS_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".into());

    // Consumer task
    let consumer = OcrConsumer::new(cfg.clone());
    tokio::spawn(async move {
        if let Err(e) = consumer.run().await {
            tracing::error!("consumer error: {}", e);
        }
    });

    // HTTP server for health
    HttpServer::new(|| {
        App::new().route(
            "/healthz",
            web::get().to(|| async { HttpResponse::Ok().body("ok") }),
        )
    })
    .bind(metrics_addr)?
    .run()
    .await?;

    Ok(())
}
