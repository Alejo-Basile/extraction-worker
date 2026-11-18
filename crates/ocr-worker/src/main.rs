use crate::config::OcrConfig;
use crate::consumer::OcrConsumer;
use actix_web::{web, App, HttpResponse, HttpServer};
use metrics_exporter_prometheus::PrometheusBuilder;
use std::env;
use std::time::{Duration, Instant};
use tokio::signal::unix::{signal, SignalKind};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

pub mod config;
pub mod consul_reg;
pub mod consumer;
pub mod pipeline;

const SHUTDOWN_GRACE_SECS: u64 = 6;
const DEFAULT_METRICS_ADDR: &str = "0.0.0.0:8080";

fn metrics_port() -> Result<u16, String> {
    let raw = env::var("METRICS_ADDR").unwrap_or_else(|_| DEFAULT_METRICS_ADDR.to_string());
    let (_, port_str) = raw.rsplit_once(':').unwrap_or(("0.0.0.0", "8080"));
    port_str
        .parse::<u16>()
        .map_err(|_| format!("METRICS_ADDR invalido: {raw:?} (se espera host:port)"))
}

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

    let metrics_addr =
        env::var("METRICS_ADDR").unwrap_or_else(|_| DEFAULT_METRICS_ADDR.to_string());
    let port = metrics_port()?;

    // shutdown signal registered before bind
    let shutdown_signal = shutdown_signal();

    // Consumer task
    let consumer = OcrConsumer::new(cfg.clone());
    let consumer_handle = tokio::spawn(async move {
        if let Err(e) = consumer.run().await {
            tracing::error!("consumer error: {}", e);
        }
    });

    // Consul registration (non-blocking)
    let consul_cfg = consul_reg::ConsulRegConfig::from_env();
    let hostname = hostname::get()
        .ok()
        .and_then(|h| h.into_string().ok())
        .unwrap_or_else(|| "localhost".to_string());
    let address = consul_reg::resolve_address(&consul_cfg.base_url);
    let registrar = std::sync::Arc::new(consul_reg::ConsulRegistrar::new(
        consul_cfg, hostname, address, port,
    ));
    let reg_cancel = CancellationToken::new();
    let mut reg_task = tokio::spawn({
        let r = registrar.clone();
        let c = reg_cancel.clone();
        async move {
            r.register_with_retry(c).await;
        }
    });

    // HTTP server for health
    let server = HttpServer::new(|| {
        App::new().route(
            "/healthz",
            web::get().to(|| async { HttpResponse::Ok().body("ok") }),
        )
    })
    .bind(&metrics_addr)?
    .shutdown_timeout(SHUTDOWN_GRACE_SECS)
    .disable_signals();

    let running = server.run();
    let handle = running.handle();
    let server_task = tokio::spawn(running);

    shutdown_signal.await;
    let started = Instant::now();
    reg_cancel.cancel();
    if tokio::time::timeout(Duration::from_millis(300), &mut reg_task)
        .await
        .is_err()
    {
        reg_task.abort();
    }
    match tokio::time::timeout(Duration::from_secs(2), registrar.deregister()).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => warn!(msg = "desregistro de consul fallido", error = %e),
        Err(_) => warn!(msg = "desregistro de consul agoto presupuesto de 2s"),
    }
    info!(msg = "apagado solicitado; drenando HTTP y consumer");
    let abort_consumer = consumer_handle.abort_handle();
    let elapsed_so_far = started.elapsed();
    let remaining = Duration::from_secs(SHUTDOWN_GRACE_SECS).saturating_sub(elapsed_so_far);
    let drain = async {
        let _ = tokio::join!(handle.stop(true), consumer_handle);
    };
    if tokio::time::timeout(remaining, drain).await.is_err() {
        warn!(msg = "presupuesto de apagado agotado; se aborta el consumer");
        abort_consumer.abort();
    }
    let _ = tokio::time::timeout(Duration::from_millis(500), server_task).await;
    info!(
        msg = "apagado completado",
        elapsed_ms = started.elapsed().as_millis()
    );
    Ok(())
}

fn shutdown_signal() -> impl std::future::Future<Output = ()> {
    let sigterm = signal(SignalKind::terminate());
    async move {
        let mut sigterm = match sigterm {
            Ok(s) => s,
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };
        tokio::select! {
            _ = sigterm.recv() => {},
            _ = tokio::signal::ctrl_c() => {},
        }
    }
}
