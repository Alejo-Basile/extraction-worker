use crate::config::OcrConfig;
use crate::consumer::OcrConsumer;
use actix_web::{web, App, HttpResponse, HttpServer};
use metrics_exporter_prometheus::PrometheusHandle;
use std::env;
use std::time::{Duration, Instant};
use tokio::signal::unix::{signal, SignalKind};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

pub mod config;
pub mod consul_reg;
pub mod consumer;
pub mod kv;
pub mod pipeline;
pub mod telemetry;

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

    // S11-P1-C30: Consul KV overlay first (max 2 s), then env defaults.
    let kv_settings = kv::settings_from_env();
    let kv_overlay = kv::load_overlay(&kv_settings).await;
    let cfg = OcrConfig::from_env_with_kv(kv_overlay);
    tracing::info!("ocr-worker iniciando con stream {}", cfg.redis_ocr_stream);

    // Prometheus metrics recorder: debe instalarse antes de emitir metricas;
    // si no, los contadores se descartan y `/metrics` sirve un body vacio.
    let prometheus = telemetry::prometheus_handle()?;
    metrics::counter!("worker_ocr_started_total").increment(1);

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

    // S11-P1-C30: every KV key is restart-only; observe changes, never apply.
    let kv_cancel = CancellationToken::new();
    tokio::spawn({
        let settings = kv_settings.clone();
        let cancel = kv_cancel.clone();
        async move {
            kv::watch_restart_required(settings, cancel).await;
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

    // HTTP server for health and metrics
    let prom_clone = prometheus.clone();
    let server = HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(prom_clone.clone()))
            .route(
                "/healthz",
                web::get().to(|| async { HttpResponse::Ok().body("ok") }),
            )
            .route("/metrics", web::get().to(metrics_handler))
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
    kv_cancel.cancel();
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

async fn metrics_handler(handle: web::Data<PrometheusHandle>) -> HttpResponse {
    HttpResponse::Ok()
        .content_type("text/plain; version=0.0.4; charset=utf-8")
        .body(handle.render())
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

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{test, App};

    #[actix_web::test]
    async fn metrics_route_exists() {
        let handle = telemetry::prometheus_handle().unwrap_or_else(|e| panic!("{e}"));
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(handle))
                .route("/metrics", web::get().to(metrics_handler)),
        )
        .await;
        let req = test::TestRequest::get().uri("/metrics").to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), actix_web::http::StatusCode::OK);
        let ct = resp
            .headers()
            .get(actix_web::http::header::CONTENT_TYPE)
            .unwrap_or_else(|| panic!("no ct"));
        assert!(ct.to_str().unwrap_or("").starts_with("text/plain"));
    }

    #[actix_web::test]
    async fn metrics_body_renders_metric() {
        let handle = telemetry::prometheus_handle().unwrap_or_else(|e| panic!("{e}"));
        metrics::counter!("worker_ocr_started_total").increment(1);
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(handle))
                .route("/metrics", web::get().to(metrics_handler)),
        )
        .await;
        let req = test::TestRequest::get().uri("/metrics").to_request();
        let resp = test::call_service(&app, req).await;
        let body = test::read_body(resp).await;
        let text = String::from_utf8_lossy(&body);
        assert!(text.contains("worker_ocr_started_total"), "body: {text}");
    }

    #[actix_web::test]
    async fn healthz_returns_ok() {
        let app = test::init_service(App::new().route(
            "/healthz",
            web::get().to(|| async { HttpResponse::Ok().body("ok") }),
        ))
        .await;
        let req = test::TestRequest::get().uri("/healthz").to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), actix_web::http::StatusCode::OK);
        let body = test::read_body(resp).await;
        assert_eq!(body, "ok");
    }
}
