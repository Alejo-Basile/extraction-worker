//! Extraction Worker Service (Rust, Tokio + Actix-web).
//!
//! S0-P3-01: esqueleto con health y metricas. F1 arranca el consumer
//! de Redis Streams con apagado limpio (S1-P3-02/03).

use std::env;

use actix_web::{middleware, App, HttpServer};
use extraction_worker::consumer;
use extraction_worker::http;
use extraction_worker::telemetry;
use tokio::signal::unix::{signal, SignalKind};
use tokio_util::sync::CancellationToken;

use consumer::{config::ConsumerConfig, runner::ConsumerRunner};

/// Timeout de gracia del apagado. El consumer con `BLOCK 5000` debe
/// cancelarse y salir en < 6s (S1-P3-03).
const SHUTDOWN_GRACE_SECS: u64 = 30;

const DEFAULT_HTTP_PORT: u16 = 8081;

/// Lee el puerto HTTP.
fn http_port() -> Result<u16, String> {
    match env::var("HTTP_PORT") {
        Ok(raw) => raw
            .parse::<u16>()
            .map_err(|_| format!("HTTP_PORT invalido: {raw:?} (se espera un numero 0-65535)")),
        Err(env::VarError::NotPresent) => Ok(DEFAULT_HTTP_PORT),
        Err(env::VarError::NotUnicode(_)) => Err("HTTP_PORT no es UTF-8 valido".to_string()),
    }
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.iter().any(|a| a == "--inspect-pel") {
        return extraction_worker::consumer::cli_inspect_pel().await;
    }
    let port = http_port().map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;

    let prometheus = telemetry::prometheus_handle()
        .map_err(|e| std::io::Error::other(format!("no se pudo instalar el recorder: {e}")))?;

    let server = HttpServer::new(move || {
        App::new()
            .app_data(actix_web::web::Data::new(prometheus.clone()))
            .wrap(middleware::from_fn(http::track_http_metrics))
            .service(http::healthz)
            .service(http::readyz)
            .service(http::metrics_handler)
    })
    .bind(("0.0.0.0", port))?
    .shutdown_timeout(SHUTDOWN_GRACE_SECS)
    .disable_signals(); // senales manejadas manualmente

    let running = server.run();
    let handle = running.handle();

    // F1: arrancar consumer con CancellationToken (S1-P3-02/03)
    let token = CancellationToken::new();
    let token_consumer = token.clone();
    let cfg = ConsumerConfig::from_env();
    // F3: pipeline real (GET raw-pdfs -> pdf-extract -> PUT extracted-txt)
    let pipeline = extraction_worker::consumer::pipeline::ExtractionPipeline::from_env(&cfg)
        .map_err(|e| std::io::Error::other(format!("configuracion S3 invalida: {e}")))?;
    let runner = ConsumerRunner::new(cfg, std::sync::Arc::new(pipeline));
    let consumer_handle = tokio::spawn(async move {
        if let Err(e) = runner.run(token_consumer).await {
            eprintln!("consumer finalizado con error: {e}");
        }
    });

    wait_for_shutdown_signal().await;
    // Cancelar consumer primero para salir en < 6s (BLOCK 5000)
    token.cancel();
    // Parar HTTP
    handle.stop(true).await;
    // Esperar consumer con timeout corto
    let _ = tokio::time::timeout(std::time::Duration::from_secs(6), consumer_handle).await;

    running.await?;
    Ok(())
}

/// SIGTERM/SIGINT con drenaje: `stop(true)` cierra listeners y drena
/// las conexiones en vuelo hasta `shutdown_timeout`.
async fn wait_for_shutdown_signal() {
    let mut sigterm = match signal(SignalKind::terminate()) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("no se pudo registrar SIGTERM: {e}; sigo solo con SIGINT");
            let _ = tokio::signal::ctrl_c().await;
            return;
        }
    };
    tokio::select! {
        _ = sigterm.recv() => {},
        _ = tokio::signal::ctrl_c() => {},
    }
}
