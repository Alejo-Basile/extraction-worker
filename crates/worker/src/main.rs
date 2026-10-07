//! Extraction Worker Service (Rust, Tokio + Actix-web).
//!
//! S0-P3-01: esqueleto con health y metricas. El consumer de Redis Streams,
//! la extraccion real y la escritura en Mongo llegan en F1/F2.
//!
//! Endpoints (solo health y metricas, SDD 3.3):
//! - `GET /healthz` — liveness: 200 si el proceso vive.
//! - `GET /readyz` — trivial en S0; en F1 verifica Redis/MinIO reales.
//! - `GET /metrics` — expositor Prometheus.

mod http;
mod telemetry;

use std::env;

use actix_web::{middleware, App, HttpServer};
use tokio::signal::unix::{signal, SignalKind};

/// Timeout de gracia del apagado (S0). En F1 el drenaje del consumer
/// (`S1-P3-03`) acepta hasta 6 s por el `BLOCK 5000` del XREADGROUP.
const SHUTDOWN_GRACE_SECS: u64 = 30;

const DEFAULT_HTTP_PORT: u16 = 8081;

/// Lee el puerto HTTP. Config minimal en S0; la configuracion con fallo
/// rapido y validacion completa llega en `S0-P3-04`.
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
    .disable_signals(); // senales manejadas manualmente abajo

    let running = server.run();
    let handle = running.handle();

    wait_for_shutdown_signal().await;
    handle.stop(true).await;

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
