//! Extraction Worker Service (Rust, Tokio + Actix-web).
//!
//! S0-P3-01: esqueleto con health y metricas. F1 arranca el consumer
//! de Redis Streams con apagado limpio (S1-P3-02/03).

use std::env;
use std::time::{Duration, Instant};

use actix_web::{middleware, App, HttpServer};
use extraction_worker::consumer;
use extraction_worker::http;
use extraction_worker::telemetry;
use tokio::signal::unix::{signal, SignalKind};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use consumer::{config::ConsumerConfig, runner::ConsumerRunner};

/// Presupuesto unico de apagado (DoD 3 / S1-P3-03): HTTP y consumer se
/// drenan en paralelo; al agotarse se aborta el consumer y el proceso sale.
const SHUTDOWN_GRACE_SECS: u64 = 6;

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
    telemetry::init_logging();
    let args: Vec<String> = std::env::args().collect();
    if args.iter().any(|a| a == "--replay-dlq") {
        return extraction_worker::consumer::cli_replay_dlq(&args).await;
    }
    if args.iter().any(|a| a == "--inspect-dlq") {
        return extraction_worker::consumer::cli_inspect_dlq(&args).await;
    }
    if args.iter().any(|a| a == "--inspect-pel") {
        return extraction_worker::consumer::cli_inspect_pel().await;
    }
    let port = http_port().map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;

    // F3: pipeline real (GET raw-pdfs -> pdf-extract -> PUT extracted-txt)
    let cfg = ConsumerConfig::from_env();
    let startup = (
        cfg.stream.clone(),
        cfg.consumer_name.clone(),
        cfg.metrics_interval,
    );
    let pipeline = extraction_worker::consumer::pipeline::ExtractionPipeline::from_env(&cfg)
        .map_err(|e| std::io::Error::other(format!("configuracion S3 invalida: {e}")))?;
    // F3b: cliente Mongo con rol acotado (ADR-0006); sin MONGO_WORKER_URI
    // valido el worker no arranca.
    let documents = extraction_worker::document_store::DocumentStore::connect(&cfg.mongo_config())
        .await
        .map_err(|e| std::io::Error::other(format!("configuracion Mongo invalida: {e}")))?;
    let documents = std::sync::Arc::new(documents);
    // /readyz real: Redis + MinIO + Mongo (DoD global punto 5).
    let s3 = extraction_worker::objects::S3Config {
        endpoint: cfg.s3_endpoint.clone(),
        bucket_raw: cfg.s3_bucket_raw.clone(),
        bucket_txt: cfg.s3_bucket_txt.clone(),
        access_key: cfg.s3_access_key.clone(),
        secret_key: cfg.s3_secret_key.clone(),
    };
    let minio = extraction_worker::objects::ObjectStoreSource::new(&s3)
        .map_err(|e| std::io::Error::other(format!("configuracion S3 invalida: {e}")))?;
    let readiness = std::sync::Arc::new(
        http::Readiness::new(&cfg.redis_url, minio, documents.clone())
            .map_err(std::io::Error::other)?,
    );
    let runner = ConsumerRunner::new(cfg, std::sync::Arc::new(pipeline), documents);

    let prometheus = telemetry::prometheus_handle()
        .map_err(|e| std::io::Error::other(format!("no se pudo instalar el recorder: {e}")))?;

    let server = HttpServer::new(move || {
        App::new()
            .app_data(actix_web::web::Data::new(prometheus.clone()))
            // `readiness` es `Arc<Readiness>`: `Data::new` registraría
            // `Data<Arc<Readiness>>` y el extractor de `readyz`, que pide
            // `Data<Readiness>`, fallaria con 500 en cada request (medido:
            // /readyz respondia 500 con el stack sano, mientras los tests
            // pasaban porque registran el tipo concreto). `Data::from(Arc<T>)`
            // registra `Data<T>`, que es lo que el handler espera.
            .app_data(actix_web::web::Data::from(readiness.clone()))
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
    let server_task = tokio::spawn(running);

    // F1: arrancar consumer con CancellationToken (S1-P3-02/03)
    let token = CancellationToken::new();
    let token_consumer = token.clone();
    let consumer_handle = tokio::spawn(async move {
        if let Err(e) = runner.run(token_consumer).await {
            warn!(
                service = telemetry::SERVICE,
                msg = "consumer finalizado con error",
                error = %e
            );
        }
    });

    info!(
        service = telemetry::SERVICE,
        msg = "worker iniciado",
        port,
        stream = %startup.0,
        consumer = %startup.1,
        metrics_interval_secs = startup.2.as_secs()
    );

    wait_for_shutdown_signal().await;
    let started = Instant::now();
    info!(
        service = telemetry::SERVICE,
        msg = "apagado solicitado; drenando HTTP y consumer"
    );
    // Cancelar consumer primero: el BLOCK de XREADGROUP se corta por token.
    token.cancel();
    let abort_consumer = consumer_handle.abort_handle();
    let budget = Duration::from_secs(SHUTDOWN_GRACE_SECS);
    // Deadline unico: HTTP y consumer drenan en paralelo, no en serie.
    let drain = async {
        let _ = tokio::join!(handle.stop(true), consumer_handle);
    };
    if tokio::time::timeout(budget, drain).await.is_err() {
        warn!(
            service = telemetry::SERVICE,
            msg = "presupuesto de apagado agotado; se aborta el consumer",
            budget_secs = SHUTDOWN_GRACE_SECS
        );
        abort_consumer.abort();
    }
    let _ = tokio::time::timeout(Duration::from_millis(500), server_task).await;
    let elapsed = started.elapsed();
    metrics::histogram!("worker_shutdown_seconds").record(elapsed.as_secs_f64());
    info!(
        service = telemetry::SERVICE,
        msg = "apagado completado",
        elapsed_ms = elapsed.as_millis()
    );
    Ok(())
}

/// SIGTERM/SIGINT con drenaje: `stop(true)` cierra listeners y drena
/// las conexiones en vuelo hasta `shutdown_timeout`.
async fn wait_for_shutdown_signal() {
    let mut sigterm = match signal(SignalKind::terminate()) {
        Ok(s) => s,
        Err(e) => {
            warn!(
                service = telemetry::SERVICE,
                msg = "no se pudo registrar SIGTERM; se sigue solo con SIGINT",
                error = %e
            );
            let _ = tokio::signal::ctrl_c().await;
            return;
        }
    };
    tokio::select! {
        _ = sigterm.recv() => {},
        _ = tokio::signal::ctrl_c() => {},
    }
}
