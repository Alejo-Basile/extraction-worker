use crate::config::OcrConfig;
use extraction_core::WorkItem;
use metrics::counter;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

pub mod config;
pub mod pipeline;

async fn process_item(cfg: &OcrConfig, item: &WorkItem) -> Result<(), String> {
    let pipe = pipeline::OcrPipeline::new(cfg.clone());
    pipe.run(item).await
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cfg = OcrConfig::from_env();
    info!("ocr-worker iniciando con stream {}", cfg.redis_ocr_stream);
    metrics::counter!("worker_ocr_started_total").increment(1);

    info!("configuración cargada: {:?}", cfg);

    let dummy_item = WorkItem {
        document_id: "dummy".into(),
        object_key: "dummy.pdf".into(),
        correlation_id: "dummy".into(),
        enqueued_at: "2026-01-01T00:00:00Z".into(),
        schema_version: 1,
    };

    match process_item(&cfg, &dummy_item).await {
        Ok(_) => {
            counter!("worker_ocr_processed_total").increment(1);
            info!("procesamiento dummy ok");
        }
        Err(e) => {
            counter!("worker_ocr_errors_total").increment(1);
            warn!("error procesando dummy: {}", e);
        }
    }

    Ok(())
}
