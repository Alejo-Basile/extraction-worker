use crate::config::OcrConfig;
use extraction_core::WorkItem;
use serde_json;
use tracing::{error, info, warn};

pub struct OcrConsumer {
    cfg: OcrConfig,
}

impl OcrConsumer {
    pub fn new(cfg: OcrConfig) -> Self {
        Self { cfg }
    }

    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        info!("OCR consumer starting, stream={}", self.cfg.redis_ocr_stream);
        // Placeholder: conexión Redis, XREADGROUP, deserialización, pipeline, ACK/DLQ
        // Implementación real pendiente
        Ok(())
    }
}
