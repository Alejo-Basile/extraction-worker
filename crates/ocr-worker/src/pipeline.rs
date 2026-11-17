use crate::config::OcrConfig;
use extraction_core::WorkItem;
use tracing::{info, warn};

pub struct OcrPipeline {
    cfg: OcrConfig,
}

impl OcrPipeline {
    pub fn new(cfg: OcrConfig) -> Self {
        Self { cfg }
    }

    pub async fn run(&self, item: &WorkItem) -> Result<(), String> {
        info!("OCR pipeline iniciado para documento {}", item.document_id);
        // 1. Descargar PDF desde S3 raw
        // 2. Rasterizar con pdftoppm -r cfg.ocr_dpi
        // 3. Aplicar limitador ImageMagick
        // 4. Ejecutar tesseract con cfg.ocr_lang, OEM, PSM
        // 5. Subir .md a S3 bucket_txt
        // 6. Actualizar Mongo
        warn!("pipeline placeholder: no implementado");
        Ok(())
    }
}
