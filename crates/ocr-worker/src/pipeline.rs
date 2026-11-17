use crate::config::OcrConfig;
use extraction_core::WorkItem;
use std::fs;
use std::process::Command;
use tempfile::TempDir;
use tracing::{info, warn, error};

#[allow(dead_code)]
pub struct OcrPipeline {
    cfg: OcrConfig,
}

impl OcrPipeline {
    pub fn new(cfg: OcrConfig) -> Self {
        Self { cfg }
    }

    pub async fn run(&self, item: &WorkItem) -> Result<(), String> {
        info!("OCR pipeline iniciado para documento {}", item.document_id);
        let tmp = TempDir::new().map_err(|e| format!("tempdir error: {e}"))?;
        let pdf_path = tmp.path().join("input.pdf");
        let out_dir = tmp.path().join("pages");
        fs::create_dir_all(&out_dir).map_err(|e| format!("mkdir error: {e}"))?;

        // Placeholder de descarga S3
        // En implementación real: object_store::Client con endpoint S3
        // Por ahora crear archivo vacío para que los comandos no fallen
        fs::write(&pdf_path, b"%PDF-1.4 placeholder").map_err(|e| format!("write error: {e}"))?;

        // 1. Rasterizar con pdftoppm
        let ppm_prefix = out_dir.join("page");
        let status = Command::new("pdftoppm")
            .arg("-png")
            .arg("-r")
            .arg(self.cfg.ocr_dpi.to_string())
            .arg(&pdf_path)
            .arg(ppm_prefix.to_str().unwrap())
            .status()
            .map_err(|e| format!("pdftoppm error: {e}"))?;
        if !status.success() {
            return Err("pdftoppm failed".into());
        }

        // 2. Limitador ImageMagick
        for entry in fs::read_dir(&out_dir).map_err(|e| format!("read dir error: {e}"))?.flatten() {
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("png") {
                let status = Command::new("convert")
                    .arg(&path)
                    .arg("-resize")
                    .arg(&format!("{}x{}>", self.cfg.ocr_limit_w, self.cfg.ocr_limit_w))
                    .arg("-resize")
                    .arg(&format!("{}@>", self.cfg.ocr_limit_pix))
                    .arg(&path)
                    .status()
                    .map_err(|e| format!("convert error: {e}"))?;
                if !status.success() {
                    warn!("convert failed for {}", path.display());
                }
            }
        }

        // 3. Tesseract OCR
        let mut ocr_text = String::new();
        for entry in fs::read_dir(&out_dir).map_err(|e| format!("read dir error: {e}"))?.flatten() {
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("png") {
                let output_base = path.with_extension("");
                let status = Command::new("tesseract")
                    .arg(&path)
                    .arg(&output_base)
                    .arg("-l")
                    .arg(&self.cfg.ocr_lang)
                    .arg("--oem")
                    .arg(self.cfg.ocr_oem.to_string())
                    .arg("--psm")
                    .arg(self.cfg.ocr_psm.to_string())
                    .status()
                    .map_err(|e| format!("tesseract error: {e}"))?;
                if status.success() {
                    let txt_path = output_base.with_extension("txt");
                    if let Ok(content) = fs::read_to_string(&txt_path) {
                        ocr_text.push_str(&content);
                        ocr_text.push('\n');
                    }
                }
            }
        }

        // 4. Placeholder de subida S3 y Mongo
        info!("OCR generado {} chars para {}", ocr_text.len(), item.document_id);
        // En implementación real: subir a S3 y actualizar Mongo

        Ok(())
    }
}
