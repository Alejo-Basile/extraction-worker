use crate::config::OcrConfig;
use extraction_core::WorkItem;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStoreExt;
use std::fs;
use std::process::Command;
use tempfile::TempDir;
use tokio::fs as async_fs;
use tracing::{info, warn};
use mongodb::{Client, Collection};
use mongodb::bson::{doc, oid::ObjectId, Document};

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

        // Descarga S3 raw
        let s3_client = AmazonS3Builder::new()
            .with_endpoint(&self.cfg.s3_endpoint)
            .with_access_key_id(&self.cfg.s3_access_key)
            .with_secret_access_key(&self.cfg.s3_secret_key)
            .with_region("us-east-1")
            .with_allow_http(true)
            .build()
            .map_err(|e| format!("s3 client error: {e}"))?;
        
        let object_path = ObjectPath::from(format!("raw/{}", item.object_key));
        
        let get_result = s3_client
            .get(&object_path)
            .await
            .map_err(|e| format!("s3 download error: {e}"))?;
        
        let bytes = get_result.bytes().await.map_err(|e| format!("s3 read bytes error: {e}"))?;
        
        async_fs::write(&pdf_path, &bytes)
            .await
            .map_err(|e| format!("write error: {e}"))?;

        // Rasterizar con pdftoppm
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

        // Limitador ImageMagick
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

        // Tesseract OCR
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

        // Subida S3 extracted-txt
        let txt_key = format!("extracted-txt/{}.md", item.document_id);
        let object_path_txt = ObjectPath::from(txt_key);
        let len = ocr_text.len();
        // Subir texto a S3
        let bytes = ocr_text.clone().into_bytes();
        let _ = s3_client.put(&object_path_txt, object_store::PutPayload::from(bytes)).await;

        // Update Mongo
        let mongo_client = Client::with_uri_str(&self.cfg.mongo_uri)
            .await
            .map_err(|e| format!("mongo connect error: {e}"))?;
        let db = mongo_client.database(&self.cfg.mongo_db);
        let collection: Collection<Document> = db.collection("documents");
        // Intentar parsear document_id como ObjectId
        if let Ok(oid) = ObjectId::parse_str(&item.document_id) {
            let update = doc! {
                "$set": {
                    "ocr_text": ocr_text.clone(),
                    "ocr_text_len": len as i64,
                    "ocr_extracted_at": mongodb::bson::DateTime::now()
                }
            };
            let _ = collection.update_one(doc! { "_id": oid }, update).await;
        } else {
            // Fallback por campo document_id string
            let update = doc! {
                "$set": {
                    "ocr_text": ocr_text,
                    "ocr_text_len": len as i64,
                    "ocr_extracted_at": mongodb::bson::DateTime::now()
                }
            };
            let _ = collection.update_one(doc! { "document_id": &item.document_id }, update).await;
        }

        info!("OCR generado {} chars para {}", len, item.document_id);

        Ok(())
    }
}
