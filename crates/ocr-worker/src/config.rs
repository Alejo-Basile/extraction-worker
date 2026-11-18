#[derive(Debug, Clone)]
pub struct OcrConfig {
    pub redis_url: String,
    pub redis_ocr_stream: String,
    pub redis_ocr_group: String,
    pub redis_ocr_dlq_stream: String,
    pub s3_endpoint: String,
    pub s3_bucket_raw: String,
    pub s3_bucket_txt: String,
    pub s3_access_key: String,
    pub s3_secret_key: String,
    pub mongo_uri: String,
    pub mongo_db: String,
    pub ocr_dpi: u32,
    pub ocr_limit_w: u32,
    pub ocr_limit_pix: u64,
    pub ocr_lang: String,
    pub ocr_oem: u8,
    pub ocr_psm: u8,
    pub ocr_max_attempts: u32,
    pub consumer_block_ms: u64,
    pub consumer_count: usize,
}

impl OcrConfig {
    /// S11-P1-C30: env defaults, optionally overridden by a validated
    /// `config/ocr-worker/` overlay. Endpoints and credentials stay env-only:
    /// they never live in KV.
    pub fn from_env_with_kv(overlay: Option<configkv::ValidatedOcrWorkerKv>) -> Self {
        let mut cfg = Self::from_env();
        if let Some(v) = overlay {
            cfg.ocr_dpi = v.ocr_dpi;
            cfg.ocr_limit_w = v.ocr_limit_w;
            cfg.ocr_limit_pix = v.ocr_limit_pix;
            cfg.ocr_lang = v.ocr_lang;
            cfg.ocr_oem = v.ocr_oem;
            cfg.ocr_psm = v.ocr_psm;
            cfg.ocr_max_attempts = v.ocr_max_attempts;
            cfg.consumer_block_ms = v.consumer_block_ms;
            cfg.consumer_count = v.consumer_count;
            cfg.redis_ocr_stream = v.redis_ocr_stream;
            cfg.redis_ocr_group = v.redis_ocr_group;
            cfg.redis_ocr_dlq_stream = v.redis_ocr_dlq_stream;
            tracing::info!("ocr_config_kv_overlay_applied");
        }
        cfg
    }

    pub fn from_env() -> Self {
        Self {
            redis_url: std::env::var("REDIS_URL")
                .unwrap_or_else(|_| "redis://127.0.0.1:6379/".into()),
            redis_ocr_stream: std::env::var("REDIS_OCR_STREAM")
                .unwrap_or_else(|_| "stream:ocr-processing".into()),
            redis_ocr_group: std::env::var("REDIS_OCR_GROUP")
                .unwrap_or_else(|_| "ocr-workers".into()),
            redis_ocr_dlq_stream: std::env::var("REDIS_OCR_DLQ_STREAM")
                .unwrap_or_else(|_| "stream:ocr-processing-dlq".into()),
            s3_endpoint: std::env::var("S3_ENDPOINT")
                .unwrap_or_else(|_| "http://127.0.0.1:9000".into()),
            s3_bucket_raw: std::env::var("S3_BUCKET_RAW").unwrap_or_else(|_| "raw-pdfs".into()),
            s3_bucket_txt: std::env::var("S3_BUCKET_TXT")
                .unwrap_or_else(|_| "extracted-txt".into()),
            s3_access_key: std::env::var("S3_ACCESS_KEY").unwrap_or_else(|_| "worker-dev".into()),
            s3_secret_key: std::env::var("S3_SECRET_KEY")
                .unwrap_or_else(|_| "worker-dev-secret".into()),
            mongo_uri: std::env::var("MONGO_WORKER_URI")
                .unwrap_or_else(|_| "mongodb://localhost:27017/app".into()),
            mongo_db: std::env::var("MONGO_DB").unwrap_or_else(|_| "app".into()),
            ocr_dpi: std::env::var("OCR_DPI")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(150),
            ocr_limit_w: std::env::var("OCR_LIMIT_W")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(3500),
            ocr_limit_pix: std::env::var("OCR_LIMIT_PIX")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(12_000_000),
            ocr_lang: std::env::var("OCR_LANG").unwrap_or_else(|_| "spa+eng".into()),
            ocr_oem: std::env::var("OCR_OEM")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(1),
            ocr_psm: std::env::var("OCR_PSM")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(3),
            ocr_max_attempts: std::env::var("OCR_MAX_ATTEMPTS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(3),
            consumer_block_ms: std::env::var("OCR_CONSUMER_BLOCK_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(5000),
            consumer_count: std::env::var("OCR_CONSUMER_COUNT")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(1),
        }
    }
}
