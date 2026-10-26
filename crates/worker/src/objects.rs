//! Adaptadores S3/MinIO sobre `object_store` (seccion 11.4, S2-P3-01).
//!
//! - `ObjectStoreSource`: descarga `raw-pdfs/<document_id>.pdf` a un archivo
//!   temporal en streaming, con limite duro de tamano (defensa en profundidad
//!   sobre el limite de subida de 30 MB).
//! - `ObjectStoreSink`: sube `extracted-txt/<document_id>.txt` (put idempotente:
//!   repetir el paso deja un solo objeto).
//!
//! El `minio` de labs es path-style y http; en produccion el endpoint sera
//! https con la identidad de P1 (el builder no cambia).

use std::sync::Arc;

use object_store::aws::AmazonS3Builder;
use object_store::path::Path as StorePath;
use object_store::{ObjectStore, ObjectStoreExt};

use crate::consumer::resilience::{CircuitBreaker, Failure};

/// Timeout de cada operacion S3 individual (S2-P3-06). El timeout del job
/// completo lo aplica el pipeline con `WORKER_JOB_TIMEOUT_SECS`.
const S3_OP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

#[derive(Debug, Clone)]
pub struct S3Config {
    pub endpoint: String,
    pub bucket_raw: String,
    pub bucket_txt: String,
    pub access_key: String,
    pub secret_key: String,
}

fn build_client(cfg: &S3Config, bucket: &str) -> Result<Arc<dyn ObjectStore>, Failure> {
    if cfg.access_key.is_empty() || cfg.secret_key.is_empty() {
        return Err(Failure::fatal(
            "S3_CONFIG",
            format!("S3_ACCESS_KEY/S3_SECRET_KEY vacios para el bucket {bucket}"),
        ));
    }
    let store = AmazonS3Builder::new()
        .with_endpoint(&cfg.endpoint)
        .with_bucket_name(bucket)
        .with_access_key_id(&cfg.access_key)
        .with_secret_access_key(&cfg.secret_key)
        // MinIO/labs es path-style y http; en https el flag es inofensivo.
        .with_allow_http(true)
        .build()
        .map_err(|e| Failure::fatal("S3_CONFIG", e.to_string()))?;
    Ok(Arc::new(store))
}

/// Clasifica un error de `object_store` (nunca es Document: si S3 falla, el
/// problema no es el contenido del documento).
pub fn classify_object_store_error(e: &object_store::Error) -> Failure {
    let message = e.to_string();
    match e {
        object_store::Error::Unauthenticated { .. }
        | object_store::Error::PermissionDenied { .. }
        | object_store::Error::UnknownConfigurationKey { .. } => Failure::fatal("S3_AUTH", message),
        _ => Failure::transient("S3_IO", message),
    }
}

pub struct ObjectStoreSource {
    raw: Arc<dyn ObjectStore>,
}

impl ObjectStoreSource {
    pub fn new(cfg: &S3Config) -> Result<Self, Failure> {
        Ok(Self {
            raw: build_client(cfg, &cfg.bucket_raw)?,
        })
    }
}

impl crate::consumer::pipeline::ObjectSource for ObjectStoreSource {
    async fn download_to(
        &self,
        key: &str,
        max_bytes: u64,
        dest: &std::path::Path,
    ) -> Result<u64, Failure> {
        download_to(&self.raw, key, max_bytes, dest).await
    }
}

pub struct ObjectStoreSink {
    txt: Arc<dyn ObjectStore>,
}

impl ObjectStoreSink {
    pub fn new(cfg: &S3Config) -> Result<Self, Failure> {
        Ok(Self {
            txt: build_client(cfg, &cfg.bucket_txt)?,
        })
    }
}

impl crate::consumer::pipeline::TextSink for ObjectStoreSink {
    async fn put_text(&self, key: &str, text: &str) -> Result<(), Failure> {
        put_text(&self.txt, key, text).await
    }
}

/// Descarga en streaming con corte por tamano (S2-P3-01).
pub async fn download_to(
    store: &Arc<dyn ObjectStore>,
    key: &str,
    max_bytes: u64,
    dest: &std::path::Path,
) -> Result<u64, Failure> {
    use futures_util::StreamExt;
    use object_store::GetOptions;
    use tokio::io::AsyncWriteExt;

    let path = StorePath::from(key);
    let get = store
        .get_opts(&path, GetOptions::default())
        .await
        .map_err(|e| classify_object_store_error(&e))?;
    let mut stream = get.into_stream();

    let mut file = tokio::fs::File::create(dest)
        .await
        .map_err(|e| Failure::transient("S3_TMP_FILE", e.to_string()))?;
    let mut total: u64 = 0;
    while let Some(chunk) = stream
        .next()
        .await
        .transpose()
        .map_err(|e| classify_object_store_error(&e))?
    {
        total += chunk.len() as u64;
        if total > max_bytes {
            return Err(Failure::Document {
                code: extraction_core::error_code::PDF_TOO_LARGE.to_string(),
                message: format!("el objeto {key} supera el limite ({total} > {max_bytes})"),
            });
        }
        file.write_all(&chunk)
            .await
            .map_err(|e| Failure::transient("S3_TMP_FILE", e.to_string()))?;
    }
    file.flush()
        .await
        .map_err(|e| Failure::transient("S3_TMP_FILE", e.to_string()))?;
    Ok(total)
}

/// Sube el texto extraido (put idempotente sobre `extracted-txt`).
pub async fn put_text(store: &Arc<dyn ObjectStore>, key: &str, text: &str) -> Result<(), Failure> {
    let path = StorePath::from(key);
    ObjectStoreExt::put(store.as_ref(), &path, text.to_string().into())
        .await
        .map_err(|e| classify_object_store_error(&e))?;
    Ok(())
}

/// Ejecuta `op` con timeout por operacion; el CB de MinIO lo maneja el caller.
pub async fn with_s3_timeout<T, F, Fut>(op: F) -> Result<T, Failure>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<T, Failure>>,
{
    match tokio::time::timeout(S3_OP_TIMEOUT, op()).await {
        Ok(r) => r,
        Err(_) => Err(Failure::transient("S3_TIMEOUT", "operacion s3 timeout")),
    }
}

/// CB propio de MinIO (S5-P3-01: un breaker por dependencia).
pub fn minio_breaker() -> Arc<CircuitBreaker> {
    Arc::new(CircuitBreaker::default())
}
