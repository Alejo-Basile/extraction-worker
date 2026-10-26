//! Pipeline de extraccion (S2-P3-01/03, seccion 11.4).
//!
//! Flujo por mensaje valido: GET `raw-pdfs/<object_key>` a un archivo temporal
//! en streaming (con limite duro de tamano), extraccion CPU-bound en
//! `spawn_blocking`, PUT `extracted-txt/<document_id>.txt` (idempotente).
//!
//! Composicion de resiliencia por operacion S3: `Retry` envuelve al breaker
//! de MinIO (instancia propia, S5-P3-01) y cada intento lleva su timeout.
//! El timeout duro del job completo (`WORKER_JOB_TIMEOUT_SECS`) envuelve a
//! todo lo anterior: `min-idle-time` de `XAUTOCLAIM` debe superar 2x este
//! valor (S1-P3-05).

use std::collections::HashMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use extraction_core::TextExtractor;
use tokio::time::timeout;

use crate::consumer::resilience::{with_retry_cb, CircuitBreaker, Failure};
use crate::objects::with_s3_timeout;

/// Puerto de descarga del PDF original. `dest` es un archivo temporal que el
/// pipeline crea y borra; el maximo se aplica en streaming (S2-P3-01).
pub trait ObjectSource: Send + Sync + 'static {
    fn download_to(
        &self,
        key: &str,
        max_bytes: u64,
        dest: &Path,
    ) -> impl Future<Output = Result<u64, Failure>> + Send;
}

/// Puerto de subida del texto extraido: put idempotente (S2-P3-08).
pub trait TextSink: Send + Sync + 'static {
    fn put_text(&self, key: &str, text: &str) -> impl Future<Output = Result<(), Failure>> + Send;
}

/// Archivo temporal con limpieza garantizada (Drop borra el archivo).
pub struct TempPdf {
    path: PathBuf,
}

impl TempPdf {
    pub fn create() -> Result<Self, Failure> {
        let dir = std::env::temp_dir();
        let name = format!(
            "extraction-worker-{}-{}-{}.pdf",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0),
            entropy_suffix(),
        );
        let path = dir.join(name);
        // Reservar el nombre creando el archivo vacio: evita colisiones.
        std::fs::File::create(&path)
            .map_err(|e| Failure::transient("TMP_CREATE", e.to_string()))?;
        Ok(Self { path })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempPdf {
    fn drop(&mut self) {
        // Best-effort: si falla, el SO limpia el directorio temporal.
        let _ = std::fs::remove_file(&self.path);
    }
}

fn entropy_suffix() -> u64 {
    use std::hash::BuildHasher;
    let h = std::collections::hash_map::RandomState::new();
    h.hash_one(std::time::Instant::now())
}

/// Clave del texto extraido en `extracted-txt` (seccion 11.4).
pub fn txt_key(document_id: &str) -> String {
    format!("{document_id}.txt")
}

pub struct ExtractionPipeline<S: ObjectSource, K: TextSink> {
    source: Arc<S>,
    sink: Arc<K>,
    extractor: Arc<dyn TextExtractor>,
    breaker: Arc<CircuitBreaker>,
    job_timeout: Duration,
    max_pdf_bytes: u64,
}

impl<S: ObjectSource, K: TextSink> ExtractionPipeline<S, K> {
    pub fn new(
        source: Arc<S>,
        sink: Arc<K>,
        extractor: Arc<dyn TextExtractor>,
        breaker: Arc<CircuitBreaker>,
        job_timeout: Duration,
        max_pdf_bytes: u64,
    ) -> Self {
        Self {
            source,
            sink,
            extractor,
            breaker,
            job_timeout,
            max_pdf_bytes,
        }
    }
    /// Procesa el item completo. Devuelve `Ok(())` si el `.txt` quedo subido;
    /// `Err(Failure::Document)` para errores del documento (van a DLQ) y
    /// `Err(Failure::Transient|Fatal|CircuitOpen)` para infraestructura.
    pub async fn process(&self, item: &extraction_core::WorkItem) -> Result<(), Failure> {
        let job = async {
            let temp = TempPdf::create()?;
            self.download(&item.object_key, &temp).await?;
            let text = self.extract(&temp).await?;
            let key = txt_key(&item.document_id);
            self.upload(&key, &text).await
        };
        match timeout(self.job_timeout, job).await {
            Ok(res) => res,
            Err(_) => Err(Failure::transient(
                "JOB_TIMEOUT",
                format!("job timeout tras {:?}", self.job_timeout),
            )),
        }
    }

    async fn download(&self, key: &str, temp: &TempPdf) -> Result<u64, Failure> {
        let source = self.source.clone();
        let dest = temp.path().to_path_buf();
        let max = self.max_pdf_bytes;
        let key = key.to_string();
        with_retry_cb(&self.breaker, || {
            let source = source.clone();
            let key = key.clone();
            let dest = dest.clone();
            async move {
                with_s3_timeout(|| async { source.download_to(&key, max, &dest).await }).await
            }
        })
        .await
    }

    async fn extract(&self, temp: &TempPdf) -> Result<String, Failure> {
        let extractor = self.extractor.clone();
        let path = temp.path().to_path_buf();
        // CPU-bound: fuera del runtime async (SDD 3.3). Un timeout no mata el
        // hilo, pero el job completa igualmente antes de soltarlo.
        match tokio::task::spawn_blocking(move || extractor.extract(&path)).await {
            Ok(Ok(text)) => Ok(text),
            Ok(Err(e)) => Err(Failure::Document {
                code: e.code,
                message: e.message,
            }),
            Err(e) => Err(Failure::transient("EXTRACT_JOIN", e.to_string())),
        }
    }

    async fn upload(&self, key: &str, text: &str) -> Result<(), Failure> {
        let sink = self.sink.clone();
        let key = key.to_string();
        let text = text.to_string();
        with_retry_cb(&self.breaker, || {
            let sink = sink.clone();
            let key = key.clone();
            let text = text.clone();
            async move { with_s3_timeout(|| async { sink.put_text(&key, &text).await }).await }
        })
        .await
    }
}

/// Fuente en memoria para tests del pipeline.
pub struct InMemorySource {
    pub files: Arc<std::sync::Mutex<HashMap<String, Vec<u8>>>>,
}

impl InMemorySource {
    pub fn with_file(key: &str, bytes: Vec<u8>) -> Self {
        let mut files = HashMap::new();
        files.insert(key.to_string(), bytes);
        Self {
            files: Arc::new(std::sync::Mutex::new(files)),
        }
    }
}

impl ObjectSource for InMemorySource {
    async fn download_to(&self, key: &str, max_bytes: u64, dest: &Path) -> Result<u64, Failure> {
        let bytes = {
            let files = self
                .files
                .lock()
                .map_err(|_| Failure::transient("TEST_LOCK", "lock envenenado"))?;
            files
                .get(key)
                .cloned()
                .ok_or_else(|| Failure::transient("S3_IO", format!("no existe {key}")))?
        };
        if bytes.len() as u64 > max_bytes {
            return Err(Failure::Document {
                code: extraction_core::error_code::PDF_TOO_LARGE.to_string(),
                message: format!("{key} supera {max_bytes}"),
            });
        }
        std::fs::write(dest, bytes)
            .map_err(|e| Failure::transient("S3_TMP_FILE", e.to_string()))?;
        Ok(std::fs::metadata(dest).map(|m| m.len()).unwrap_or_default())
    }
}

/// Sink en memoria para tests del pipeline.
#[derive(Default)]
pub struct InMemorySink {
    pub files: Arc<std::sync::Mutex<HashMap<String, String>>>,
}

impl TextSink for InMemorySink {
    async fn put_text(&self, key: &str, text: &str) -> Result<(), Failure> {
        self.files
            .lock()
            .map_err(|_| Failure::transient("TEST_LOCK", "lock envenenado"))?
            .insert(key.to_string(), text.to_string());
        Ok(())
    }
}

/// Pipeline real: MinIO (object_store) + `pdf-extract`, construido desde
/// `ConsumerConfig` (S2-P3-01).
impl ExtractionPipeline<crate::objects::ObjectStoreSource, crate::objects::ObjectStoreSink> {
    /// Arma el pipeline real (MinIO + pdf-extract) desde la configuracion.
    pub fn from_env(cfg: &super::config::ConsumerConfig) -> Result<Self, Failure> {
        let s3 = crate::objects::S3Config {
            endpoint: cfg.s3_endpoint.clone(),
            bucket_raw: cfg.s3_bucket_raw.clone(),
            bucket_txt: cfg.s3_bucket_txt.clone(),
            access_key: cfg.s3_access_key.clone(),
            secret_key: cfg.s3_secret_key.clone(),
        };
        let source = crate::objects::ObjectStoreSource::new(&s3)?;
        let sink = crate::objects::ObjectStoreSink::new(&s3)?;
        Ok(Self {
            source: Arc::new(source),
            sink: Arc::new(sink),
            extractor: Arc::new(crate::extract::PdfExtractor),
            breaker: crate::objects::minio_breaker(),
            job_timeout: cfg.job_timeout,
            max_pdf_bytes: cfg.max_pdf_bytes,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use extraction_core::fake::FakeTextExtractor;
    use std::sync::atomic::{AtomicU32, Ordering};

    fn work_item() -> extraction_core::WorkItem {
        extraction_core::WorkItem {
            document_id: "01J9Z8QK3M7X2V0N4P6R8T1Y0B".to_string(),
            object_key: "raw-pdfs/01J9Z8QK3M7X2V0N4P6R8T1Y0B.pdf".to_string(),
            correlation_id: "01J9Z8QK3M7X2V0N4P6R8T1Y0B".to_string(),
            enqueued_at: "2026-11-30T10:15:03Z".to_string(),
            schema_version: 1,
        }
    }

    const KEY: &str = "raw-pdfs/01J9Z8QK3M7X2V0N4P6R8T1Y0B.pdf";

    fn pipeline(
        source: InMemorySource,
        extractor: Arc<dyn TextExtractor>,
        job_timeout: Duration,
    ) -> ExtractionPipeline<InMemorySource, InMemorySink> {
        let sink = InMemorySink::default();
        ExtractionPipeline::new(
            Arc::new(source),
            Arc::new(sink),
            extractor,
            Arc::new(CircuitBreaker::default()),
            job_timeout,
            1024 * 1024,
        )
    }

    fn sink_of(
        p: &ExtractionPipeline<InMemorySource, InMemorySink>,
    ) -> Arc<std::sync::Mutex<HashMap<String, String>>> {
        p.sink.files.clone()
    }

    #[tokio::test]
    async fn camino_feliz_extrae_y_sube() {
        let source = InMemorySource::with_file(KEY, b"%PDF-1.4 dummy".to_vec());
        let p = pipeline(
            source,
            Arc::new(FakeTextExtractor::returning("hola mundo")),
            Duration::from_secs(5),
        );
        let item = work_item();
        p.process(&item)
            .await
            .unwrap_or_else(|e| panic!("pipeline debe triunfar: {e}"));
        let files = sink_of(&p);
        let files = files.lock().unwrap_or_else(|e| e.into_inner());
        let text = files
            .get(&txt_key("01J9Z8QK3M7X2V0N4P6R8T1Y0B"))
            .unwrap_or_else(|| panic!("falta el .txt en el sink"));
        assert_eq!(text, "hola mundo");
    }

    #[tokio::test]
    async fn error_de_documento_propaga_codigo_y_kind() {
        let source = InMemorySource::with_file(KEY, b"%PDF-1.4 dummy".to_vec());
        let p = pipeline(
            source,
            Arc::new(FakeTextExtractor::failing("PDF_CORRUPT", "xref roto")),
            Duration::from_secs(5),
        );
        let item = work_item();
        match p.process(&item).await {
            Err(Failure::Document { code, .. }) => assert_eq!(code, "PDF_CORRUPT"),
            other => panic!("debe ser un fallo de documento, obtuvo: {other:?}"),
        }
    }

    #[tokio::test]
    async fn objeto_grande_falla_como_documento() {
        let source = InMemorySource::with_file(KEY, vec![0u8; 2 * 1024 * 1024]);
        let p = pipeline(
            source,
            Arc::new(FakeTextExtractor::returning("x")),
            Duration::from_secs(5),
        );
        let item = work_item();
        match p.process(&item).await {
            Err(Failure::Document { code, .. }) => {
                assert_eq!(code, extraction_core::error_code::PDF_TOO_LARGE)
            }
            other => panic!("debe ser PDF_TOO_LARGE, obtuvo: {other:?}"),
        }
    }

    struct SlowExtractor {
        delay: Duration,
        calls: AtomicU32,
    }

    impl TextExtractor for SlowExtractor {
        fn extract(&self, _path: &Path) -> Result<String, extraction_core::ExtractorError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            std::thread::sleep(self.delay);
            Ok("tarde".to_string())
        }
    }

    #[tokio::test]
    async fn job_timeout_devuelve_transitorio() {
        let source = InMemorySource::with_file(KEY, b"%PDF-1.4".to_vec());
        let p = pipeline(
            source,
            Arc::new(SlowExtractor {
                delay: Duration::from_millis(300),
                calls: AtomicU32::new(0),
            }),
            Duration::from_millis(50),
        );
        let item = work_item();
        match p.process(&item).await {
            Err(Failure::Transient { code, .. }) => assert_eq!(code, "JOB_TIMEOUT"),
            other => panic!("debe ser JOB_TIMEOUT, obtuvo: {other:?}"),
        }
    }

    #[tokio::test]
    async fn transitorio_se_reintenta_hasta_exito() {
        // La descarga falla 2 veces y luego triunfa: retry + breaker lo cubren.
        struct FlakySource {
            fails: AtomicU32,
        }
        impl ObjectSource for FlakySource {
            async fn download_to(
                &self,
                _key: &str,
                _max: u64,
                dest: &Path,
            ) -> Result<u64, Failure> {
                if self.fails.fetch_sub(1, Ordering::Relaxed) > 0 {
                    return Err(Failure::transient("S3_IO", "timeout simulado"));
                }
                std::fs::write(dest, b"%PDF-1.4")
                    .map_err(|e| Failure::transient("S3_TMP_FILE", e.to_string()))?;
                Ok(8)
            }
        }
        let p = ExtractionPipeline::new(
            Arc::new(FlakySource {
                fails: AtomicU32::new(2),
            }),
            Arc::new(InMemorySink::default()),
            Arc::new(FakeTextExtractor::returning("ok")),
            Arc::new(CircuitBreaker::default()),
            Duration::from_secs(5),
            1024 * 1024,
        );
        let item = work_item();
        p.process(&item)
            .await
            .unwrap_or_else(|e| panic!("con reintentos debe triunfar: {e}"));
    }
}
