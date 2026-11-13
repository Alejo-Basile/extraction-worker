//! Corpus de PDFs patologicos y hostiles (S5-P3-05).
//!
//! Los casos se generan en el momento del test para no versionar binarios
//! grandes ni secretos: cada fixture es determinista y el `xref` queda
//! correcto salvo en los casos donde la corrupcion es justamente el defecto.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use actix_web::{http::StatusCode, App};
use extraction_core::error_code;
use extraction_core::TextExtractor;
use extraction_worker::consumer::pipeline::{
    ExtractionPipeline, InMemorySink, InMemorySource, PipelineLimits,
};
use extraction_worker::consumer::resilience::{CircuitBreaker, Failure};
use extraction_worker::extract::PdfExtractor;
use lopdf::Object;

fn must<T, E: std::fmt::Display>(r: Result<T, E>) -> T {
    match r {
        Ok(v) => v,
        Err(e) => panic!("fallo inesperado: {e}"),
    }
}

fn now_nanos() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

fn temp_path(tag: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "pdf-corpus-{tag}-{}-{}.pdf",
        std::process::id(),
        now_nanos()
    ))
}

fn write_bytes(path: &Path, bytes: &[u8]) {
    must(std::fs::write(path, bytes));
}

/// Serializa objetos consecutivos (1..n) en un PDF con xref clasica.
fn build_pdf(objects: &[String]) -> Vec<u8> {
    let mut out: Vec<u8> = b"%PDF-1.4\n".to_vec();
    let mut offsets: Vec<usize> = Vec::new();
    for (idx, body) in objects.iter().enumerate() {
        offsets.push(out.len());
        out.extend_from_slice(format!("{} 0 obj\n{}\nendobj\n", idx + 1, body).as_bytes());
    }

    let xref_pos = out.len();
    out.extend_from_slice(b"xref\n0 ");
    out.extend_from_slice(format!("{}\n", objects.len() + 1).as_bytes());
    out.extend_from_slice(b"0000000000 65535 f \n");
    for off in &offsets {
        out.extend_from_slice(format!("{off:010} 00000 n \n").as_bytes());
    }
    out.extend_from_slice(
        format!(
            "trailer<</Size {}/Root 1 0 R>>\nstartxref\n{xref_pos}\n%%EOF\n",
            objects.len() + 1
        )
        .as_bytes(),
    );
    out
}

fn text_pdf_objects(page_count: usize) -> Vec<String> {
    let mut objects = vec![
        "<</Type/Catalog/Pages 2 0 R>>".to_string(),
        format!(
            "<</Type/Pages/Count {page_count}/Kids[{}]>>",
            (0..page_count)
                .map(|i| format!("{} 0 R", 4 + i * 2))
                .collect::<Vec<_>>()
                .join(" ")
        ),
        "<</Type/Font/Subtype/Type1/BaseFont/Helvetica>>".to_string(),
    ];
    for page in 0..page_count {
        let content = format!("BT /F1 12 Tf 20 {} Td (Pagina {page}) Tj ET\n", 792 - 20);
        objects.push(format!(
            "<</Type/Page/Parent 2 0 R/MediaBox[0 0 612 792]/Contents {} 0 R/Resources<</Font<</F1 3 0 R>>>>>>",
            5 + page * 2
        ));
        objects.push(format!(
            "<</Length {}>>\nstream\n{}endstream",
            content.len(),
            content
        ));
    }
    objects
}

fn text_pdf(page_count: usize) -> Vec<u8> {
    build_pdf(&text_pdf_objects(page_count))
}

fn scanned_pdf() -> Vec<u8> {
    let objects = vec![
        "<</Type/Catalog/Pages 2 0 R>>".to_string(),
        "<</Type/Pages/Count 1/Kids[4 0 R]>>".to_string(),
        "<</Type/Font/Subtype/Type1/BaseFont/Helvetica>>".to_string(),
        "<</Type/Page/Parent 2 0 R/MediaBox[0 0 612 792]/Contents 5 0 R/Resources<</Font<</F1 3 0 R>>>>>>".to_string(),
        "<</Length 0>>\nstream\n\nendstream".to_string(),
    ];
    build_pdf(&objects)
}

fn javascript_pdf() -> Vec<u8> {
    let mut objects = text_pdf_objects(1);
    objects[0] = "<</Type/Catalog/Pages 2 0 R/OpenAction 7 0 R>>".to_string();
    objects.push("<</S/JavaScript/JS(app.alert\\(\"hola\"\\))>>".to_string());
    build_pdf(&objects)
}

fn malformed_font_pdf() -> Vec<u8> {
    let mut objects = text_pdf_objects(1);
    objects[2] = "<</Type/Font/Subtype/Type1/BaseFont/Helvetica/Encoding 999 0 R>>".to_string();
    build_pdf(&objects)
}

fn cyclic_refs_pdf() -> Vec<u8> {
    let objects = vec![
        "<</Type/Catalog/Pages 2 0 R>>".to_string(),
        "<</Type/Pages/Count 1/Kids[3 0 R]>>".to_string(),
        "<</Type/Pages/Count 1/Kids[2 0 R]>>".to_string(),
    ];
    build_pdf(&objects)
}

fn encrypted_pdf(path: &Path) {
    let bytes = text_pdf(1);
    write_bytes(path, &bytes);
    let mut doc = must(lopdf::Document::load(path));
    let id = Object::Array(vec![
        Object::string_literal(b"extraction-worker-corpus".to_vec()),
        Object::string_literal(b"extraction-worker-corpus".to_vec()),
    ]);
    doc.trailer.set("ID", id);
    let version = lopdf::EncryptionVersion::V1 {
        document: &doc,
        owner_password: "owner-secret",
        user_password: "user-secret",
        permissions: lopdf::Permissions::all(),
    };
    let state = must(lopdf::EncryptionState::try_from(version));
    must(doc.encrypt(&state));
    must(doc.save(path));
}

fn assert_document_error(result: Result<String, extraction_core::ExtractorError>, expected: &str) {
    match result {
        Err(e) => {
            assert_eq!(e.code, expected, "mensaje: {}", e.message);
            assert_eq!(
                e.kind,
                extraction_core::ErrorKind::Business,
                "los defectos del PDF son negocio, no infraestructura"
            );
        }
        Ok(text) => panic!("debe fallar, obtuvo texto: {text:?}"),
    }
}

/// Ejecuta una extraccion con presupuesto: un PDF hostil puede tardar, pero
/// no puede colgar el test.
fn extract_with_limit(
    ext: &PdfExtractor,
    path: &Path,
) -> Result<String, extraction_core::ExtractorError> {
    let ext = std::sync::Arc::new(PdfExtractor::new(ext.max_pages));
    let path = path.to_path_buf();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(ext.extract(&path));
    });
    match rx.recv_timeout(Duration::from_secs(5)) {
        Ok(result) => result,
        Err(_) => panic!("la extraccion excedio el limite de 5s"),
    }
}

#[test]
fn pdf_vacio_y_no_pdf_son_corruptos() {
    let ext = PdfExtractor::new(2000);

    let empty = temp_path("vacio");
    write_bytes(&empty, &[]);
    assert_document_error(extract_with_limit(&ext, &empty), error_code::PDF_CORRUPT);
    let _ = std::fs::remove_file(&empty);

    let not_pdf = temp_path("no-pdf");
    write_bytes(&not_pdf, b"esto no es un pdf");
    assert_document_error(extract_with_limit(&ext, &not_pdf), error_code::PDF_CORRUPT);
    let _ = std::fs::remove_file(&not_pdf);
}

#[test]
fn pdf_cifrado_es_error_de_negocio() {
    let path = temp_path("cifrado");
    encrypted_pdf(&path);
    let ext = PdfExtractor::new(2000);
    assert_document_error(extract_with_limit(&ext, &path), error_code::PDF_ENCRYPTED);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn pdf_escaneado_sin_texto_es_error_de_negocio() {
    let path = temp_path("sin-texto");
    write_bytes(&path, &scanned_pdf());
    let ext = PdfExtractor::new(2000);
    assert_document_error(extract_with_limit(&ext, &path), error_code::PDF_NO_TEXT);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn pdf_con_miles_de_paginas_muere_en_el_conteo_previo() {
    let path = temp_path("miles-de-paginas");
    write_bytes(&path, &text_pdf(1200));
    let ext = PdfExtractor::new(1000);
    assert_document_error(
        extract_with_limit(&ext, &path),
        error_code::PDF_TOO_MANY_PAGES,
    );
    let _ = std::fs::remove_file(&path);
}

#[test]
fn pdf_con_javascript_embebido_no_se_ejecuta_ni_cuelga() {
    let path = temp_path("javascript");
    write_bytes(&path, &javascript_pdf());
    let ext = PdfExtractor::new(2000);
    match extract_with_limit(&ext, &path) {
        Ok(text) => assert!(
            text.contains("Pagina 0"),
            "el texto normal debe extraerse, obtuvo: {text:?}"
        ),
        Err(e) => panic!("un PDF con JS debe ignorar la accion, fallo: {e}"),
    }
    let _ = std::fs::remove_file(&path);
}

#[test]
fn pdf_con_fuente_malformada_no_cuelga_ni_paniquea() {
    let path = temp_path("fuente-malformada");
    write_bytes(&path, &malformed_font_pdf());
    let ext = PdfExtractor::new(2000);
    match extract_with_limit(&ext, &path) {
        Err(e) => {
            assert_eq!(e.code, error_code::PDF_CORRUPT);
            assert_eq!(e.kind, extraction_core::ErrorKind::Business);
        }
        Ok(text) => assert!(
            text.contains("Pagina 0"),
            "si la libreria es tolerante, debe extraer texto normal, obtuvo: {text:?}"
        ),
    }
    let _ = std::fs::remove_file(&path);
}

#[test]
fn pdf_con_referencias_ciclicas_no_cuelga_ni_paniquea() {
    let path = temp_path("refs-ciclicas");
    write_bytes(&path, &cyclic_refs_pdf());
    let ext = PdfExtractor::new(2000);
    match extract_with_limit(&ext, &path) {
        Err(e) => {
            assert_eq!(e.kind, extraction_core::ErrorKind::Business);
            assert!(
                e.code == error_code::PDF_CORRUPT || e.code == error_code::PDF_NO_TEXT,
                "codigo inesperado: {}",
                e.code
            );
        }
        Ok(text) => assert!(
            text.is_empty(),
            "si la libreria es tolerante, no debe inventar texto, obtuvo: {text:?}"
        ),
    }
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn objeto_gigante_corta_por_tamano_antes_de_extraer() {
    const KEY: &str = "raw-pdfs/gigante.pdf";
    let source = InMemorySource::with_file(KEY, vec![0u8; 1024]);
    let sink = InMemorySink::default();
    let pipeline = ExtractionPipeline::new(
        std::sync::Arc::new(source),
        std::sync::Arc::new(sink),
        std::sync::Arc::new(PdfExtractor::new(2000)),
        std::sync::Arc::new(CircuitBreaker::default()),
        PipelineLimits {
            job_timeout: Duration::from_secs(5),
            s3_op_timeout: Duration::from_secs(1),
            max_pdf_bytes: 512,
            max_output_bytes: 1024,
        },
    );
    let item = extraction_core::WorkItem {
        document_id: "gigante".to_string(),
        object_key: KEY.to_string(),
        correlation_id: "gigante".to_string(),
        enqueued_at: "2026-11-30T10:15:03Z".to_string(),
        schema_version: 1,
    };
    match pipeline.process(&item).await {
        Err(Failure::Document { code, .. }) => assert_eq!(code, error_code::PDF_TOO_LARGE),
        other => panic!("debe cortar por tamano, obtuvo: {other:?}"),
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

#[actix_web::test]
async fn healthz_responde_durante_timeout_de_job() {
    const KEY: &str = "raw-pdfs/hostil-lento.pdf";
    let source = InMemorySource::with_file(KEY, b"%PDF-1.4".to_vec());
    let sink = InMemorySink::default();
    let pipeline = ExtractionPipeline::new(
        std::sync::Arc::new(source),
        std::sync::Arc::new(sink),
        std::sync::Arc::new(SlowExtractor {
            delay: Duration::from_millis(300),
            calls: AtomicU32::new(0),
        }),
        std::sync::Arc::new(CircuitBreaker::default()),
        PipelineLimits {
            job_timeout: Duration::from_millis(50),
            s3_op_timeout: Duration::from_secs(1),
            max_pdf_bytes: 1024,
            max_output_bytes: 1024,
        },
    );
    let item = extraction_core::WorkItem {
        document_id: "hostil-lento".to_string(),
        object_key: KEY.to_string(),
        correlation_id: "hostil-lento".to_string(),
        enqueued_at: "2026-11-30T10:15:03Z".to_string(),
        schema_version: 1,
    };

    let job = tokio::spawn(async move { pipeline.process(&item).await });
    let app =
        actix_web::test::init_service(App::new().service(extraction_worker::http::healthz)).await;
    let req = actix_web::test::TestRequest::get()
        .uri("/healthz")
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::OK);

    match must(job.await) {
        Err(Failure::Transient { code, .. }) => assert_eq!(code, "JOB_TIMEOUT"),
        other => panic!("debe morir por timeout, obtuvo: {other:?}"),
    }
}

/// Envuelve el extractor lento y cuenta sus invocaciones con un contador
/// compartido, sin modificar `SlowExtractor` (la prueba de liveness la
/// sigue usando intacta).
struct CountingSlowExtractor {
    inner: SlowExtractor,
    calls: std::sync::Arc<AtomicU32>,
}

impl TextExtractor for CountingSlowExtractor {
    fn extract(&self, path: &Path) -> Result<String, extraction_core::ExtractorError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.inner.extract(path)
    }
}

#[actix_web::test]
async fn job_timeout_devuelve_transitorio() {
    const KEY: &str = "raw-pdfs/hostil-lento-timeout.pdf";
    let source = InMemorySource::with_file(KEY, b"%PDF-1.4".to_vec());
    let sink = InMemorySink::default();
    let written = sink.files.clone();
    let calls = std::sync::Arc::new(AtomicU32::new(0));
    let pipeline = ExtractionPipeline::new(
        std::sync::Arc::new(source),
        std::sync::Arc::new(sink),
        std::sync::Arc::new(CountingSlowExtractor {
            inner: SlowExtractor {
                delay: Duration::from_millis(300),
                calls: AtomicU32::new(0),
            },
            calls: std::sync::Arc::clone(&calls),
        }),
        std::sync::Arc::new(CircuitBreaker::default()),
        PipelineLimits {
            job_timeout: Duration::from_millis(50),
            s3_op_timeout: Duration::from_secs(1),
            max_pdf_bytes: 1024,
            max_output_bytes: 1024,
        },
    );
    let item = extraction_core::WorkItem {
        document_id: "hostil-lento".to_string(),
        object_key: KEY.to_string(),
        correlation_id: "hostil-lento".to_string(),
        enqueued_at: "2026-11-30T10:15:03Z".to_string(),
        schema_version: 1,
    };

    match pipeline.process(&item).await {
        Err(Failure::Transient { code, .. }) => assert_eq!(code, "JOB_TIMEOUT"),
        other => panic!("debe morir por timeout, obtuvo: {other:?}"),
    }

    // El extractor corre en un hilo bloqueante: al vencer el timeout el
    // contador ya fue incrementado, pero se espera acotado por si la
    // planificacion se atrasa.
    for _ in 0..50 {
        if calls.load(Ordering::Relaxed) > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        calls.load(Ordering::Relaxed),
        1,
        "el extractor lento debe haber entrado exactamente una vez"
    );

    // Un job en timeout no escribe el .txt: el pipeline falla y no hay ACK.
    let files = written.lock().unwrap_or_else(|e| e.into_inner());
    assert!(
        files.is_empty(),
        "un job en timeout no debe escribir el .txt: {files:?}"
    );
}
