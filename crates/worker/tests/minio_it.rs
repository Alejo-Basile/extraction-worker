//! Tests de integracion contra MinIO real (S2-P3-01, seccion 11.4).
//!
//! Requieren el stack de labs:
//! `cd labs && cp .env.example .env && docker compose up -d`
//! `cargo test --all-features -- --ignored`

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use extraction_worker::consumer::config::ConsumerConfig;
use extraction_worker::consumer::pipeline::ExtractionPipeline;
use extraction_worker::objects;
use object_store::ObjectStoreExt;

fn s3_config() -> objects::S3Config {
    objects::S3Config {
        endpoint: std::env::var("S3_ENDPOINT")
            .unwrap_or_else(|_| "http://127.0.0.1:9000".to_string()),
        bucket_raw: "raw-pdfs".to_string(),
        bucket_txt: "extracted-txt".to_string(),
        access_key: std::env::var("S3_ACCESS_KEY").unwrap_or_else(|_| "worker-dev".to_string()),
        secret_key: std::env::var("S3_SECRET_KEY")
            .unwrap_or_else(|_| "worker-dev-secret".to_string()),
    }
}

fn must<T, E: std::fmt::Display>(r: Result<T, E>) -> T {
    match r {
        Ok(v) => v,
        Err(e) => panic!("fallo inesperado: {e}"),
    }
}

fn now_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

fn fixture_bytes() -> Vec<u8> {
    must(std::fs::read(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../tests/fixtures/minimal.pdf"
    )))
}

async fn get_raw_store() -> Arc<dyn object_store::ObjectStore> {
    let s3 = s3_config();
    let store = AmazonS3Builder::new()
        .with_endpoint(&s3.endpoint)
        .with_bucket_name(&s3.bucket_raw)
        .with_access_key_id(&s3.access_key)
        .with_secret_access_key(&s3.secret_key)
        .with_allow_http(true)
        .build();
    Arc::new(must(store))
}

async fn get_txt_store() -> Arc<dyn object_store::ObjectStore> {
    let s3 = s3_config();
    let store = AmazonS3Builder::new()
        .with_endpoint(&s3.endpoint)
        .with_bucket_name(&s3.bucket_txt)
        .with_access_key_id(&s3.access_key)
        .with_secret_access_key(&s3.secret_key)
        .with_allow_http(true)
        .build();
    Arc::new(must(store))
}

use object_store::aws::AmazonS3Builder;
use object_store::path::Path as StorePath;

async fn cleanup(stores: &[Arc<dyn object_store::ObjectStore>], keys: &[&str]) {
    for store in stores {
        for key in keys {
            let _ = ObjectStoreExt::delete(store.as_ref(), &StorePath::from(*key)).await;
        }
    }
}

#[tokio::test]
#[ignore]
async fn pipeline_extrae_pdf_de_minio_y_publica_txt() {
    let raw = get_raw_store().await;
    let txt = get_txt_store().await;
    let id = format!("it-f3a-{}-{}", std::process::id(), now_millis());
    let raw_key = format!("raw-pdfs/{id}.pdf");
    let txt_key = format!("{id}.txt");

    // 1. Subir el PDF de prueba a raw-pdfs (como lo haria el cliente via URL
    // prefirmada).
    let raw_store: Arc<dyn object_store::ObjectStore> = raw.clone();
    must(
        ObjectStoreExt::put(
            raw_store.as_ref(),
            &StorePath::from(raw_key.clone()),
            fixture_bytes().into(),
        )
        .await,
    );

    // 2. Pipeline real (usa los buckets del stack).
    let cfg = ConsumerConfig {
        s3_endpoint: s3_config().endpoint,
        s3_bucket_raw: "raw-pdfs".to_string(),
        s3_bucket_txt: "extracted-txt".to_string(),
        s3_access_key: s3_config().access_key,
        s3_secret_key: s3_config().secret_key,
        redis_url: String::new(),
        stream: String::new(),
        group: String::new(),
        consumer_name: String::new(),
        block_ms: 0,
        count: 1,
        job_timeout: Duration::from_secs(30),
        min_idle_ms: 0,
        xautoclaim_interval: Duration::from_secs(1),
        max_attempts: 5,
        dlq_stream: String::new(),
        max_concurrency: 1,
        max_pdf_bytes: 1024 * 1024,
    };
    let pipeline = must(ExtractionPipeline::from_env(&cfg));
    let item = extraction_core::WorkItem {
        document_id: id.clone(),
        object_key: raw_key.clone(),
        correlation_id: id.clone(),
        enqueued_at: "2026-11-30T10:15:03Z".to_string(),
        schema_version: 1,
    };

    // 3. Procesar: GET -> extraer -> PUT.
    pipeline
        .process(&item)
        .await
        .unwrap_or_else(|e| panic!("pipeline debe triunfar: {e}"));

    // 4. El .txt esta en extracted-txt con texto del fixture.
    let got = must(ObjectStoreExt::get(txt.as_ref(), &StorePath::from(txt_key.clone())).await);
    let bytes = must(got.bytes().await);
    let text = String::from_utf8_lossy(&bytes).to_string();
    assert!(
        text.contains("Hello extraction"),
        "el texto extraido debe contener el del fixture, obtuvo: {text:?}"
    );

    // 5. Idempotencia (S2-P3-08): repetir el proceso deja un solo objeto.
    pipeline
        .process(&item)
        .await
        .unwrap_or_else(|e| panic!("reproceso idempotente debe triunfar: {e}"));

    cleanup(&[raw, txt.clone()], &[&raw_key, &txt_key]).await;
}

#[tokio::test]
#[ignore]
async fn download_corta_por_limite_de_tamano() {
    let raw = get_raw_store().await;
    let id = format!("it-f3a-big-{}-{}", std::process::id(), now_millis());
    let key = format!("raw-pdfs/{id}.bin");
    let big: Vec<u8> = vec![0u8; 512 * 1024];

    let raw_store: Arc<dyn object_store::ObjectStore> = raw.clone();
    must(
        ObjectStoreExt::put(
            raw_store.as_ref(),
            &StorePath::from(key.clone()),
            big.into(),
        )
        .await,
    );

    let tmp = std::env::temp_dir().join(format!("f3a-limit-{}.bin", now_millis()));
    let res = objects::download_to(&raw, &key, 1024, &tmp).await;
    let _ = std::fs::remove_file(&tmp);
    match res {
        Err(extraction_worker::consumer::resilience::Failure::Document { code, .. }) => {
            assert_eq!(code, extraction_core::error_code::PDF_TOO_LARGE);
        }
        other => panic!("debe cortar por tamano, obtuvo: {other:?}"),
    }
    cleanup(&[raw], &[&key]).await;
}

#[allow(dead_code)]
fn unused(_m: &HashMap<String, String>) {}
