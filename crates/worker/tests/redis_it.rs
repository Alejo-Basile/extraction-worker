//! Tests de integracion contra Redis real (S1-P3-04, S2-P3-07).
//!
//! Requieren un Redis vivo; por eso van con `#[ignore]` y corren en el job
//! `integration` de CI (servicio `redis:7-alpine`) y en local con:
//! `docker run -d -p 6379:6379 redis:7-alpine`
//! `REDIS_TEST_URL=redis://127.0.0.1:6379/ cargo test --all-features -- --ignored`

use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use extraction_core::WorkItem;
use extraction_worker::consumer::config::ConsumerConfig;
use extraction_worker::consumer::{dlq, group};
use redis::aio::MultiplexedConnection;
use redis::streams::{StreamPendingCountReply, StreamReadOptions, StreamReadReply};
use redis::AsyncCommands;

fn test_url() -> String {
    std::env::var("REDIS_TEST_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".to_string())
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

fn test_cfg(stream: &str) -> ConsumerConfig {
    ConsumerConfig {
        redis_url: test_url(),
        stream: stream.to_string(),
        group: "it-group".to_string(),
        consumer_name: format!("it-consumer-{}", std::process::id()),
        block_ms: 100,
        count: 1,
        job_timeout: Duration::from_secs(1),
        min_idle_ms: 10,
        xautoclaim_interval: Duration::from_secs(1),
        max_attempts: 2,
        dlq_stream: format!("{stream}-dlq"),
        max_concurrency: 2,
        max_pdf_bytes: 1024 * 1024,
        s3_endpoint: std::env::var("S3_ENDPOINT")
            .unwrap_or_else(|_| "http://127.0.0.1:9000".to_string()),
        s3_bucket_raw: "raw-pdfs".to_string(),
        s3_bucket_txt: "extracted-txt".to_string(),
        s3_access_key: std::env::var("S3_ACCESS_KEY").unwrap_or_else(|_| "worker-dev".to_string()),
        s3_secret_key: std::env::var("S3_SECRET_KEY")
            .unwrap_or_else(|_| "worker-dev-secret".to_string()),
        mongo_uri: std::env::var("MONGO_WORKER_URI").unwrap_or_else(|_| {
            "mongodb://worker:worker-dev-secret@127.0.0.1:27017/app?authSource=app&directConnection=true"
                .to_string()
        }),
        mongo_db: "app".to_string(),
        mongo_timeout: Duration::from_secs(2),
        max_pages: 2000,
        max_output_bytes: 1024 * 1024,
        metrics_interval: std::time::Duration::from_secs(15),
        s3_op_timeout: std::time::Duration::from_secs(5),
        shutdown_drain: std::time::Duration::from_secs(1),
        dlq_replay_audit_stream: format!("{stream}-dlq-replay"),
        dlq_replayed_key: format!("{stream}-dlq-replayed"),
        dead_consumer_ms: 10,
    }
}

async fn get_conn() -> MultiplexedConnection {
    let client = must(redis::Client::open(test_url().as_str()));
    must(client.get_multiplexed_async_connection().await)
}

/// XADD de un mensaje plano (schema_version viaja como texto, como lo
/// entrega un relay de campos planos) y devuelve el id.
async fn xadd_flat(conn: &mut MultiplexedConnection, stream: &str, document_id: &str) -> String {
    let id: String = must(
        redis::cmd("XADD")
            .arg(stream)
            .arg("*")
            .arg("document_id")
            .arg(document_id)
            .arg("object_key")
            .arg(format!("raw-pdfs/{document_id}.pdf"))
            .arg("correlation_id")
            .arg(document_id)
            .arg("enqueued_at")
            .arg("2026-11-30T10:15:03Z")
            .arg("schema_version")
            .arg("1")
            .query_async(conn)
            .await,
    );
    id
}

async fn deliver_one(conn: &mut MultiplexedConnection, cfg: &ConsumerConfig) -> StreamReadReply {
    let opts = StreamReadOptions::default().group(&cfg.group, &cfg.consumer_name);
    must(
        conn.xread_options(&[cfg.stream.as_str()], &[">".to_string()], &opts)
            .await,
    )
}

fn field_str(map: &HashMap<String, redis::Value>, key: &str) -> String {
    match map.get(key) {
        Some(redis::Value::BulkString(b)) => String::from_utf8_lossy(b).to_string(),
        other => panic!("campo {key} ausente o con tipo inesperado: {other:?}"),
    }
}

async fn cleanup(conn: &mut MultiplexedConnection, cfg: &ConsumerConfig) {
    let _: i64 = must(
        conn.del(&[cfg.stream.as_str(), cfg.dlq_stream.as_str()])
            .await,
    );
}

#[tokio::test]
#[ignore]
async fn dlq_atomica_escribe_contrato_y_hace_ack() {
    let mut conn = get_conn().await;
    let stream = format!("it:worker:dlq:{}:{}", std::process::id(), now_millis());
    let cfg = test_cfg(&stream);
    must(group::ensure_group(&mut conn, &cfg).await);

    let msg_id = xadd_flat(&mut conn, &cfg.stream, "doc-it-1").await;
    let read = deliver_one(&mut conn, &cfg).await;
    assert!(
        !read.keys.is_empty(),
        "el mensaje recien agregado debe entregarse al grupo"
    );

    // El mensaje esta pendiente con delivery count 1.
    let pend: StreamPendingCountReply = must(
        conn.xpending_count(&cfg.stream, &cfg.group, "-", "+", 100)
            .await,
    );
    assert_eq!(pend.ids.len(), 1);
    assert_eq!(pend.ids[0].id, msg_id);
    assert_eq!(pend.ids[0].times_delivered, 1);

    // Mover a DLQ con causa de contrato.
    let item = WorkItem {
        document_id: "doc-it-1".to_string(),
        object_key: "raw-pdfs/doc-it-1.pdf".to_string(),
        correlation_id: "doc-it-1".to_string(),
        enqueued_at: "2026-11-30T10:15:03Z".to_string(),
        schema_version: 1,
    };
    let moved = must(
        dlq::move_to_dlq_with_cause(
            &mut conn,
            &cfg,
            &msg_id,
            &item,
            "INVALID_CONTRACT",
            "prueba de integracion",
            extraction_core::ErrorKind::Business,
        )
        .await,
    );
    assert!(moved, "la primera llamada debe mover el mensaje");

    // La DLQ tiene exactamente una entrada con los 10 campos del contrato.
    let dlq_entries = must(
        conn.xrange_all::<_, redis::streams::StreamRangeReply>(&cfg.dlq_stream)
            .await,
    );
    assert_eq!(dlq_entries.ids.len(), 1, "una sola entrada en la DLQ");
    let fields = &dlq_entries.ids[0].map;
    assert_eq!(field_str(fields, "document_id"), "doc-it-1");
    assert_eq!(field_str(fields, "object_key"), "raw-pdfs/doc-it-1.pdf");
    assert_eq!(field_str(fields, "correlation_id"), "doc-it-1");
    assert_eq!(field_str(fields, "enqueued_at"), "2026-11-30T10:15:03Z");
    assert_eq!(field_str(fields, "schema_version"), "1");
    assert_eq!(field_str(fields, "delivery_count"), "1");
    assert_eq!(field_str(fields, "error_code"), "INVALID_CONTRACT");
    assert_eq!(field_str(fields, "error_kind"), "business");
    assert!(!field_str(fields, "failed_at").is_empty());
    assert!(!field_str(fields, "error_message").is_empty());

    // El original ya no esta pendiente (XACK dentro del script).
    let pend: StreamPendingCountReply = must(
        conn.xpending_count(&cfg.stream, &cfg.group, "-", "+", 100)
            .await,
    );
    assert!(
        pend.ids.is_empty(),
        "el mensaje original debe estar ACK-eado"
    );

    // Idempotencia: reintentar la misma operacion es un no-op que no duplica.
    let moved_again = must(
        dlq::move_to_dlq_with_cause(
            &mut conn,
            &cfg,
            &msg_id,
            &item,
            "INVALID_CONTRACT",
            "prueba de integracion",
            extraction_core::ErrorKind::Business,
        )
        .await,
    );
    assert!(!moved_again, "el reintento no debe mover nada");
    let dlq_entries = must(
        conn.xrange_all::<_, redis::streams::StreamRangeReply>(&cfg.dlq_stream)
            .await,
    );
    assert_eq!(dlq_entries.ids.len(), 1, "sin duplicados en la DLQ");

    cleanup(&mut conn, &cfg).await;
}

#[tokio::test]
#[ignore]
async fn delivery_count_sigue_los_reintentos_reales() {
    let mut conn = get_conn().await;
    let stream = format!("it:worker:cnt:{}:{}", std::process::id(), now_millis());
    let cfg = test_cfg(&stream);
    must(group::ensure_group(&mut conn, &cfg).await);

    let msg_id = xadd_flat(&mut conn, &cfg.stream, "doc-it-2").await;
    let _ = deliver_one(&mut conn, &cfg).await;

    let count = must(dlq::get_delivery_count_by_id(&mut conn, &cfg, &msg_id).await);
    assert_eq!(count, 1, "la primera entrega es el intento 1");

    // Un XCLAIM re-deliverea y Redis incrementa el delivery count.
    let _: redis::Value = must(
        redis::cmd("XCLAIM")
            .arg(&cfg.stream)
            .arg(&cfg.group)
            .arg(&cfg.consumer_name)
            .arg(0)
            .arg(&msg_id)
            .query_async(&mut conn)
            .await,
    );
    let count = must(dlq::get_delivery_count_by_id(&mut conn, &cfg, &msg_id).await);
    assert_eq!(count, 2, "la re-entrega incrementa el intento");

    // Un id inexistente se reporta como no pendiente (1, y la DLQ no-op).
    let count = must(dlq::get_delivery_count_by_id(&mut conn, &cfg, "9999999999999-0").await);
    assert_eq!(count, 1);

    cleanup(&mut conn, &cfg).await;
}

#[tokio::test]
#[ignore]
async fn inspeccion_de_pel_muestra_consumidor_y_antiguedad() {
    let mut conn = get_conn().await;
    let stream = format!("it:worker:pel:{}:{}", std::process::id(), now_millis());
    let cfg = test_cfg(&stream);
    must(group::ensure_group(&mut conn, &cfg).await);

    let _ = xadd_flat(&mut conn, &cfg.stream, "doc-it-3").await;
    let _ = deliver_one(&mut conn, &cfg).await;

    let summary = must(group::pending_summary(&mut conn, &cfg).await);
    assert_eq!(summary["pending_count"], 1);
    let consumers = match summary["consumers"].as_object() {
        Some(m) => m,
        None => panic!("consumers debe ser un objeto: {summary}"),
    };
    assert!(
        consumers.contains_key(&cfg.consumer_name),
        "el consumidor con pendientes debe aparecer: {consumers:?}"
    );

    let detailed = must(group::pending_detailed(&mut conn, &cfg).await);
    assert_eq!(detailed["count_sampled"], 1);
    let entries = match detailed["entries"].as_array() {
        Some(a) => a,
        None => panic!("entries debe ser un arreglo: {detailed}"),
    };
    assert_eq!(entries[0]["consumer"], cfg.consumer_name.as_str());
    assert_eq!(entries[0]["delivery_count"], 1);

    cleanup(&mut conn, &cfg).await;
}
