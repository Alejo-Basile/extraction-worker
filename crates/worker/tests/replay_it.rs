//! Tests de integracion de replay de DLQ y reaper (S4-P3-01, S4-P3-02,
//! S6-P3-01). Requieren Redis vivo:
//! `REDIS_TEST_URL=redis://127.0.0.1:6379/ cargo test --all-features -- --ignored`

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use extraction_core::WorkItem;
use extraction_worker::consumer::config::ConsumerConfig;
use extraction_worker::consumer::replay::{self, ReplayOutcome};
use extraction_worker::consumer::{dlq, group};
use redis::aio::MultiplexedConnection;
use redis::streams::StreamReadOptions;
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
        s3_endpoint: "http://127.0.0.1:9000".to_string(),
        s3_bucket_raw: "raw-pdfs".to_string(),
        s3_bucket_txt: "extracted-txt".to_string(),
        s3_access_key: "worker-dev".to_string(),
        s3_secret_key: "worker-dev-secret".to_string(),
        mongo_uri: "mongodb://127.0.0.1:27017/app".to_string(),
        mongo_db: "app".to_string(),
        mongo_timeout: Duration::from_secs(2),
        max_pages: 2000,
        max_output_bytes: 1024 * 1024,
        metrics_interval: Duration::from_secs(15),
        s3_op_timeout: Duration::from_secs(5),
        shutdown_drain: Duration::from_secs(1),
        dlq_replay_audit_stream: format!("{stream}-dlq-replay"),
        dlq_replayed_key: format!("{stream}-dlq-replayed"),
        dead_consumer_ms: 10,
    }
}

async fn get_conn() -> MultiplexedConnection {
    let client = must(redis::Client::open(test_url().as_str()));
    must(client.get_multiplexed_async_connection().await)
}

async fn add_dlq_entry(
    conn: &mut MultiplexedConnection,
    cfg: &ConsumerConfig,
    document_id: &str,
) -> String {
    must(
        redis::cmd("XADD")
            .arg(&cfg.dlq_stream)
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
            .arg("delivery_count")
            .arg("2")
            .arg("failed_at")
            .arg("2026-11-30T10:15:04Z")
            .arg("error_code")
            .arg("INVALID_CONTRACT")
            .arg("error_message")
            .arg("prueba")
            .arg("error_kind")
            .arg("business")
            .query_async(conn)
            .await,
    )
}

fn stream_field(entry: &redis::streams::StreamId, key: &str) -> String {
    match entry
        .map
        .get(key)
        .and_then(|v| redis::from_redis_value::<String>(v.clone()).ok())
    {
        Some(value) => value,
        None => panic!("campo {key} ausente en la entrada"),
    }
}

fn item(document_id: &str) -> WorkItem {
    WorkItem {
        document_id: document_id.to_string(),
        object_key: format!("raw-pdfs/{document_id}.pdf"),
        correlation_id: document_id.to_string(),
        enqueued_at: "2026-11-30T10:15:03Z".to_string(),
        schema_version: 1,
    }
}

async fn cleanup(conn: &mut MultiplexedConnection, cfg: &ConsumerConfig) {
    let _: i64 = must(
        conn.del(&[
            cfg.stream.as_str(),
            cfg.dlq_stream.as_str(),
            cfg.dlq_replay_audit_stream.as_str(),
            cfg.dlq_replayed_key.as_str(),
        ])
        .await,
    );
}

#[tokio::test]
#[ignore]
async fn replay_mueve_dlq_a_stream_y_audita_una_sola_vez() {
    let mut conn = get_conn().await;
    let stream = format!("it:worker:replay:{}:{}", std::process::id(), now_millis());
    let cfg = test_cfg(&stream);
    let document_id = "doc-replay-1";
    let dlq_id = add_dlq_entry(&mut conn, &cfg, document_id).await;
    let entry = match must(dlq::read_entries(&mut conn, &cfg, 10).await)
        .into_iter()
        .find(|e| e.id == dlq_id)
    {
        Some(entry) => entry,
        None => panic!("la entrada debe existir en la DLQ"),
    };
    let work = item(document_id);

    let (outcome, new_id) = must(
        replay::replay_one(
            &mut conn,
            &cfg,
            &entry,
            &work,
            "tester",
            "2026-11-30T10:20:00Z",
            "INVALID_CONTRACT",
        )
        .await,
    );
    assert_eq!(outcome, ReplayOutcome::Replayed);
    assert!(new_id.is_some(), "el replay devuelve el nuevo id");

    let dlq_len: u64 = must(conn.xlen(&cfg.dlq_stream).await);
    let main_len: u64 = must(conn.xlen(&cfg.stream).await);
    let audit_len: u64 = must(conn.xlen(&cfg.dlq_replay_audit_stream).await);
    let seen: bool = must(conn.hexists(&cfg.dlq_replayed_key, &dlq_id).await);
    assert_eq!(dlq_len, 0, "la DLQ queda vacia");
    assert_eq!(main_len, 1, "el mensaje se reemite al stream principal");
    assert_eq!(audit_len, 1, "se registra una entrada de auditoria");
    assert!(seen, "se registra el id replayeado");

    let audit: redis::streams::StreamRangeReply =
        must(conn.xrange_all(&cfg.dlq_replay_audit_stream).await);
    let audit_entry = &audit.ids[0];
    assert_eq!(stream_field(audit_entry, "replayed_by"), "tester");
    assert_eq!(stream_field(audit_entry, "document_id"), document_id);
    assert_eq!(stream_field(audit_entry, "error_code"), "INVALID_CONTRACT");

    let main_entry: redis::streams::StreamRangeReply = must(conn.xrange_all(&cfg.stream).await);
    assert_eq!(stream_field(&main_entry.ids[0], "document_id"), document_id);

    let (outcome, _) = must(
        replay::replay_one(
            &mut conn,
            &cfg,
            &entry,
            &work,
            "tester",
            "2026-11-30T10:20:01Z",
            "INVALID_CONTRACT",
        )
        .await,
    );
    assert_eq!(outcome, ReplayOutcome::AlreadyReplayed);
    let main_len: u64 = must(conn.xlen(&cfg.stream).await);
    assert_eq!(main_len, 1, "replay repetido no duplica el mensaje");

    cleanup(&mut conn, &cfg).await;
}

#[tokio::test]
#[ignore]
async fn replay_de_id_inexistente_es_no_op() {
    let mut conn = get_conn().await;
    let stream = format!("it:worker:missing:{}:{}", std::process::id(), now_millis());
    let cfg = test_cfg(&stream);
    let entry = dlq::DlqEntry {
        id: "9999999999999-0".to_string(),
        fields: Default::default(),
    };
    let (outcome, _) = must(
        replay::replay_one(
            &mut conn,
            &cfg,
            &entry,
            &item("doc-missing"),
            "tester",
            "2026-11-30T10:20:00Z",
            "INVALID_CONTRACT",
        )
        .await,
    );
    assert_eq!(outcome, ReplayOutcome::Missing);
    cleanup(&mut conn, &cfg).await;
}

#[tokio::test]
#[ignore]
async fn reaper_detecta_consumidor_con_pendientes_sin_senal() {
    let mut conn = get_conn().await;
    let stream = format!("it:worker:reaper:{}:{}", std::process::id(), now_millis());
    let cfg = test_cfg(&stream);
    must(group::ensure_group(&mut conn, &cfg).await);
    let _: String = must(
        redis::cmd("XADD")
            .arg(&cfg.stream)
            .arg("*")
            .arg("document_id")
            .arg("doc-reaper")
            .arg("object_key")
            .arg("raw-pdfs/doc-reaper.pdf")
            .arg("correlation_id")
            .arg("doc-reaper")
            .arg("enqueued_at")
            .arg("2026-11-30T10:15:03Z")
            .arg("schema_version")
            .arg("1")
            .query_async(&mut conn)
            .await,
    );
    let opts = StreamReadOptions::default().group(&cfg.group, &cfg.consumer_name);
    let _: redis::streams::StreamReadReply = must(
        conn.xread_options(&[cfg.stream.as_str()], &[">".to_string()], &opts)
            .await,
    );
    tokio::time::sleep(Duration::from_millis(30)).await;

    let dead = must(group::dead_consumers(&mut conn, &cfg).await);
    let found = match dead.iter().find(|c| c.name == cfg.consumer_name) {
        Some(consumer) => consumer,
        None => panic!("el consumidor con un pendiente y idle > umbral debe aparecer"),
    };
    assert_eq!(found.pending, 1);

    cleanup(&mut conn, &cfg).await;
}

#[tokio::test]
#[ignore]
async fn pel_detallada_incluye_identificadores_del_documento() {
    let mut conn = get_conn().await;
    let stream = format!("it:worker:pel:{}:{}", std::process::id(), now_millis());
    let cfg = test_cfg(&stream);
    must(group::ensure_group(&mut conn, &cfg).await);
    let _: String = must(
        redis::cmd("XADD")
            .arg(&cfg.stream)
            .arg("*")
            .arg("document_id")
            .arg("doc-pel")
            .arg("object_key")
            .arg("raw-pdfs/doc-pel.pdf")
            .arg("correlation_id")
            .arg("doc-pel")
            .arg("enqueued_at")
            .arg("2026-11-30T10:15:03Z")
            .arg("schema_version")
            .arg("1")
            .query_async(&mut conn)
            .await,
    );
    let opts = StreamReadOptions::default().group(&cfg.group, &cfg.consumer_name);
    let _: redis::streams::StreamReadReply = must(
        conn.xread_options(&[cfg.stream.as_str()], &[">".to_string()], &opts)
            .await,
    );
    let detail = must(group::pending_detailed(&mut conn, &cfg).await);
    let entries = match detail["entries"].as_array() {
        Some(entries) => entries,
        None => panic!("entries es un array"),
    };
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["document_id"], "doc-pel");
    assert_eq!(entries[0]["correlation_id"], "doc-pel");
    cleanup(&mut conn, &cfg).await;
}
