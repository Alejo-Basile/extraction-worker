//! IT de failover Sentinel (T-08): re-resolucion del master y continuidad
//! de consumo.
//!
//! Requiere un stack Sentinel vivo (master + replica + 3 sentinels); por eso
//! va con `#[ignore]` y corre en el job `integration` de CI (servicios
//! `redis-master`/`redis-replica`/`sentinel-*`) y en local con:
//! ```text
//! REDIS_SENTINEL_ADDRS=127.0.0.1:26379,127.0.0.1:26380,127.0.0.1:26381 \
//! REDIS_MASTER_NAME=myprimary \
//! cargo test --all-features --test sentinel_it -- --ignored --nocapture
//! ```
//!
//! El runner corre con mocks (`InMemorySource`/`InMemorySink`/
//! `FakeTextExtractor`) y un Mongo muerto: sin MinIO ni Mongo reales. El
//! mensaje post-failover se verifica entregado en la PEL (at-least-once);
//! NO se afirma perdida cero ni tiempo exacto de reconexion.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use extraction_core::fake::FakeTextExtractor;
use extraction_worker::consumer::config::ConsumerConfig;
use extraction_worker::consumer::pipeline::{
    ExtractionPipeline, InMemorySink, InMemorySource, PipelineLimits,
};
use extraction_worker::consumer::redis_conn::RedisClient;
use extraction_worker::consumer::resilience::CircuitBreaker;
use extraction_worker::consumer::runner::ConsumerRunner;
use extraction_worker::document_store::{DocumentStore, MongoConfig};
use extraction_worker::http::Readiness;
use extraction_worker::objects::{ObjectStoreSource, S3Config};
use redis::aio::MultiplexedConnection;
use redis::streams::StreamPendingCountReply;
use redis::AsyncCommands;
use tokio_util::sync::CancellationToken;

/// Serializa los IT de Sentinel: ambos provocan failover sobre el mismo
/// stack y, en paralelo, se interferirian entre si.
static SENTINEL_IT_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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

/// Entorno obligatorio del stack Sentinel (CSV de sentinels + master name).
fn sentinel_env() -> (Vec<String>, String) {
    let addrs: Vec<String> = match std::env::var("REDIS_SENTINEL_ADDRS") {
        Ok(raw) => raw
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_owned)
            .collect(),
        Err(_) => panic!(
            "REDIS_SENTINEL_ADDRS (CSV host:port) es obligatoria para sentinel_it; \
             ver el encabezado de este archivo"
        ),
    };
    assert!(
        !addrs.is_empty(),
        "REDIS_SENTINEL_ADDRS al menos debe nombrar un sentinel"
    );
    let master = match std::env::var("REDIS_MASTER_NAME") {
        Ok(v) if !v.trim().is_empty() => v,
        _ => panic!("REDIS_MASTER_NAME es obligatoria para sentinel_it"),
    };
    (addrs, master)
}

/// Config Sentinel para el IT: stream/grupo unicos por proceso; el resto del
/// entorno nunca se toca (mocks + Mongo muerto).
fn sentinel_cfg(stream: &str, group: &str, addrs: Vec<String>, master: String) -> ConsumerConfig {
    ConsumerConfig {
        // En modo Sentinel la URL solo aporta el password (vacio aca).
        redis_url: "redis://127.0.0.1:6379/".to_string(),
        redis_sentinel_addrs: addrs,
        redis_master_name: Some(master),
        redis_sentinel_password: None,
        stream: stream.to_string(),
        group: group.to_string(),
        consumer_name: format!("it-sentinel-consumer-{}", std::process::id()),
        block_ms: 100,
        count: 1,
        job_timeout: Duration::from_secs(5),
        min_idle_ms: 10,
        xautoclaim_interval: Duration::from_secs(1),
        max_attempts: 3,
        dlq_stream: format!("{stream}-dlq"),
        max_concurrency: 2,
        max_pdf_bytes: 1024 * 1024,
        s3_endpoint: "http://127.0.0.1:1".to_string(),
        s3_bucket_raw: "raw-pdfs".to_string(),
        s3_bucket_txt: "extracted-txt".to_string(),
        s3_access_key: "it".to_string(),
        s3_secret_key: "it".to_string(),
        // Mongo muerto: el claim falla rapido y el mensaje queda pendiente
        // (la verificacion es la ENTREGA, no el procesamiento).
        mongo_uri: "mongodb://127.0.0.1:1/?directConnection=true&serverSelectionTimeoutMS=100"
            .to_string(),
        mongo_db: "app".to_string(),
        mongo_timeout: Duration::from_millis(200),
        max_pages: 100,
        max_output_bytes: 1024 * 1024,
        metrics_interval: Duration::from_secs(15),
        s3_op_timeout: Duration::from_secs(1),
        shutdown_drain: Duration::from_secs(2),
        dlq_replay_audit_stream: format!("{stream}-dlq-replay"),
        dlq_replayed_key: format!("{stream}-dlq-replayed"),
        dead_consumer_ms: 10_000,
    }
}

async fn ping(conn: &mut MultiplexedConnection) -> String {
    must(redis::cmd("PING").query_async(conn).await)
}

/// Primera entrada de `ROLE` (`master`/`slave`...).
async fn role_of(conn: &mut MultiplexedConnection) -> String {
    let reply: redis::Value = must(redis::cmd("ROLE").query_async(conn).await);
    match reply {
        redis::Value::Array(items) => match items.into_iter().next() {
            Some(redis::Value::BulkString(b)) => String::from_utf8_lossy(&b).to_string(),
            _ => String::new(),
        },
        _ => String::new(),
    }
}

/// `run_id` de `INFO server`: identidad unica de la instancia.
async fn run_id_of(conn: &mut MultiplexedConnection) -> String {
    let info: String = must(redis::cmd("INFO").arg("server").query_async(conn).await);
    info.lines()
        .find_map(|line| line.strip_prefix("run_id:"))
        .map(str::to_owned)
        .unwrap_or_default()
}

/// `SENTINEL FAILOVER <name>` contra el primer sentinel del CSV.
async fn trigger_failover(master_name: &str, sentinel_addr: &str) {
    let client = must(redis::Client::open(
        format!("redis://{sentinel_addr}/").as_str(),
    ));
    let mut conn = must(client.get_multiplexed_async_connection().await);
    let _: redis::Value = must(
        redis::cmd("SENTINEL")
            .arg("FAILOVER")
            .arg(master_name)
            .query_async(&mut conn)
            .await,
    );
}

/// Re-resuelve con `RedisClient::reset` + `connect` hasta observar OTRO
/// instancia en rol master (poll con presupuesto; sin asumir tiempos).
async fn wait_new_master(
    rc: &Arc<RedisClient>,
    initial_run_id: &str,
    budget: Duration,
) -> MultiplexedConnection {
    let deadline = Instant::now() + budget;
    loop {
        match rc.reset_async().await {
            Ok(()) => {
                if let Ok(mut conn) = rc.connect().await {
                    let run = run_id_of(&mut conn).await;
                    let role = role_of(&mut conn).await;
                    if !run.is_empty() && run != initial_run_id && role == "master" {
                        return conn;
                    }
                }
            }
            Err(e) => {
                eprintln!("reset para re-resolver el master: {e}");
            }
        }
        assert!(
            Instant::now() < deadline,
            "el master nuevo no convergio en {budget:?}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// `SENTINEL get-master-addr-by-name <name>`: (host, port) que el sentinel
/// reporta como master en este momento.
async fn master_addr_of(master_name: &str, sentinel_addr: &str) -> (String, u16) {
    let client = must(redis::Client::open(
        format!("redis://{sentinel_addr}/").as_str(),
    ));
    let mut conn = must(client.get_multiplexed_async_connection().await);
    let reply: Vec<String> = must(
        redis::cmd("SENTINEL")
            .arg("get-master-addr-by-name")
            .arg(master_name)
            .query_async(&mut conn)
            .await,
    );
    let host = reply.first().cloned().unwrap_or_default();
    let port = reply
        .get(1)
        .and_then(|port| port.parse().ok())
        .unwrap_or(6379);
    (host, port)
}

/// Espera a que la instancia en `url` reporte `expected` en `ROLE`.
async fn wait_role_at(url: &str, expected: &str, budget: Duration) {
    let deadline = Instant::now() + budget;
    loop {
        if let Ok(client) = redis::Client::open(url) {
            if let Ok(mut conn) = client.get_multiplexed_async_connection().await {
                if role_of(&mut conn).await == expected {
                    return;
                }
            }
        }
        assert!(
            Instant::now() < deadline,
            "la instancia en {url} no alcanzo el rol {expected} en {budget:?}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Entrada `redis` de `check_all` (los otros checks no aplican a este IT).
async fn redis_result(readiness: &Readiness) -> Result<(), String> {
    readiness
        .check_all()
        .await
        .into_iter()
        .find(|(name, _)| *name == "redis")
        .map(|(_, result)| result)
        .unwrap_or_else(|| Err("falta el chequeo redis".to_string()))
}

/// Poll de la entrada `redis` hasta verla ok o agotar el presupuesto.
async fn wait_redis_ok(readiness: &Readiness, budget: Duration) -> Result<(), String> {
    let deadline = Instant::now() + budget;
    loop {
        let result = redis_result(readiness).await;
        if result.is_ok() || Instant::now() >= deadline {
            return result;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// XADD de un mensaje con el contrato plano del relay (como redis_it).
async fn xadd_flat(conn: &mut MultiplexedConnection, stream: &str, document_id: &str) -> String {
    must(
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
    )
}

/// Poll hasta que `msg_id` figure en la PEL del grupo (entregado por el
/// reader del runner). At-least-once: solo se pide la entrega, no mas.
async fn wait_delivered(
    conn: &mut MultiplexedConnection,
    stream: &str,
    group: &str,
    msg_id: &str,
    budget: Duration,
    runner_alive: &tokio::task::JoinHandle<
        Result<(), extraction_worker::consumer::runner::RunnerError>,
    >,
) -> bool {
    let deadline = Instant::now() + budget;
    let mut probe = 0u32;
    loop {
        let pending: StreamPendingCountReply =
            must(conn.xpending_count(stream, group, "-", "+", 100).await);
        if pending.ids.iter().any(|entry| entry.id == msg_id) {
            return true;
        }
        if probe.is_multiple_of(20) {
            eprintln!(
                "[diag] t={}s pendientes={} runner_finished={}",
                probe / 20,
                pending.ids.len(),
                runner_alive.is_finished()
            );
        }
        probe += 1;
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

#[tokio::test]
#[ignore]
async fn failover_sentinel_reconecta_y_sigue_consumiendo() {
    let _serial = SENTINEL_IT_LOCK.lock().await;
    // Visibilidad de los warn! del runner durante el diagnóstico del IT.
    extraction_worker::telemetry::init_logging();
    let (addrs, master_name) = sentinel_env();
    let stream = format!("it:sentinel:{}:{}", std::process::id(), now_millis());
    let group = format!("it-sentinel-group-{}", std::process::id());
    let cfg = sentinel_cfg(&stream, &group, addrs.clone(), master_name.clone());
    must(cfg.validate());

    // --- 1) Resolucion inicial del master ---
    let rc = Arc::new(must(RedisClient::new(&cfg)));
    assert!(rc.is_sentinel(), "la config debe ser modo Sentinel");
    let mut conn = must(rc.connect().await);
    assert_eq!(ping(&mut conn).await, "PONG", "el master inicial responde");
    assert_eq!(
        role_of(&mut conn).await,
        "master",
        "la resolucion inicial debe dar el master"
    );
    let run0 = run_id_of(&mut conn).await;
    assert!(!run0.is_empty(), "el master inicial expone run_id");

    // --- Runner con mocks: sin MinIO ni Mongo reales ---
    let pipeline = ExtractionPipeline::new(
        Arc::new(InMemorySource {
            files: Arc::new(std::sync::Mutex::new(HashMap::new())),
        }),
        Arc::new(InMemorySink::default()),
        Arc::new(FakeTextExtractor::returning("ok")),
        Arc::new(CircuitBreaker::default()),
        PipelineLimits {
            job_timeout: Duration::from_secs(5),
            s3_op_timeout: Duration::from_secs(1),
            max_pdf_bytes: 1024 * 1024,
            max_output_bytes: 1024 * 1024,
        },
    );
    let documents = must(
        DocumentStore::connect(&MongoConfig {
            uri: cfg.mongo_uri.clone(),
            db: cfg.mongo_db.clone(),
            timeout: cfg.mongo_timeout,
        })
        .await,
    );
    let runner = ConsumerRunner::new(cfg.clone(), Arc::new(pipeline), Arc::new(documents));
    let token = CancellationToken::new();
    let token_for_runner = token.clone();
    let mut runner_task = tokio::spawn(async move { runner.run(token_for_runner).await });
    // El reader arranca, crea el grupo y entra en el ciclo de BLOCK.
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // --- 2) Failover y re-resolucion ---
    trigger_failover(&master_name, &addrs[0]).await;
    let mut conn_new = wait_new_master(&rc, &run0, Duration::from_secs(60)).await;
    assert_eq!(
        role_of(&mut conn_new).await,
        "master",
        "tras reset+connect el master nuevo debe ser master"
    );
    let run1 = run_id_of(&mut conn_new).await;
    assert_ne!(run0, run1, "el master nuevo debe ser otra instancia");

    // --- 3) Continuidad: XADD post-failover entregado por el runner ---
    let document_id = format!("doc-sentinel-{}-{}", std::process::id(), now_millis());
    let msg_id = xadd_flat(&mut conn_new, &stream, &document_id).await;
    eprintln!(
        "[diag] runner_finished_al_inicio={}",
        runner_task.is_finished()
    );
    let delivered = wait_delivered(
        &mut conn_new,
        &stream,
        &group,
        &msg_id,
        Duration::from_secs(45),
        &runner_task,
    )
    .await;
    if !delivered {
        // Diagnostico: estado real del stream/grupo antes de fallar.
        let groups: redis::Value = redis::cmd("XINFO")
            .arg("GROUPS")
            .arg(&stream)
            .query_async(&mut conn_new)
            .await
            .unwrap_or(redis::Value::Nil);
        eprintln!("[diag] XINFO GROUPS = {groups:?}");
        let stream_info: redis::Value = redis::cmd("XINFO")
            .arg("STREAM")
            .arg(&stream)
            .query_async(&mut conn_new)
            .await
            .unwrap_or(redis::Value::Nil);
        eprintln!("[diag] XINFO STREAM = {stream_info:?}");
        eprintln!("[diag] runner_finished_final={}", runner_task.is_finished());
    }
    assert!(
        delivered,
        "el mensaje XADD-eado despues del failover debe entregarse \
         (at-least-once; no se afirma perdida cero): {msg_id}"
    );

    // --- Apagado y limpieza ---
    token.cancel();
    if tokio::time::timeout(Duration::from_secs(10), &mut runner_task)
        .await
        .is_err()
    {
        runner_task.abort();
        let _ = runner_task.await;
    }
    let _: i64 = must(
        conn_new
            .del(&[
                cfg.stream.as_str(),
                cfg.dlq_stream.as_str(),
                cfg.dlq_replay_audit_stream.as_str(),
                cfg.dlq_replayed_key.as_str(),
            ])
            .await,
    );
}

/// T-08: `/readyz` debe seguir al master vigente tras un failover. El
/// camino `&ConsumerConfig` hace PING + `ROLE`; una replica se rechaza.
#[tokio::test]
#[ignore]
async fn readyz_sentinel_apunta_al_master_vigente_tras_failover() {
    let _serial = SENTINEL_IT_LOCK.lock().await;
    extraction_worker::telemetry::init_logging();
    let (addrs, master_name) = sentinel_env();
    let stream = format!("it:sentinel-readyz:{}:{}", std::process::id(), now_millis());
    let group = format!("it-sentinel-readyz-group-{}", std::process::id());
    let cfg = sentinel_cfg(&stream, &group, addrs.clone(), master_name.clone());
    must(cfg.validate());

    // --- 1) Master inicial y su direccion (luego quedara como replica) ---
    let rc = Arc::new(must(RedisClient::new(&cfg)));
    assert!(rc.is_sentinel(), "la config debe ser modo Sentinel");
    let mut conn = must(rc.connect().await);
    assert_eq!(ping(&mut conn).await, "PONG", "el master inicial responde");
    assert_eq!(
        role_of(&mut conn).await,
        "master",
        "la resolucion inicial debe dar el master"
    );
    let run0 = run_id_of(&mut conn).await;
    assert!(!run0.is_empty(), "el master inicial expone run_id");
    let (old_host, old_port) = master_addr_of(&master_name, &addrs[0]).await;

    // --- 2) Readiness como en el binario: camino &ConsumerConfig ---
    let s3 = S3Config {
        endpoint: "http://127.0.0.1:1".to_string(),
        bucket_raw: "raw-pdfs".to_string(),
        bucket_txt: "extracted-txt".to_string(),
        access_key: "it".to_string(),
        secret_key: "it".to_string(),
    };
    let mongo = Arc::new(must(
        DocumentStore::connect(&MongoConfig {
            uri: cfg.mongo_uri.clone(),
            db: cfg.mongo_db.clone(),
            timeout: cfg.mongo_timeout,
        })
        .await,
    ));
    let readiness = must(Readiness::new(
        &cfg,
        must(ObjectStoreSource::new(&s3)),
        mongo.clone(),
    ));
    let before = redis_result(&readiness).await;
    assert!(
        before.is_ok(),
        "antes del failover el chequeo redis debe pasar: {before:?}"
    );

    // --- 3) Failover y convergencia al master nuevo ---
    trigger_failover(&master_name, &addrs[0]).await;
    let mut conn_new = wait_new_master(&rc, &run0, Duration::from_secs(60)).await;
    assert_eq!(
        role_of(&mut conn_new).await,
        "master",
        "tras reset+connect el master nuevo debe ser master"
    );
    let run1 = run_id_of(&mut conn_new).await;
    assert_ne!(run0, run1, "el master nuevo debe ser otra instancia");

    // --- 4) El master viejo ya es replica: readiness debe rechazarlo ---
    let replica_url = format!("redis://{old_host}:{old_port}/");
    wait_role_at(&replica_url, "slave", Duration::from_secs(30)).await;
    let mut replica_cfg = cfg.clone();
    replica_cfg.redis_url = replica_url;
    replica_cfg.redis_sentinel_addrs.clear();
    replica_cfg.redis_master_name = None;
    let replica_readiness = must(Readiness::new(
        &replica_cfg,
        must(ObjectStoreSource::new(&s3)),
        mongo,
    ));
    let on_replica = redis_result(&replica_readiness).await;
    assert!(
        on_replica.is_err(),
        "una replica nunca debe reportarse como master: {on_replica:?}"
    );

    // --- 5) Tras el failover, el camino Sentinel re-resuelve y da ok ---
    let after = wait_redis_ok(&readiness, Duration::from_secs(30)).await;
    assert!(
        after.is_ok(),
        "readiness redis debe pasar contra el master vigente: {after:?}"
    );
}
