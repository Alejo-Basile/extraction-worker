//! S6-P3-03: modo degradado consciente ante la caida de MinIO.
//!
//! El criterio de la tarea: durante una caida de MinIO, ningun mensaje debe
//! ir a la DLQ (no se queman delivery counts) y los mensajes nuevos quedan en
//! el stream; al volver MinIO, todo se procesa y completa.
//!
//! La caida se simula con un proxy TCP toggleable entre el worker y MinIO:
//! vivo reenvia bytes, muerto corta las conexiones (el cliente ve el cierre
//! como fallo transitorio de S3). Requiere el stack de labs:
//! `cd labs && cp .env.example .env && docker compose up -d`
//! `cargo test --all-features -- --ignored`

use std::net::SocketAddr;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use mongodb::bson::{doc, Document as BsonDocument};
use mongodb::options::ClientOptions;
use mongodb::Client;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as StorePath;
use object_store::ObjectStoreExt;
use redis::aio::MultiplexedConnection;
use redis::streams::StreamPendingCountReply;
use redis::AsyncCommands;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;

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

fn test_url() -> String {
    std::env::var("REDIS_TEST_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".to_string())
}

fn s3_endpoint() -> String {
    std::env::var("S3_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:9000".to_string())
}

fn s3_access_key() -> String {
    std::env::var("S3_ACCESS_KEY").unwrap_or_else(|_| "worker-dev".to_string())
}

fn s3_secret_key() -> String {
    std::env::var("S3_SECRET_KEY").unwrap_or_else(|_| "worker-dev-secret".to_string())
}

fn mongo_worker_uri() -> String {
    std::env::var("MONGO_WORKER_URI").unwrap_or_else(|_| {
        "mongodb://worker:worker-dev-secret@127.0.0.1:27017/app?authSource=app&directConnection=true"
            .to_string()
    })
}

fn mongo_root_uri() -> String {
    std::env::var("MONGO_TEST_ROOT_URI").unwrap_or_else(|_| {
        "mongodb://root:worker-dev-root@127.0.0.1:27017/?directConnection=true".to_string()
    })
}

fn fixture_bytes() -> Vec<u8> {
    must(std::fs::read(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../tests/fixtures/minimal.pdf"
    )))
}

fn free_port() -> u16 {
    let listener = must(std::net::TcpListener::bind("127.0.0.1:0"));
    must(listener.local_addr()).port()
}

/// Proxy TCP hacia MinIO que se puede "matar" y "revivir" en caliente (S6-P3-03).
///
/// Vivo: reenvia bytes en ambas direcciones (el worker no nota nada).
/// Muerto: corta las conexiones existentes y cuelga las nuevas; el cliente ve
/// cierres de conexion que el worker clasifica como `S3_IO` transitorio, que
/// es lo que abre el circuit breaker.
struct ToggleProxy {
    addr: SocketAddr,
    healthy: Arc<AtomicBool>,
    conns: Arc<Mutex<JoinSet<()>>>,
}

impl ToggleProxy {
    async fn start(upstream: SocketAddr) -> Self {
        let listener = must(TcpListener::bind("127.0.0.1:0").await);
        let addr = must(listener.local_addr());
        let healthy = Arc::new(AtomicBool::new(true));
        let conns: Arc<Mutex<JoinSet<()>>> = Arc::new(Mutex::new(JoinSet::new()));

        let h = healthy.clone();
        let cs = conns.clone();
        tokio::spawn(async move {
            loop {
                let Ok((client, _)) = listener.accept().await else {
                    break;
                };
                // Muerto: aceptar y colgar de inmediato. El cliente ve el
                // cierre antes de cualquier respuesta.
                if !h.load(Ordering::SeqCst) {
                    continue;
                }
                let cs = cs.clone();
                must(cs.lock()).spawn(async move {
                    let up = match TcpStream::connect(upstream).await {
                        Ok(u) => u,
                        Err(_) => return, // cuelga el lado del cliente
                    };
                    let (mut cr, mut cw) = client.into_split();
                    let (mut ur, mut uw) = up.into_split();
                    let to_up = tokio::io::copy(&mut cr, &mut uw);
                    let to_client = tokio::io::copy(&mut ur, &mut cw);
                    // Al cortarse cualquiera de los dos lados, se corta el otro.
                    let _ = tokio::try_join!(to_up, to_client);
                });
            }
        });

        Self {
            addr,
            healthy,
            conns,
        }
    }

    fn endpoint(&self) -> String {
        format!("http://{}", self.addr)
    }

    fn kill(&self) {
        self.healthy.store(false, Ordering::SeqCst);
        must(self.conns.lock()).abort_all();
    }

    fn restore(&self) {
        self.healthy.store(true, Ordering::SeqCst);
    }
}

fn minio_addr() -> SocketAddr {
    let endpoint = s3_endpoint();
    let hostport = endpoint.trim_start_matches("http://").trim_end_matches('/');
    must(hostport.parse())
}

/// Mata el proceso del worker en el drop aunque el test falle a mitad de camino.
struct WorkerGuard(Child);

impl Drop for WorkerGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn spawn_worker(port: u16, s3_endpoint: &str, stream: &str, group: &str) -> Child {
    must(
        Command::new(env!("CARGO_BIN_EXE_extraction-worker"))
            .env("HTTP_PORT", port.to_string())
            .env("RUST_LOG", "off")
            .env("LOG_FORMAT", "text")
            .env("REDIS_URL", test_url())
            .env("REDIS_STREAM", stream)
            .env("REDIS_GROUP", group)
            .env("REDIS_DLQ_STREAM", format!("{stream}-dlq"))
            .env(
                "REDIS_DLQ_REPLAY_AUDIT_STREAM",
                format!("{stream}-dlq-replay"),
            )
            .env("REDIS_DLQ_REPLAYED_KEY", format!("{stream}-dlq-replayed"))
            .env(
                "WORKER_CONSUMER_NAME",
                format!("it-deg-{}", std::process::id()),
            )
            .env("WORKER_BLOCK_MS", "200")
            .env("WORKER_BATCH_COUNT", "1")
            .env("WORKER_MAX_CONCURRENCY", "2")
            // Ajustado para que la caida sea demoledora si el gate fallara:
            // a la tercera entrega el mensaje quemaria en la DLQ.
            // Con 3 intentos, una unica deferral por CircuitOpen en half-open
            // no quema el mensaje; sin el gate, el autoclaim durante la caida
            // lo llevaria a la 4ta entrega y a la DLQ.
            .env("WORKER_MAX_ATTEMPTS", "3")
            // Debe superar el tiempo que tarda el breaker en abrir (~5s): si
            // el autoclaim pudiera reclamar el mensaje mientras su job
            // original todavia reintenta, lo ACK-earia como duplicado y el
            // documento quedaria varado en RETRYING.
            .env("WORKER_MIN_IDLE_MS", "15000")
            .env("WORKER_DEAD_CONSUMER_MS", "15000")
            .env("WORKER_XAUTOCLAIM_INTERVAL_SECS", "1")
            .env("WORKER_METRICS_INTERVAL_SECS", "1")
            .env("WORKER_JOB_TIMEOUT_SECS", "30")
            .env("S3_ENDPOINT", s3_endpoint)
            .env("S3_OP_TIMEOUT_MS", "1000")
            .env("S3_ACCESS_KEY", s3_access_key())
            .env("S3_SECRET_KEY", s3_secret_key())
            .env("MONGO_WORKER_URI", mongo_worker_uri())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn(),
    )
}

async fn get_conn() -> MultiplexedConnection {
    let client = must(redis::Client::open(test_url().as_str()));
    must(client.get_multiplexed_async_connection().await)
}

async fn root_client() -> Client {
    let opts = must(ClientOptions::parse(mongo_root_uri()).await);
    must(Client::with_options(opts))
}

/// XADD con el contrato de WorkItem (mismos campos que produce el relay).
async fn xadd(conn: &mut MultiplexedConnection, stream: &str, document_id: &str) -> String {
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

async fn create_group(conn: &mut MultiplexedConnection, stream: &str, group: &str) {
    let res: Result<(), redis::RedisError> = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg(stream)
        .arg(group)
        .arg("0")
        .arg("MKSTREAM")
        .query_async(conn)
        .await;
    match res {
        Ok(()) => {}
        // BUSYGROUP: ya existe, camino feliz en reinicios.
        Err(e) if e.to_string().contains("BUSYGROUP") => {}
        Err(e) => panic!("XGROUP CREATE fallo: {e}"),
    }
}

async fn seed_doc(root: &Client, id: &str) {
    let documents = root.database("app").collection::<BsonDocument>("documents");
    must(
        documents
            .insert_one(doc! {
                "_id": id,
                "status": "QUEUED",
                "object_key": format!("raw-pdfs/{id}.pdf"),
                "correlation_id": id,
                "schema_version": 1,
                "status_history": [],
            })
            .await,
    );
}

async fn doc_status(root: &Client, id: &str) -> String {
    let documents = root.database("app").collection::<BsonDocument>("documents");
    let found = must(documents.find_one(doc! {"_id": id}).await);
    match found {
        Some(d) => d.get_str("status").unwrap_or("sin-status").to_string(),
        None => "inexistente".to_string(),
    }
}

async fn count_status(root: &Client, ids: &[String], status: &str) -> u64 {
    let documents = root.database("app").collection::<BsonDocument>("documents");
    must(
        documents
            .count_documents(doc! {"_id": {"$in": ids}, "status": status})
            .await,
    )
}

/// GET /metrics sobre HTTP/1.0 (el server cierra la conexion al responder).
/// `None` mientras el server no acepta conexiones (arranque/apagado).
async fn try_metrics(port: u16) -> Option<String> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.ok()?;
    stream
        .write_all(b"GET /metrics HTTP/1.0\r\nHost: localhost\r\n\r\n")
        .await
        .ok()?;
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.ok()?;
    Some(String::from_utf8_lossy(&buf).to_string())
}

async fn get_metrics(port: u16) -> String {
    try_metrics(port)
        .await
        .unwrap_or_else(|| panic!("/metrics no respondio en el puerto {port}"))
}

/// Valor de una serie Prometheus exacta (`worker_degraded` o
/// `worker_breaker_open{dependency="minio"}`).
fn gauge_value(metrics: &str, series: &str) -> Option<f64> {
    metrics
        .lines()
        .find(|l| l.starts_with(series))
        .and_then(|l| l.split_whitespace().last())
        .and_then(|v| v.parse().ok())
}

/// PEL del grupo: pares (msg_id, delivery_count) para assert de estabilidad.
async fn pending_snapshot(
    conn: &mut MultiplexedConnection,
    stream: &str,
    group: &str,
) -> Vec<(String, usize)> {
    let reply: StreamPendingCountReply =
        must(conn.xpending_count(stream, group, "-", "+", 100).await);
    reply
        .ids
        .iter()
        .map(|e| (e.id.clone(), e.times_delivered))
        .collect()
}

/// Espera (async) a que el /metrics del worker responda: señal de arranque.
async fn wait_metrics(port: u16, limit: Duration) -> bool {
    let started = Instant::now();
    while started.elapsed() < limit {
        if try_metrics(port).await.is_some_and(|m| m.len() > 1024) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

async fn wait_gauge(port: u16, series: &str, want: f64, limit: Duration) -> bool {
    let started = Instant::now();
    while started.elapsed() < limit {
        if try_metrics(port)
            .await
            .is_some_and(|m| gauge_value(&m, series) == Some(want))
        {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    false
}

async fn wait_completed(root: &Client, ids: &[String], want: u64, limit: Duration) -> u64 {
    let started = Instant::now();
    loop {
        let n = count_status(root, ids, "COMPLETED").await;
        if n >= want {
            return n;
        }
        if started.elapsed() >= limit {
            return n;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn cleanup(
    root: &Client,
    conn: &mut MultiplexedConnection,
    streams: &[String],
    ids: &[String],
) {
    for stream in streams {
        let _: u64 = must(conn.xlen(stream).await);
        let _: () = must(conn.del(stream).await);
    }
    let documents = root.database("app").collection::<BsonDocument>("documents");
    let _ = documents.delete_many(doc! {"_id": {"$in": ids}}).await;

    let raw = raw_store().await;
    let txt = txt_store().await;
    for id in ids {
        let _ =
            ObjectStoreExt::delete(raw.as_ref(), &StorePath::from(format!("raw-pdfs/{id}.pdf")))
                .await;
        let _ = ObjectStoreExt::delete(txt.as_ref(), &StorePath::from(format!("{id}.txt"))).await;
    }
}

async fn raw_store() -> Arc<dyn object_store::ObjectStore> {
    let store = AmazonS3Builder::new()
        .with_endpoint(s3_endpoint())
        .with_bucket_name("raw-pdfs")
        .with_access_key_id(s3_access_key())
        .with_secret_access_key(s3_secret_key())
        .with_allow_http(true)
        .build();
    Arc::new(must(store))
}

async fn txt_store() -> Arc<dyn object_store::ObjectStore> {
    let store = AmazonS3Builder::new()
        .with_endpoint(s3_endpoint())
        .with_bucket_name("extracted-txt")
        .with_access_key_id(s3_access_key())
        .with_secret_access_key(s3_secret_key())
        .with_allow_http(true)
        .build();
    Arc::new(must(store))
}

#[tokio::test]
#[ignore]
async fn caida_de_minio_no_quema_mensajes_y_recupera_sin_dlq() {
    let tag = format!("it-deg-{}-{}", std::process::id(), now_millis());
    let stream = format!("it:worker:deg:{tag}");
    let group = format!("grp-{tag}");
    let dlq_stream = format!("{stream}-dlq");

    // M documentos sanos (fase 1), 1 que descubre la caida (abre el breaker)
    // y K que llegan cuando el worker ya esta pausado.
    let m_ids: Vec<String> = (0..6).map(|i| format!("{tag}-m{i}")).collect();
    let stuck = format!("{tag}-stuck");
    let k_ids: Vec<String> = (0..6).map(|i| format!("{tag}-k{i}")).collect();
    let all_ids: Vec<String> = m_ids
        .iter()
        .chain(std::iter::once(&stuck))
        .chain(k_ids.iter())
        .cloned()
        .collect();

    // Sembrado directo contra el stack real (no via proxy): PDFs en MinIO y
    // documentos QUEUED en Mongo.
    let root = root_client().await;
    let raw = raw_store().await;
    for id in &all_ids {
        must(
            ObjectStoreExt::put(
                raw.as_ref(),
                &StorePath::from(format!("raw-pdfs/{id}.pdf")),
                fixture_bytes().into(),
            )
            .await,
        );
        seed_doc(&root, id).await;
    }

    let mut conn = get_conn().await;
    create_group(&mut conn, &stream, &group).await;
    for id in &m_ids {
        xadd(&mut conn, &stream, id).await;
    }

    let proxy = ToggleProxy::start(minio_addr()).await;
    let port = free_port();
    let mut worker = WorkerGuard(spawn_worker(port, &proxy.endpoint(), &stream, &group));
    assert!(
        wait_metrics(port, Duration::from_secs(10)).await,
        "el worker no arranco en 10s"
    );

    // Fase sana: los M completan a traves del proxy vivo.
    let completed = wait_completed(&root, &m_ids, 6, Duration::from_secs(60)).await;
    assert_eq!(completed, 6, "la fase sana debe completar los M documentos");

    // Caida de MinIO + un documento que la descubre (abre el breaker).
    proxy.kill();
    let stuck_msg = xadd(&mut conn, &stream, &stuck).await;
    let breaker_series = "worker_breaker_open{dependency=\"minio\"}";
    assert!(
        wait_gauge(port, breaker_series, 1.0, Duration::from_secs(45)).await,
        "el breaker de MinIO no abrio tras la caida"
    );
    assert_eq!(
        gauge_value(&get_metrics(port).await, "worker_degraded"),
        Some(1.0),
        "el gauge de modo degradado debe estar en 1 mientras el cooldown rechaza"
    );

    // Mensajes nuevos mientras el worker esta degradado.
    for id in &k_ids {
        xadd(&mut conn, &stream, id).await;
    }
    // Margen para que reader/autoclaim pudieran mover algo si el gate fallara.
    tokio::time::sleep(Duration::from_secs(3)).await;

    // El "stuck" quedo pendiente con delivery 1. Sin el gate, el autoclaim
    // (cada segundo) lo reclamaria en bucle hasta la 4ta entrega: DLQ.
    let expected = vec![(stuck_msg.clone(), 1usize)];
    let snap1 = pending_snapshot(&mut conn, &stream, &group).await;
    tokio::time::sleep(Duration::from_secs(4)).await;
    let snap2 = pending_snapshot(&mut conn, &stream, &group).await;
    assert_eq!(
        snap1, expected,
        "la PEL debe tener solo el stuck, con delivery count 1"
    );
    assert_eq!(
        snap2, expected,
        "la PEL debe quedar congelada durante la caida (delivery counts estables)"
    );
    assert_eq!(
        gauge_value(&get_metrics(port).await, "worker_degraded"),
        Some(1.0),
        "las muestras deben tomarse con el modo degradado activo"
    );

    // En Mongo: los K siguen QUEUED (el reader esta pausado) y nada completo.
    assert_eq!(count_status(&root, &k_ids, "QUEUED").await, 6);
    let completed = count_status(&root, &all_ids, "COMPLETED").await;
    assert_eq!(completed, 6, "nada debe completar durante la caida");
    let stuck_status = doc_status(&root, &stuck).await;
    assert!(
        stuck_status == "RETRYING" || stuck_status == "PROCESSING",
        "el stuck debe quedar diferido, obtuvo {stuck_status}"
    );

    // Recuperacion: MinIO vuelve, el cooldown del breaker (30s) vence, el
    // probe pasa y el consumo se reanuda.
    proxy.restore();
    let completed = wait_completed(&root, &all_ids, 13, Duration::from_secs(150)).await;
    assert_eq!(completed, 13, "tras recuperar MinIO, todo debe completar");

    let dlq_len: u64 = must(conn.xlen(&dlq_stream).await);
    assert_eq!(dlq_len, 0, "la caida no debe mandar nada a la DLQ");
    assert!(
        wait_gauge(port, breaker_series, 0.0, Duration::from_secs(30)).await,
        "el breaker debe cerrarse tras la recuperacion"
    );
    assert_eq!(
        gauge_value(&get_metrics(port).await, "worker_degraded"),
        Some(0.0),
        "el modo degradado debe levantarse tras la recuperacion"
    );

    // Apagado limpio del worker y limpieza del laboratorio.
    // SAFETY: solo se envia una senal al PID del hijo recien creado.
    let rc = unsafe { libc::kill(worker.0.id() as libc::pid_t, libc::SIGTERM) };
    assert_eq!(rc, 0, "kill(SIGTERM) fallo");
    let started = Instant::now();
    loop {
        match worker.0.try_wait() {
            Ok(Some(status)) => {
                assert!(status.success(), "el worker salio con error: {status:?}");
                break;
            }
            Ok(None) => {}
            Err(e) => panic!("try_wait fallo: {e}"),
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "el worker no termino tras SIGTERM"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let audit_stream = format!("{stream}-dlq-replay");
    cleanup(
        &root,
        &mut conn,
        &[stream, dlq_stream, audit_stream],
        &all_ids,
    )
    .await;
}
