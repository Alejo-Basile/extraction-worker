//! S5-P3-06: test de sostenimiento con 1000 documentos.
//!
//! Lanza el binario real contra el stack de labs, procesa 1000 PDFs de
//! tamanos mixtos (1, 5 y 15 paginas) y muestrea cada ~500ms:
//! - RSS y CPU del worker desde `/proc/<pid>` (`status` y `stat`),
//! - descriptores abiertos desde `/proc/<pid>/fd`,
//! - gauges del worker desde `/metrics` (inflight, stream depth, PEL).
//!
//! Criterios: RSS estable despues del calentamiento (el primer cuarto de las
//! muestras como linea base, delta < 128 MiB), descriptores constantes (+-3)
//! entre esa linea base y el final, todo COMPLETED y DLQ vacia.
//!
//! Requiere el stack de labs:
//! `cd labs && cp .env.example .env && docker compose up -d`
//! `cargo test --all-features -- --ignored --nocapture`

use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use mongodb::bson::{doc, Document as BsonDocument};
use mongodb::options::ClientOptions;
use mongodb::Client;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as StorePath;
use object_store::ObjectStoreExt;
use redis::aio::MultiplexedConnection;
use redis::AsyncCommands;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::task::JoinSet;

const DOCS: usize = 1000;
const RSS_DELTA_LIMIT_MIB: i64 = 128;
const FD_DELTA_LIMIT: i64 = 3;

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

fn free_port() -> u16 {
    let listener = must(std::net::TcpListener::bind("127.0.0.1:0"));
    must(listener.local_addr()).port()
}

/// Generador de PDFs con texto (misma tecnica que el corpus de S5-P3-05):
/// objetos consecutivos y xref clasica, para no versionar binarios.
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

fn text_pdf(page_count: usize) -> Vec<u8> {
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
        let content = format!(
            "BT /F1 12 Tf 20 {} Td (Sostenimiento pagina {page}) Tj ET\n",
            792 - 20
        );
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
    build_pdf(&objects)
}

fn spawn_worker(port: u16, stream: &str, group: &str) -> Child {
    must(
        Command::new(env!("CARGO_BIN_EXE_extraction-worker"))
            .env("HTTP_PORT", port.to_string())
            .env("RUST_LOG", "off")
            .env("LOG_FORMAT", "text")
            // El job `integration` de CI exporta vars de Sentinel para
            // sentinel_it; el worker de este IT debe quedarse en modo directo
            // (REDIS_URL), asi que se las quitamos al hijo.
            .env_remove("REDIS_SENTINEL_ADDRS")
            .env_remove("REDIS_MASTER_NAME")
            .env_remove("REDIS_SENTINEL_PASSWORD")
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
                format!("it-sus-{}", std::process::id()),
            )
            .env("WORKER_BLOCK_MS", "200")
            .env("WORKER_BATCH_COUNT", "10")
            .env("WORKER_MAX_CONCURRENCY", "4")
            .env("WORKER_MAX_ATTEMPTS", "5")
            .env("WORKER_MIN_IDLE_MS", "10000")
            .env("WORKER_DEAD_CONSUMER_MS", "10000")
            .env("WORKER_XAUTOCLAIM_INTERVAL_SECS", "5")
            .env("WORKER_METRICS_INTERVAL_SECS", "1")
            .env("WORKER_JOB_TIMEOUT_SECS", "30")
            .env("S3_ENDPOINT", s3_endpoint())
            .env("S3_ACCESS_KEY", s3_access_key())
            .env("S3_SECRET_KEY", s3_secret_key())
            .env("MONGO_WORKER_URI", mongo_worker_uri())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn(),
    )
}

struct WorkerGuard(Child);

impl Drop for WorkerGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

async fn get_conn() -> MultiplexedConnection {
    let client = must(redis::Client::open(test_url().as_str()));
    must(client.get_multiplexed_async_connection().await)
}

async fn root_client() -> Client {
    let opts = must(ClientOptions::parse(mongo_root_uri()).await);
    must(Client::with_options(opts))
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

/// Lee `VmRSS` (KiB) de `/proc/<pid>/status`.
fn rss_kib(pid: u32) -> Option<i64> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    status.lines().find_map(|line| {
        let rest = line.strip_prefix("VmRSS:")?;
        rest.split_whitespace().next()?.parse().ok()
    })
}

fn fd_count(pid: u32) -> i64 {
    std::fs::read_dir(format!("/proc/{pid}/fd"))
        .map(|d| d.count() as i64)
        .unwrap_or(-1)
}

/// utime + stime (ticks de CPU) de `/proc/<pid>/stat`. El nombre del proceso
/// va entre parentesis y puede contener espacios: se parsea desde el ultimo
/// `)`, donde empieza el campo 3 (`state`).
fn cpu_ticks(pid: u32) -> Option<i64> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let tail = stat.rsplit_once(')').map(|(_, t)| t)?;
    let fields: Vec<&str> = tail.split_whitespace().collect();
    // tail[0] es `state` (campo 3); utime es el campo 14 (indice 11) y
    // stime el 15 (indice 12).
    let utime: i64 = fields.get(11)?.parse().ok()?;
    let stime: i64 = fields.get(12)?.parse().ok()?;
    Some(utime + stime)
}

async fn count_completed(root: &Client, ids: &[String]) -> u64 {
    let documents = root.database("app").collection::<BsonDocument>("documents");
    must(
        documents
            .count_documents(doc! {"_id": {"$in": ids}, "status": "COMPLETED"})
            .await,
    )
}

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

fn gauge_value(metrics: &str, series: &str) -> Option<f64> {
    metrics
        .lines()
        .find(|l| l.starts_with(series))
        .and_then(|l| l.split_whitespace().last())
        .and_then(|v| v.parse().ok())
}

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

#[derive(Debug, Clone, Copy)]
struct Sample {
    elapsed: Duration,
    completed: u64,
    rss_kib: i64,
    fds: i64,
    cpu_ticks: i64,
}

#[tokio::test]
#[ignore]
async fn sostenimiento_1000_documentos_sin_fugas() {
    let tag = format!("it-sus-{}-{}", std::process::id(), now_millis());
    let stream = format!("it:worker:sus:{tag}");
    let group = format!("grp-{tag}");
    let dlq_stream = format!("{stream}-dlq");

    let ids: Vec<String> = (0..DOCS).map(|i| format!("{tag}-{i:04}")).collect();

    // Sembrado: 3 tamanos de PDF (1, 5 y 15 paginas) rotando.
    let pdfs = [text_pdf(1), text_pdf(5), text_pdf(15)];
    let raw = raw_store().await;
    let mut puts: JoinSet<()> = JoinSet::new();
    for (idx, id) in ids.iter().enumerate() {
        let store = raw.clone();
        let key = format!("raw-pdfs/{id}.pdf");
        let bytes = pdfs[idx % pdfs.len()].clone();
        puts.spawn(async move {
            must(ObjectStoreExt::put(store.as_ref(), &StorePath::from(key), bytes.into()).await);
        });
        if puts.len() >= 32 {
            if let Some(res) = puts.join_next().await {
                must(res);
            }
        }
    }
    while let Some(res) = puts.join_next().await {
        must(res);
    }

    let root = root_client().await;
    let documents = root.database("app").collection::<BsonDocument>("documents");
    for chunk in ids.chunks(250) {
        let docs: Vec<BsonDocument> = chunk
            .iter()
            .map(|id| {
                doc! {
                    "_id": id,
                    "status": "QUEUED",
                    "object_key": format!("raw-pdfs/{id}.pdf"),
                    "correlation_id": id,
                    "schema_version": 1,
                    "status_history": [],
                }
            })
            .collect();
        must(documents.insert_many(docs).await);
    }

    let mut conn = get_conn().await;
    let group_res: Result<(), redis::RedisError> = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg(&stream)
        .arg(&group)
        .arg("0")
        .arg("MKSTREAM")
        .query_async(&mut conn)
        .await;
    if let Err(e) = group_res {
        assert!(e.to_string().contains("BUSYGROUP"), "XGROUP fallo: {e}");
    }
    // XADD en pipelines: 1000 comandos sueltos serian lentos por RTT.
    for chunk in ids.chunks(100) {
        let mut pipe = redis::pipe();
        for id in chunk {
            pipe.cmd("XADD")
                .arg(&stream)
                .arg("*")
                .arg("document_id")
                .arg(id)
                .arg("object_key")
                .arg(format!("raw-pdfs/{id}.pdf"))
                .arg("correlation_id")
                .arg(id)
                .arg("enqueued_at")
                .arg("2026-11-30T10:15:03Z")
                .arg("schema_version")
                .arg("1");
        }
        let _: () = must(pipe.query_async(&mut conn).await);
    }

    let port = free_port();
    let worker = WorkerGuard(spawn_worker(port, &stream, &group));
    let pid = worker.0.id();
    assert!(
        wait_metrics(port, Duration::from_secs(10)).await,
        "el worker no arranco en 10s"
    );

    // Muestreo cada 250ms hasta completar los 1000 (o 600s de tope).
    let started = Instant::now();
    let mut samples: Vec<Sample> = Vec::new();
    let mut completed = 0u64;
    let mut tick: u32 = 0;
    let mut last_metrics = String::new();
    while completed < DOCS as u64 && started.elapsed() < Duration::from_secs(600) {
        completed = count_completed(&root, &ids).await;
        if tick.is_multiple_of(10) {
            if let Some(m) = try_metrics(port).await {
                last_metrics = m;
            }
        }
        samples.push(Sample {
            elapsed: started.elapsed(),
            completed,
            rss_kib: rss_kib(pid).unwrap_or(-1),
            fds: fd_count(pid),
            cpu_ticks: cpu_ticks(pid).unwrap_or(-1),
        });
        tick += 1;
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let total_elapsed = started.elapsed();

    assert_eq!(
        completed, DOCS as u64,
        "deben completarse los {DOCS} documentos; quedaron {completed} en {total_elapsed:?}"
    );

    // Linea base: el primer cuarto de las muestras (calentamiento de pools,
    // buffers y cache de pdf-extract). En hosts rapidos los 1000 documentos
    // pueden terminar en ~10s, asi que la ventana se define por muestras y no
    // por cantidad de completados.
    assert!(
        samples.len() >= 8,
        "muy pocas muestras ({}) para evaluar sostenimiento",
        samples.len()
    );
    let warm_count = (samples.len() / 4).max(3);
    let warm_span = samples[warm_count - 1].elapsed;
    let baseline_rss = samples[..warm_count]
        .iter()
        .map(|s| s.rss_kib)
        .max()
        .unwrap_or(0);
    let baseline_fds = samples[warm_count - 1].fds;
    let after = &samples[warm_count..];
    let peak_rss = after.iter().map(|s| s.rss_kib).max().unwrap_or(0);
    let final_fds = samples.last().map(|s| s.fds).unwrap_or(0);
    let final_rss = samples.last().map(|s| s.rss_kib).unwrap_or(0);
    let first = samples.first().copied().unwrap_or(Sample {
        elapsed: Duration::ZERO,
        completed: 0,
        rss_kib: 0,
        fds: 0,
        cpu_ticks: 0,
    });
    let cpu_secs =
        (samples.last().map(|s| s.cpu_ticks).unwrap_or(0) - first.cpu_ticks) as f64 / 100.0;

    let rss_delta_mib = (peak_rss - baseline_rss) as f64 / 1024.0;
    let fd_delta = final_fds - baseline_fds;

    eprintln!(
        "sostenimiento: docs={DOCS} completados={} muestras={} tiempo={:.1}s \
         calentamiento={warm_span:?} rss_base={:.1}MiB rss_pico={:.1}MiB rss_final={:.1}MiB \
         delta_pico={:.1}MiB fds_base={} fds_final={} delta_fds={} cpu={:.1}s",
        samples.last().map(|s| s.completed).unwrap_or(0),
        samples.len(),
        total_elapsed.as_secs_f64(),
        baseline_rss as f64 / 1024.0,
        peak_rss as f64 / 1024.0,
        final_rss as f64 / 1024.0,
        rss_delta_mib,
        baseline_fds,
        final_fds,
        fd_delta,
        cpu_secs
    );

    assert!(
        rss_delta_mib < RSS_DELTA_LIMIT_MIB as f64,
        "el RSS crecio {rss_delta_mib:.1}MiB tras el calentamiento (tope {RSS_DELTA_LIMIT_MIB}MiB)"
    );
    assert!(
        fd_delta.abs() <= FD_DELTA_LIMIT,
        "los descriptores variaron {fd_delta} (tope +-{FD_DELTA_LIMIT}): base={baseline_fds} final={final_fds}"
    );

    let dlq_len: u64 = must(conn.xlen(&dlq_stream).await);
    assert_eq!(dlq_len, 0, "no debe haber documentos en la DLQ");

    // Al final la PEL debe quedar drenada (todos ACK-eados). Ojo:
    // `worker_stream_depth` es XLEN y las entradas ACK-eadas siguen en el
    // stream hasta que se recorte (TRIM), asi que no es un indicador de lag.
    let drained = Instant::now();
    loop {
        if let Some(m) = try_metrics(port).await {
            last_metrics = m;
        }
        let pending = gauge_value(&last_metrics, "worker_pel_pending").unwrap_or(0.0);
        if pending < 1.0 {
            break;
        }
        assert!(
            drained.elapsed() < Duration::from_secs(10),
            "la PEL no dreno tras completar: pending={pending}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    // Apagado limpio.
    // SAFETY: solo se envia una senal al PID del hijo recien creado.
    let rc = unsafe { libc::kill(pid as libc::pid_t, libc::SIGTERM) };
    assert_eq!(rc, 0, "kill(SIGTERM) fallo");

    // Limpieza: streams, documentos y objetos del bucket.
    let _: () = must(conn.del(&[stream.as_str(), dlq_stream.as_str()]).await);
    let _: () = must(conn.del(&[format!("{stream}-dlq-replay").as_str()]).await);
    let _ = documents.delete_many(doc! {"_id": {"$in": &ids}}).await;
    let txt = txt_store().await;
    let mut dels: JoinSet<()> = JoinSet::new();
    for id in &ids {
        let r = raw.clone();
        let t = txt.clone();
        let raw_key = format!("raw-pdfs/{id}.pdf");
        let txt_key = format!("{id}.txt");
        dels.spawn(async move {
            let _ = ObjectStoreExt::delete(r.as_ref(), &StorePath::from(raw_key)).await;
            let _ = ObjectStoreExt::delete(t.as_ref(), &StorePath::from(txt_key)).await;
        });
        if dels.len() >= 32 {
            if let Some(res) = dels.join_next().await {
                must(res);
            }
        }
    }
    while let Some(res) = dels.join_next().await {
        must(res);
    }
}
