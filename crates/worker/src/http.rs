//! Handlers HTTP: health, readiness y metricas. Sin API publica ni CRUD (README).

use std::sync::Arc;
use std::time::Duration;

use actix_web::{
    body::MessageBody,
    dev::{ServiceRequest, ServiceResponse},
    get,
    middleware::Next,
    web, Error, HttpResponse, Responder,
};
use metrics::{counter, histogram};
use metrics_exporter_prometheus::PrometheusHandle;
use tracing::warn;

use crate::consumer::config::ConsumerConfig;
use crate::consumer::redis_conn;
use crate::document_store::DocumentStore;
use crate::objects::ObjectStoreSource;

/// Stream y consumer group que `/readyz` valida en produccion (S1-P3-02).
const READYZ_STREAM: &str = "stream:pdf-processing";
const READYZ_GROUP: &str = "extraction-workers";

#[get("/healthz")]
pub async fn healthz() -> impl Responder {
    HttpResponse::Ok().json(serde_json::json!({"status": "ok"}))
}

/// Fuente de Redis para [`Readiness`].
///
/// - `&ConsumerConfig`: camino principal (T-08) — Sentinel-aware via
///   `redis_conn::build_client`; PING + `ROLE`, y el master se re-resuelve
///   ante un PING fallido o un nodo que ya no es master.
/// - `&str`: compatibilidad historica del constructor — URL explicita en
///   modo directo sobre esa URL (ignora las claves sentinel del entorno,
///   que es lo que los call sites con `&str` esperan).
pub trait ReadinessRedis {
    fn readiness_client(&self) -> Result<Arc<redis_conn::RedisClient>, String>;

    /// `true` si este camino debe confirmar con `ROLE` que la instancia
    /// vigente es el master (T-08). Por defecto solo PING (compatibilidad).
    fn readiness_checks_master(&self) -> bool {
        false
    }

    /// Stream y consumer group que `/readyz` debe validar (H-07). Por
    /// defecto los valores historicos; `&ConsumerConfig` los toma de la
    /// configuracion (`REDIS_STREAM`/`REDIS_GROUP`).
    fn stream_group(&self) -> (String, String) {
        (
            "stream:pdf-processing".to_string(),
            "extraction-workers".to_string(),
        )
    }
}

fn client_from_cfg(cfg: &ConsumerConfig) -> Result<Arc<redis_conn::RedisClient>, String> {
    Ok(Arc::new(
        redis_conn::RedisClient::new(cfg).map_err(|e| format!("redis: {e}"))?,
    ))
}

impl ReadinessRedis for &ConsumerConfig {
    fn readiness_client(&self) -> Result<Arc<redis_conn::RedisClient>, String> {
        client_from_cfg(self)
    }

    fn readiness_checks_master(&self) -> bool {
        true
    }

    fn stream_group(&self) -> (String, String) {
        (self.stream.clone(), self.group.clone())
    }
}

impl ReadinessRedis for &str {
    fn readiness_client(&self) -> Result<Arc<redis_conn::RedisClient>, String> {
        let mut cfg = ConsumerConfig::from_env();
        cfg.redis_url = (*self).to_string();
        cfg.redis_sentinel_addrs.clear();
        cfg.redis_master_name = None;
        // T-10: modo directo ignora Sentinel por completo, auth incluida.
        cfg.redis_sentinel_password = None;
        client_from_cfg(&cfg)
    }
}

/// Verificacion real de dependencias para `/readyz` (DoD global punto 5,
/// F3b): Redis (PING al master ACTUAL, Sentinel-aware en T-08), MinIO
/// (head de una clave sonda) y Mongo (PING).
pub struct Readiness {
    redis: Arc<redis_conn::RedisClient>,
    minio: ObjectStoreSource,
    mongo: Arc<DocumentStore>,
    redis_timeout: Duration,
    redis_checks_master: bool,
    /// Stream y grupo configurados (H-07): el group check no usa constantes.
    stream: String,
    group: String,
}

impl Readiness {
    pub fn new(
        redis: impl ReadinessRedis,
        minio: ObjectStoreSource,
        mongo: Arc<DocumentStore>,
    ) -> Result<Self, String> {
        let redis_checks_master = redis.readiness_checks_master();
        let (stream, group) = redis.stream_group();
        Ok(Self {
            redis: redis.readiness_client()?,
            minio,
            mongo,
            redis_timeout: Duration::from_secs(2),
            redis_checks_master,
            stream,
            group,
        })
    }

    /// Stream y consumer group que valida `/readyz`. Test y diagnostico.
    #[allow(dead_code)]
    pub(crate) fn stream_group(&self) -> (&str, &str) {
        (&self.stream, &self.group)
    }

    /// Chequeos en paralelo; cada uno con su propio timeout acotado.
    pub async fn check_all(&self) -> Vec<(&'static str, Result<(), String>)> {
        let redis_check = async {
            if self.redis_checks_master {
                Self::redis_ping(&self.redis, self.redis_timeout).await
            } else {
                Self::redis_ping_only(&self.redis, self.redis_timeout).await
            }
        };
        let group_check =
            Self::redis_group_check(&self.redis, self.redis_timeout, &self.stream, &self.group);
        let minio_check = self.minio.check();
        let mongo_check = self.mongo.ping();
        let (r, grp, m, g) = tokio::join!(redis_check, group_check, minio_check, mongo_check,);
        vec![
            ("redis", r),
            ("redis_group", grp),
            ("minio", m.map_err(|e| e.to_string())),
            ("mongo", g.map_err(|e| e.to_string())),
        ]
    }

    /// PING y `ROLE` al master vigente (T-08). Si el nodo no es master
    /// (p.ej. quedo como replica tras un failover) o el PING falla, se
    /// re-resuelve via la fabrica (`RedisClient::reset`) y se reintenta UNA
    /// vez: `/readyz` siempre comprueba el master actual, no la IP cacheada
    /// al arrancar.
    async fn redis_ping(
        redis: &Arc<redis_conn::RedisClient>,
        timeout: Duration,
    ) -> Result<(), String> {
        Self::ping_with_reset(redis, timeout, true).await
    }

    /// Camino historico (`&str`): solo PING, sin comprobacion de `ROLE`.
    /// Conserva el reintento tras fallo de transporte para no cambiar el
    /// comportamiento que esperan los call sites existentes.
    async fn redis_ping_only(
        redis: &Arc<redis_conn::RedisClient>,
        timeout: Duration,
    ) -> Result<(), String> {
        Self::ping_with_reset(redis, timeout, false).await
    }

    async fn ping_with_reset(
        redis: &Arc<redis_conn::RedisClient>,
        timeout: Duration,
        verify_master: bool,
    ) -> Result<(), String> {
        match Self::ping_once(redis, timeout, verify_master).await {
            Ok(()) => Ok(()),
            Err(first) => {
                warn!(
                    service = crate::telemetry::SERVICE,
                    msg = "readyz: el master no respondio o no es master; re-resolviendo",
                    error = %first
                );
                if let Err(e) = redis.reset_async().await {
                    warn!(
                        service = crate::telemetry::SERVICE,
                        msg = "readyz: la re-resolucion del master fallo",
                        error = %e
                    );
                }
                Self::ping_once(redis, timeout, verify_master).await
            }
        }
    }

    async fn ping_once(
        redis: &Arc<redis_conn::RedisClient>,
        timeout: Duration,
        verify_master: bool,
    ) -> Result<(), String> {
        let mut conn = tokio::time::timeout(timeout, redis.connect())
            .await
            .map_err(|_| "redis: timeout conectando".to_string())?
            .map_err(|e| format!("redis: {e}"))?;
        let pong: String = tokio::time::timeout(timeout, redis::cmd("PING").query_async(&mut conn))
            .await
            .map_err(|_| "redis: timeout en PING".to_string())?
            .map_err(|e| format!("redis: {e}"))?;
        if pong != "PONG" {
            return Err(format!("redis: respuesta inesperada {pong:?}"));
        }
        if !verify_master {
            return Ok(());
        }
        let role = Self::role_of(&mut conn, timeout).await?;
        if role == "master" {
            Ok(())
        } else {
            Err(format!("redis: la instancia no es master (ROLE={role})"))
        }
    }

    /// Primer elemento de `ROLE` (`master`/`slave`/...); una respuesta
    /// inesperada se traduce a rol vacio y, por tanto, a un chequeo fallido.
    async fn role_of(
        conn: &mut redis::aio::MultiplexedConnection,
        timeout: Duration,
    ) -> Result<String, String> {
        let reply: redis::Value =
            tokio::time::timeout(timeout, redis::cmd("ROLE").query_async(conn))
                .await
                .map_err(|_| "redis: timeout en ROLE".to_string())?
                .map_err(|e| format!("redis: {e}"))?;
        let role = match reply {
            redis::Value::Array(items) => match items.into_iter().next() {
                Some(redis::Value::BulkString(bytes)) => {
                    String::from_utf8_lossy(&bytes).to_string()
                }
                Some(redis::Value::SimpleString(s)) => s,
                _ => String::new(),
            },
            _ => String::new(),
        };
        Ok(role)
    }

    async fn redis_group_check(
        redis: &Arc<redis_conn::RedisClient>,
        timeout: Duration,
        stream: &str,
        group: &str,
    ) -> Result<(), String> {
        let mut conn = tokio::time::timeout(timeout, redis.connect())
            .await
            .map_err(|_| "redis: timeout conectando para group check".to_string())?
            .map_err(|e| format!("redis: {e}"))?;
        let groups: Vec<redis::Value> = tokio::time::timeout(
            timeout,
            redis::cmd("XINFO")
                .arg("GROUPS")
                .arg(stream)
                .query_async(&mut conn),
        )
        .await
        .map_err(|_| "redis: timeout en XINFO GROUPS".to_string())?
        .map_err(|e| format!("redis: {e}"))?;
        for g in groups {
            if let redis::Value::Array(fields) = g {
                // XINFO GROUPS devuelve pares clave/valor; la primera clave es name
                let mut name = String::new();
                let mut iter = fields.into_iter();
                while let Some(k) = iter.next() {
                    if let redis::Value::BulkString(kb) = k {
                        if let Ok(kstr) = String::from_utf8(kb) {
                            if kstr == "name" {
                                if let Some(redis::Value::BulkString(vb)) = iter.next() {
                                    name = String::from_utf8_lossy(&vb).to_string();
                                    break;
                                }
                            } else {
                                // saltar valor
                                let _ = iter.next();
                            }
                        }
                    }
                }
                if name == group {
                    return Ok(());
                }
            }
        }
        Err(format!(
            "redis: consumer group {group} no encontrado en {stream}"
        ))
    }
}

#[get("/readyz")]
pub async fn readyz(readiness: web::Data<Readiness>) -> impl Responder {
    let checks = readiness.check_all().await;
    let mut body = serde_json::Map::new();
    let mut healthy = true;
    let mut checks_json = serde_json::Map::new();
    for (name, res) in checks {
        match res {
            Ok(()) => {
                checks_json.insert(
                    name.to_string(),
                    serde_json::Value::String("ok".to_string()),
                );
            }
            Err(e) => {
                healthy = false;
                checks_json.insert(name.to_string(), serde_json::Value::String(e));
            }
        }
    }
    body.insert(
        "status".to_string(),
        serde_json::Value::String(if healthy { "ok" } else { "unhealthy" }.to_string()),
    );
    body.insert("checks".to_string(), serde_json::Value::Object(checks_json));
    if healthy {
        HttpResponse::Ok().json(serde_json::Value::Object(body))
    } else {
        HttpResponse::ServiceUnavailable().json(serde_json::Value::Object(body))
    }
}

// El handler se llama metrics_handler (no `metrics`) para no ambiguar
// el identificador con la crate `metrics`.
#[get("/metrics")]
pub async fn metrics_handler(handle: web::Data<PrometheusHandle>) -> impl Responder {
    // H-02: Prometheus v3.16 rechaza el scrape sin Content-Type; el formato
    // de texto 0.0.4 es el que espera el job `extraction-worker`.
    HttpResponse::Ok()
        .content_type("text/plain; version=0.0.4; charset=utf-8")
        .body(handle.render())
}

/// Middleware de metricas HTTP. Etiquetas de baja cardinalidad
/// (ruta y estado), nunca `document_id` ni `correlation_id`
/// (restriccion de S0-P3-05 / S1-P1-12); este servicio solo sirve 3 rutas.
pub async fn track_http_metrics(
    req: ServiceRequest,
    next: Next<impl MessageBody>,
) -> Result<ServiceResponse<impl MessageBody>, Error> {
    let path = req
        .match_pattern()
        .unwrap_or_else(|| req.path().to_string());
    let start = std::time::Instant::now();

    let resp = next.call(req).await?;

    let status = resp.status().as_u16().to_string();
    let elapsed = start.elapsed().as_secs_f64();
    counter!("http_requests_total", "route" => path.clone(), "status" => status).increment(1);
    histogram!("http_request_duration_seconds", "route" => path).record(elapsed);

    Ok(resp)
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{http::StatusCode, middleware, test, App};

    /// App de prueba identica a la del binario. Macro para no declarar el
    /// tipo complejo del ServiceFactory.
    /// Readiness apuntando a puertos muertos: /readyz debe responder 503.
    async fn readiness_with(cfg: &ConsumerConfig) -> Result<Readiness, String> {
        let s3 = crate::objects::S3Config {
            endpoint: "http://127.0.0.1:1".to_string(),
            bucket_raw: "raw-pdfs".to_string(),
            bucket_txt: "extracted-txt".to_string(),
            access_key: "x".to_string(),
            secret_key: "x".to_string(),
        };
        let minio = crate::objects::ObjectStoreSource::new(&s3).map_err(|e| e.to_string())?;
        let mongo =
            crate::document_store::DocumentStore::connect(&crate::document_store::MongoConfig {
                uri: "mongodb://127.0.0.1:1/?directConnection=true&serverSelectionTimeoutMS=100"
                    .to_string(),
                db: "app".to_string(),
                timeout: Duration::from_millis(200),
            })
            .await
            .map_err(|e| e.to_string())?;
        Readiness::new(cfg, minio, Arc::new(mongo))
    }

    async fn dead_readiness() -> Result<Readiness, String> {
        let cfg = {
            let _guard = crate::consumer::config::test_env::lock();
            std::env::remove_var("REDIS_SENTINEL_ADDRS");
            std::env::remove_var("REDIS_MASTER_NAME");
            std::env::set_var("REDIS_URL", "redis://127.0.0.1:1/");
            crate::consumer::config::ConsumerConfig::from_env()
        };
        readiness_with(&cfg).await
    }

    macro_rules! test_app {
        () => {{
            let handle = crate::telemetry::prometheus_handle().map_err(|e| format!("{e}"))?;
            let readiness = dead_readiness().await?;
            let app = test::init_service(
                App::new()
                    .app_data(web::Data::new(handle))
                    .app_data(web::Data::new(readiness))
                    .wrap(middleware::from_fn(track_http_metrics))
                    .service(healthz)
                    .service(readyz)
                    .service(metrics_handler),
            )
            .await;
            Ok::<_, String>(app)
        }};
    }

    #[actix_web::test]
    async fn healthz_responde_200() -> Result<(), String> {
        let app = test_app!()?;
        let req = test::TestRequest::get().uri("/healthz").to_request();
        assert_eq!(test::call_service(&app, req).await.status(), StatusCode::OK);
        Ok(())
    }

    #[actix_web::test]
    async fn readyz_con_dependencias_muertas_responde_503() -> Result<(), String> {
        let app = test_app!()?;
        let req = test::TestRequest::get().uri("/readyz").to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = test::read_body(resp).await;
        let json: serde_json::Value =
            serde_json::from_slice(&body).map_err(|e| format!("readyz debe ser JSON: {e}"))?;
        assert_eq!(json["status"], "unhealthy");
        for dep in ["redis", "minio", "mongo"] {
            assert!(
                json["checks"][dep].is_string(),
                "el check {dep} debe reportar error: {json}"
            );
        }
        Ok(())
    }

    #[actix_web::test]
    async fn readyz_group_check_usa_stream_y_group_de_config() -> Result<(), String> {
        let _guard = crate::consumer::config::test_env::lock();
        std::env::remove_var("REDIS_SENTINEL_ADDRS");
        std::env::remove_var("REDIS_MASTER_NAME");
        std::env::set_var("REDIS_URL", "redis://127.0.0.1:1/");
        std::env::set_var("REDIS_STREAM", "stream:custom");
        std::env::set_var("REDIS_GROUP", "group-custom");
        let cfg = ConsumerConfig::from_env();
        let readiness = readiness_with(&cfg).await?;
        assert_eq!(
            readiness.stream_group(),
            ("stream:custom", "group-custom"),
            "H-07: /readyz debe validar el stream y el grupo configurados"
        );
        Ok(())
    }

    #[actix_web::test]
    async fn str_mantiene_stream_y_group_por_defecto() {
        let (stream, group) = "redis://127.0.0.1:1/".stream_group();
        assert_eq!(
            (stream.as_str(), group.as_str()),
            ("stream:pdf-processing", "extraction-workers"),
            "el camino &str conserva los valores historicos"
        );
    }

    #[actix_web::test]
    async fn metrics_declara_content_type_de_prometheus() -> Result<(), String> {
        let app = test_app!()?;
        let req = test::TestRequest::get().uri("/metrics").to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let content_type = resp
            .headers()
            .get(actix_web::http::header::CONTENT_TYPE)
            .ok_or_else(|| {
                "la respuesta de /metrics debe declarar Content-Type (H-02)".to_string()
            })?
            .to_str()
            .map_err(|e| format!("Content-Type no es ASCII: {e}"))?;
        assert_eq!(content_type, "text/plain; version=0.0.4; charset=utf-8");
        Ok(())
    }

    #[actix_web::test]
    async fn metrics_devuelve_texto_parseable_por_prometheus() -> Result<(), String> {
        let app = test_app!()?;
        // Genera al menos una metrica: pegarle a healthz primero.
        let req = test::TestRequest::get().uri("/healthz").to_request();
        test::call_service(&app, req).await;

        let req = test::TestRequest::get().uri("/metrics").to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = test::read_body(resp).await;
        let text = String::from_utf8_lossy(&body);
        assert!(
            text.contains("http_requests_total"),
            "el output debe contener la metrica HTTP: {text}"
        );
        Ok(())
    }

    // --- T-08: /readyz Sentinel-aware ---

    /// Respuesta RESP de `ROLE` en una replica.
    const ROLE_SLAVE: &[u8] = b"*5\r\n$5\r\nslave\r\n$9\r\n127.0.0.1\r\n:6379\r\n$40\r\n0123456789012345678901234567890123456789\r\n:0\r\n";
    /// Respuesta RESP de `ROLE` en el master.
    const ROLE_MASTER: &[u8] = b"*3\r\n$6\r\nmaster\r\n:0\r\n*0\r\n\r\n";

    /// Busca el proximo `\r\n` desde `from`.
    fn find_crlf(buf: &[u8], from: usize) -> Option<usize> {
        buf.get(from..)?
            .windows(2)
            .position(|w| w == b"\r\n")
            .map(|p| from + p)
    }

    /// Extrae una orden RESP completa (`*N ...`) del buffer: devuelve el
    /// nombre (en mayusculas) y los bytes consumidos; `None` si aun falta.
    fn take_resp_command(buf: &[u8]) -> Option<(String, usize)> {
        if buf.first() != Some(&b'*') {
            return None;
        }
        let nl = find_crlf(buf, 1)?;
        let argc: usize = std::str::from_utf8(&buf[1..nl]).ok()?.parse().ok()?;
        let mut pos = nl + 2;
        let mut command = String::new();
        for index in 0..argc {
            if buf.get(pos) != Some(&b'$') {
                return None;
            }
            let nl = find_crlf(buf, pos + 1)?;
            let len: usize = std::str::from_utf8(&buf[pos + 1..nl]).ok()?.parse().ok()?;
            let start = nl + 2;
            let end = start.checked_add(len)?;
            if buf.len() < end + 2 {
                return None;
            }
            if index == 0 {
                command = String::from_utf8_lossy(&buf[start..end]).to_uppercase();
            }
            pos = end + 2;
        }
        Some((command, pos))
    }

    /// Servidor RESP guionizado por conexion: setup (CLIENT) -> `+OK`,
    /// PING -> `+PONG` y ROLE -> la respuesta indicada para esa conexion.
    /// Cada `reset` del cliente abre una conexion nueva, de modo que el
    /// indice de conexion selecciona el rol observado (replica o master).
    fn spawn_role_server(role_replies: Vec<&'static [u8]>) -> std::net::SocketAddr {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap_or_else(|e| panic!("bind server: {e}"));
        let addr = listener
            .local_addr()
            .unwrap_or_else(|e| panic!("local_addr: {e}"));
        let conn_seq = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { break };
                let index = conn_seq.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let role = role_replies
                    .get(index)
                    .copied()
                    .or_else(|| role_replies.last().copied())
                    .unwrap_or(ROLE_MASTER);
                std::thread::spawn(move || {
                    let mut pending: Vec<u8> = Vec::new();
                    let mut chunk = [0u8; 4096];
                    loop {
                        while let Some((command, consumed)) = take_resp_command(&pending) {
                            pending.drain(..consumed);
                            let reply: &[u8] = match command.as_str() {
                                "PING" => b"+PONG\r\n",
                                "ROLE" => role,
                                _ => b"+OK\r\n",
                            };
                            if stream.write_all(reply).is_err() {
                                return;
                            }
                        }
                        match stream.read(&mut chunk) {
                            Ok(0) | Err(_) => return,
                            Ok(n) => pending.extend_from_slice(&chunk[..n]),
                        }
                    }
                });
            }
        });
        addr
    }

    fn cfg_with_url(url: &str) -> crate::consumer::config::ConsumerConfig {
        let _guard = crate::consumer::config::test_env::lock();
        std::env::remove_var("REDIS_SENTINEL_ADDRS");
        std::env::remove_var("REDIS_MASTER_NAME");
        std::env::set_var("REDIS_URL", url);
        crate::consumer::config::ConsumerConfig::from_env()
    }

    fn client_for(url: &str) -> Arc<crate::consumer::redis_conn::RedisClient> {
        let cfg = cfg_with_url(url);
        Arc::new(
            crate::consumer::redis_conn::RedisClient::new(&cfg)
                .unwrap_or_else(|e| panic!("client: {e}")),
        )
    }

    fn redis_test_url() -> String {
        std::env::var("REDIS_TEST_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".to_string())
    }

    #[tokio::test]
    #[ignore = "requiere Redis real (REDIS_TEST_URL)"]
    async fn redis_group_check_detecta_grupo_ausente_y_presente() {
        use std::time::{SystemTime, UNIX_EPOCH};

        let url = redis_test_url();
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        let stream = format!("it:readyz:group:{}:{millis}", std::process::id());
        let group = "it-readyz-group";
        let client = client_for(&url);
        let raw = redis::Client::open(url.as_str())
            .unwrap_or_else(|e| panic!("cliente Redis para el test: {e}"));
        let mut conn = raw
            .get_multiplexed_async_connection()
            .await
            .unwrap_or_else(|e| panic!("conexion Redis para el test: {e}"));

        let _: redis::Value = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(&stream)
            .arg(group)
            .arg("0")
            .arg("MKSTREAM")
            .query_async(&mut conn)
            .await
            .unwrap_or_else(|e| panic!("XGROUP CREATE: {e}"));

        let present =
            Readiness::redis_group_check(&client, Duration::from_secs(2), &stream, group).await;
        assert!(
            present.is_ok(),
            "con el grupo creado el check debe dar ok: {present:?}"
        );

        let _: redis::Value = redis::cmd("XGROUP")
            .arg("DESTROY")
            .arg(&stream)
            .arg(group)
            .query_async(&mut conn)
            .await
            .unwrap_or_else(|e| panic!("XGROUP DESTROY: {e}"));

        let absent =
            Readiness::redis_group_check(&client, Duration::from_secs(2), &stream, group).await;
        let err = match absent {
            Err(e) => e,
            Ok(()) => panic!("sin el grupo el check debe fallar"),
        };
        assert!(
            err.contains("no encontrado"),
            "el error debe explicar que falta el grupo: {err}"
        );

        let _: i64 = redis::cmd("DEL")
            .arg(&stream)
            .query_async(&mut conn)
            .await
            .unwrap_or_else(|e| panic!("DEL del stream de prueba: {e}"));
    }

    #[tokio::test]
    async fn redis_ping_reintenta_tras_rol_replica_y_triunfa() {
        // Primera conexion replica (master degradado); tras reset, master.
        let addr = spawn_role_server(vec![ROLE_SLAVE, ROLE_MASTER]);
        let client = client_for(&format!("redis://{addr}/"));
        let res = Readiness::redis_ping(&client, Duration::from_secs(2)).await;
        assert!(
            res.is_ok(),
            "tras re-resolver, el ROLE del master vigente debe triunfar: {res:?}"
        );
    }

    #[tokio::test]
    async fn redis_ping_falla_si_el_nodo_sigue_siendo_replica() {
        // Ambas conexiones reportan replica: /readyz nunca debe dar ok.
        let addr = spawn_role_server(vec![ROLE_SLAVE, ROLE_SLAVE]);
        let client = client_for(&format!("redis://{addr}/"));
        let res = Readiness::redis_ping(&client, Duration::from_secs(2)).await;
        let err = match res {
            Err(e) => e,
            Ok(()) => panic!("un nodo replica nunca debe reportarse como master"),
        };
        assert!(
            err.starts_with("redis:"),
            "el error conserva el prefijo estable: {err}"
        );
        assert!(
            !err.contains("redis://"),
            "el error no filtra la URL: {err}"
        );
    }

    #[tokio::test]
    async fn redis_ping_sin_redis_falla_luego_de_reintentar() {
        // Puerto cerrado: ambos intentos fallan y el error se reporta.
        let listener =
            std::net::TcpListener::bind("127.0.0.1:0").unwrap_or_else(|e| panic!("bind: {e}"));
        let addr = listener
            .local_addr()
            .unwrap_or_else(|e| panic!("local_addr: {e}"));
        drop(listener);
        let cfg = cfg_with_url(&format!("redis://{addr}/"));
        let client = Arc::new(
            crate::consumer::redis_conn::RedisClient::new(&cfg)
                .unwrap_or_else(|e| panic!("client: {e}")),
        );
        let res = Readiness::redis_ping(&client, Duration::from_millis(500)).await;
        let err = match res {
            Err(e) => e,
            Ok(()) => panic!("sin Redis el chequeo debe fallar"),
        };
        assert!(
            err.starts_with("redis:"),
            "el error conserva el prefijo estable: {err}"
        );
        assert!(
            !err.contains("redis://"),
            "el error no filtra la URL: {err}"
        );
    }

    #[actix_web::test]
    async fn str_ignora_claves_sentinel_incluida_auth() {
        let _guard = crate::consumer::config::test_env::lock();
        std::env::remove_var("REDIS_SENTINEL_ADDRS");
        std::env::remove_var("REDIS_MASTER_NAME");
        std::env::set_var("REDIS_SENTINEL_PASSWORD", "sentinel-secret");
        let res: Result<(), String> = "redis://127.0.0.1:1/".readiness_client().map(|_| ());
        assert!(res.is_ok(), "modo directo ignora la auth sentinel: {res:?}");
        std::env::remove_var("REDIS_SENTINEL_PASSWORD");
    }
}
