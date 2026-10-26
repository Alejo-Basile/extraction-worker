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

use crate::document_store::DocumentStore;
use crate::objects::ObjectStoreSource;

#[get("/healthz")]
pub async fn healthz() -> impl Responder {
    HttpResponse::Ok().json(serde_json::json!({"status": "ok"}))
}

/// Verificacion real de dependencias para `/readyz` (DoD global punto 5,
/// F3b): Redis (PING), MinIO (head de una clave sonda) y Mongo (PING).
pub struct Readiness {
    redis: redis::Client,
    minio: ObjectStoreSource,
    mongo: Arc<DocumentStore>,
    redis_timeout: Duration,
}

impl Readiness {
    pub fn new(
        redis_url: &str,
        minio: ObjectStoreSource,
        mongo: Arc<DocumentStore>,
    ) -> Result<Self, String> {
        let redis =
            redis::Client::open(redis_url).map_err(|e| format!("REDIS_URL invalida: {e}"))?;
        Ok(Self {
            redis,
            minio,
            mongo,
            redis_timeout: Duration::from_secs(2),
        })
    }

    /// Chequeos en paralelo; cada uno con su propio timeout acotado.
    pub async fn check_all(&self) -> Vec<(&'static str, Result<(), String>)> {
        let redis_check = Self::redis_ping(&self.redis, self.redis_timeout);
        let minio_check = self.minio.check();
        let mongo_check = self.mongo.ping();
        let (r, m, g) = tokio::join!(redis_check, minio_check, mongo_check,);
        vec![
            ("redis", r),
            ("minio", m.map_err(|e| e.to_string())),
            ("mongo", g.map_err(|e| e.to_string())),
        ]
    }

    async fn redis_ping(client: &redis::Client, timeout: Duration) -> Result<(), String> {
        let mut conn = tokio::time::timeout(timeout, client.get_multiplexed_async_connection())
            .await
            .map_err(|_| "redis: timeout conectando".to_string())?
            .map_err(|e| format!("redis: {e}"))?;
        let pong: String = tokio::time::timeout(timeout, redis::cmd("PING").query_async(&mut conn))
            .await
            .map_err(|_| "redis: timeout en PING".to_string())?
            .map_err(|e| format!("redis: {e}"))?;
        if pong == "PONG" {
            Ok(())
        } else {
            Err(format!("redis: respuesta inesperada {pong:?}"))
        }
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
    HttpResponse::Ok().body(handle.render())
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
    async fn dead_readiness() -> Result<Readiness, String> {
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
        Readiness::new("redis://127.0.0.1:1/", minio, Arc::new(mongo))
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
}
