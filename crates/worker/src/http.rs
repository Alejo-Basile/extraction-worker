//! Handlers HTTP: health y metricas. Sin API publica ni CRUD (README).

use actix_web::{
    body::MessageBody,
    dev::{ServiceRequest, ServiceResponse},
    get,
    middleware::Next,
    web, Error, HttpResponse, Responder,
};
use metrics::{counter, histogram};
use metrics_exporter_prometheus::PrometheusHandle;

#[get("/healthz")]
pub async fn healthz() -> impl Responder {
    HttpResponse::Ok().json(serde_json::json!({"status": "ok"}))
}

/// Readiness trivial en S0 (proceso vivo). En F1 pasa a verificar
/// dependencias reales (Redis, MinIO) — DoD global, punto 5.
#[get("/readyz")]
pub async fn readyz() -> impl Responder {
    HttpResponse::Ok().json(serde_json::json!({"ready": true}))
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
    macro_rules! test_app {
        () => {{
            let handle = crate::telemetry::prometheus_handle().map_err(|e| format!("{e}"))?;
            let app = test::init_service(
                App::new()
                    .app_data(web::Data::new(handle))
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
    async fn readyz_responde_200() -> Result<(), String> {
        let app = test_app!()?;
        let req = test::TestRequest::get().uri("/readyz").to_request();
        assert_eq!(test::call_service(&app, req).await.status(), StatusCode::OK);
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
