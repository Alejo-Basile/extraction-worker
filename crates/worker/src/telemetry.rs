//! Logs estructurados JSON (DoD 3 / S0-P2-03) y metricas Prometheus
//! (S0-P3-05, base).
//!
//! Reglas de logging:
//! - Formato JSON por defecto; `LOG_FORMAT=text` para desarrollo local.
//! - Campos estables: `timestamp`, `level`, `service`, `msg` y, cuando
//!   aplica, `correlation_id`, `document_id` y `error`.
//! - Cardinalidad controlada por convencion del repo: nunca etiquetar una
//!   metrica con `document_id`, `correlation_id` ni `object_key`.

use std::sync::OnceLock;

use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use tracing_subscriber::EnvFilter;

/// Nombre del servicio en cada linea de log.
pub const SERVICE: &str = "extraction-worker";

static HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();
static LOGGING: OnceLock<()> = OnceLock::new();
static INSTALL: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn log_filter() -> EnvFilter {
    EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"))
}

fn log_format_json() -> bool {
    match std::env::var("LOG_FORMAT") {
        Ok(v) => !v.eq_ignore_ascii_case("text"),
        Err(_) => true,
    }
}

/// Instala el subscriber global una sola vez. Si ya habia uno instalado
/// (otro test, arranque doble) no falla ni paniquea.
pub fn init_logging() {
    LOGGING.get_or_init(|| {
        let filter = log_filter();
        if log_format_json() {
            let _ = tracing_subscriber::fmt()
                .json()
                .flatten_event(true)
                .with_current_span(true)
                .with_span_list(false)
                .with_env_filter(filter)
                .try_init();
        } else {
            let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
        }
    });
}

/// Instala el recorder global de Prometheus (una sola vez por proceso) y
/// devuelve el handle de renderizado. Serializa la instalacion para que dos
/// tests en paralelo no compitan por el recorder global.
pub fn prometheus_handle() -> Result<PrometheusHandle, String> {
    if let Some(h) = HANDLE.get() {
        return Ok(h.clone());
    }
    let _guard = INSTALL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(h) = HANDLE.get() {
        return Ok(h.clone());
    }
    let handle = PrometheusBuilder::new()
        .install_recorder()
        .map_err(|e| format!("{e}"))?;
    Ok(HANDLE.get_or_init(|| handle).clone())
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::sync::{Arc, Mutex};

    use super::*;
    use tracing_subscriber::fmt::MakeWriter;

    #[derive(Clone, Default)]
    struct Buffer(Arc<Mutex<Vec<u8>>>);

    impl io::Write for Buffer {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            let mut guard = self
                .0
                .lock()
                .map_err(|_| io::Error::other("buffer envenenado"))?;
            guard.extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for Buffer {
        type Writer = Buffer;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    fn render_json_buffer(buf: &Buffer) -> String {
        let bytes = match buf.0.lock() {
            Ok(g) => g.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        };
        String::from_utf8(bytes).unwrap_or_default()
    }

    #[test]
    fn json_incluye_campos_estables_y_de_correlacion() {
        let buf = Buffer::default();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .flatten_event(true)
            .with_current_span(true)
            .with_span_list(false)
            .with_writer(buf.clone())
            .finish();

        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(
                service = SERVICE,
                msg = "documento procesado",
                correlation_id = "corr-1",
                document_id = "doc-1"
            );
        });

        let raw = render_json_buffer(&buf);
        let line = raw.lines().next().unwrap_or_default();
        let json: serde_json::Value =
            serde_json::from_str(line).unwrap_or_else(|e| panic!("linea JSON invalida: {e}"));

        assert_eq!(json["service"], SERVICE);
        assert_eq!(json["msg"], "documento procesado");
        assert_eq!(json["correlation_id"], "corr-1");
        assert_eq!(json["document_id"], "doc-1");
        assert_eq!(json["level"], "INFO");
        assert!(json.get("timestamp").is_some(), "falta timestamp: {json}");
    }

    #[test]
    fn json_incluye_el_error_cuando_esta_presente() {
        let buf = Buffer::default();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .flatten_event(true)
            .with_writer(buf.clone())
            .finish();

        tracing::subscriber::with_default(subscriber, || {
            let causa = "S3_TIMEOUT";
            tracing::warn!(
                service = SERVICE,
                msg = "fallo transitorio",
                correlation_id = "corr-2",
                document_id = "doc-2",
                error = %causa
            );
        });

        let raw = render_json_buffer(&buf);
        let line = raw.lines().next().unwrap_or_default();
        let json: serde_json::Value =
            serde_json::from_str(line).unwrap_or_else(|e| panic!("linea JSON invalida: {e}"));

        assert_eq!(json["error"], "S3_TIMEOUT");
        assert_eq!(json["level"], "WARN");
    }

    #[test]
    fn el_handle_es_idempotente() -> Result<(), String> {
        let a = prometheus_handle()?;
        let b = prometheus_handle()?;
        // Segunda llamada reutiliza el recorder ya instalado.
        let _ = (a, b);
        Ok(())
    }

    #[test]
    fn el_handle_renderiza_metricas_registradas() -> Result<(), String> {
        let h = prometheus_handle()?;
        metrics::counter!("telemetry_test_counter_total").increment(1);
        let rendered = h.render();
        assert!(
            rendered.contains("telemetry_test_counter_total"),
            "debe renderizar la metrica: {rendered}"
        );
        Ok(())
    }
}
