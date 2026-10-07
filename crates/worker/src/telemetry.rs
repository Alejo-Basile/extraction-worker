//! Metricas Prometheus (S0-P3-05, base).
//!
//! Cardinalidad controlada por convencion del repo: nunca etiquetar con
//! `document_id`, `correlation_id` ni `object_key`.

use std::sync::OnceLock;

use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

static HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

/// Instala el recorder global de Prometheus y devuelve el handle de
/// renderizado. Falla si ya habia un recorder global instalado.
pub fn install_recorder() -> Result<PrometheusHandle, String> {
    match HANDLE.get() {
        Some(h) => Ok(h.clone()),
        None => {
            let handle = PrometheusBuilder::new()
                .install_recorder()
                .map_err(|e| format!("{e}"))?;
            Ok(HANDLE.get_or_init(|| handle).clone())
        }
    }
}

/// Handle idempotente para tests y para el binario (un solo recorder por
/// proceso). Devuelve el ya instalado o lo instala.
pub fn prometheus_handle() -> Result<PrometheusHandle, String> {
    if let Some(h) = HANDLE.get() {
        return Ok(h.clone());
    }
    match install_recorder() {
        Ok(h) => Ok(HANDLE.get_or_init(|| h).clone()),
        // Carrera en tests paralelos: otro thread instalo primero.
        Err(e) => HANDLE
            .get()
            .cloned()
            .ok_or_else(|| format!("instalacion propia fallo y no hay handle previo: {e}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
