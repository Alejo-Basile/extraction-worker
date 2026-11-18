use std::sync::OnceLock;

use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

static HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();
static INSTALL: std::sync::Mutex<()> = std::sync::Mutex::new(());

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
