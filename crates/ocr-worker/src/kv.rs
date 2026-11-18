//! S11-P1-C30: Consul KV overlay for `config/ocr-worker/`.
//!
//! Every key of this service is `restart` (ADR-0022) because `OcrConfig` is
//! built once at process start. The startup read applies values; a later change
//! is only observed and logged as `ocr_config_restart_required`, never applied
//! to the running process.
//!
//! Consul is not on the critical path: startup waits at most 2 s, then falls
//! back to the last-known-good cache and finally to `from_env()` defaults.

use configkv::{
    from_consul_entries_ocr, validate_ocr_values, Client, ClientSettings, OcrCache,
    ValidatedOcrWorkerKv,
};
use std::collections::BTreeMap;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

/// Message emitted when KV changed for a restart-only service.
pub const LOG_RESTART_REQUIRED: &str = "ocr_config_restart_required";
const STARTUP_BUDGET: Duration = Duration::from_secs(2);
const POLL_INTERVAL: Duration = Duration::from_secs(60);

/// Client settings for this service.
///
/// `CONSUL_KV_PREFIX`/`CONSUL_KV_CACHE_PATH` are overridable, but the fallback
/// is the ocr-worker prefix and cache path, never the extraction-worker ones
/// that `ClientSettings::from_env()` assumes.
pub fn settings_from_env() -> ClientSettings {
    let mut settings = ClientSettings::from_env();
    if std::env::var("CONSUL_KV_PREFIX").is_err() {
        settings.prefix = "config/ocr-worker/".to_string();
    }
    if std::env::var("CONSUL_KV_CACHE_PATH").is_err() {
        settings.cache_path = OcrCache::default_ocr_path();
    }
    settings
}

/// Resolve the effective configuration overlay.
///
/// Order: Consul KV → last-known-good cache → `None` (env defaults only).
/// Never blocks longer than [`STARTUP_BUDGET`] and never fails startup.
pub async fn load_overlay(settings: &ClientSettings) -> Option<ValidatedOcrWorkerKv> {
    if settings.kv_disabled {
        info!(msg = "ocr_config_kv_disabled");
        return None;
    }
    let client = match Client::new(settings.clone()) {
        Ok(c) => c,
        Err(e) => {
            warn!(msg = "ocr_config_kv_client_failed", error = %e);
            return cache_overlay(settings);
        }
    };
    match tokio::time::timeout(STARTUP_BUDGET, client.get_once()).await {
        Ok(Ok(page)) => match from_consul_entries_ocr(&page.entries) {
            Ok(values) => {
                let cache = OcrCache::new(settings.cache_path.clone());
                if let Err(e) = cache.save_atomic(&values, page.index) {
                    warn!(msg = "ocr_config_kv_cache_write_failed", error = %e);
                }
                metrics::counter!("worker_ocr_config_kv_applied_total").increment(1);
                info!(msg = "ocr_config_kv_applied", index = page.index);
                Some(values)
            }
            Err(rejection) => {
                warn!(
                    msg = "ocr_config_kv_rejected",
                    key = rejection.key.clone().unwrap_or_default(),
                    reason = rejection.reason.as_str()
                );
                metrics::counter!("worker_ocr_config_kv_rejected_total").increment(1);
                cache_overlay(settings)
            }
        },
        Ok(Err(e)) => {
            warn!(msg = "ocr_config_kv_unavailable", error = %e);
            cache_overlay(settings)
        }
        Err(_) => {
            warn!(msg = "ocr_config_kv_timeout", budget_ms = 2000);
            cache_overlay(settings)
        }
    }
}

fn cache_overlay(settings: &ClientSettings) -> Option<ValidatedOcrWorkerKv> {
    let cache = OcrCache::new(settings.cache_path.clone());
    match cache.load() {
        Ok(Some((values, index))) => {
            info!(msg = "ocr_config_cache_used", index = index);
            Some(values)
        }
        Ok(None) => {
            info!(msg = "ocr_config_defaults_used");
            None
        }
        Err(e) => {
            warn!(msg = "ocr_config_cache_unreadable", error = %e);
            None
        }
    }
}

/// Observe later KV changes without applying them (every key is `restart`).
///
/// Runs until `cancel` fires. It never blocks startup or shutdown: a failed
/// poll is logged and the loop keeps its previous baseline.
pub async fn watch_restart_required(settings: ClientSettings, cancel: CancellationToken) {
    if settings.kv_disabled {
        return;
    }
    let client = match Client::new(settings) {
        Ok(c) => c,
        Err(_) => return,
    };
    let mut baseline = match snapshot(&client).await {
        Some(m) => m,
        None => return,
    };
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(POLL_INTERVAL) => {}
        }
        let current = match snapshot(&client).await {
            Some(m) => m,
            None => continue,
        };
        let changed = changed_keys(&baseline, &current);
        if !changed.is_empty() {
            warn!(
                msg = LOG_RESTART_REQUIRED,
                service = "ocr-worker",
                keys = %changed.join(","),
                hint = "restart the process to apply"
            );
            metrics::counter!("worker_ocr_config_restart_required_total").increment(1);
            baseline = current;
        }
    }
}

/// Raw key→value snapshot used only for change detection.
async fn snapshot(client: &Client) -> Option<BTreeMap<String, String>> {
    let page = match client.get_once().await {
        Ok(p) => p,
        Err(_) => return None,
    };
    let mut out = BTreeMap::new();
    for entry in &page.entries {
        let name = entry
            .key
            .rsplit('/')
            .next()
            .unwrap_or(&entry.key)
            .to_string();
        if let Ok(decoded) = entry.decode_value() {
            // A key holding a full JSON document is validated elsewhere; for the
            // restart signal we only need a stable textual representation.
            let value = serde_json::from_str::<serde_json::Value>(&decoded)
                .map(|v| v.to_string())
                .unwrap_or(decoded);
            out.insert(name, value);
        }
    }
    Some(out)
}

/// Keys whose value was added, removed or changed between two snapshots.
pub fn changed_keys(
    previous: &BTreeMap<String, String>,
    current: &BTreeMap<String, String>,
) -> Vec<String> {
    let mut changed: Vec<String> = previous
        .keys()
        .chain(current.keys())
        .filter(|k| previous.get(*k) != current.get(*k))
        .cloned()
        .collect();
    changed.sort();
    changed.dedup();
    changed
}

/// Validate an already fetched KV document. Exposed for tests; the runtime path
/// uses [`from_consul_entries_ocr`] on the wire representation.
pub fn validate_document(
    map: &serde_json::Map<String, serde_json::Value>,
) -> Option<ValidatedOcrWorkerKv> {
    validate_ocr_values(map).ok()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use serde_json::json;

    fn map(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    #[test]
    fn no_change_is_empty() {
        let a = map(&[("ocr_dpi", "150"), ("ocr_lang", "spa+eng")]);
        assert!(changed_keys(&a, &a).is_empty());
    }

    #[test]
    fn detects_modified_added_and_removed() {
        let previous = map(&[("ocr_dpi", "150"), ("ocr_lang", "spa+eng")]);
        let current = map(&[("ocr_dpi", "200"), ("consumer_count", "2")]);
        let changed = changed_keys(&previous, &current);
        assert_eq!(
            changed,
            vec![
                "consumer_count".to_string(),
                "ocr_dpi".to_string(),
                "ocr_lang".to_string()
            ]
        );
    }

    #[test]
    fn empty_string_language_is_rejected() {
        let mut m = serde_json::Map::new();
        m.insert("ocr_lang".into(), json!(""));
        assert!(validate_document(&m).is_none());
    }

    #[test]
    fn valid_document_maps_every_field() {
        let mut m = serde_json::Map::new();
        m.insert("ocr_dpi".into(), json!(300));
        m.insert("consumer_count".into(), json!(4));
        m.insert("redis_ocr_group".into(), json!("ocr-test"));
        let v = validate_document(&m).unwrap();
        assert_eq!(v.ocr_dpi, 300);
        assert_eq!(v.consumer_count, 4);
        assert_eq!(v.redis_ocr_group, "ocr-test");
        // untouched keys keep their default
        assert_eq!(v.ocr_lang, "spa+eng");
    }
}
