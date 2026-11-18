use crate::cache::Cache;
use crate::client::Client;
use crate::metrics;
use crate::validate::{from_consul_entries, ValidatedWorkerKv};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio_util::sync::CancellationToken;

pub struct Watch {
    client: Client,
    cache: Cache,
}
impl Watch {
    pub fn new(client: Client, cache: Cache) -> Self {
        Self { client, cache }
    }
    pub async fn run(
        self,
        cancel: CancellationToken,
        tx: tokio::sync::watch::Sender<ValidatedWorkerKv>,
    ) {
        if self.client.settings().kv_disabled {
            ::metrics::gauge!(metrics::STALE).set(1.0);
            return;
        }
        if let Ok(Some((cfg, _idx))) = self.cache.load() {
            let _ = tx.send(cfg);
        }
        let mut last_index: Option<u64> = None;
        let mut backoff_ms = 1000u64;
        loop {
            if cancel.is_cancelled() {
                return;
            }
            match self.client.get_recurse(last_index).await {
                Ok(page) => {
                    backoff_ms = 1000;
                    match from_consul_entries(&page.entries) {
                        Ok(cfg) => {
                            let _ = tx.send(cfg.clone());
                            let _ = self.cache.save_atomic(&cfg, page.index);
                            ::metrics::gauge!(metrics::LAST_SUCCESS).set(unix_now() as f64);
                            ::metrics::gauge!(metrics::STALE).set(0.0);
                        }
                        Err(rej) => {
                            ::metrics::counter!(metrics::REJECTED, "key" => rej.key.clone().unwrap_or_else(|| "unknown".to_string())).increment(1);
                            tracing::warn!(key = ?rej.key, reason = rej.reason.as_str(), "config kv rejected");
                        }
                    }
                    last_index = Some(page.index);
                }
                Err(_e) => {
                    ::metrics::gauge!(metrics::STALE).set(1.0);
                    let seed = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_nanos())
                        .unwrap_or(0);
                    let sleep_ms = backoff_with_jitter(backoff_ms, seed);
                    tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
                    backoff_ms = (backoff_ms * 2).min(30000);
                }
            }
        }
    }
}

pub(crate) fn backoff_with_jitter(base_ms: u64, seed: u128) -> u64 {
    let cap = base_ms.clamp(1000, 30000);
    let window = (cap as f64 * 0.1) as u64;
    let jitter = (seed % (window.max(1) as u128 + 1)) as u64;
    let val = cap.saturating_add(jitter).min(30000);
    val.clamp(1000, 30000)
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    #[test]
    fn backoff_bounds() {
        assert!(backoff_with_jitter(1000, 0) >= 1000 && backoff_with_jitter(1000, 0) <= 30000);
        assert!(backoff_with_jitter(30000, 5) <= 30000);
        assert!(backoff_with_jitter(500, 0) >= 1000);
    }
}
