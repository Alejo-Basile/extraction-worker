use crate::client::KvEntry;
use crate::schema::{self, ValueKind};
use serde_json::Value;

#[derive(Clone, Debug)]
pub struct ValidatedWorkerKv {
    pub bulkhead_max_concurrency: u32,
    pub retry_max_elapsed_seconds: u64,
    pub retry_initial_interval_ms: u64,
    pub retry_multiplier: f64,
    pub retry_max_interval_ms: u64,
    pub retry_randomization_factor: f64,
    pub breaker_failure_threshold: u32,
    pub breaker_timeout_seconds: u64,
    pub job_timeout_seconds: u64,
    pub max_pages: u32,
    pub min_idle_ms: u64,
}

impl Default for ValidatedWorkerKv {
    fn default() -> Self {
        Self {
            bulkhead_max_concurrency: 4,
            retry_max_elapsed_seconds: 30,
            retry_initial_interval_ms: 500,
            retry_multiplier: 1.5,
            retry_max_interval_ms: 60000,
            retry_randomization_factor: 0.5,
            breaker_failure_threshold: 5,
            breaker_timeout_seconds: 30,
            job_timeout_seconds: 300,
            max_pages: 2000,
            min_idle_ms: 601000,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RejectReason {
    MalformedJson,
    UnknownKey,
    Type,
    Range,
    Coherence,
}
impl RejectReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            RejectReason::MalformedJson => "malformed_json",
            RejectReason::UnknownKey => "unknown_key",
            RejectReason::Type => "type",
            RejectReason::Range => "range",
            RejectReason::Coherence => "coherence",
        }
    }
}

#[derive(Clone, Debug)]
pub struct Rejection {
    pub key: Option<String>,
    pub reason: RejectReason,
}

pub fn validate_str(raw: &str) -> Result<ValidatedWorkerKv, Rejection> {
    let v: Value = serde_json::from_str(raw).map_err(|_| Rejection {
        key: None,
        reason: RejectReason::MalformedJson,
    })?;
    let obj = v.as_object().ok_or(Rejection {
        key: None,
        reason: RejectReason::MalformedJson,
    })?;
    validate_values(obj)
}

pub fn validate_values(
    map: &serde_json::Map<String, Value>,
) -> Result<ValidatedWorkerKv, Rejection> {
    let mut out = ValidatedWorkerKv::default();
    // unknown keys first
    for k in map.keys() {
        if schema::spec(k).is_none() {
            return Err(Rejection {
                key: Some(k.clone()),
                reason: RejectReason::UnknownKey,
            });
        }
    }
    // type + range per key, in schema key order
    for key in schema::keys() {
        if let Some(v) = map.get(*key) {
            let sp = match schema::spec(key) {
                Some(s) => s,
                None => {
                    return Err(Rejection {
                        key: Some(key.to_string()),
                        reason: RejectReason::UnknownKey,
                    })
                }
            };
            match sp.kind {
                ValueKind::Int => {
                    // must be integral number
                    let n = v.as_i64().or_else(|| v.as_u64().map(|x| x as i64));
                    let is_integral =
                        v.is_number() && v.as_f64().map(|f| f.fract() == 0.0).unwrap_or(false);
                    if !is_integral {
                        return Err(Rejection {
                            key: Some(key.to_string()),
                            reason: RejectReason::Type,
                        });
                    }
                    let val = n.unwrap_or(0);
                    let f = val as f64;
                    if let Some(mn) = sp.min {
                        if f < mn {
                            return Err(Rejection {
                                key: Some(key.to_string()),
                                reason: RejectReason::Range,
                            });
                        }
                    }
                    if let Some(mx) = sp.max {
                        if f > mx {
                            return Err(Rejection {
                                key: Some(key.to_string()),
                                reason: RejectReason::Range,
                            });
                        }
                    }
                    // assign
                    match *key {
                        "bulkhead_max_concurrency" => out.bulkhead_max_concurrency = val as u32,
                        "retry_max_elapsed_seconds" => out.retry_max_elapsed_seconds = val as u64,
                        "retry_initial_interval_ms" => out.retry_initial_interval_ms = val as u64,
                        "retry_max_interval_ms" => out.retry_max_interval_ms = val as u64,
                        "breaker_failure_threshold" => out.breaker_failure_threshold = val as u32,
                        "breaker_timeout_seconds" => out.breaker_timeout_seconds = val as u64,
                        "job_timeout_seconds" => out.job_timeout_seconds = val as u64,
                        "max_pages" => out.max_pages = val as u32,
                        "min_idle_ms" => out.min_idle_ms = val as u64,
                        _ => {}
                    }
                }
                ValueKind::Float => {
                    let f = v.as_f64().ok_or(Rejection {
                        key: Some(key.to_string()),
                        reason: RejectReason::Type,
                    })?;
                    if v.is_string() {
                        return Err(Rejection {
                            key: Some(key.to_string()),
                            reason: RejectReason::Type,
                        });
                    }
                    if let Some(mn) = sp.min {
                        if f < mn {
                            return Err(Rejection {
                                key: Some(key.to_string()),
                                reason: RejectReason::Range,
                            });
                        }
                    }
                    if let Some(mx) = sp.max {
                        if f > mx {
                            return Err(Rejection {
                                key: Some(key.to_string()),
                                reason: RejectReason::Range,
                            });
                        }
                    }
                    match *key {
                        "retry_multiplier" => out.retry_multiplier = f,
                        "retry_randomization_factor" => out.retry_randomization_factor = f,
                        _ => {}
                    }
                }
            }
        }
    }
    // coherence
    if out.min_idle_ms < out.job_timeout_seconds * 2000 + 1000 {
        return Err(Rejection {
            key: Some("min_idle_ms".to_string()),
            reason: RejectReason::Coherence,
        });
    }
    Ok(out)
}

pub fn from_consul_entries(entries: &[KvEntry]) -> Result<ValidatedWorkerKv, Rejection> {
    // base64-decode each Value, parse as JSON scalar, build map
    let mut map = serde_json::Map::new();
    for e in entries {
        // key is full path; strip prefix? Use last segment
        let short = e.key.rsplit('/').next().unwrap_or(&e.key);
        let decoded = e.decode_value().map_err(|_| Rejection {
            key: Some(short.to_string()),
            reason: RejectReason::MalformedJson,
        })?;
        let v: Value = serde_json::from_str(&decoded).map_err(|_| Rejection {
            key: Some(short.to_string()),
            reason: RejectReason::MalformedJson,
        })?;
        map.insert(short.to_string(), v);
    }
    validate_values(&map)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use serde_json::json;
    #[test]
    fn unknown_key() {
        let mut m = serde_json::Map::new();
        m.insert("bulkhead_queue_limit".into(), json!(1));
        let r = validate_values(&m).unwrap_err();
        assert_eq!(r.key.as_deref(), Some("bulkhead_queue_limit"));
        assert_eq!(r.reason, RejectReason::UnknownKey);
    }
    #[test]
    fn type_string_for_multiplier() {
        let mut m = serde_json::Map::new();
        m.insert("retry_multiplier".into(), json!("1.5"));
        let r = validate_values(&m).unwrap_err();
        assert_eq!(r.key.as_deref(), Some("retry_multiplier"));
        assert_eq!(r.reason, RejectReason::Type);
    }
    #[test]
    fn type_float_for_int_key() {
        let mut m = serde_json::Map::new();
        m.insert("max_pages".into(), json!(4.5));
        let r = validate_values(&m).unwrap_err();
        assert_eq!(r.key.as_deref(), Some("max_pages"));
        assert_eq!(r.reason, RejectReason::Type);
    }
    #[test]
    fn range() {
        let mut m = serde_json::Map::new();
        m.insert("bulkhead_max_concurrency".into(), json!(100));
        let r = validate_values(&m).unwrap_err();
        assert_eq!(r.key.as_deref(), Some("bulkhead_max_concurrency"));
        assert_eq!(r.reason, RejectReason::Range);
    }
    #[test]
    fn coherence() {
        let mut m = serde_json::Map::new();
        m.insert("min_idle_ms".into(), json!(1000));
        m.insert("job_timeout_seconds".into(), json!(300));
        let r = validate_values(&m).unwrap_err();
        assert_eq!(r.key.as_deref(), Some("min_idle_ms"));
        assert_eq!(r.reason, RejectReason::Coherence);
    }
    #[test]
    fn malformed() {
        let r = validate_str("{bad").unwrap_err();
        assert_eq!(r.key, None);
        assert_eq!(r.reason, RejectReason::MalformedJson);
    }
    #[test]
    fn happy_path() {
        // Vendored copy of the C10 contract fixture: this repository must build
        // and test without checking out the platform repo.
        let raw = include_str!("../tests/fixtures/extraction-worker.valid.json");
        // strip _expected
        let v: serde_json::Value = serde_json::from_str(raw).unwrap();
        let mut obj = v.as_object().cloned().unwrap();
        obj.remove("_expected");
        let cfg = validate_values(&obj).unwrap();
        assert_eq!(cfg.max_pages, 2000);
    }
}
