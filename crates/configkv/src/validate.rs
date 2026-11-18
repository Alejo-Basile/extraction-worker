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

/// S11-P1-C30: validated view of `config/ocr-worker/`.
///
/// Every key is restart-only (ADR-0022): the OCR worker reads its configuration
/// once at start, so a KV change is observed and logged but never applied to a
/// running process.
#[derive(Clone, Debug, PartialEq)]
pub struct ValidatedOcrWorkerKv {
    pub ocr_dpi: u32,
    pub ocr_limit_w: u32,
    pub ocr_limit_pix: u64,
    pub ocr_lang: String,
    pub ocr_oem: u8,
    pub ocr_psm: u8,
    pub ocr_max_attempts: u32,
    pub consumer_block_ms: u64,
    pub consumer_count: usize,
    pub redis_ocr_stream: String,
    pub redis_ocr_group: String,
    pub redis_ocr_dlq_stream: String,
}

impl Default for ValidatedOcrWorkerKv {
    fn default() -> Self {
        Self {
            ocr_dpi: 150,
            ocr_limit_w: 3500,
            ocr_limit_pix: 12_000_000,
            ocr_lang: "spa+eng".to_string(),
            ocr_oem: 1,
            ocr_psm: 3,
            ocr_max_attempts: 3,
            consumer_block_ms: 5000,
            consumer_count: 1,
            redis_ocr_stream: "stream:ocr-processing".to_string(),
            redis_ocr_group: "ocr-workers".to_string(),
            redis_ocr_dlq_stream: "stream:ocr-processing-dlq".to_string(),
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
                ValueKind::String => {
                    // The extraction-worker contract declares no string keys.
                    // Reject instead of silently ignoring an unexpected shape.
                    return Err(Rejection {
                        key: Some(key.to_string()),
                        reason: RejectReason::Type,
                    });
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
    let map = decode_entries(entries)?;
    validate_values(&map)
}

/// Shared decoder: base64-decode every Consul value and rebuild the flat map by
/// the last path segment (the key name).
fn decode_entries(entries: &[KvEntry]) -> Result<serde_json::Map<String, Value>, Rejection> {
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
    Ok(map)
}

/// S11-P1-C30: parse and validate a raw JSON document against the ocr-worker
/// contract. Partial documents are accepted; absent keys keep their default.
pub fn validate_ocr_str(raw: &str) -> Result<ValidatedOcrWorkerKv, Rejection> {
    let v: Value = serde_json::from_str(raw).map_err(|_| Rejection {
        key: None,
        reason: RejectReason::MalformedJson,
    })?;
    let obj = v.as_object().ok_or(Rejection {
        key: None,
        reason: RejectReason::MalformedJson,
    })?;
    validate_ocr_values(obj)
}

pub fn validate_ocr_values(
    map: &serde_json::Map<String, Value>,
) -> Result<ValidatedOcrWorkerKv, Rejection> {
    let mut out = ValidatedOcrWorkerKv::default();
    for k in map.keys() {
        if schema::ocr_spec(k).is_none() {
            return Err(Rejection {
                key: Some(k.clone()),
                reason: RejectReason::UnknownKey,
            });
        }
    }
    for key in schema::ocr_keys() {
        let v = match map.get(*key) {
            Some(v) => v,
            None => continue,
        };
        let sp = match schema::ocr_spec(key) {
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
                let n = v.as_i64().or_else(|| v.as_u64().map(|x| x as i64));
                let is_integral =
                    v.is_number() && v.as_f64().map(|f| f.fract() == 0.0).unwrap_or(false);
                if !is_integral {
                    return Err(Rejection {
                        key: Some(key.to_string()),
                        reason: RejectReason::Type,
                    });
                }
                let val = match n {
                    Some(x) if x >= 0 => x,
                    _ => {
                        return Err(Rejection {
                            key: Some(key.to_string()),
                            reason: RejectReason::Type,
                        })
                    }
                };
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
                match *key {
                    "ocr_dpi" => out.ocr_dpi = val as u32,
                    "ocr_limit_w" => out.ocr_limit_w = val as u32,
                    "ocr_limit_pix" => out.ocr_limit_pix = val as u64,
                    "ocr_oem" => out.ocr_oem = val as u8,
                    "ocr_psm" => out.ocr_psm = val as u8,
                    "ocr_max_attempts" => out.ocr_max_attempts = val as u32,
                    "consumer_block_ms" => out.consumer_block_ms = val as u64,
                    "consumer_count" => out.consumer_count = val as usize,
                    _ => {}
                }
            }
            ValueKind::String => {
                let s = match v.as_str() {
                    Some(s) => s,
                    None => {
                        return Err(Rejection {
                            key: Some(key.to_string()),
                            reason: RejectReason::Type,
                        })
                    }
                };
                // For string keys, min/max are accepted lengths.
                let len = s.chars().count() as f64;
                if let Some(mn) = sp.min {
                    if len < mn {
                        return Err(Rejection {
                            key: Some(key.to_string()),
                            reason: RejectReason::Range,
                        });
                    }
                }
                if let Some(mx) = sp.max {
                    if len > mx {
                        return Err(Rejection {
                            key: Some(key.to_string()),
                            reason: RejectReason::Range,
                        });
                    }
                }
                match *key {
                    "ocr_lang" => out.ocr_lang = s.to_string(),
                    "redis_ocr_stream" => out.redis_ocr_stream = s.to_string(),
                    "redis_ocr_group" => out.redis_ocr_group = s.to_string(),
                    "redis_ocr_dlq_stream" => out.redis_ocr_dlq_stream = s.to_string(),
                    _ => {}
                }
            }
            ValueKind::Float => {
                return Err(Rejection {
                    key: Some(key.to_string()),
                    reason: RejectReason::Type,
                })
            }
        }
    }
    Ok(out)
}

/// S11-P1-C30: build the ocr-worker view from Consul entries.
///
/// KV values are raw strings (the sync script writes them verbatim), so a
/// string-typed key is used as-is and every other key is parsed as a JSON
/// scalar. Parsing everything as JSON first would reject legitimate values such
/// as `spa+eng` as `malformed_json`.
pub fn from_consul_entries_ocr(entries: &[KvEntry]) -> Result<ValidatedOcrWorkerKv, Rejection> {
    let mut map = serde_json::Map::new();
    for e in entries {
        let short = e.key.rsplit('/').next().unwrap_or(&e.key);
        let decoded = e.decode_value().map_err(|_| Rejection {
            key: Some(short.to_string()),
            reason: RejectReason::MalformedJson,
        })?;
        let kind = schema::ocr_spec(short).map(|s| s.kind);
        let value = if kind == Some(ValueKind::String) {
            Value::String(decoded)
        } else {
            serde_json::from_str(&decoded).unwrap_or(Value::String(decoded))
        };
        map.insert(short.to_string(), value);
    }
    validate_ocr_values(&map)
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
    fn ocr_entries_keep_raw_string_values() {
        use base64::Engine;
        let b64 = |s: &str| base64::engine::general_purpose::STANDARD.encode(s);
        let entries = vec![
            KvEntry {
                key: "config/ocr-worker/ocr_lang".into(),
                value: b64("spa+eng"),
            },
            KvEntry {
                key: "config/ocr-worker/ocr_dpi".into(),
                value: b64("300"),
            },
        ];
        let v = from_consul_entries_ocr(&entries).expect("raw KV values must validate");
        assert_eq!(v.ocr_lang, "spa+eng");
        assert_eq!(v.ocr_dpi, 300);
    }

    #[test]
    fn ocr_entries_reject_out_of_range_raw_int() {
        use base64::Engine;
        let b64 = |s: &str| base64::engine::general_purpose::STANDARD.encode(s);
        let entries = vec![KvEntry {
            key: "config/ocr-worker/ocr_dpi".into(),
            value: b64("1000"),
        }];
        let r = from_consul_entries_ocr(&entries).unwrap_err();
        assert_eq!(r.key.as_deref(), Some("ocr_dpi"));
        assert_eq!(r.reason, RejectReason::Range);
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
