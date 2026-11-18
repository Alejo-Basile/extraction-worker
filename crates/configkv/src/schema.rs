#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ValueKind {
    Int,
    Float,
    String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReloadMode {
    Hot,
    HotAumento,
    HotJobsNuevos,
    Restart,
}

#[derive(Clone, Copy, Debug)]
pub struct KeySpec {
    pub key: &'static str,
    pub kind: ValueKind,
    pub min: Option<f64>,
    pub max: Option<f64>,
    pub reload: ReloadMode,
}

static SPECS: [KeySpec; 11] = [
    KeySpec {
        key: "bulkhead_max_concurrency",
        kind: ValueKind::Int,
        min: Some(1.0),
        max: Some(64.0),
        reload: ReloadMode::HotAumento,
    },
    KeySpec {
        key: "retry_max_elapsed_seconds",
        kind: ValueKind::Int,
        min: Some(1.0),
        max: Some(600.0),
        reload: ReloadMode::Hot,
    },
    KeySpec {
        key: "retry_initial_interval_ms",
        kind: ValueKind::Int,
        min: Some(10.0),
        max: Some(60000.0),
        reload: ReloadMode::Hot,
    },
    KeySpec {
        key: "retry_multiplier",
        kind: ValueKind::Float,
        min: Some(1.0),
        max: Some(10.0),
        reload: ReloadMode::Hot,
    },
    KeySpec {
        key: "retry_max_interval_ms",
        kind: ValueKind::Int,
        min: Some(100.0),
        max: Some(300000.0),
        reload: ReloadMode::Hot,
    },
    KeySpec {
        key: "retry_randomization_factor",
        kind: ValueKind::Float,
        min: Some(0.0),
        max: Some(1.0),
        reload: ReloadMode::Hot,
    },
    KeySpec {
        key: "breaker_failure_threshold",
        kind: ValueKind::Int,
        min: Some(1.0),
        max: Some(100.0),
        reload: ReloadMode::Hot,
    },
    KeySpec {
        key: "breaker_timeout_seconds",
        kind: ValueKind::Int,
        min: Some(1.0),
        max: Some(300.0),
        reload: ReloadMode::Hot,
    },
    KeySpec {
        key: "job_timeout_seconds",
        kind: ValueKind::Int,
        min: Some(30.0),
        max: Some(3600.0),
        reload: ReloadMode::HotJobsNuevos,
    },
    KeySpec {
        key: "max_pages",
        kind: ValueKind::Int,
        min: Some(1.0),
        max: Some(100000.0),
        reload: ReloadMode::HotJobsNuevos,
    },
    KeySpec {
        key: "min_idle_ms",
        kind: ValueKind::Int,
        min: Some(1000.0),
        max: Some(86400000.0),
        reload: ReloadMode::Restart,
    },
];

static KEY_NAMES: [&str; 11] = [
    "bulkhead_max_concurrency",
    "retry_max_elapsed_seconds",
    "retry_initial_interval_ms",
    "retry_multiplier",
    "retry_max_interval_ms",
    "retry_randomization_factor",
    "breaker_failure_threshold",
    "breaker_timeout_seconds",
    "job_timeout_seconds",
    "max_pages",
    "min_idle_ms",
];

pub fn spec(key: &str) -> Option<&'static KeySpec> {
    SPECS.iter().find(|s| s.key == key)
}
pub fn keys() -> &'static [&'static str] {
    &KEY_NAMES
}
pub fn reload_mode(key: &str) -> Option<ReloadMode> {
    spec(key).map(|s| s.reload)
}

// ---------------------------------------------------------------------------
// S11-P1-C30: ocr-worker contract. Every key is `restart` because
// `OcrConfig` is built once at process start (ADR-0022).
// For `ValueKind::String`, `min`/`max` are accepted value lengths.
// ---------------------------------------------------------------------------
static OCR_SPECS: [KeySpec; 12] = [
    KeySpec {
        key: "ocr_dpi",
        kind: ValueKind::Int,
        min: Some(72.0),
        max: Some(600.0),
        reload: ReloadMode::Restart,
    },
    KeySpec {
        key: "ocr_limit_w",
        kind: ValueKind::Int,
        min: Some(100.0),
        max: Some(10000.0),
        reload: ReloadMode::Restart,
    },
    KeySpec {
        key: "ocr_limit_pix",
        kind: ValueKind::Int,
        min: Some(1000000.0),
        max: Some(100000000.0),
        reload: ReloadMode::Restart,
    },
    KeySpec {
        key: "ocr_lang",
        kind: ValueKind::String,
        min: Some(1.0),
        max: Some(255.0),
        reload: ReloadMode::Restart,
    },
    KeySpec {
        key: "ocr_oem",
        kind: ValueKind::Int,
        min: Some(0.0),
        max: Some(3.0),
        reload: ReloadMode::Restart,
    },
    KeySpec {
        key: "ocr_psm",
        kind: ValueKind::Int,
        min: Some(0.0),
        max: Some(13.0),
        reload: ReloadMode::Restart,
    },
    KeySpec {
        key: "ocr_max_attempts",
        kind: ValueKind::Int,
        min: Some(1.0),
        max: Some(10.0),
        reload: ReloadMode::Restart,
    },
    KeySpec {
        key: "consumer_block_ms",
        kind: ValueKind::Int,
        min: Some(100.0),
        max: Some(60000.0),
        reload: ReloadMode::Restart,
    },
    KeySpec {
        key: "consumer_count",
        kind: ValueKind::Int,
        min: Some(1.0),
        max: Some(16.0),
        reload: ReloadMode::Restart,
    },
    KeySpec {
        key: "redis_ocr_stream",
        kind: ValueKind::String,
        min: Some(1.0),
        max: Some(255.0),
        reload: ReloadMode::Restart,
    },
    KeySpec {
        key: "redis_ocr_group",
        kind: ValueKind::String,
        min: Some(1.0),
        max: Some(255.0),
        reload: ReloadMode::Restart,
    },
    KeySpec {
        key: "redis_ocr_dlq_stream",
        kind: ValueKind::String,
        min: Some(1.0),
        max: Some(255.0),
        reload: ReloadMode::Restart,
    },
];

static OCR_KEY_NAMES: [&str; 12] = [
    "ocr_dpi",
    "ocr_limit_w",
    "ocr_limit_pix",
    "ocr_lang",
    "ocr_oem",
    "ocr_psm",
    "ocr_max_attempts",
    "consumer_block_ms",
    "consumer_count",
    "redis_ocr_stream",
    "redis_ocr_group",
    "redis_ocr_dlq_stream",
];

pub fn ocr_spec(key: &str) -> Option<&'static KeySpec> {
    OCR_SPECS.iter().find(|s| s.key == key)
}
pub fn ocr_keys() -> &'static [&'static str] {
    &OCR_KEY_NAMES
}
pub fn ocr_reload_mode(key: &str) -> Option<ReloadMode> {
    ocr_spec(key).map(|s| s.reload)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    #[test]
    fn keys_count_and_resolve() {
        assert_eq!(keys().len(), 11);
        for k in keys() {
            assert!(spec(k).is_some());
        }
        assert_eq!(reload_mode("min_idle_ms"), Some(ReloadMode::Restart));
        assert_eq!(
            reload_mode("job_timeout_seconds"),
            Some(ReloadMode::HotJobsNuevos)
        );
        assert_eq!(
            reload_mode("bulkhead_max_concurrency"),
            Some(ReloadMode::HotAumento)
        );
    }

    #[test]
    fn ocr_keys_are_all_restart() {
        assert_eq!(ocr_keys().len(), 12);
        for k in ocr_keys() {
            assert!(ocr_spec(k).is_some(), "missing ocr spec for {k}");
            assert_eq!(ocr_reload_mode(k), Some(ReloadMode::Restart));
        }
        assert!(ocr_spec("ocr_lang")
            .map(|s| s.kind == ValueKind::String)
            .unwrap_or(false));
    }
}
