#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ValueKind {
    Int,
    Float,
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
}
