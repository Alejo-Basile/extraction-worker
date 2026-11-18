use configkv::ValidatedWorkerKv;
use std::time::Duration;

pub struct WorkerKvOverlay {
    pub bulkhead_max_concurrency: Option<u32>,
    pub retry_max_elapsed_seconds: Option<u64>,
    pub retry_initial_interval_ms: Option<u64>,
    pub retry_multiplier: Option<f64>,
    pub retry_max_interval_ms: Option<u64>,
    pub retry_randomization_factor: Option<f64>,
    pub breaker_failure_threshold: Option<u32>,
    pub breaker_timeout_seconds: Option<u64>,
    pub job_timeout_seconds: Option<u64>,
    pub max_pages: Option<u32>,
    pub min_idle_ms: Option<u64>,
}

impl WorkerKvOverlay {
    pub fn from_validated(v: &ValidatedWorkerKv) -> Self {
        Self {
            bulkhead_max_concurrency: Some(v.bulkhead_max_concurrency),
            retry_max_elapsed_seconds: Some(v.retry_max_elapsed_seconds),
            retry_initial_interval_ms: Some(v.retry_initial_interval_ms),
            retry_multiplier: Some(v.retry_multiplier),
            retry_max_interval_ms: Some(v.retry_max_interval_ms),
            retry_randomization_factor: Some(v.retry_randomization_factor),
            breaker_failure_threshold: Some(v.breaker_failure_threshold),
            breaker_timeout_seconds: Some(v.breaker_timeout_seconds),
            job_timeout_seconds: Some(v.job_timeout_seconds),
            max_pages: Some(v.max_pages),
            min_idle_ms: Some(v.min_idle_ms),
        }
    }
    pub fn apply(&self, cfg: &mut crate::consumer::config::ConsumerConfig) {
        if let Some(v) = self.bulkhead_max_concurrency {
            cfg.max_concurrency = v as usize;
        }
        if let Some(v) = self.job_timeout_seconds {
            cfg.job_timeout = Duration::from_secs(v);
        }
        if let Some(v) = self.max_pages {
            cfg.max_pages = v;
        }
        // min_idle_ms is restart-only: never applied by the overlay.
        // retry/breaker fields consumed by C19 (no fields on ConsumerConfig yet)
    }
    pub fn apply_tunables(&self, t: &crate::consumer::resilience::WorkerTunables) {
        use std::sync::atomic::Ordering;
        if let Some(v) = self.breaker_failure_threshold {
            t.breaker
                .failure_threshold
                .store(v as usize, Ordering::Relaxed);
        }
        if let Some(v) = self.breaker_timeout_seconds {
            t.breaker.timeout_ms.store(v * 1000, Ordering::Relaxed);
        }
        if let Some(v) = self.retry_max_elapsed_seconds {
            t.retry.max_elapsed_seconds.store(v, Ordering::Relaxed);
        }
        if let Some(v) = self.retry_initial_interval_ms {
            t.retry.initial_interval_ms.store(v, Ordering::Relaxed);
        }
        if let Some(v) = self.retry_max_interval_ms {
            t.retry.max_interval_ms.store(v, Ordering::Relaxed);
        }
        if let Some(v) = self.retry_multiplier {
            t.retry.multiplier.store(v, Ordering::Relaxed);
        }
        if let Some(v) = self.retry_randomization_factor {
            t.retry.randomization_factor.store(v, Ordering::Relaxed);
        }
    }
    // accessors for C19
    pub fn retry_max_elapsed_seconds(&self) -> Option<u64> {
        self.retry_max_elapsed_seconds
    }
    pub fn retry_initial_interval_ms(&self) -> Option<u64> {
        self.retry_initial_interval_ms
    }
    pub fn retry_multiplier(&self) -> Option<f64> {
        self.retry_multiplier
    }
    pub fn retry_max_interval_ms(&self) -> Option<u64> {
        self.retry_max_interval_ms
    }
    pub fn retry_randomization_factor(&self) -> Option<f64> {
        self.retry_randomization_factor
    }
    pub fn breaker_failure_threshold(&self) -> Option<u32> {
        self.breaker_failure_threshold
    }
    pub fn breaker_timeout_seconds(&self) -> Option<u64> {
        self.breaker_timeout_seconds
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use crate::consumer::config::ConsumerConfig;
    use std::sync::Mutex;
    static LOCK: Mutex<()> = Mutex::new(());
    #[test]
    fn from_env_defaults() {
        let _g = LOCK.lock().unwrap();
        let cfg = ConsumerConfig::from_env();
        assert_eq!(cfg.max_concurrency, 4);
        assert_eq!(cfg.job_timeout, std::time::Duration::from_secs(300));
        assert_eq!(cfg.max_pages, 2000);
        assert_eq!(cfg.min_idle_ms, 601000);
    }
    #[test]
    fn apply_tunables_aplica_solo_campos_presentes() {
        let _g = LOCK.lock().unwrap();
        let t = crate::consumer::resilience::WorkerTunables::new();
        let ov = WorkerKvOverlay {
            bulkhead_max_concurrency: None,
            retry_max_elapsed_seconds: Some(10),
            retry_initial_interval_ms: None,
            retry_multiplier: None,
            retry_max_interval_ms: None,
            retry_randomization_factor: None,
            breaker_failure_threshold: Some(1),
            breaker_timeout_seconds: None,
            job_timeout_seconds: None,
            max_pages: None,
            min_idle_ms: None,
        };
        ov.apply_tunables(&t);
        use std::sync::atomic::Ordering;
        assert_eq!(t.breaker.failure_threshold.load(Ordering::Relaxed), 1);
        assert_eq!(t.retry.max_elapsed_seconds.load(Ordering::Relaxed), 10);
        assert_eq!(t.breaker.timeout_ms.load(Ordering::Relaxed), 30_000);
        assert_eq!(t.retry.initial_interval_ms.load(Ordering::Relaxed), 500);
    }

    #[test]
    fn apply_tunables_umbral_compartido_abre_breakers_existentes() {
        let _g = LOCK.lock().unwrap();
        let t = crate::consumer::resilience::WorkerTunables::new();
        let cb1 = crate::consumer::resilience::CircuitBreaker::with_config(t.breaker.clone());
        let cb2 = crate::consumer::resilience::CircuitBreaker::with_config(t.breaker.clone());
        let ov = WorkerKvOverlay {
            bulkhead_max_concurrency: None,
            retry_max_elapsed_seconds: None,
            retry_initial_interval_ms: None,
            retry_multiplier: None,
            retry_max_interval_ms: None,
            retry_randomization_factor: None,
            breaker_failure_threshold: Some(1),
            breaker_timeout_seconds: None,
            job_timeout_seconds: None,
            max_pages: None,
            min_idle_ms: None,
        };
        ov.apply_tunables(&t);
        cb1.on_failure();
        cb2.on_failure();
        assert!(cb1.is_open());
        assert!(cb2.is_open());
    }

    #[test]
    fn only_max_pages_changes() {
        let _g = LOCK.lock().unwrap();
        let mut cfg = ConsumerConfig::from_env();
        let before_mc = cfg.max_concurrency;
        let before_jt = cfg.job_timeout;
        let before_mi = cfg.min_idle_ms;
        let ov = WorkerKvOverlay {
            bulkhead_max_concurrency: None,
            retry_max_elapsed_seconds: None,
            retry_initial_interval_ms: None,
            retry_multiplier: None,
            retry_max_interval_ms: None,
            retry_randomization_factor: None,
            breaker_failure_threshold: None,
            breaker_timeout_seconds: None,
            job_timeout_seconds: None,
            max_pages: Some(10),
            min_idle_ms: None,
        };
        ov.apply(&mut cfg);
        assert_eq!(cfg.max_pages, 10);
        assert_eq!(cfg.max_concurrency, before_mc);
        assert_eq!(cfg.job_timeout, before_jt);
        assert_eq!(cfg.min_idle_ms, before_mi);
    }

    #[test]
    fn apply_no_aplica_min_idle_ms() {
        let _g = LOCK.lock().unwrap();
        let v = configkv::ValidatedWorkerKv {
            min_idle_ms: 900_000,
            ..configkv::ValidatedWorkerKv::default()
        };
        let mut cfg = ConsumerConfig::from_env();
        let before = cfg.min_idle_ms;
        WorkerKvOverlay::from_validated(&v).apply(&mut cfg);
        assert_eq!(cfg.min_idle_ms, before);
        assert_eq!(cfg.min_idle_ms, 601_000);
    }

    #[test]
    fn job_timeout_caliente_no_recalcula_min_idle_ms() {
        let _g = LOCK.lock().unwrap();
        let mut cfg = ConsumerConfig::from_env();
        let ov = WorkerKvOverlay {
            bulkhead_max_concurrency: None,
            retry_max_elapsed_seconds: None,
            retry_initial_interval_ms: None,
            retry_multiplier: None,
            retry_max_interval_ms: None,
            retry_randomization_factor: None,
            breaker_failure_threshold: None,
            breaker_timeout_seconds: None,
            job_timeout_seconds: Some(600),
            max_pages: None,
            min_idle_ms: Some(900_000),
        };
        ov.apply(&mut cfg);
        assert_eq!(cfg.job_timeout, std::time::Duration::from_secs(600));
        assert_eq!(cfg.min_idle_ms, 601_000);
    }
}
