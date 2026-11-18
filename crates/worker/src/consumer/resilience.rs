//! Resiliencia (S2-P3-02).
//!
//! Composicion correcta: `Retry` envuelve al `CircuitBreaker`, y el breaker
//! envuelve la llamada individual (SDD seccion 7.3). Reglas:
//! - `Failure::Document` es problema del documento: va directo a DLQ, no se
//!   reintenta y no cuenta para el breaker.
//! - `Failure::Transient` se reintenta con backoff y cuenta para el breaker.
//! - `Failure::Fatal` es infraestructura no recuperable (auth/config): no se
//!   reintenta pero si cuenta para el breaker.
//! - `Failure::CircuitOpen` es un rechazo local del breaker: la llamada no
//!   toco la red, no se reintenta y no cuenta (el breaker ya hizo su trabajo).

use std::future::Future;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tracing::warn;

use crate::consumer::min_idle::MinIdleRestartState;

/// Clasificacion de un fallo en el pipeline del consumer (S2-P3-02/07).
#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum Failure {
    #[error("documento: {code}: {message}")]
    Document { code: String, message: String },
    #[error("transitorio: {code}: {message}")]
    Transient { code: String, message: String },
    #[error("fatal: {code}: {message}")]
    Fatal { code: String, message: String },
    #[error("circuit breaker abierto")]
    CircuitOpen,
}

impl Failure {
    pub fn transient(code: &str, message: impl Into<String>) -> Self {
        Failure::Transient {
            code: code.to_string(),
            message: message.into(),
        }
    }

    pub fn fatal(code: &str, message: impl Into<String>) -> Self {
        Failure::Fatal {
            code: code.to_string(),
            message: message.into(),
        }
    }
}

#[derive(Debug)]
enum CbState {
    Closed {
        failures: u32,
    },
    Open {
        until: Instant,
    },
    /// Con un solo probe en vuelo: el resto de llamadas se rechaza hasta
    /// que el probe defina el estado (evita retry storms al rearmar).
    HalfOpen {
        probe_in_flight: bool,
    },
}

/// Circuit Breaker (S2-P3-05): umbral por fallos consecutivos de
/// infraestructura, `timeout` para pasar a HalfOpen y rearme con probe unico.
/// Atomic f64 (std lacks AtomicF64). Stored as bits.
#[derive(Debug)]
pub struct AtomicF64 {
    inner: AtomicU64,
}

impl AtomicF64 {
    pub fn new(v: f64) -> Self {
        Self {
            inner: AtomicU64::new(v.to_bits()),
        }
    }
    pub fn load(&self, o: Ordering) -> f64 {
        f64::from_bits(self.inner.load(o))
    }
    pub fn store(&self, v: f64, o: Ordering) {
        self.inner.store(v.to_bits(), o);
    }
}

#[derive(Debug)]
pub struct BreakerConfig {
    pub failure_threshold: Arc<AtomicUsize>,
    pub timeout_ms: Arc<AtomicU64>,
}

impl Default for BreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: Arc::new(AtomicUsize::new(5)),
            timeout_ms: Arc::new(AtomicU64::new(30_000)),
        }
    }
}

#[derive(Debug)]
pub struct CircuitBreaker {
    state: Mutex<CbState>,
    config: Arc<BreakerConfig>,
}

impl Default for CircuitBreaker {
    fn default() -> Self {
        Self::new(5, Duration::from_secs(30))
    }
}

impl CircuitBreaker {
    pub fn new(failure_threshold: u32, timeout: Duration) -> Self {
        let config = Arc::new(BreakerConfig {
            failure_threshold: Arc::new(AtomicUsize::new(failure_threshold as usize)),
            timeout_ms: Arc::new(AtomicU64::new(timeout.as_millis() as u64)),
        });
        Self {
            state: Mutex::new(CbState::Closed { failures: 0 }),
            config,
        }
    }
    pub fn with_config(config: Arc<BreakerConfig>) -> Self {
        Self {
            state: Mutex::new(CbState::Closed { failures: 0 }),
            config,
        }
    }
    pub fn config(&self) -> Arc<BreakerConfig> {
        self.config.clone()
    }
}

impl CircuitBreaker {
    pub fn allow(&self) -> bool {
        let mut st = match self.state.lock() {
            Ok(s) => s,
            Err(_) => return true,
        };
        match *st {
            CbState::Closed { .. } => true,
            CbState::Open { until } => {
                if Instant::now() >= until {
                    *st = CbState::HalfOpen {
                        probe_in_flight: true,
                    };
                    true
                } else {
                    false
                }
            }
            CbState::HalfOpen { probe_in_flight } => {
                if probe_in_flight {
                    false
                } else {
                    *st = CbState::HalfOpen {
                        probe_in_flight: true,
                    };
                    true
                }
            }
        }
    }

    /// Estado observado por las metricas `worker_breaker_open`.
    /// `HalfOpen` cuenta como abierto: sigue rechazando trafico (salvo el
    /// unico probe) hasta que el exito lo cierre.
    pub fn is_open(&self) -> bool {
        match self.state.lock() {
            Ok(st) => !matches!(*st, CbState::Closed { .. }),
            Err(_) => false,
        }
    }

    /// Cierto mientras el breaker rechaza por cooldown vigente (S6-P3-03):
    /// `Open` con el plazo aun no vencido.
    ///
    /// `HalfOpen` NO cuenta: deja pasar el probe que decide si el breaker se
    /// cierra o se reabre. El modo degradado del consumer usa este metodo y
    /// no `is_open()` precisamente para no pausar el unico camino de
    /// recuperacion.
    pub fn rejecting(&self) -> bool {
        match self.state.lock() {
            Ok(st) => match *st {
                CbState::Closed { .. } => false,
                CbState::HalfOpen { .. } => false,
                CbState::Open { until } => Instant::now() < until,
            },
            Err(_) => false,
        }
    }

    pub fn on_success(&self) {
        if let Ok(mut st) = self.state.lock() {
            *st = CbState::Closed { failures: 0 };
        }
    }

    pub fn on_failure(&self) {
        let threshold = self.config.failure_threshold.load(Ordering::Relaxed);
        let timeout = Duration::from_millis(self.config.timeout_ms.load(Ordering::Relaxed));
        let mut st = match self.state.lock() {
            Ok(s) => s,
            Err(_) => return,
        };
        match *st {
            CbState::Closed { failures } => {
                let next = failures + 1;
                if next >= threshold as u32 {
                    *st = CbState::Open {
                        until: Instant::now() + timeout,
                    };
                } else {
                    *st = CbState::Closed { failures: next };
                }
            }
            CbState::HalfOpen { .. } => {
                *st = CbState::Open {
                    until: Instant::now() + timeout,
                };
            }
            CbState::Open { .. } => {}
        }
    }
}

#[derive(Debug, Clone)]
pub struct RetryConfig {
    pub max_elapsed_seconds: Arc<AtomicU64>,
    pub initial_interval_ms: Arc<AtomicU64>,
    pub multiplier: Arc<AtomicF64>,
    pub max_interval_ms: Arc<AtomicU64>,
    pub randomization_factor: Arc<AtomicF64>,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_elapsed_seconds: Arc::new(AtomicU64::new(30)),
            initial_interval_ms: Arc::new(AtomicU64::new(500)),
            multiplier: Arc::new(AtomicF64::new(1.5)),
            max_interval_ms: Arc::new(AtomicU64::new(60_000)),
            randomization_factor: Arc::new(AtomicF64::new(0.5)),
        }
    }
}

#[derive(Debug, Clone)]
pub struct WorkerTunables {
    pub breaker: Arc<BreakerConfig>,
    pub retry: Arc<RetryConfig>,
    pub job_timeout_ms: Arc<AtomicU64>,
    pub max_pages: Arc<AtomicU32>,
    pub bulkhead_target: Arc<AtomicUsize>,
    pub bulkhead_applied: Arc<AtomicUsize>,
    pub min_idle: Arc<MinIdleRestartState>,
}

impl Default for WorkerTunables {
    fn default() -> Self {
        Self {
            breaker: Arc::new(BreakerConfig::default()),
            retry: Arc::new(RetryConfig::default()),
            job_timeout_ms: Arc::new(AtomicU64::new(300_000)),
            max_pages: Arc::new(AtomicU32::new(2000)),
            bulkhead_target: Arc::new(AtomicUsize::new(4)),
            bulkhead_applied: Arc::new(AtomicUsize::new(4)),
            min_idle: Arc::new(MinIdleRestartState::new(601_000)),
        }
    }
}

impl WorkerTunables {
    pub fn new() -> Self {
        Self::default()
    }
}

impl From<&crate::consumer::config::ConsumerConfig> for WorkerTunables {
    fn from(cfg: &crate::consumer::config::ConsumerConfig) -> Self {
        Self {
            breaker: Arc::new(BreakerConfig::default()),
            retry: Arc::new(RetryConfig::default()),
            job_timeout_ms: Arc::new(AtomicU64::new(cfg.job_timeout.as_millis() as u64)),
            max_pages: Arc::new(AtomicU32::new(cfg.max_pages)),
            bulkhead_target: Arc::new(AtomicUsize::new(cfg.max_concurrency)),
            bulkhead_applied: Arc::new(AtomicUsize::new(cfg.max_concurrency)),
            min_idle: Arc::new(MinIdleRestartState::new(cfg.min_idle_ms)),
        }
    }
}

pub async fn with_retry_cb<F, Fut, T>(cb: &CircuitBreaker, op: F) -> Result<T, Failure>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T, Failure>>,
{
    with_retry_cb_cfg(cb, &RetryConfig::default(), op).await
}

pub async fn with_retry_cb_cfg<F, Fut, T>(
    cb: &CircuitBreaker,
    cfg: &RetryConfig,
    op: F,
) -> Result<T, Failure>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T, Failure>>,
{
    use backoff::{backoff::Backoff, ExponentialBackoffBuilder};
    let max_elapsed = Duration::from_secs(cfg.max_elapsed_seconds.load(Ordering::Relaxed));
    let mut bo = ExponentialBackoffBuilder::new()
        .with_initial_interval(Duration::from_millis(
            cfg.initial_interval_ms.load(Ordering::Relaxed),
        ))
        .with_multiplier(cfg.multiplier.load(Ordering::Relaxed))
        .with_max_interval(Duration::from_millis(
            cfg.max_interval_ms.load(Ordering::Relaxed),
        ))
        .with_randomization_factor(cfg.randomization_factor.load(Ordering::Relaxed))
        .with_max_elapsed_time(Some(max_elapsed))
        .build();
    loop {
        if !cb.allow() {
            // Rechazo sin red: el caller deja el mensaje pendiente y el
            // breaker rearmara solo cuando expire su timeout.
            return Err(Failure::CircuitOpen);
        }
        match op().await {
            Ok(v) => {
                cb.on_success();
                return Ok(v);
            }
            Err(e) => {
                if e.counts_for_breaker() {
                    cb.on_failure();
                }
                // Sin este log, el unico sintoma visible es el `CircuitOpen`
                // final, que esconde cual fue el error real del primer
                // intento. Fue exactamente lo que impidio diagnosticar el
                // head-of-line del XREADGROUP bloqueante.
                warn!(
                    service = crate::telemetry::SERVICE,
                    msg = "operacion fallo; evaluando reintento",
                    error = %e,
                    retryable = e.is_retryable()
                );
                if !e.is_retryable() {
                    return Err(e);
                }
                if let Some(d) = bo.next_backoff() {
                    tokio::time::sleep(d).await;
                    continue;
                }
                return Err(e);
            }
        }
    }
}

impl Failure {
    fn is_retryable(&self) -> bool {
        matches!(self, Failure::Transient { .. })
    }

    fn counts_for_breaker(&self) -> bool {
        matches!(self, Failure::Transient { .. } | Failure::Fatal { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn breaker_abre_tras_umbral_de_fallos_consecutivos() {
        let cb = CircuitBreaker::new(3, Duration::from_millis(50));
        assert!(cb.allow());
        cb.on_failure();
        cb.on_failure();
        assert!(cb.allow(), "por debajo del umbral sigue cerrado");
        cb.on_failure();
        assert!(!cb.allow(), "al alcanzar el umbral debe abrir");
    }

    #[test]
    fn breaker_reporta_estado_para_metricas() {
        let cb = CircuitBreaker::new(1, Duration::from_millis(500));
        assert!(!cb.is_open());
        cb.on_failure();
        assert!(cb.is_open());
        std::thread::sleep(Duration::from_millis(520));
        assert!(cb.allow(), "pasa a halfopen");
        assert!(cb.is_open(), "halfopen sigue degradado hasta el exito");
        cb.on_success();
        assert!(!cb.is_open());
    }

    /// S6-P3-03: el modo degradado debe pausar el consumo solo mientras el
    /// cooldown rechaza de verdad; en `HalfOpen` el probe tiene que poder
    /// pasar o el worker nunca recuperaria.
    #[test]
    fn rejecting_solo_durante_el_cooldown() {
        let cb = CircuitBreaker::new(1, Duration::from_millis(50));
        assert!(!cb.rejecting(), "cerrado no rechaza");

        cb.on_failure();
        assert!(cb.is_open());
        assert!(cb.rejecting(), "abierto con cooldown vigente rechaza");

        std::thread::sleep(Duration::from_millis(60));
        assert!(!cb.rejecting(), "vencido el cooldown el probe debe pasar");
        assert!(cb.allow(), "el probe entra en halfopen");
        assert!(!cb.rejecting(), "halfopen no pausa el consumo");

        cb.on_failure();
        assert!(cb.rejecting(), "probe fallido reabre con cooldown nuevo");
    }

    #[test]
    fn breaker_cierra_con_exito_y_resetea_el_contador() {
        let cb = CircuitBreaker::new(2, Duration::from_millis(50));
        cb.on_failure();
        cb.on_success();
        cb.on_failure();
        assert!(cb.allow(), "un exito resetea los fallos consecutivos");
    }

    #[test]
    fn halfopen_permite_un_solo_probe() {
        let cb = CircuitBreaker::new(1, Duration::from_millis(20));
        assert!(cb.allow());
        cb.on_failure();
        assert!(!cb.allow(), "abierto rechaza");
        std::thread::sleep(Duration::from_millis(40));
        assert!(cb.allow(), "expirado el timeout pasa a halfopen con probe");
        assert!(!cb.allow(), "solo un probe en vuelo");
        cb.on_success();
        assert!(cb.allow(), "probe exitoso cierra el breaker");
    }

    #[test]
    fn fallo_del_probe_reabre_el_breaker() {
        let cb = CircuitBreaker::new(1, Duration::from_millis(20));
        assert!(cb.allow());
        cb.on_failure();
        std::thread::sleep(Duration::from_millis(40));
        assert!(cb.allow());
        cb.on_failure();
        assert!(!cb.allow(), "probe fallido debe reabrir");
        std::thread::sleep(Duration::from_millis(40));
        assert!(cb.allow(), "vuelve a halfopen tras el timeout");
    }

    #[test]
    fn circuito_abierto_rechaza_sin_llamar_la_operacion() {
        let cb = CircuitBreaker::new(1, Duration::from_millis(500));
        assert!(cb.allow());
        cb.on_failure();
        let op = || async { Ok::<u8, Failure>(1) };
        match futures_block(with_retry_cb(&cb, op)) {
            Err(Failure::CircuitOpen) => {}
            other => panic!("debe rechazar con CircuitOpen, obtuvo: {other:?}"),
        }
    }

    #[test]
    fn documento_no_reintenta_ni_cuenta_para_el_breaker() {
        let cb = CircuitBreaker::new(1, Duration::from_millis(500));
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let seen = calls.clone();
        let op = move || {
            let seen = seen.clone();
            async move {
                seen.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Err::<u8, Failure>(Failure::Document {
                    code: "INVALID_CONTRACT".to_string(),
                    message: "x".to_string(),
                })
            }
        };
        match futures_block(with_retry_cb(&cb, op)) {
            Err(Failure::Document { .. }) => {}
            other => panic!("debe devolver el error de documento, obtuvo: {other:?}"),
        }
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::Relaxed),
            1,
            "un error de documento no se reintenta"
        );
        assert!(cb.allow(), "un error de documento no abre el breaker");
    }

    #[test]
    fn transitorio_reintenta_hasta_exitoso() {
        let cb = CircuitBreaker::default();
        let fails = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(2));
        let seen = fails.clone();
        let op = move || {
            let seen = seen.clone();
            async move {
                if seen.fetch_sub(1, std::sync::atomic::Ordering::Relaxed) > 0 {
                    Err::<u8, Failure>(Failure::transient("REDIS_IO", "connection refused"))
                } else {
                    Ok(7u8)
                }
            }
        };
        match futures_block(with_retry_cb(&cb, op)) {
            Ok(v) => assert_eq!(v, 7),
            other => panic!("debe reintentar y triunfar, obtuvo: {other:?}"),
        }
    }

    #[test]
    fn fatal_no_reintenta_pero_cuenta_para_el_breaker() {
        let cb = CircuitBreaker::new(2, Duration::from_millis(500));
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let seen = calls.clone();
        let op = move || {
            let seen = seen.clone();
            async move {
                seen.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Err::<u8, Failure>(Failure::fatal("REDIS_CONFIG", "auth failed"))
            }
        };
        match futures_block(with_retry_cb(&cb, op)) {
            Err(Failure::Fatal { .. }) => {}
            other => panic!("fatal no se reintenta, obtuvo: {other:?}"),
        }
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::Relaxed),
            1,
            "un fatal no se reintenta"
        );
        cb.on_failure();
        assert!(!cb.allow(), "los fatales cuentan para el umbral");
    }

    #[test]
    fn breaker_umbral_cambiado_en_caliente_no_cierra_open() {
        let cfg = Arc::new(BreakerConfig {
            failure_threshold: Arc::new(AtomicUsize::new(2)),
            timeout_ms: Arc::new(AtomicU64::new(50)),
        });
        let cb = CircuitBreaker::with_config(cfg.clone());
        cb.on_failure();
        cb.on_failure();
        assert!(cb.is_open(), "debe abrir con 2 fallos");
        cfg.failure_threshold
            .store(100, std::sync::atomic::Ordering::Relaxed);
        assert!(cb.is_open(), "sigue abierto tras cambiar umbral");
    }

    #[test]
    fn breaker_umbral_compartido_afecta_instancias_existentes() {
        let cfg = Arc::new(BreakerConfig {
            failure_threshold: Arc::new(AtomicUsize::new(2)),
            timeout_ms: Arc::new(AtomicU64::new(50)),
        });
        let cb1 = CircuitBreaker::with_config(cfg.clone());
        cb1.on_failure();
        cb1.on_failure();
        assert!(cb1.is_open());
        cfg.failure_threshold
            .store(100, std::sync::atomic::Ordering::Relaxed);
        let cb2 = CircuitBreaker::with_config(cfg.clone());
        for _ in 0..99 {
            cb2.on_failure();
            if cb2.is_open() {
                break;
            }
        }
        assert!(!cb2.is_open(), "nuevo breaker necesita 100 fallos");
    }

    #[test]
    fn retry_params_vivos_respetan_max_elapsed() {
        let cfg = RetryConfig {
            max_elapsed_seconds: Arc::new(AtomicU64::new(1)),
            initial_interval_ms: Arc::new(AtomicU64::new(10)),
            multiplier: Arc::new(AtomicF64::new(1.0)),
            max_interval_ms: Arc::new(AtomicU64::new(10)),
            randomization_factor: Arc::new(AtomicF64::new(0.0)),
        };
        let cb = CircuitBreaker::default();
        let start = std::time::Instant::now();
        let op = || async { Err::<u8, Failure>(Failure::transient("X", "y")) };
        let _ = futures_block(with_retry_cb_cfg(&cb, &cfg, op));
        assert!(start.elapsed().as_secs_f64() < 1.5, "debe terminar rapido");
    }

    /// Puente minimo para correr futures de tokio en tests sin depender del
    /// runtime del workspace (los tests unitarios no usan #[tokio::test]).
    fn futures_block<F: Future>(f: F) -> F::Output {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap_or_else(|e| panic!("runtime de test: {e}"));
        rt.block_on(f)
    }
}

#[cfg(test)]
mod hot_reload_tests2 {
    use super::*;
    use crate::consumer::config::ConsumerConfig;
    #[test]
    fn worker_tunables_desde_config_siembra_env() {
        let _g = crate::consumer::config::test_env::lock();
        let cfg = ConsumerConfig::from_env();
        let t = WorkerTunables::from(&cfg);
        use std::sync::atomic::Ordering;
        assert_eq!(
            t.job_timeout_ms.load(Ordering::Relaxed),
            cfg.job_timeout.as_millis() as u64
        );
        assert_eq!(t.max_pages.load(Ordering::Relaxed), cfg.max_pages);
        assert_eq!(
            t.bulkhead_target.load(Ordering::Relaxed),
            cfg.max_concurrency
        );
        assert_eq!(
            t.bulkhead_applied.load(Ordering::Relaxed),
            cfg.max_concurrency
        );
    }

    #[test]
    fn tunables_min_idle_efectivo_es_el_de_arranque() {
        let _g = crate::consumer::config::test_env::lock();
        let cfg = crate::consumer::config::ConsumerConfig::from_env();
        let t = WorkerTunables::from(&cfg);
        assert_eq!(t.min_idle.effective_ms(), cfg.min_idle_ms);
        t.min_idle.note(cfg.min_idle_ms + 1_000);
        assert_eq!(t.min_idle.effective_ms(), cfg.min_idle_ms);
        assert_eq!(t.min_idle.pending_ms(), Some(cfg.min_idle_ms + 1_000));
    }
}
