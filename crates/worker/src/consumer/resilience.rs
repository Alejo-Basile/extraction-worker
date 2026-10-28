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
use std::sync::Mutex;
use std::time::{Duration, Instant};

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
#[derive(Debug)]
pub struct CircuitBreaker {
    state: Mutex<CbState>,
    failure_threshold: u32,
    timeout: Duration,
}

impl Default for CircuitBreaker {
    fn default() -> Self {
        Self::new(5, Duration::from_secs(30))
    }
}

impl CircuitBreaker {
    pub fn new(failure_threshold: u32, timeout: Duration) -> Self {
        Self {
            state: Mutex::new(CbState::Closed { failures: 0 }),
            failure_threshold,
            timeout,
        }
    }

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

    pub fn on_success(&self) {
        if let Ok(mut st) = self.state.lock() {
            *st = CbState::Closed { failures: 0 };
        }
    }

    pub fn on_failure(&self) {
        let mut st = match self.state.lock() {
            Ok(s) => s,
            Err(_) => return,
        };
        match *st {
            CbState::Closed { failures } => {
                let next = failures + 1;
                if next >= self.failure_threshold {
                    *st = CbState::Open {
                        until: Instant::now() + self.timeout,
                    };
                } else {
                    *st = CbState::Closed { failures: next };
                }
            }
            // El probe fallo: se reabre y el contador arranca de cero en el
            // proximo ciclo.
            CbState::HalfOpen { .. } => {
                *st = CbState::Open {
                    until: Instant::now() + self.timeout,
                };
            }
            CbState::Open { .. } => {}
        }
    }
}

/// Retry envuelve al Circuit Breaker (S2-P3-04): reintenta solo fallos
/// reintentables con backoff exponencial + jitter; el breaker decide cuando
/// dejar de tocar la red.
pub async fn with_retry_cb<F, Fut, T>(cb: &CircuitBreaker, op: F) -> Result<T, Failure>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T, Failure>>,
{
    use backoff::{backoff::Backoff, ExponentialBackoffBuilder};
    let mut bo = ExponentialBackoffBuilder::new()
        .with_max_elapsed_time(Some(Duration::from_secs(30)))
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
