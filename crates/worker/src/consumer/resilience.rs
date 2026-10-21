//! Resiliencia (S2-P3-02).
//!
//! Composición correcta: Retry envuelve a Circuit Breaker. Circuit Breaker
//! envuelve la llamada individual para evitar retry storms.
//!
//! Clasificación: Transient (reintentable) vs Business (no reintentable).

use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum ErrorKind {
    Transient,
    Business,
    Unknown,
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct ClassifiedError {
    pub kind: ErrorKind,
    pub message: String,
}

#[derive(Debug)]
enum CbState {
    Closed,
    Open { until: std::time::Instant },
    HalfOpen,
}

/// Circuit Breaker simple (sin deps externas)
#[derive(Debug)]
pub struct CircuitBreaker {
    state: std::sync::Mutex<CbState>,
    failure_threshold: u32,
    consecutive_failures: std::sync::atomic::AtomicU32,
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
            state: std::sync::Mutex::new(CbState::Closed),
            failure_threshold,
            consecutive_failures: std::sync::atomic::AtomicU32::new(0),
            timeout,
        }
    }

    #[allow(dead_code)]
    pub fn allow(&self) -> bool {
        if let Ok(mut st) = self.state.lock() {
            match *st {
                CbState::Closed => true,
                CbState::Open { until } => {
                    if std::time::Instant::now() >= until {
                        *st = CbState::HalfOpen;
                        true
                    } else {
                        false
                    }
                }
                CbState::HalfOpen => true,
            }
        } else {
            true
        }
    }

    #[allow(dead_code)]
    pub fn on_success(&self) {
        self.consecutive_failures
            .store(0, std::sync::atomic::Ordering::Relaxed);
        if let Ok(mut st) = self.state.lock() {
            *st = CbState::Closed;
        }
    }

    #[allow(dead_code)]
    pub fn on_failure(&self) {
        let c = self
            .consecutive_failures
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1;
        if c >= self.failure_threshold {
            if let Ok(mut st) = self.state.lock() {
                *st = CbState::Open {
                    until: std::time::Instant::now() + self.timeout,
                };
            }
        }
    }
}

/// Retry envuelve al Circuit Breaker (composición correcta). Solo reintenta Transient.
#[allow(dead_code)]
pub async fn with_retry_cb<F, Fut, T>(cb: &CircuitBreaker, op: F) -> Result<T, ClassifiedError>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<T, ClassifiedError>>,
{
    use backoff::{backoff::Backoff, ExponentialBackoffBuilder};
    let mut bo = ExponentialBackoffBuilder::new()
        .with_max_elapsed_time(Some(Duration::from_secs(30)))
        .build();
    loop {
        if !cb.allow() {
            return Err(ClassifiedError {
                kind: ErrorKind::Transient,
                message: "circuit open".to_string(),
            });
        }
        match op().await {
            Ok(v) => {
                cb.on_success();
                return Ok(v);
            }
            Err(e) => {
                cb.on_failure();
                if e.kind != ErrorKind::Transient {
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
