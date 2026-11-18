//! Restart-only handling of `min_idle_ms` (S11-P1-C21).

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

pub const LOG_MSG: &str = "min_idle_change_requires_restart";

#[derive(Debug)]
pub enum MinIdleOutcome {
    NoChange,
    LoggedRequiresRestart,
    AlreadyLogged,
}

#[derive(Debug)]
pub struct MinIdleRestartState {
    effective_ms: u64,
    pending_ms: AtomicU64,
    last_logged_ms: AtomicU64,
}

impl MinIdleRestartState {
    pub fn new(effective_ms: u64) -> Self {
        Self {
            effective_ms,
            pending_ms: AtomicU64::new(0),
            last_logged_ms: AtomicU64::new(0),
        }
    }

    pub fn effective_ms(&self) -> u64 {
        self.effective_ms
    }

    pub fn pending_ms(&self) -> Option<u64> {
        let v = self.pending_ms.load(Ordering::Relaxed);
        if v == 0 {
            None
        } else {
            Some(v)
        }
    }

    pub fn note(&self, requested_ms: u64) -> MinIdleOutcome {
        if requested_ms == self.effective_ms {
            self.pending_ms.store(0, Ordering::Relaxed);
            self.last_logged_ms.store(0, Ordering::Relaxed);
            return MinIdleOutcome::NoChange;
        }
        let current_pending = self.pending_ms.load(Ordering::Relaxed);
        if requested_ms == current_pending {
            return MinIdleOutcome::AlreadyLogged;
        }
        self.pending_ms.store(requested_ms, Ordering::Relaxed);
        self.last_logged_ms.store(requested_ms, Ordering::Relaxed);
        MinIdleOutcome::LoggedRequiresRestart
    }
}

pub fn coherence_violation(min_idle_ms: u64, job_timeout: Duration) -> bool {
    let threshold = job_timeout.as_millis() as u64 * 2;
    min_idle_ms < threshold
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sin_cambio_no_loguea() {
        let st = MinIdleRestartState::new(601_000);
        assert!(matches!(st.note(601_000), MinIdleOutcome::NoChange));
        assert_eq!(st.pending_ms(), None);
    }
    #[test]
    fn cambio_se_loguea_una_vez() {
        let st = MinIdleRestartState::new(601_000);
        assert!(matches!(
            st.note(900_000),
            MinIdleOutcome::LoggedRequiresRestart
        ));
        assert_eq!(st.pending_ms(), Some(900_000));
        assert_eq!(st.effective_ms(), 601_000);
    }
    #[test]
    fn mismo_valor_no_re_loguea() {
        let st = MinIdleRestartState::new(601_000);
        st.note(900_000);
        assert!(matches!(st.note(900_000), MinIdleOutcome::AlreadyLogged));
    }
    #[test]
    fn valor_distinto_re_loguea() {
        let st = MinIdleRestartState::new(601_000);
        st.note(900_000);
        assert!(matches!(
            st.note(950_000),
            MinIdleOutcome::LoggedRequiresRestart
        ));
    }
    #[test]
    fn volver_al_efectivo_limpia_pendiente() {
        let st = MinIdleRestartState::new(601_000);
        st.note(900_000);
        assert!(matches!(st.note(601_000), MinIdleOutcome::NoChange));
        assert_eq!(st.pending_ms(), None);
        assert_eq!(st.effective_ms(), 601_000);
    }
    #[test]
    fn coherencia_exige_2x_job_timeout() {
        use std::time::Duration;
        assert!(!coherence_violation(601_000, Duration::from_secs(300)));
        assert!(coherence_violation(599_999, Duration::from_secs(300)));
        assert!(coherence_violation(1_000, Duration::from_secs(300)));
    }
}
