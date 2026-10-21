//! Consumidor Redis Streams (F1-P3).
//!
//! Responsabilidades:
//! - S1-P3-01: deserializar `WorkItem` con validacion (errores van a DLQ).
//! - S1-P3-02: consumer group con nombre unico por instancia.
//! - S1-P3-03: cancelacion con `CancellationToken` para apagado < 6s.
//! - S1-P3-04: inspeccion de PEL.
//! - S1-P3-05: `XAUTOCLAIM` con `min-idle-time` > 2x `job_timeout`.

pub mod config;
pub mod group;
pub mod runner;
