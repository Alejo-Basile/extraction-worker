//! Consumidor Redis Streams (F1/F2).
//!
//! Responsabilidades:
//! - S1-P3-01: deserializar `WorkItem` con validacion (errores van a DLQ).
//! - S1-P3-02: consumer group con nombre unico por instancia.
//! - S1-P3-03: cancelacion con `CancellationToken` para apagado < 6s.
//! - S1-P3-04: inspeccion de PEL.
//! - S1-P3-05: `XAUTOCLAIM` con `min-idle-time` > 2x `job_timeout`.
//! - S2-P3-07: DLQ atomica (XADD + XACK en Lua) y conteo via XPENDING.

pub mod classify;
pub mod cli;
pub mod config;
pub mod dlq;
pub mod group;
pub mod pipeline;
pub mod resilience;
pub mod runner;

pub use cli::inspect_pel as cli_inspect_pel;
