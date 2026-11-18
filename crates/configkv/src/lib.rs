//! Shared Consul KV client crate for extraction workers.
pub mod cache;
pub mod client;
pub mod error;
pub mod metrics;
pub mod schema;
pub mod validate;
pub mod watch;

pub use cache::{Cache, OcrCache};
pub use client::{Client, ClientSettings, KvEntry, KvPage};
pub use error::ConfigKvError;
pub use validate::{
    from_consul_entries, from_consul_entries_ocr, validate_ocr_str, validate_ocr_values,
    validate_str, validate_values, RejectReason, Rejection, ValidatedOcrWorkerKv,
    ValidatedWorkerKv,
};
pub use watch::Watch;
