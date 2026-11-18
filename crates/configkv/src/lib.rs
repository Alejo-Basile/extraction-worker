//! Shared Consul KV client crate for extraction workers.
pub mod cache;
pub mod client;
pub mod error;
pub mod metrics;
pub mod schema;
pub mod validate;
pub mod watch;

pub use cache::Cache;
pub use client::{Client, ClientSettings, KvEntry, KvPage};
pub use error::ConfigKvError;
pub use validate::{
    from_consul_entries, validate_str, validate_values, RejectReason, Rejection, ValidatedWorkerKv,
};
pub use watch::Watch;
