use crate::error::ConfigKvError;
use crate::validate::ValidatedWorkerKv;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;

#[derive(Serialize, Deserialize)]
struct CachedConfig {
    last_index: u64,
    saved_at_unix: u64,
    values: std::collections::BTreeMap<String, serde_json::Value>,
}

pub struct Cache {
    path: PathBuf,
}
impl Cache {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }
    pub fn default_worker_path() -> PathBuf {
        std::path::PathBuf::from("/var/lib/worker/config-cache.json")
    }
    pub fn load(&self) -> Result<Option<(ValidatedWorkerKv, u64)>, ConfigKvError> {
        let data = match fs::read_to_string(&self.path) {
            Ok(d) => d,
            Err(_) => return Ok(None),
        };
        let c: CachedConfig = match serde_json::from_str(&data) {
            Ok(c) => c,
            Err(_) => return Ok(None),
        };
        // rebuild map values
        let mut map = serde_json::Map::new();
        for (k, v) in c.values {
            map.insert(k, v);
        }
        match crate::validate::validate_values(&map) {
            Ok(v) => Ok(Some((v, c.last_index))),
            Err(_) => Ok(None),
        }
    }
    pub fn save_atomic(
        &self,
        cfg: &ValidatedWorkerKv,
        last_index: u64,
    ) -> Result<(), ConfigKvError> {
        if let Some(parent) = self.path.parent() {
            let _ = fs::create_dir_all(parent);
        }
        let tmp = self.path.with_extension("tmp");
        // build values map from cfg
        let mut m = serde_json::Map::new();
        m.insert(
            "bulkhead_max_concurrency".into(),
            serde_json::json!(cfg.bulkhead_max_concurrency),
        );
        m.insert(
            "retry_max_elapsed_seconds".into(),
            serde_json::json!(cfg.retry_max_elapsed_seconds),
        );
        m.insert(
            "retry_initial_interval_ms".into(),
            serde_json::json!(cfg.retry_initial_interval_ms),
        );
        m.insert(
            "retry_multiplier".into(),
            serde_json::json!(cfg.retry_multiplier),
        );
        m.insert(
            "retry_max_interval_ms".into(),
            serde_json::json!(cfg.retry_max_interval_ms),
        );
        m.insert(
            "retry_randomization_factor".into(),
            serde_json::json!(cfg.retry_randomization_factor),
        );
        m.insert(
            "breaker_failure_threshold".into(),
            serde_json::json!(cfg.breaker_failure_threshold),
        );
        m.insert(
            "breaker_timeout_seconds".into(),
            serde_json::json!(cfg.breaker_timeout_seconds),
        );
        m.insert(
            "job_timeout_seconds".into(),
            serde_json::json!(cfg.job_timeout_seconds),
        );
        m.insert("max_pages".into(), serde_json::json!(cfg.max_pages));
        m.insert("min_idle_ms".into(), serde_json::json!(cfg.min_idle_ms));
        let c = CachedConfig {
            last_index,
            saved_at_unix: 0,
            values: m.into_iter().collect(),
        };
        let json =
            serde_json::to_string_pretty(&c).map_err(|e| ConfigKvError::CacheIo(e.to_string()))?;
        fs::write(&tmp, json).map_err(|e| ConfigKvError::CacheIo(e.to_string()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let perms = fs::Permissions::from_mode(0o600);
            let _ = fs::set_permissions(&tmp, perms);
        }
        fs::rename(&tmp, &self.path).map_err(|e| ConfigKvError::CacheIo(e.to_string()))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use crate::validate::ValidatedWorkerKv;
    use tempfile::tempdir;
    #[test]
    fn roundtrip() {
        let d = tempdir().unwrap();
        let p = d.path().join("c.json");
        let c = Cache::new(p.clone());
        let cfg = ValidatedWorkerKv::default();
        c.save_atomic(&cfg, 42).unwrap();
        let (loaded, idx) = c.load().unwrap().unwrap();
        assert_eq!(idx, 42);
        assert_eq!(loaded.max_pages, cfg.max_pages);
    }
    #[test]
    fn corrupt() {
        let d = tempdir().unwrap();
        let p = d.path().join("c.json");
        std::fs::write(&p, "{bad").unwrap();
        let c = Cache::new(p);
        assert!(c.load().unwrap().is_none());
    }
    #[test]
    fn missing_parent_created() {
        let d = tempdir().unwrap();
        let p = d.path().join("sub").join("c.json");
        let c = Cache::new(p.clone());
        c.save_atomic(&ValidatedWorkerKv::default(), 1).unwrap();
        assert!(p.exists());
    }
}
