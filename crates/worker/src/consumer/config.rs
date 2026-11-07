use std::env;
use std::time::Duration;

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ConsumerConfig {
    pub redis_url: String,
    /// Sentinel (T-07): `REDIS_SENTINEL_ADDRS`, lista `host:port` separada
    /// por comas. Vacio => modo directo con `redis_url`.
    pub redis_sentinel_addrs: Vec<String>,
    /// Sentinel (T-07): `REDIS_MASTER_NAME` (p.ej. `myprimary`). Ausente o
    /// vacio => `None`. Debe ir junto con `redis_sentinel_addrs`.
    pub redis_master_name: Option<String>,
    /// Sentinel (T-10): `REDIS_SENTINEL_PASSWORD`, password para autenticar
    /// contra los Sentinels (opt-in). Ausente o vacio => `None`: no se manda
    /// `AUTH` a los Sentinels (este stack corre Sentinels sin `requirepass`).
    /// NO cae al password de `REDIS_URL`: ese es el del master.
    pub redis_sentinel_password: Option<String>,
    pub stream: String,
    pub group: String,
    pub consumer_name: String,
    pub block_ms: u64,
    pub count: usize,
    pub job_timeout: Duration,
    pub min_idle_ms: u64,
    pub xautoclaim_interval: Duration,
    pub max_attempts: usize,
    pub dlq_stream: String,
    /// Bulkhead por instancia (S2-P3-03): extracciones en paralelo.
    pub max_concurrency: usize,
    /// Limite duro del PDF descargado (defensa en profundidad, S2-P3-01).
    pub max_pdf_bytes: u64,
    /// S3/MinIO (seccion 11.4): GET raw-pdfs y PUT extracted-txt.
    pub s3_endpoint: String,
    pub s3_bucket_raw: String,
    pub s3_bucket_txt: String,
    pub s3_access_key: String,
    pub s3_secret_key: String,
    /// Mongo con rol acotado a `update` sobre `documents` (ADR-0006).
    pub mongo_uri: String,
    pub mongo_db: String,
    pub mongo_timeout: Duration,
    /// Tope de paginas por documento (defensa ante PDF hostil, §11.5).
    pub max_pages: u32,
    /// Tope del texto extraido antes del PUT (§11.5).
    pub max_output_bytes: u64,
    /// Periodo de refresco de los gauges de cola (DoD 3 / S0-P3-05).
    pub metrics_interval: Duration,
    /// Timeout de cada operacion S3 (S2-P3-06, configurable en tests).
    pub s3_op_timeout: Duration,
    /// Presupuesto para terminar jobs en vuelo durante el apagado
    /// (S4-P3-04). Debe dejar margen dentro de los 6s globales.
    pub shutdown_drain: Duration,
    /// Stream de auditoria de replays de la DLQ (S4-P3-01).
    pub dlq_replay_audit_stream: String,
    /// Hash de DLQ ids ya replayeados: evita reemitir dos veces.
    pub dlq_replayed_key: String,
    /// Antiguedad a partir de la cual un consumidor con pendientes se
    /// considera sin senal de vida (S6-P3-01).
    pub dead_consumer_ms: u64,
}

fn env_or(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_u64_or(key: &str, default: u64) -> u64 {
    env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_usize_or(key: &str, default: usize) -> usize {
    env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Lista CSV del entorno: parte por comas, recorta cada item y descarta los
/// vacios. Ausente o en blanco => vec vacio (modo directo).
fn env_csv(key: &str) -> Vec<String> {
    match env::var(key) {
        Ok(raw) => split_csv(&raw),
        Err(_) => Vec::new(),
    }
}

/// Option del entorno: ausente o en blanco (tras trim) => `None`.
fn env_opt(key: &str) -> Option<String> {
    env::var(key)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

/// Parte un CSV sin env: trim por item y items vacios descartados.
fn split_csv(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(str::to_owned)
        .collect()
}

/// Parsea una addr `host:port` de `REDIS_SENTINEL_ADDRS`. La addr no es
/// secreta: el mensaje la nombra, pero jamas incluye `redis_url` ni un
/// password.
pub(crate) fn parse_sentinel_addr(raw: &str) -> Result<(String, u16), String> {
    let invalid = || {
        format!("REDIS_SENTINEL_ADDRS: addr invalida \"{raw}\" (se espera host:port con puerto 1..=65535)")
    };
    let (host, port) = raw.rsplit_once(':').ok_or_else(invalid)?;
    if host.is_empty() {
        return Err(invalid());
    }
    let port: u16 = port.parse().map_err(|_| invalid())?;
    if port == 0 {
        return Err(invalid());
    }
    Ok((host.to_string(), port))
}

impl ConsumerConfig {
    pub fn from_env() -> Self {
        let host = hostname::get()
            .ok()
            .and_then(|h| h.into_string().ok())
            .unwrap_or_else(|| "unknown".to_string());
        let pid = std::process::id();
        let consumer_name = env_or("WORKER_CONSUMER_NAME", &format!("consumer-{host}-{pid}"));

        let job_timeout_secs = env_u64_or("WORKER_JOB_TIMEOUT_SECS", 300);
        let job_timeout = Duration::from_secs(job_timeout_secs);
        // S1-P3-05: por encima de 2x job_timeout
        let min_idle_ms = env_u64_or("WORKER_MIN_IDLE_MS", job_timeout_secs * 2000 + 1000);

        Self {
            redis_url: env_or("REDIS_URL", "redis://127.0.0.1:6379/"),
            redis_sentinel_addrs: env_csv("REDIS_SENTINEL_ADDRS"),
            redis_master_name: env_opt("REDIS_MASTER_NAME"),
            redis_sentinel_password: env_opt("REDIS_SENTINEL_PASSWORD"),
            stream: env_or("REDIS_STREAM", "stream:pdf-processing"),
            group: env_or("REDIS_GROUP", "extraction-workers"),
            consumer_name,
            block_ms: env_u64_or("WORKER_BLOCK_MS", 5000),
            count: env_usize_or("WORKER_BATCH_COUNT", 1),
            job_timeout,
            min_idle_ms,
            xautoclaim_interval: Duration::from_secs(env_u64_or(
                "WORKER_XAUTOCLAIM_INTERVAL_SECS",
                30,
            )),
            max_attempts: env_usize_or("WORKER_MAX_ATTEMPTS", 5),
            dlq_stream: env_or("REDIS_DLQ_STREAM", "stream:pdf-processing-dlq"),
            max_concurrency: env_usize_or("WORKER_MAX_CONCURRENCY", 4),
            max_pdf_bytes: env_u64_or("WORKER_MAX_PDF_BYTES", 30 * 1024 * 1024),
            s3_endpoint: env_or("S3_ENDPOINT", "http://127.0.0.1:9000"),
            s3_bucket_raw: env_or("S3_BUCKET_RAW", "raw-pdfs"),
            s3_bucket_txt: env_or("S3_BUCKET_TXT", "extracted-txt"),
            s3_access_key: env_or("S3_ACCESS_KEY", ""),
            s3_secret_key: env_or("S3_SECRET_KEY", ""),
            mongo_uri: env_or(
                "MONGO_WORKER_URI",
                "mongodb://worker:worker-dev-secret@127.0.0.1:27017/app?authSource=app&directConnection=true",
            ),
            mongo_db: env_or("MONGO_DB", "app"),
            mongo_timeout: Duration::from_millis(env_u64_or("MONGO_TIMEOUT_MS", 3000)),
            max_pages: env_u64_or("WORKER_MAX_PAGES", 2000) as u32,
            max_output_bytes: env_u64_or("WORKER_MAX_OUTPUT_BYTES", 52_428_800),
            metrics_interval: Duration::from_secs(
                env_u64_or("WORKER_METRICS_INTERVAL_SECS", 15).max(1),
            ),
            s3_op_timeout: Duration::from_millis(
                env_u64_or(
                    "S3_OP_TIMEOUT_MS",
                    crate::objects::DEFAULT_S3_OP_TIMEOUT_MS,
                )
                .max(1),
            ),
            shutdown_drain: Duration::from_secs(
                env_u64_or("WORKER_SHUTDOWN_DRAIN_SECS", 4).min(5),
            ),
            dlq_replay_audit_stream: env_or(
                "REDIS_DLQ_REPLAY_AUDIT_STREAM",
                "stream:pdf-processing-dlq-replay",
            ),
            dlq_replayed_key: env_or(
                "REDIS_DLQ_REPLAYED_KEY",
                "stream:pdf-processing-dlq-replayed",
            ),
            dead_consumer_ms: env_u64_or("WORKER_DEAD_CONSUMER_MS", min_idle_ms),
        }
    }
}

impl ConsumerConfig {
    /// Chequeo cruzado de Sentinel (T-07): o estan ambas claves o ninguna.
    /// El mensaje jamas incluye `redis_url` ni un password: solo nombres de
    /// variables y addrs (no secretas).
    pub fn validate(&self) -> Result<(), String> {
        if self.redis_sentinel_addrs.is_empty() && self.redis_master_name.is_some() {
            return Err("REDIS_MASTER_NAME requiere REDIS_SENTINEL_ADDRS".into());
        }
        if !self.redis_sentinel_addrs.is_empty() && self.redis_master_name.is_none() {
            return Err("REDIS_SENTINEL_ADDRS requiere REDIS_MASTER_NAME".into());
        }
        // T-10: la auth de Sentinel es opt-in y solo tiene sentido en modo
        // Sentinel. Un password suelto se rechaza en vez de ignorarse.
        if self.redis_sentinel_password.is_some() && self.redis_sentinel_addrs.is_empty() {
            return Err("REDIS_SENTINEL_PASSWORD requiere REDIS_SENTINEL_ADDRS".into());
        }
        for addr in &self.redis_sentinel_addrs {
            parse_sentinel_addr(addr)?;
        }
        Ok(())
    }

    pub fn mongo_config(&self) -> crate::document_store::MongoConfig {
        crate::document_store::MongoConfig {
            uri: self.mongo_uri.clone(),
            db: self.mongo_db.clone(),
            timeout: self.mongo_timeout,
        }
    }
}

#[cfg(test)]
pub(crate) mod test_env {
    use std::sync::{Mutex, MutexGuard};

    /// `std::env` es proceso-compartido y los tests unitarios corren en
    /// paralelo: cualquier prueba que toca variables de entorno debe
    /// serializarse con este candado compartido entre modulos.
    pub(crate) fn lock() -> MutexGuard<'static, ()> {
        static LOCK: Mutex<()> = Mutex::new(());
        LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::test_env::lock as env_lock;
    use super::*;

    /// Configuracion base sin claves sentinel en el entorno.
    fn config_with(addrs: &[&str], master: Option<&str>) -> ConsumerConfig {
        let _guard = env_lock();
        env::remove_var("REDIS_SENTINEL_ADDRS");
        env::remove_var("REDIS_MASTER_NAME");
        env::remove_var("REDIS_SENTINEL_PASSWORD");
        let mut cfg = ConsumerConfig::from_env();
        cfg.redis_sentinel_addrs = addrs.iter().map(|a| (*a).to_string()).collect();
        cfg.redis_master_name = master.map(str::to_owned);
        cfg
    }

    #[test]
    fn validate_con_addrs_sin_master_name_falla() {
        let cfg = config_with(&["sentinel-1:26379"], None);
        let err = match cfg.validate() {
            Err(e) => e,
            Ok(()) => panic!("addrs sin REDIS_MASTER_NAME debe fallar"),
        };
        assert_eq!(err, "REDIS_SENTINEL_ADDRS requiere REDIS_MASTER_NAME");
    }

    #[test]
    fn validate_con_master_name_sin_addrs_falla() {
        let cfg = config_with(&[], Some("myprimary"));
        let err = match cfg.validate() {
            Err(e) => e,
            Ok(()) => panic!("master name sin REDIS_SENTINEL_ADDRS debe fallar"),
        };
        assert_eq!(err, "REDIS_MASTER_NAME requiere REDIS_SENTINEL_ADDRS");
    }

    #[test]
    fn validate_con_addr_malformada_falla_nombrando_la_addr() {
        for bad in ["host", "host:0", "host:65536", "host:abc"] {
            let cfg = config_with(&[bad], Some("myprimary"));
            let err = match cfg.validate() {
                Err(e) => e,
                Ok(()) => panic!("addr \"{bad}\" debe ser invalida"),
            };
            assert!(
                err.contains(bad),
                "el error debe nombrar la addr \"{bad}\": {err}"
            );
        }
    }

    #[test]
    fn validate_vacio_es_modo_directo() {
        let cfg = config_with(&[], None);
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn validate_sentinel_completo_es_ok() {
        let cfg = config_with(&["s1:26379", "s2:26379"], Some("myprimary"));
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn from_env_sin_claves_da_modo_directo() {
        let _guard = env_lock();
        env::remove_var("REDIS_SENTINEL_ADDRS");
        env::remove_var("REDIS_MASTER_NAME");
        let cfg = ConsumerConfig::from_env();
        assert!(cfg.redis_sentinel_addrs.is_empty());
        assert!(cfg.redis_master_name.is_none());
        assert!(cfg.validate().is_ok());

        // Claves presentes pero vacias tambien son modo directo.
        env::set_var("REDIS_SENTINEL_ADDRS", "");
        env::set_var("REDIS_MASTER_NAME", "");
        let cfg = ConsumerConfig::from_env();
        assert!(cfg.redis_sentinel_addrs.is_empty());
        assert!(cfg.redis_master_name.is_none());
    }

    #[test]
    fn from_env_parsea_addrs_separadas_por_coma() {
        let _guard = env_lock();
        env::set_var("REDIS_SENTINEL_ADDRS", "s1:26379,s2:26379");
        env::set_var("REDIS_MASTER_NAME", "myprimary");
        let cfg = ConsumerConfig::from_env();
        assert_eq!(cfg.redis_sentinel_addrs, vec!["s1:26379", "s2:26379"]);
        assert_eq!(cfg.redis_master_name.as_deref(), Some("myprimary"));
        assert!(cfg.validate().is_ok());
        env::remove_var("REDIS_SENTINEL_ADDRS");
        env::remove_var("REDIS_MASTER_NAME");
    }

    #[test]
    fn from_env_recorta_espacios_y_descarta_items_vacios() {
        let _guard = env_lock();
        env::set_var("REDIS_SENTINEL_ADDRS", " s1:26379 , ,s2:26379,");
        env::set_var("REDIS_MASTER_NAME", " myprimary ");
        let cfg = ConsumerConfig::from_env();
        assert_eq!(cfg.redis_sentinel_addrs, vec!["s1:26379", "s2:26379"]);
        assert_eq!(cfg.redis_master_name.as_deref(), Some("myprimary"));
        env::remove_var("REDIS_SENTINEL_ADDRS");
        env::remove_var("REDIS_MASTER_NAME");
    }

    // --- T-10: auth de Sentinel opt-in (REDIS_SENTINEL_PASSWORD) ---

    #[test]
    fn from_env_sin_sentinel_password_no_configura_auth_de_sentinel() {
        let _guard = env_lock();
        env::remove_var("REDIS_SENTINEL_PASSWORD");
        let cfg = ConsumerConfig::from_env();
        assert!(
            cfg.redis_sentinel_password.is_none(),
            "sin REDIS_SENTINEL_PASSWORD no debe haber auth hacia Sentinels"
        );
    }

    #[test]
    fn from_env_parsea_sentinel_password() {
        let _guard = env_lock();
        env::set_var("REDIS_SENTINEL_PASSWORD", " sentinel-secret ");
        let cfg = ConsumerConfig::from_env();
        assert_eq!(
            cfg.redis_sentinel_password.as_deref(),
            Some("sentinel-secret")
        );
        env::remove_var("REDIS_SENTINEL_PASSWORD");
    }

    #[test]
    fn validate_sentinel_password_sin_addrs_falla() {
        let mut cfg = config_with(&[], None);
        cfg.redis_sentinel_password = Some("sentinel-secret".to_string());
        let err = match cfg.validate() {
            Err(e) => e,
            Ok(()) => panic!("REDIS_SENTINEL_PASSWORD sin addrs debe fallar"),
        };
        assert_eq!(err, "REDIS_SENTINEL_PASSWORD requiere REDIS_SENTINEL_ADDRS");
    }

    #[test]
    fn validate_sentinel_password_con_sentinel_es_ok() {
        let mut cfg = config_with(&["s1:26379", "s2:26379"], Some("myprimary"));
        cfg.redis_sentinel_password = Some("sentinel-secret".to_string());
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn validate_error_no_expone_credenciales() {
        let _guard = env_lock();
        env::set_var("REDIS_URL", "redis://:secreto123@127.0.0.1:6379/");
        let mut cfg = ConsumerConfig::from_env();
        // Config invalida: addrs sin master name; la URL lleva el password.
        cfg.redis_sentinel_addrs = vec!["sentinel-1:26379".to_string()];
        cfg.redis_master_name = None;
        let err = match cfg.validate() {
            Err(e) => e,
            Ok(()) => panic!("addrs sin master name debe fallar"),
        };
        assert!(
            !err.contains("secreto123"),
            "el error no puede contener el password: {err}"
        );
        assert!(
            !err.contains("redis://"),
            "el error no puede contener la URL: {err}"
        );
        env::remove_var("REDIS_URL");
    }
}
