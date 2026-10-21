use std::env;
use std::time::Duration;

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ConsumerConfig {
    pub redis_url: String,
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
        }
    }
}
