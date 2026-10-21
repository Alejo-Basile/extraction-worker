use redis::{aio::MultiplexedConnection, RedisResult};
use serde_json::{json, Value};
use std::collections::HashMap;

use super::config::ConsumerConfig;

#[derive(thiserror::Error, Debug)]
pub enum GroupError {
    #[error("redis: {0}")]
    Redis(#[from] redis::RedisError),
}

pub async fn ensure_group(
    conn: &mut MultiplexedConnection,
    cfg: &ConsumerConfig,
) -> Result<(), GroupError> {
    // S1-P3-02: XGROUP CREATE con MKSTREAM
    let res: RedisResult<()> = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg(&cfg.stream)
        .arg(&cfg.group)
        .arg("0")
        .arg("MKSTREAM")
        .query_async(conn)
        .await;
    match res {
        Ok(_) => Ok(()),
        Err(e) => {
            let msg = e.to_string();
            if msg.contains("BUSYGROUP") {
                Ok(())
            } else {
                Err(GroupError::Redis(e))
            }
        }
    }
}

/// S1-P3-04: Inspeccion de PEL
/// XPENDING stream group [start end count [consumer]]
#[allow(dead_code)]
pub async fn pending_summary(
    conn: &mut MultiplexedConnection,
    cfg: &ConsumerConfig,
) -> Result<Value, GroupError> {
    // XPENDING sin rango detallado -> resumen general
    let res: redis::Value = redis::cmd("XPENDING")
        .arg(&cfg.stream)
        .arg(&cfg.group)
        .query_async(conn)
        .await?;
    // Intentamos extraer info básica si viene como array
    Ok(json!({"raw": format!("{:?}", res)}))
}

/// XPENDING con detalle por consumidor y antigüedad
#[allow(dead_code)]
pub async fn pending_detailed(
    conn: &mut MultiplexedConnection,
    cfg: &ConsumerConfig,
) -> Result<Value, GroupError> {
    // XPENDING stream group - + 100
    let res: redis::Value = redis::cmd("XPENDING")
        .arg(&cfg.stream)
        .arg(&cfg.group)
        .arg("-")
        .arg("+")
        .arg(100)
        .query_async(conn)
        .await?;
    let mut consumers: HashMap<String, i32> = HashMap::new();
    if let redis::Value::Array(arr) = &res {
        for item in arr.iter() {
            if let redis::Value::Array(parts) = item {
                if parts.len() >= 4 {
                    if let redis::Value::BulkString(b) = &parts[3] {
                        if let Ok(c) = String::from_utf8(b.clone()) {
                            *consumers.entry(c).or_insert(0) += 1;
                        }
                    }
                }
            }
        }
    }
    Ok(json!({
        "stream": cfg.stream,
        "group": cfg.group,
        "count_sampled": consumers.values().sum::<i32>(),
        "consumers": consumers
    }))
}
