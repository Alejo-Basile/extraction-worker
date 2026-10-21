//! Consumer group e inspeccion de la PEL (S1-P3-02/04).

use redis::aio::MultiplexedConnection;
use redis::streams::{StreamPendingCountReply, StreamPendingReply};
use redis::AsyncCommands;
use serde_json::{json, Value};

use super::config::ConsumerConfig;

#[derive(thiserror::Error, Debug)]
pub enum GroupError {
    #[error("redis: {0}")]
    Redis(#[from] redis::RedisError),
}

/// true si el error es `NOGROUP`: el stream o el grupo todavia no existen.
/// Util para diagnosticos: inspeccionar una PEL vacia no debe fallar.
pub fn is_nogroup(e: &GroupError) -> bool {
    match e {
        GroupError::Redis(r) => r.to_string().contains("NOGROUP"),
    }
}

/// Crea el grupo con `XGROUP CREATE ... MKSTREAM` si no existe (S1-P3-02).
pub async fn ensure_group(
    conn: &mut MultiplexedConnection,
    cfg: &ConsumerConfig,
) -> Result<(), GroupError> {
    let res: redis::RedisResult<()> = redis::cmd("XGROUP")
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
            // BUSYGROUP: el grupo ya existe, es el camino feliz en reinicios.
            if e.to_string().contains("BUSYGROUP") {
                Ok(())
            } else {
                Err(GroupError::Redis(e))
            }
        }
    }
}

/// Resumen de la PEL: total pendiente y por consumidor (S1-P3-04).
pub async fn pending_summary(
    conn: &mut MultiplexedConnection,
    cfg: &ConsumerConfig,
) -> Result<Value, GroupError> {
    let reply: StreamPendingReply = conn.xpending(&cfg.stream, &cfg.group).await?;
    Ok(match reply {
        StreamPendingReply::Empty => json!({
            "stream": cfg.stream,
            "group": cfg.group,
            "pending_count": 0,
            "start_id": Value::Null,
            "end_id": Value::Null,
            "consumers": {},
        }),
        StreamPendingReply::Data(d) => {
            let consumers: serde_json::Map<String, Value> = d
                .consumers
                .iter()
                .map(|c| {
                    (
                        c.name.clone(),
                        json!({"pending": c.pending, "idle_ms": c.idle}),
                    )
                })
                .collect();
            json!({
                "stream": cfg.stream,
                "group": cfg.group,
                "pending_count": d.count,
                "start_id": d.start_id,
                "end_id": d.end_id,
                "consumers": consumers,
            })
        }
        // El tipo es #[non_exhaustive]: variantes nuevas se tratan como vacio.
        _ => json!({
            "stream": cfg.stream,
            "group": cfg.group,
            "pending_count": 0,
            "start_id": Value::Null,
            "end_id": Value::Null,
            "consumers": {},
        }),
    })
}

/// Detalle de la PEL: entrada por mensaje con consumidor, antiguedad y
/// delivery count (S1-P3-04). Muestra hasta 100 entradas.
pub async fn pending_detailed(
    conn: &mut MultiplexedConnection,
    cfg: &ConsumerConfig,
) -> Result<Value, GroupError> {
    let reply: StreamPendingCountReply = conn
        .xpending_count(&cfg.stream, &cfg.group, "-", "+", 100)
        .await?;
    let entries: Vec<Value> = reply
        .ids
        .iter()
        .map(|p| {
            json!({
                "id": p.id,
                "consumer": p.consumer,
                "idle_ms": p.last_delivered_ms,
                "delivery_count": p.times_delivered,
            })
        })
        .collect();
    Ok(json!({
        "stream": cfg.stream,
        "group": cfg.group,
        "count_sampled": entries.len(),
        "entries": entries,
    }))
}
