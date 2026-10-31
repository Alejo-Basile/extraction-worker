//! Consumer group e inspeccion de la PEL (S1-P3-02/04).

use redis::aio::MultiplexedConnection;
use redis::streams::{
    StreamInfoConsumer, StreamInfoConsumersReply, StreamPendingCountReply, StreamPendingReply,
    StreamRangeReply,
};
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

/// Detalle de la PEL: entrada por mensaje con consumidor, antiguedad,
/// delivery count y los identificadores del documento (S1-P3-04, S4-P3-02).
/// Muestra hasta 100 entradas.
pub async fn pending_detailed(
    conn: &mut MultiplexedConnection,
    cfg: &ConsumerConfig,
) -> Result<Value, GroupError> {
    let reply: StreamPendingCountReply = conn
        .xpending_count(&cfg.stream, &cfg.group, "-", "+", 100)
        .await?;
    let mut entries = Vec::with_capacity(reply.ids.len());
    for p in &reply.ids {
        let (document_id, correlation_id) = message_identity(conn, &cfg.stream, &p.id).await?;
        entries.push(json!({
            "id": p.id,
            "consumer": p.consumer,
            "idle_ms": p.last_delivered_ms,
            "delivery_count": p.times_delivered,
            "document_id": document_id,
            "correlation_id": correlation_id,
        }));
    }
    Ok(json!({
        "stream": cfg.stream,
        "group": cfg.group,
        "count_sampled": entries.len(),
        "entries": entries,
    }))
}

/// Consumidores con mensajes pendientes y sin senal de vida (S6-P3-01).
/// Un consumidor vivo que esta procesando un job no se marca: el umbral es
/// `WORKER_DEAD_CONSUMER_MS`, por defecto `min_idle_ms` (> 2x job_timeout).
pub async fn dead_consumers(
    conn: &mut MultiplexedConnection,
    cfg: &ConsumerConfig,
) -> Result<Vec<StreamInfoConsumer>, GroupError> {
    let reply: StreamInfoConsumersReply = conn.xinfo_consumers(&cfg.stream, &cfg.group).await?;
    Ok(reply
        .consumers
        .into_iter()
        .filter(|c| c.pending > 0 && c.idle as u64 >= cfg.dead_consumer_ms)
        .collect())
}

async fn message_identity(
    conn: &mut MultiplexedConnection,
    stream: &str,
    id: &str,
) -> Result<(Value, Value), GroupError> {
    let reply: StreamRangeReply = conn.xrange_count(stream, id, id, 1).await?;
    let field = |name: &str| -> Value {
        reply
            .ids
            .first()
            .and_then(|entry| entry.map.get(name))
            .and_then(|v| redis::from_redis_value::<String>(v.clone()).ok())
            .map(Value::String)
            .unwrap_or(Value::Null)
    };
    Ok((field("document_id"), field("correlation_id")))
}
