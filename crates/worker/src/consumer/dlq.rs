//! DLQ atómica (S2-P3-07).
//!
//! `XACK` + `XADD` a la DLQ debe ser atómico para evitar duplicados o pérdida.
//! Se usa un script Lua para ejecutar ambas operaciones en una sola llamada.

use redis::aio::MultiplexedConnection;

use super::config::ConsumerConfig;

#[derive(thiserror::Error, Debug)]
pub enum DlqError {
    #[error("redis: {0}")]
    Redis(#[from] redis::RedisError),
}

/// Mueve un mensaje a DLQ y hace ACK atómicamente via script Lua.
#[allow(dead_code)]
pub async fn move_to_dlq_atomic(
    conn: &mut MultiplexedConnection,
    cfg: &ConsumerConfig,
    msg_id: &str,
    fields: &[(String, String)],
) -> Result<(), DlqError> {
    let mut cmd = redis::cmd("EVAL");
    cmd.arg(
        r#"
        local dlq = KEYS[1]
        local stream = KEYS[2]
        local group = ARGV[1]
        local id = ARGV[2]
        -- XADD dlq con todos los campos a partir de ARGV[3...]
        local xadd_args = {dlq, '*'}
        for i=3, #ARGV do
            table.insert(xadd_args, ARGV[i])
        end
        redis.call('XADD', unpack(xadd_args))
        return redis.call('XACK', stream, group, id)
    "#,
    )
    .arg(2)
    .arg(&cfg.dlq_stream)
    .arg(&cfg.stream)
    .arg(&cfg.group)
    .arg(msg_id);

    for (k, v) in fields {
        cmd.arg(k).arg(v);
    }

    let _: redis::Value = cmd.query_async(conn).await?;
    Ok(())
}

/// Obtener delivery count (intentos) via XPENDING
#[allow(dead_code)]
pub async fn get_delivery_count(
    conn: &mut MultiplexedConnection,
    cfg: &ConsumerConfig,
) -> Result<u64, DlqError> {
    let res: redis::Value = redis::cmd("XPENDING")
        .arg(&cfg.stream)
        .arg(&cfg.group)
        .arg("-")
        .arg("+")
        .arg(1)
        .arg(&cfg.consumer_name)
        .query_async(conn)
        .await?;

    if let redis::Value::Array(arr) = res {
        if !arr.is_empty() {
            if let redis::Value::Array(parts) = &arr[0] {
                if parts.len() >= 4 {
                    if let redis::Value::Int(n) = parts[3] {
                        return Ok(n as u64);
                    }
                    if let redis::Value::BulkString(b) = &parts[3] {
                        if let Ok(s) = String::from_utf8(b.clone()) {
                            if let Ok(n) = s.parse::<u64>() {
                                return Ok(n);
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(1)
}
