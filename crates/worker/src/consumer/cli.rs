//! `--inspect-pel`: inspeccion de la PEL para el operador (S1-P3-04).
//!
//! Salida JSON por stdout con el resumen (total y por consumidor) y el
//! detalle (entrada por mensaje con consumidor, antiguedad e intentos).
//! Una PEL vacia (stream o grupo inexistente) no es un error: imprime ceros.

use std::io;

use serde_json::json;

use super::config::ConsumerConfig;
use super::group::{self, is_nogroup};
use redis::aio::MultiplexedConnection;

pub async fn inspect_pel() -> io::Result<()> {
    let cfg = ConsumerConfig::from_env();
    let client =
        redis::Client::open(cfg.redis_url.as_str()).map_err(|e| io::Error::other(e.to_string()))?;
    let mut conn: MultiplexedConnection = client
        .get_multiplexed_async_connection()
        .await
        .map_err(|e| io::Error::other(e.to_string()))?;

    let summary = match group::pending_summary(&mut conn, &cfg).await {
        Ok(v) => v,
        Err(e) if is_nogroup(&e) => empty_summary(&cfg),
        Err(e) => return Err(io::Error::other(e.to_string())),
    };
    let detailed = match group::pending_detailed(&mut conn, &cfg).await {
        Ok(v) => v,
        Err(e) if is_nogroup(&e) => json!({
            "stream": cfg.stream,
            "group": cfg.group,
            "count_sampled": 0,
            "entries": [],
        }),
        Err(e) => return Err(io::Error::other(e.to_string())),
    };

    println!(
        "{}",
        serde_json::to_string_pretty(&summary).unwrap_or_default()
    );
    println!(
        "{}",
        serde_json::to_string_pretty(&detailed).unwrap_or_default()
    );
    Ok(())
}

fn empty_summary(cfg: &ConsumerConfig) -> serde_json::Value {
    json!({
        "stream": cfg.stream,
        "group": cfg.group,
        "pending_count": 0,
        "start_id": serde_json::Value::Null,
        "end_id": serde_json::Value::Null,
        "consumers": {},
    })
}
