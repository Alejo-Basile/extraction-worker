//! `--inspect-pel`: inspeccion de la PEL para el operador (S1-P3-04).
//!
//! Salida JSON por stdout con el resumen (total y por consumidor) y el
//! detalle (entrada por mensaje con consumidor, antiguedad e intentos).
//! Una PEL vacia (stream o grupo inexistente) no es un error: imprime ceros.

use std::io;

use serde_json::json;

use super::config::ConsumerConfig;
use super::dlq;
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

/// `--inspect-dlq`: volcado JSON de la DLQ con profundidad, antiguedad de la
/// entrada mas vieja y los campos de cada mensaje (S4-P3-02). Una DLQ vacia
/// o inexistente no es un error.
pub async fn inspect_dlq(args: &[String]) -> io::Result<()> {
    let cfg = ConsumerConfig::from_env();
    let limit = inspect_limit(args)?;
    let client =
        redis::Client::open(cfg.redis_url.as_str()).map_err(|e| io::Error::other(e.to_string()))?;
    let mut conn: MultiplexedConnection = client
        .get_multiplexed_async_connection()
        .await
        .map_err(|e| io::Error::other(e.to_string()))?;

    let entries = dlq::read_entries(&mut conn, &cfg, limit)
        .await
        .map_err(|e| io::Error::other(e.to_string()))?;
    let stats = dlq::stats(&mut conn, &cfg)
        .await
        .map_err(|e| io::Error::other(e.to_string()))?;
    let entries: Vec<serde_json::Value> = entries
        .into_iter()
        .map(|entry| {
            let mut obj = serde_json::Map::new();
            obj.insert("id".to_string(), json!(entry.id));
            for (k, v) in entry.fields {
                obj.insert(k, json!(v));
            }
            serde_json::Value::Object(obj)
        })
        .collect();

    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "stream": cfg.dlq_stream,
            "depth": stats.depth,
            "oldest_age_secs": stats.oldest_age_secs,
            "count_sampled": entries.len(),
            "entries": entries,
        }))
        .unwrap_or_default()
    );
    Ok(())
}

fn inspect_limit(args: &[String]) -> io::Result<usize> {
    let mut limit = 100usize;
    let mut iter = args.iter().peekable();
    while let Some(arg) = iter.next() {
        let raw = if let Some(v) = arg.strip_prefix("--limit=") {
            Some(v.to_string())
        } else if arg == "--limit" {
            iter.next().cloned()
        } else {
            None
        };
        if let Some(raw) = raw {
            limit = raw
                .parse::<usize>()
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "limit invalido"))?
                .clamp(1, 1000);
        }
    }
    Ok(limit)
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
