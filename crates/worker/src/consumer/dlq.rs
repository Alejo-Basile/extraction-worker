//! DLQ atomica con causa e idempotencia (S2-P3-07).
//!
//! Un unico script Lua hace:
//! 1. `XPENDING` del id exacto: si el mensaje ya no esta pendiente (otro
//!    worker lo DLQ-eo o se ACK-eo), es un no-op -> reintentar nunca duplica.
//! 2. Lee el delivery count de esa misma respuesta (atomico, sin carrera).
//! 3. `XADD` a la DLQ con el contrato `DlqMessage` (10 campos) y `XACK`
//!    del original. Si el proceso muere a mitad del script, Redis no deja
//!    efectos parciales: o esta todo hecho o nada.
//!
//! El `delivery_count` no viaja como argumento: el script lo reemplaza por
//! el valor fresco de `XPENDING`.

use std::collections::HashMap;

use extraction_core::{error_code, DlqMessage, ErrorKind, WorkItem};
use redis::aio::MultiplexedConnection;
use redis::streams::StreamPendingCountReply;
use redis::AsyncCommands;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use super::classify::classify_redis_error;
use super::config::ConsumerConfig;
use super::resilience::Failure;

#[derive(thiserror::Error, Debug)]
pub enum DlqError {
    #[error("redis: {0}")]
    Redis(#[from] redis::RedisError),
    #[error("serialize: {0}")]
    Serialize(String),
}

impl From<DlqError> for Failure {
    fn from(e: DlqError) -> Self {
        match e {
            DlqError::Redis(re) => classify_redis_error(&re),
            DlqError::Serialize(s) => Failure::fatal("DLQ_SERIALIZE", s),
        }
    }
}

const DLQ_LUA: &str = r#"
local dlq = KEYS[1]
local stream = KEYS[2]
local group = ARGV[1]
local id = ARGV[2]
local pending = redis.call('XPENDING', stream, group, id, id, 1)
if type(pending) ~= 'table' or #pending == 0 then
  return 0
end
local delivery = tonumber(pending[1][4]) or 1
local args = {dlq, '*'}
local i = 3
while i < #ARGV do
  if ARGV[i] == 'delivery_count' then
    table.insert(args, ARGV[i])
    table.insert(args, tostring(delivery))
  else
    table.insert(args, ARGV[i])
    table.insert(args, ARGV[i + 1])
  end
  i = i + 2
end
redis.call('XADD', unpack(args))
redis.call('XACK', stream, group, id)
return delivery
"#;

/// Mueve `msg_id` a la DLQ con su causa y hace ACK, atomicamente.
///
/// Devuelve `Ok(true)` si el mensaje se movio, `Ok(false)` si ya no estaba
/// pendiente (operacion idempotente: nada se duplica).
pub async fn move_to_dlq_with_cause(
    conn: &mut MultiplexedConnection,
    cfg: &ConsumerConfig,
    msg_id: &str,
    item: &WorkItem,
    error_code: &str,
    error_message: &str,
    error_kind: ErrorKind,
) -> Result<bool, DlqError> {
    let failed_at = OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .map_err(|e| DlqError::Serialize(format!("failed_at rfc3339: {e}")))?;
    let dlq_msg = DlqMessage {
        document_id: item.document_id.clone(),
        object_key: item.object_key.clone(),
        correlation_id: item.correlation_id.clone(),
        enqueued_at: item.enqueued_at.clone(),
        schema_version: item.schema_version,
        // El script reemplaza este placeholder por el delivery count fresco
        // de XPENDING dentro de la misma operacion atomica.
        delivery_count: 0,
        failed_at,
        error_code: error_code.to_string(),
        error_message: error_message.to_string(),
        error_kind,
    };
    let value = serde_json::to_value(&dlq_msg)
        .map_err(|e| DlqError::Serialize(format!("dlq message: {e}")))?;
    let obj = match value.as_object() {
        Some(o) => o,
        None => {
            return Err(DlqError::Serialize(
                "dlq message no es un objeto".to_string(),
            ))
        }
    };

    let mut cmd = redis::cmd("EVAL");
    cmd.arg(DLQ_LUA)
        .arg(2)
        .arg(&cfg.dlq_stream)
        .arg(&cfg.stream)
        .arg(&cfg.group)
        .arg(msg_id);
    for (k, v) in obj {
        match v.as_str() {
            Some(s) => cmd.arg(k).arg(s),
            None => cmd.arg(k).arg(v.to_string()),
        };
    }

    let moved: i64 = cmd.query_async(conn).await?;
    Ok(moved > 0)
}

/// Delivery count (intentos) del mensaje `msg_id`, via `XPENDING` del id
/// exacto (S2-P3-07). Devuelve 1 si el mensaje no esta pendiente: no se
/// puede DLQ-ear algo que Redis ya no tiene pendiente.
pub async fn get_delivery_count_by_id(
    conn: &mut MultiplexedConnection,
    cfg: &ConsumerConfig,
    msg_id: &str,
) -> Result<u64, DlqError> {
    let reply: StreamPendingCountReply = conn
        .xpending_count(&cfg.stream, &cfg.group, msg_id, msg_id, 1)
        .await?;
    Ok(reply
        .ids
        .first()
        .map(|p| (p.times_delivered.max(1)) as u64)
        .unwrap_or(1))
}

/// Item best-effort para la DLQ cuando el mensaje no cumple el contrato:
/// se conservan los campos reconocidos tal cual llegaron.
pub fn best_effort_item(fields: &HashMap<String, String>) -> WorkItem {
    WorkItem {
        document_id: fields.get("document_id").cloned().unwrap_or_default(),
        object_key: fields.get("object_key").cloned().unwrap_or_default(),
        correlation_id: fields.get("correlation_id").cloned().unwrap_or_default(),
        enqueued_at: fields.get("enqueued_at").cloned().unwrap_or_default(),
        schema_version: fields
            .get("schema_version")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0),
    }
}

/// Mensaje de causa para los dos caminos de la DLQ en F2.
pub fn contract_cause(issue: &extraction_core::ContractError) -> (&'static str, ErrorKind) {
    match issue {
        extraction_core::ContractError::UnsupportedSchemaVersion(_) => {
            (error_code::UNSUPPORTED_SCHEMA_VERSION, ErrorKind::Business)
        }
        extraction_core::ContractError::Invalid(_) => {
            (error_code::INVALID_CONTRACT, ErrorKind::Business)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use extraction_core::ContractError;

    #[test]
    fn causa_de_contrato_invalido_es_business() {
        let (code, kind) = contract_cause(&ContractError::Invalid("campo desconocido".into()));
        assert_eq!(code, error_code::INVALID_CONTRACT);
        assert_eq!(kind, ErrorKind::Business);
    }

    #[test]
    fn causa_de_schema_no_soportada_es_business() {
        let (code, kind) = contract_cause(&ContractError::UnsupportedSchemaVersion(2));
        assert_eq!(code, error_code::UNSUPPORTED_SCHEMA_VERSION);
        assert_eq!(kind, ErrorKind::Business);
    }

    #[test]
    fn best_effort_item_conserva_lo_reconocible() {
        let mut fields = HashMap::new();
        fields.insert("document_id".to_string(), "doc-1".to_string());
        fields.insert("schema_version".to_string(), "9".to_string());
        fields.insert("basura".to_string(), "x".to_string());
        let item = best_effort_item(&fields);
        assert_eq!(item.document_id, "doc-1");
        assert_eq!(item.schema_version, 9);
        assert_eq!(item.object_key, "");
    }
}
