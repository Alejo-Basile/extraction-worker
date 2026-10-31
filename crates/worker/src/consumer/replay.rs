//! `--replay-dlq`: reemision controlada de mensajes de la DLQ (S4-P3-01).
//!
//! Garantias:
//! - Lua atomico: XADD al stream principal + XDEL de la DLQ + registro de
//!   idempotencia + auditoria, todo o nada.
//! - Un DLQ id ya replayeado no se reemite dos veces (hash `seen`).
//! - Antes de reemitir, el documento se mueve de `PROCESSING` a `RETRYING`
//!   (o se confirma que ya es reclamable). Los estados terminales no se
//!   tocan: quedan para triage manual.
//! - `--dry-run` no escribe nada.
//!
//! Si el replay Redis falla despues del reset de Mongo, la entrada sigue en
//! la DLQ y el documento queda `RETRYING`; repetir el comando lo recupera.

use std::collections::HashMap;
use std::env;
use std::io;

use redis::aio::MultiplexedConnection;
use serde_json::json;
use tracing::{info, warn};

use extraction_core::WorkItem;

use super::config::ConsumerConfig;
use super::dlq::{self, DlqEntry, DlqError};
use crate::document_store::DocumentStore;

const PAGE_SIZE: usize = 100;
const MAX_LIMIT: usize = 10_000;
const DEFAULT_LIMIT: usize = 100;

const REPLAY_LUA: &str = r#"
local main = KEYS[1]
local dlq = KEYS[2]
local seen = KEYS[3]
local audit = KEYS[4]
local dlq_id = ARGV[1]
local result = {0, ''}

if redis.call('HEXISTS', seen, dlq_id) == 1 then
  return result
end
if #redis.call('XRANGE', dlq, dlq_id, dlq_id) == 0 then
  result[1] = -1
  return result
end

local names = {'document_id', 'object_key', 'correlation_id', 'enqueued_at', 'schema_version'}
local args = {main, '*'}
for i = 1, #names do
  table.insert(args, names[i])
  table.insert(args, ARGV[i + 4])
end
local new_id = redis.call('XADD', unpack(args))
redis.call('XDEL', dlq, dlq_id)
redis.call('HSET', seen, dlq_id, new_id)
redis.call('XADD', audit, '*',
  'dlq_id', dlq_id,
  'new_id', new_id,
  'replayed_by', ARGV[2],
  'replayed_at', ARGV[3],
  'error_code', ARGV[4],
  'document_id', ARGV[5],
  'correlation_id', ARGV[7])
result[1] = 1
result[2] = new_id
return result
"#;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayOutcome {
    Replayed,
    AlreadyReplayed,
    Missing,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayOptions {
    pub document_id: Option<String>,
    pub error_code: Option<String>,
    pub limit: usize,
    pub dry_run: bool,
    pub all: bool,
    pub by: String,
    pub reset_state: bool,
}

impl ReplayOptions {
    pub fn from_args(args: &[String]) -> Result<Self, String> {
        let mut opts = Self {
            document_id: None,
            error_code: None,
            limit: DEFAULT_LIMIT,
            dry_run: false,
            all: false,
            by: default_operator(),
            reset_state: true,
        };
        let mut iter = args.iter().skip(1).peekable();
        while let Some(arg) = iter.next() {
            let (flag, inline) = match arg.split_once('=') {
                Some((f, v)) => (f, Some(v.to_string())),
                None => (arg.as_str(), None),
            };
            let mut value = |name: &str| -> Result<String, String> {
                inline
                    .clone()
                    .or_else(|| iter.next().cloned())
                    .ok_or_else(|| format!("{name} requiere un valor"))
            };
            match flag {
                "--replay-dlq" => {}
                "--dry-run" => opts.dry_run = true,
                "--all" => opts.all = true,
                "--no-reset-state" => opts.reset_state = false,
                "--document-id" => opts.document_id = Some(value("--document-id")?),
                "--error-code" => opts.error_code = Some(value("--error-code")?),
                "--by" => opts.by = value("--by")?,
                "--limit" => {
                    let raw = value("--limit")?;
                    let parsed = raw
                        .parse::<usize>()
                        .map_err(|_| format!("--limit invalido: {raw:?}"))?;
                    opts.limit = parsed.clamp(1, MAX_LIMIT);
                }
                other => return Err(format!("argumento desconocido: {other}")),
            }
        }
        if !opts.all && opts.document_id.is_none() && opts.error_code.is_none() {
            return Err("replay acotado: usa --document-id, --error-code o --all".to_string());
        }
        Ok(opts)
    }

    fn matches(&self, entry: &DlqEntry) -> bool {
        if let Some(id) = &self.document_id {
            if entry.fields.get("document_id") != Some(id) {
                return false;
            }
        }
        if let Some(code) = &self.error_code {
            if entry.fields.get("error_code") != Some(code) {
                return false;
            }
        }
        true
    }
}

pub async fn replay_dlq(args: &[String]) -> io::Result<()> {
    let opts = ReplayOptions::from_args(args)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    let cfg = ConsumerConfig::from_env();
    let client =
        redis::Client::open(cfg.redis_url.as_str()).map_err(|e| io::Error::other(e.to_string()))?;
    let mut conn: MultiplexedConnection = client
        .get_multiplexed_async_connection()
        .await
        .map_err(|e| io::Error::other(e.to_string()))?;

    let documents = if opts.reset_state && !opts.dry_run {
        Some(
            DocumentStore::connect(&cfg.mongo_config())
                .await
                .map_err(|e| io::Error::other(format!("Mongo no disponible: {e}")))?,
        )
    } else {
        None
    };

    let replayed_at = now_rfc3339();
    let mut scanned = 0usize;
    let mut processed = 0usize;
    let mut replayed = 0usize;
    let mut already = 0usize;
    let mut missing = 0usize;
    let mut skipped_state = 0usize;
    let mut skipped_contract = 0usize;
    let mut skipped_mongo = 0usize;
    let mut results = Vec::new();
    let mut start = "-".to_string();

    'outer: loop {
        let page = dlq::read_entries_from(&mut conn, &cfg, &start, PAGE_SIZE)
            .await
            .map_err(|e| io::Error::other(e.to_string()))?;
        if page.is_empty() {
            break;
        }
        let last = page.last().map(|e| e.id.clone()).unwrap_or_default();
        for entry in page {
            scanned += 1;
            if !opts.matches(&entry) {
                continue;
            }
            if processed >= opts.limit {
                break 'outer;
            }
            processed += 1;

            let item = match parse_work_item(&entry) {
                Ok(item) => item,
                Err(reason) => {
                    skipped_contract += 1;
                    results.push(json!({
                        "dlq_id": entry.id,
                        "result": "skipped",
                        "reason": reason,
                    }));
                    continue;
                }
            };

            if opts.dry_run {
                results.push(json!({
                    "dlq_id": entry.id,
                    "document_id": item.document_id,
                    "result": "would_replay",
                }));
                continue;
            }

            if let Some(documents) = &documents {
                match documents
                    .prepare_replay(&item.document_id, &item.correlation_id, &replayed_at)
                    .await
                {
                    Ok(true) => {}
                    Ok(false) => {
                        skipped_state += 1;
                        results.push(json!({
                            "dlq_id": entry.id,
                            "document_id": item.document_id,
                            "result": "skipped",
                            "reason": "estado_no_reclamable",
                        }));
                        continue;
                    }
                    Err(e) => {
                        skipped_mongo += 1;
                        warn!(
                            document_id = %item.document_id,
                            dlq_id = %entry.id,
                            error = %e,
                            "replay: no se pudo preparar el documento"
                        );
                        results.push(json!({
                            "dlq_id": entry.id,
                            "document_id": item.document_id,
                            "result": "skipped",
                            "reason": "mongo_error",
                        }));
                        continue;
                    }
                }
            }

            let error_code = entry.fields.get("error_code").cloned().unwrap_or_default();
            match replay_one(
                &mut conn,
                &cfg,
                &entry,
                &item,
                &opts.by,
                &replayed_at,
                &error_code,
            )
            .await
            {
                Ok((ReplayOutcome::Replayed, new_id)) => {
                    replayed += 1;
                    info!(
                        document_id = %item.document_id,
                        correlation_id = %item.correlation_id,
                        dlq_id = %entry.id,
                        new_id = %new_id.as_deref().unwrap_or_default(),
                        replayed_by = %opts.by,
                        "replay: mensaje reemitido"
                    );
                    results.push(json!({
                        "dlq_id": entry.id,
                        "new_id": new_id,
                        "document_id": item.document_id,
                        "result": "replayed",
                    }));
                }
                Ok((ReplayOutcome::AlreadyReplayed, _)) => {
                    already += 1;
                    results.push(json!({
                        "dlq_id": entry.id,
                        "document_id": item.document_id,
                        "result": "already_replayed",
                    }));
                }
                Ok((ReplayOutcome::Missing, _)) => {
                    missing += 1;
                    results.push(json!({
                        "dlq_id": entry.id,
                        "document_id": item.document_id,
                        "result": "missing",
                    }));
                }
                Err(e) => {
                    warn!(
                        document_id = %item.document_id,
                        dlq_id = %entry.id,
                        error = %e,
                        "replay: fallo Redis; la entrada sigue en la DLQ"
                    );
                    results.push(json!({
                        "dlq_id": entry.id,
                        "document_id": item.document_id,
                        "result": "redis_error",
                    }));
                }
            }
        }
        start = format!("({last}");
    }

    let report = json!({
        "stream": cfg.stream,
        "dlq_stream": cfg.dlq_stream,
        "dry_run": opts.dry_run,
        "replayed_by": opts.by,
        "reset_state": opts.reset_state,
        "limit": opts.limit,
        "scanned": scanned,
        "processed": processed,
        "replayed": replayed,
        "already_replayed": already,
        "missing": missing,
        "skipped_state": skipped_state,
        "skipped_contract": skipped_contract,
        "skipped_mongo": skipped_mongo,
        "results": results,
    });
    println!(
        "{}",
        serde_json::to_string_pretty(&report).unwrap_or_default()
    );
    Ok(())
}

pub async fn replay_one(
    conn: &mut MultiplexedConnection,
    cfg: &ConsumerConfig,
    entry: &DlqEntry,
    item: &WorkItem,
    replayed_by: &str,
    replayed_at: &str,
    error_code: &str,
) -> Result<(ReplayOutcome, Option<String>), DlqError> {
    let (code, new_id): (i64, String) = redis::Script::new(REPLAY_LUA)
        .key(&cfg.stream)
        .key(&cfg.dlq_stream)
        .key(&cfg.dlq_replayed_key)
        .key(&cfg.dlq_replay_audit_stream)
        .arg(&entry.id)
        .arg(replayed_by)
        .arg(replayed_at)
        .arg(error_code)
        .arg(&item.document_id)
        .arg(&item.object_key)
        .arg(&item.correlation_id)
        .arg(&item.enqueued_at)
        .arg(item.schema_version)
        .invoke_async(conn)
        .await?;
    let outcome = match code {
        1 => ReplayOutcome::Replayed,
        0 => ReplayOutcome::AlreadyReplayed,
        -1 => ReplayOutcome::Missing,
        other => {
            return Err(DlqError::Serialize(format!(
                "resultado inesperado de replay: {other}"
            )))
        }
    };
    let new_id = (outcome == ReplayOutcome::Replayed).then_some(new_id);
    Ok((outcome, new_id))
}

fn parse_work_item(entry: &DlqEntry) -> Result<WorkItem, &'static str> {
    const CONTRACT: [&str; 5] = [
        "document_id",
        "object_key",
        "correlation_id",
        "enqueued_at",
        "schema_version",
    ];
    let mut fields = HashMap::with_capacity(CONTRACT.len());
    for key in CONTRACT {
        let value = entry
            .fields
            .get(key)
            .ok_or("contrato_invalido: falta un campo del WorkItem")?;
        fields.insert(key.to_string(), value.clone());
    }
    let item = WorkItem::from_stream_fields(&fields)
        .map_err(|_| "contrato_invalido: WorkItem invalido")?;
    item.validate_schema()
        .map_err(|_| "contrato_invalido: schema no soportado")?;
    Ok(item)
}

fn default_operator() -> String {
    env::var("USER")
        .or_else(|_| env::var("USERNAME"))
        .unwrap_or_else(|_| "unknown".to_string())
}

fn now_rfc3339() -> String {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(raw: &[&str]) -> Vec<String> {
        raw.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn exige_un_filtro_o_all() {
        assert!(ReplayOptions::from_args(&args(&["--replay-dlq"])).is_err());
        let opts = match ReplayOptions::from_args(&args(&[
            "--replay-dlq",
            "--document-id",
            "doc-1",
            "--limit",
            "5",
            "--dry-run",
        ])) {
            Ok(opts) => opts,
            Err(e) => panic!("args validos: {e}"),
        };
        assert_eq!(opts.document_id.as_deref(), Some("doc-1"));
        assert_eq!(opts.limit, 5);
        assert!(opts.dry_run);
        assert!(opts.reset_state);
    }

    #[test]
    fn acepta_all_y_valores_inline() {
        let opts = match ReplayOptions::from_args(&args(&[
            "--replay-dlq",
            "--all",
            "--error-code=INVALID_CONTRACT",
            "--by=ops",
            "--no-reset-state",
        ])) {
            Ok(opts) => opts,
            Err(e) => panic!("args validos: {e}"),
        };
        assert!(opts.all);
        assert_eq!(opts.error_code.as_deref(), Some("INVALID_CONTRACT"));
        assert_eq!(opts.by, "ops");
        assert!(!opts.reset_state);
    }

    #[test]
    fn rechaza_argumentos_desconocidos_y_limite_invalido() {
        assert!(ReplayOptions::from_args(&args(&["--replay-dlq", "--all", "--wat"])).is_err());
        assert!(
            ReplayOptions::from_args(&args(&["--replay-dlq", "--all", "--limit", "x"])).is_err()
        );
    }
}
