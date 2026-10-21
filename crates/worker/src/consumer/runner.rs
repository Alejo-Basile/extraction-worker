//! Consumer loop (S1-P3-02/03/05, S2-P3-02/04/05/06/07).
//!
//! Flujo F2 por mensaje entregado:
//! 1. Validar contrato (`WorkItem::from_stream_fields`) y `schema_version`.
//!    Contrato invalido o schema no soportada -> DLQ con causa (business).
//! 2. Si el delivery count de `XPENDING` supera `WORKER_MAX_ATTEMPTS`
//!    (comparacion `>` estricta: la entrega 1 es el primer intento real) ->
//!    DLQ con `MAX_ATTEMPTS_EXCEEDED` (transient).
//! 3. Mensaje valido: queda pendiente (sin ACK). En F3 el procesamiento
//!    real hara el trabajo y el `XACK` al terminar.
//!
//! Regla de ACK (S2-P3-07/08): el `XACK` solo ocurre dentro del script Lua
//! de la DLQ o, en F3, tras procesar el mensaje. Nunca se hace ACK de un
//! mensaje valido sin procesarlo.
//!
//! Composicion de resiliencia: `Retry` envuelve al `CircuitBreaker` y el
//! breaker envuelve cada llamada individual con timeout (SDD 7.1-7.3).

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use extraction_core::error_code::MAX_ATTEMPTS_EXCEEDED;
use extraction_core::{ErrorKind, WorkItem};
use redis::streams::{
    StreamAutoClaimOptions, StreamAutoClaimReply, StreamReadOptions, StreamReadReply,
};
use redis::{aio::MultiplexedConnection, AsyncCommands};
use tokio::time::{interval, timeout, MissedTickBehavior};
use tokio_util::sync::CancellationToken;

use super::classify::classify_redis_error;
use super::config::ConsumerConfig;
use super::dlq::{self, best_effort_item, contract_cause};
use super::group;
use super::resilience::{with_retry_cb, CircuitBreaker, Failure};

#[derive(thiserror::Error, Debug)]
pub enum RunnerError {
    #[error("config: {0}")]
    Config(String),
    #[error("redis: {0}")]
    Redis(#[from] redis::RedisError),
    #[error("grupo: {0}")]
    Group(#[from] group::GroupError),
    #[error("dlq: {0}")]
    Dlq(#[from] dlq::DlqError),
    #[error("operacion redis: {0}")]
    Op(#[from] Failure),
}

/// Timeout duro de cada operacion Redis individual (S2-P3-06). Las lecturas
/// bloqueantes llevan su propio margen sobre `block_ms`.
const REDIS_OP_TIMEOUT: Duration = Duration::from_secs(5);
const REDIS_TIMEOUT_CODE: &str = "REDIS_TIMEOUT";

/// Resultado de procesar un mensaje entregado.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageOutcome {
    /// Contrato valido: en F2 queda pendiente en la PEL hasta que F3
    /// implemente el procesamiento y el ACK.
    NeedsProcessing,
    /// Derivado a la DLQ con su causa (atomico, con ACK).
    SentToDlq,
}

pub struct ConsumerRunner {
    cfg: ConsumerConfig,
    cb: Arc<CircuitBreaker>,
}

impl ConsumerRunner {
    pub fn new(cfg: ConsumerConfig) -> Self {
        Self {
            cfg,
            cb: Arc::new(CircuitBreaker::default()),
        }
    }

    pub async fn run(&self, token: CancellationToken) -> Result<(), RunnerError> {
        let client = redis::Client::open(self.cfg.redis_url.as_str())
            .map_err(|e| RunnerError::Config(e.to_string()))?;
        let mut conn = self.connect(&client).await?;
        group::ensure_group(&mut conn, &self.cfg).await?;

        let mut autoclaim_ticker = interval(self.cfg.xautoclaim_interval);
        autoclaim_ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut autoclaim_cursor = "0-0".to_string();

        loop {
            tokio::select! {
                _ = token.cancelled() => break,
                _ = autoclaim_ticker.tick() => {
                    if let Err(e) = self.do_autoclaim(&conn, &mut autoclaim_cursor).await {
                        eprintln!("xautoclaim error: {e}");
                    }
                }
                res = self.read_and_process(&conn) => {
                    match res {
                        Ok(Some(MessageOutcome::NeedsProcessing)) => {
                            eprintln!("mensaje valido, pendiente de procesamiento (F3)");
                        }
                        Ok(_) => {}
                        Err(e) => {
                            eprintln!("consumer error: {e}");
                            tokio::time::sleep(Duration::from_millis(500)).await;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Conexion inicial con Retry+CB: corta cuando el breaker lo decide o se
    /// agota el tiempo maximo de reintento.
    async fn connect(&self, client: &redis::Client) -> Result<MultiplexedConnection, RunnerError> {
        with_retry_cb(&self.cb, || async {
            match timeout(REDIS_OP_TIMEOUT, client.get_multiplexed_async_connection()).await {
                Ok(Ok(c)) => Ok(c),
                Ok(Err(e)) => Err(classify_redis_error(&e)),
                Err(_) => Err(Failure::transient(
                    REDIS_TIMEOUT_CODE,
                    "redis connect timeout",
                )),
            }
        })
        .await
        .map_err(RunnerError::from)
    }

    /// Corre `op` con Retry+CB sobre un clon del pool de conexion
    /// (`MultiplexedConnection: Clone`; los clones comparten el mismo socket).
    async fn retry_redis<T, F, Fut>(
        &self,
        conn: &MultiplexedConnection,
        op: F,
    ) -> Result<T, Failure>
    where
        F: Fn(MultiplexedConnection) -> Fut,
        Fut: Future<Output = Result<T, Failure>>,
    {
        with_retry_cb(&self.cb, || op(conn.clone())).await
    }

    async fn read_and_process(
        &self,
        conn: &MultiplexedConnection,
    ) -> Result<Option<MessageOutcome>, RunnerError> {
        let cfg = &self.cfg;
        let streams: StreamReadReply = self
            .retry_redis(conn, |mut c| async move {
                let opts = StreamReadOptions::default()
                    .group(&cfg.group, &cfg.consumer_name)
                    .count(cfg.count)
                    .block(cfg.block_ms as usize);
                let keys = vec![cfg.stream.clone()];
                match timeout(
                    Duration::from_millis(cfg.block_ms + 2000),
                    c.xread_options(&keys, &[">".to_string()], &opts),
                )
                .await
                {
                    Ok(Ok(s)) => Ok(s),
                    Ok(Err(e)) => Err(classify_redis_error(&e)),
                    Err(_) => Err(Failure::transient(REDIS_TIMEOUT_CODE, "xreadgroup timeout")),
                }
            })
            .await?;

        let Some(entry) = streams.keys.into_iter().next() else {
            return Ok(None);
        };
        let Some(first) = entry.ids.into_iter().next() else {
            return Ok(None);
        };
        let id = first.id.clone();
        let fields: HashMap<String, String> = first
            .map
            .iter()
            .filter_map(|(k, v)| match v {
                redis::Value::BulkString(b) => {
                    String::from_utf8(b.clone()).ok().map(|s| (k.clone(), s))
                }
                _ => None,
            })
            .collect();
        self.process_message(conn, &id, &fields).await.map(Some)
    }

    /// Politica F2 compartida por `XREADGROUP` y `XAUTOCLAIM`.
    async fn process_message(
        &self,
        conn: &MultiplexedConnection,
        msg_id: &str,
        fields: &HashMap<String, String>,
    ) -> Result<MessageOutcome, RunnerError> {
        match WorkItem::from_stream_fields(fields) {
            Ok(item) => match item.validate_schema() {
                Ok(()) => {
                    let attempts = self.delivery_count(conn, msg_id).await?;
                    if attempts > self.cfg.max_attempts as u64 {
                        eprintln!(
                            "max_attempts superado ({attempts} > {}) -> DLQ atomica",
                            self.cfg.max_attempts
                        );
                        let message = format!(
                            "agoto {} intentos (delivery count {attempts})",
                            self.cfg.max_attempts
                        );
                        return self
                            .dlq_move(
                                conn,
                                msg_id,
                                &item,
                                MAX_ATTEMPTS_EXCEEDED,
                                &message,
                                ErrorKind::Transient,
                            )
                            .await
                            .map(|_| MessageOutcome::SentToDlq);
                    }
                    Ok(MessageOutcome::NeedsProcessing)
                }
                Err(issue) => self.dlq_by_contract(conn, msg_id, &item, &issue).await,
            },
            Err(issue) => {
                eprintln!("contrato invalido: {issue} -> DLQ atomica");
                let item = best_effort_item(fields);
                self.dlq_by_contract(conn, msg_id, &item, &issue).await
            }
        }
    }

    async fn dlq_by_contract(
        &self,
        conn: &MultiplexedConnection,
        msg_id: &str,
        item: &WorkItem,
        issue: &extraction_core::ContractError,
    ) -> Result<MessageOutcome, RunnerError> {
        let (code, kind) = contract_cause(issue);
        self.dlq_move(conn, msg_id, item, code, &issue.to_string(), kind)
            .await
            .map(|_| MessageOutcome::SentToDlq)
    }

    /// `XAUTOCLAIM` con cursor persistente (S1-P3-05): reclama huerfanos con
    /// `min_idle_ms > 2x job_timeout` y les aplica la misma politica F2.
    async fn do_autoclaim(
        &self,
        conn: &MultiplexedConnection,
        cursor: &mut String,
    ) -> Result<(), RunnerError> {
        let cfg = &self.cfg;
        let reply: StreamAutoClaimReply = self
            .retry_redis(conn, |mut c| {
                let start = cursor.clone();
                async move {
                    let opts = StreamAutoClaimOptions::default().count(cfg.count);
                    match timeout(
                        REDIS_OP_TIMEOUT,
                        c.xautoclaim_options(
                            &cfg.stream,
                            &cfg.group,
                            &cfg.consumer_name,
                            cfg.min_idle_ms,
                            start.as_str(),
                            opts,
                        ),
                    )
                    .await
                    {
                        Ok(Ok(r)) => Ok(r),
                        Ok(Err(e)) => Err(classify_redis_error(&e)),
                        Err(_) => Err(Failure::transient(REDIS_TIMEOUT_CODE, "xautoclaim timeout")),
                    }
                }
            })
            .await?;
        *cursor = reply.next_stream_id;

        for entry in &reply.claimed {
            let fields: HashMap<String, String> = entry
                .map
                .iter()
                .filter_map(|(k, v)| match v {
                    redis::Value::BulkString(b) => {
                        String::from_utf8(b.clone()).ok().map(|s| (k.clone(), s))
                    }
                    _ => None,
                })
                .collect();
            if let Err(e) = self.process_message(conn, &entry.id, &fields).await {
                eprintln!("xautoclaim: no se pudo procesar {}: {e}", entry.id);
            }
        }
        if !reply.deleted_ids.is_empty() {
            eprintln!(
                "xautoclaim: {} mensajes ya no existen en el stream",
                reply.deleted_ids.len()
            );
        }
        Ok(())
    }

    /// Delivery count del mensaje con Retry+CB; si falla se propaga para
    /// dejar el mensaje pendiente (nunca se DLQ-ea a ciegas).
    async fn delivery_count(
        &self,
        conn: &MultiplexedConnection,
        msg_id: &str,
    ) -> Result<u64, RunnerError> {
        let cfg = &self.cfg;
        let n = self
            .retry_redis(conn, |mut c| {
                let id = msg_id.to_string();
                async move {
                    match timeout(
                        REDIS_OP_TIMEOUT,
                        dlq::get_delivery_count_by_id(&mut c, cfg, &id),
                    )
                    .await
                    {
                        Ok(Ok(n)) => Ok(n),
                        Ok(Err(e)) => Err(Failure::from(e)),
                        Err(_) => Err(Failure::transient(REDIS_TIMEOUT_CODE, "xpending timeout")),
                    }
                }
            })
            .await?;
        Ok(n)
    }

    /// DLQ atomica (XADD + XACK en un script Lua) con Retry+CB: el script es
    /// idempotente, asi que reintentar no duplica entradas en la DLQ.
    #[allow(clippy::too_many_arguments)]
    async fn dlq_move(
        &self,
        conn: &MultiplexedConnection,
        msg_id: &str,
        item: &WorkItem,
        error_code: &str,
        error_message: &str,
        error_kind: ErrorKind,
    ) -> Result<bool, RunnerError> {
        let cfg = &self.cfg;
        let moved = self
            .retry_redis(conn, |mut c| {
                let id = msg_id.to_string();
                let item = item.clone();
                let message = error_message.to_string();
                async move {
                    match timeout(
                        REDIS_OP_TIMEOUT,
                        dlq::move_to_dlq_with_cause(
                            &mut c, cfg, &id, &item, error_code, &message, error_kind,
                        ),
                    )
                    .await
                    {
                        Ok(Ok(moved)) => Ok(moved),
                        Ok(Err(e)) => Err(Failure::from(e)),
                        Err(_) => Err(Failure::transient(REDIS_TIMEOUT_CODE, "dlq eval timeout")),
                    }
                }
            })
            .await?;
        if moved {
            eprintln!(
                "documento {} movido a DLQ ({error_code}: {error_message})",
                item.document_id
            );
        } else {
            eprintln!("mensaje {msg_id} ya no esta pendiente: DLQ omitida (idempotente)");
        }
        Ok(moved)
    }
}
