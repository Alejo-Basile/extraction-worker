//! Consumer loop (S1-P3-02/03/05, S2-P3-01..08).
//!
//! Flujo F3 por mensaje entregado (tarea con permiso del bulkhead):
//! 1. Validar contrato y `schema_version` -> DLQ con causa (business).
//! 2. Delivery count de `XPENDING` > `WORKER_MAX_ATTEMPTS` -> DLQ
//!    `MAX_ATTEMPTS_EXCEEDED` (transient), sin re-extraer.
//! 3. Pipeline: GET raw-pdfs -> archivo temporal -> extraccion -> PUT
//!    extracted-txt. Exito -> `XACK`. `Failure::Document` -> DLQ (business).
//!    Transient/Fatal/CircuitOpen -> el mensaje queda pendiente (se reclama
//!    con `XAUTOCLAIM` y repite).
//!
//! Regla de ACK (S2-P3-07/08): el `XACK` solo ocurre en el script Lua de la
//! DLQ o cuando el pipeline completo el trabajo. Nunca antes.
//!
//! Bulkhead (S2-P3-03): `Semaphore(WORKER_MAX_CONCURRENCY)` acota las
//! extracciones en vuelo por instancia; `/readyz` vive en el server HTTP y
//! sigue respondiendo con todos los permisos tomados. Las tareas esperando
//! permiso solo retienen el payload chico del mensaje (KBs), no el PDF.
//!
//! Apagado: el `JoinSet` se dropea y aborta las tareas en vuelo; los
//! mensajes sin terminar quedan pendientes y otro worker los reclama.
//!
//! Composicion de resiliencia (SDD 7.1-7.3): `Retry` envuelve al
//! `CircuitBreaker` de Redis y, dentro del pipeline, al breaker de MinIO.

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
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;
use tokio::time::{interval, timeout, MissedTickBehavior};
use tokio_util::sync::CancellationToken;

use crate::document_store::{mongo_breaker, DocumentStore};

use super::classify::classify_redis_error;
use super::config::ConsumerConfig;
use super::dlq::{self, best_effort_item, contract_cause};
use super::group;
use super::pipeline::{ExtractionPipeline, ObjectSource, TextSink};
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

/// Contexto clonable que lleva cada tarea de procesamiento. La conexion se
/// pasa por parametro: se establece una vez en `run()`. `Clone` es manual:
/// los adaptadores `S`/`K` no son clonables, pero viajan en `Arc`.
pub struct JobContext<S: ObjectSource, K: TextSink> {
    pub cfg: ConsumerConfig,
    pub pipeline: Arc<ExtractionPipeline<S, K>>,
    pub documents: Arc<DocumentStore>,
    pub redis_breaker: Arc<CircuitBreaker>,
    pub mongo_breaker: Arc<CircuitBreaker>,
}

impl<S: ObjectSource, K: TextSink> Clone for JobContext<S, K> {
    fn clone(&self) -> Self {
        Self {
            cfg: self.cfg.clone(),
            pipeline: self.pipeline.clone(),
            documents: self.documents.clone(),
            redis_breaker: self.redis_breaker.clone(),
            mongo_breaker: self.mongo_breaker.clone(),
        }
    }
}

pub struct ConsumerRunner<S: ObjectSource, K: TextSink> {
    ctx: JobContext<S, K>,
    /// Bulkhead de extracciones en vuelo (S2-P3-03).
    sem: Arc<Semaphore>,
}

impl<S: ObjectSource, K: TextSink> ConsumerRunner<S, K> {
    pub fn new(
        cfg: ConsumerConfig,
        pipeline: Arc<ExtractionPipeline<S, K>>,
        documents: Arc<DocumentStore>,
    ) -> Self {
        let sem = Arc::new(Semaphore::new(cfg.max_concurrency));
        Self {
            ctx: JobContext {
                cfg,
                pipeline,
                documents,
                redis_breaker: Arc::new(CircuitBreaker::default()),
                mongo_breaker: mongo_breaker(),
            },
            sem,
        }
    }

    pub async fn run(&self, token: CancellationToken) -> Result<(), RunnerError> {
        let ctx = &self.ctx;
        let client = redis::Client::open(ctx.cfg.redis_url.as_str())
            .map_err(|e| RunnerError::Config(e.to_string()))?;
        let mut conn = Self::connect(ctx, &client).await?;
        group::ensure_group(&mut conn, &ctx.cfg).await?;

        let mut autoclaim_ticker = interval(ctx.cfg.xautoclaim_interval);
        autoclaim_ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut autoclaim_cursor = "0-0".to_string();
        // JoinSet: al drop-ear (apagado) aborta las tareas en vuelo; los
        // mensajes sin terminar quedan pendientes y otro worker los reclama.
        let mut jobs: JoinSet<()> = JoinSet::new();

        loop {
            tokio::select! {
                _ = token.cancelled() => break,
                _ = autoclaim_ticker.tick() => {
                    if let Err(e) = Self::do_autoclaim(ctx, &conn, &mut autoclaim_cursor, &self.sem, &mut jobs).await {
                        eprintln!("xautoclaim error: {e}");
                    }
                }
                res = Self::read_batch(ctx, &conn) => {
                    match res {
                        Ok(batch) => {
                            for (id, fields) in batch {
                                Self::spawn_job(ctx, &self.sem, &conn, &mut jobs, id, fields);
                            }
                        }
                        Err(e) => {
                            eprintln!("consumer error: {e}");
                            tokio::time::sleep(Duration::from_millis(500)).await;
                        }
                    }
                }
            }
        }
        drop(jobs);
        Ok(())
    }

    /// Conexion inicial con Retry+CB de Redis: corta cuando el breaker lo
    /// decide o se agota el tiempo maximo de reintento.
    async fn connect(
        ctx: &JobContext<S, K>,
        client: &redis::Client,
    ) -> Result<MultiplexedConnection, RunnerError> {
        with_retry_cb(&ctx.redis_breaker, || async {
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

    /// Corre `op` con Retry+CB de Redis sobre un clon del pool de conexion
    /// (`MultiplexedConnection: Clone`; los clones comparten el mismo socket).
    async fn retry_redis<T, F, Fut>(
        ctx: &JobContext<S, K>,
        conn: &MultiplexedConnection,
        op: F,
    ) -> Result<T, Failure>
    where
        F: Fn(MultiplexedConnection) -> Fut,
        Fut: Future<Output = Result<T, Failure>>,
    {
        with_retry_cb(&ctx.redis_breaker, || op(conn.clone())).await
    }

    async fn read_batch(
        ctx: &JobContext<S, K>,
        conn: &MultiplexedConnection,
    ) -> Result<Vec<(String, HashMap<String, String>)>, RunnerError> {
        let cfg = &ctx.cfg;
        let streams: StreamReadReply = Self::retry_redis(ctx, conn, |mut c| async move {
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

        Ok(streams
            .keys
            .into_iter()
            .flat_map(|k| k.ids.into_iter())
            .map(|sid| {
                let fields: HashMap<String, String> = sid
                    .map
                    .iter()
                    .filter_map(|(k, v)| match v {
                        redis::Value::BulkString(b) => {
                            String::from_utf8(b.clone()).ok().map(|s| (k.clone(), s))
                        }
                        _ => None,
                    })
                    .collect();
                (sid.id, fields)
            })
            .collect())
    }

    /// Despacha una tarea de procesamiento. El permiso del bulkhead se toma
    /// dentro de la tarea: mientras espera, solo retiene el payload chico.
    fn spawn_job(
        ctx: &JobContext<S, K>,
        sem: &Arc<Semaphore>,
        conn: &MultiplexedConnection,
        jobs: &mut JoinSet<()>,
        msg_id: String,
        fields: HashMap<String, String>,
    ) {
        let ctx = ctx.clone();
        let conn = conn.clone();
        let sem = sem.clone();
        jobs.spawn(async move {
            let _permit: OwnedSemaphorePermit = match sem.acquire_owned().await {
                Ok(p) => p,
                Err(_) => return, // sem cerrado: apagado en curso
            };
            Self::run_job(ctx, conn, msg_id, fields).await;
        });
    }

    /// Politica F3 compartida por `XREADGROUP` y `XAUTOCLAIM`.
    async fn run_job(
        ctx: JobContext<S, K>,
        conn: MultiplexedConnection,
        msg_id: String,
        fields: HashMap<String, String>,
    ) {
        let cfg = &ctx.cfg;
        let item = match WorkItem::from_stream_fields(&fields) {
            Ok(item) => item,
            Err(issue) => {
                eprintln!("contrato invalido: {issue} -> DLQ atomica");
                let item = best_effort_item(&fields);
                let (code, kind) = contract_cause(&issue);
                Self::dlq_and_log(&ctx, &conn, &msg_id, &item, code, &issue.to_string(), kind)
                    .await;
                return;
            }
        };
        if let Err(issue) = item.validate_schema() {
            let (code, kind) = contract_cause(&issue);
            eprintln!("{issue} -> DLQ atomica");
            Self::dlq_and_log(&ctx, &conn, &msg_id, &item, code, &issue.to_string(), kind).await;
            return;
        }

        // S2-P3-07: sin reintentar el trabajo si ya agoto sus intentos.
        let attempts = match Self::delivery_count(&ctx, &conn, &msg_id).await {
            Ok(a) => a,
            Err(e) => {
                eprintln!("no se pudo leer el delivery count de {msg_id}: {e}; queda pendiente");
                return;
            }
        };
        if attempts > cfg.max_attempts as u64 {
            eprintln!(
                "max_attempts superado ({attempts} > {}) -> DLQ atomica",
                cfg.max_attempts
            );
            let message = format!(
                "agoto {} intentos (delivery count {attempts})",
                cfg.max_attempts
            );
            Self::dlq_and_log(
                &ctx,
                &conn,
                &msg_id,
                &item,
                MAX_ATTEMPTS_EXCEEDED,
                &message,
                ErrorKind::Transient,
            )
            .await;
            return;
        }

        // Reclamo en Mongo (S2-P3-08): QUEUED|RETRYING -> PROCESSING con
        // status_history. Si no matchea, otro actor ya movio el estado: el
        // mensaje es una entrega duplicada y se ACK-ea sin reprocesar.
        match Self::claim(&ctx, &item).await {
            Ok(true) => {}
            Ok(false) => {
                eprintln!(
                    "documento {} no reclambiable (estado movido por otro actor): entrega duplicada, se ACK-ea",
                    item.document_id
                );
                Self::ack_and_log(&ctx, &conn, &msg_id, &item.document_id).await;
                return;
            }
            Err(e) => {
                eprintln!(
                    "documento {}: no se pudo reclamar en Mongo ({e}); queda pendiente",
                    item.document_id
                );
                return;
            }
        }

        // S2-P3-01/03: pipeline con bulkhead implicito (esta tarea tiene
        // permiso). Documento roto -> DLQ; infra -> pendiente.
        if let Err(f) = ctx.pipeline.process(&item).await {
            match f {
                Failure::Document { code, message } => {
                    Self::dlq_and_log(
                        &ctx,
                        &conn,
                        &msg_id,
                        &item,
                        &code,
                        &message,
                        ErrorKind::Business,
                    )
                    .await;
                }
                Failure::Transient { code, message } | Failure::Fatal { code, message } => {
                    eprintln!(
                        "documento {}: infraestructura {} ({message}); queda pendiente",
                        item.document_id, code
                    );
                }
                Failure::CircuitOpen => {
                    eprintln!(
                        "documento {}: circuito de MinIO abierto; queda pendiente",
                        item.document_id
                    );
                }
            }
            return;
        }

        // Paso 5 (S2-P3-08): cierre condicional PROCESSING -> COMPLETED.
        match Self::complete(&ctx, &item).await {
            Ok(true) | Ok(false) => {
                // Ok(false): otro actor cerro o movio el documento; el .txt
                // subido es identico (put idempotente) y el estado ajeno no
                // se pisa. En ambos casos la entrega se confirma.
                Self::ack_and_log(&ctx, &conn, &msg_id, &item.document_id).await;
            }
            Err(e) => {
                // Compensacion (S2-P3-09): borrar el .txt y dejar el documento
                // en PROCESSING para el reconciliador. El mensaje queda
                // pendiente y se reclamara de nuevo.
                eprintln!(
                    "documento {}: el cierre en Mongo fallo ({e}); se compensa el .txt y queda pendiente",
                    item.document_id
                );
                match ctx.pipeline.compensate(&item).await {
                    Ok(()) => eprintln!(
                        "documento {}: .txt compensado (0 huerfanos)",
                        item.document_id
                    ),
                    Err(ce) => eprintln!(
                        "documento {}: la compensacion del .txt fallo ({ce}); revisar el bucket",
                        item.document_id
                    ),
                }
            }
        }
    }

    async fn claim(ctx: &JobContext<S, K>, item: &WorkItem) -> Result<bool, Failure> {
        let documents = ctx.documents.clone();
        let document_id = item.document_id.clone();
        let correlation_id = item.correlation_id.clone();
        let now = now_rfc3339();
        let mongo_timeout = ctx.cfg.mongo_timeout;
        with_retry_cb(&ctx.mongo_breaker, || {
            let documents = documents.clone();
            let document_id = document_id.clone();
            let correlation_id = correlation_id.clone();
            let now = now.clone();
            async move {
                match timeout(
                    mongo_timeout,
                    documents.claim(&document_id, &correlation_id, &now),
                )
                .await
                {
                    Ok(Ok(claimed)) => Ok(claimed),
                    Ok(Err(e)) => Err(e),
                    Err(_) => Err(Failure::transient("MONGO_TIMEOUT", "claim timeout")),
                }
            }
        })
        .await
    }

    async fn complete(ctx: &JobContext<S, K>, item: &WorkItem) -> Result<bool, Failure> {
        let documents = ctx.documents.clone();
        let document_id = item.document_id.clone();
        let correlation_id = item.correlation_id.clone();
        let txt_ref = super::pipeline::txt_key(&item.document_id);
        let now = now_rfc3339();
        let mongo_timeout = ctx.cfg.mongo_timeout;
        with_retry_cb(&ctx.mongo_breaker, || {
            let documents = documents.clone();
            let document_id = document_id.clone();
            let correlation_id = correlation_id.clone();
            let txt_ref = txt_ref.clone();
            let now = now.clone();
            async move {
                match timeout(
                    mongo_timeout,
                    documents.complete(&document_id, &txt_ref, &correlation_id, &now),
                )
                .await
                {
                    Ok(Ok(closed)) => Ok(closed),
                    Ok(Err(e)) => Err(e),
                    Err(_) => Err(Failure::transient("MONGO_TIMEOUT", "complete timeout")),
                }
            }
        })
        .await
    }

    async fn ack_and_log(
        ctx: &JobContext<S, K>,
        conn: &MultiplexedConnection,
        msg_id: &str,
        document_id: &str,
    ) {
        match Self::ack(ctx, conn, msg_id).await {
            Ok(()) => eprintln!("documento {document_id} completado y ACK-eado"),
            Err(e) => eprintln!(
                "documento {document_id} procesado pero el XACK fallo ({e}): queda pendiente y se reprocesara (put idempotente)"
            ),
        }
    }

    async fn ack(
        ctx: &JobContext<S, K>,
        conn: &MultiplexedConnection,
        msg_id: &str,
    ) -> Result<(), Failure> {
        let cfg = &ctx.cfg;
        Self::retry_redis(ctx, conn, |mut c| {
            let id = msg_id.to_string();
            async move {
                let acked: i64 = match timeout(
                    REDIS_OP_TIMEOUT,
                    c.xack(&cfg.stream, &cfg.group, &[id.as_str()]),
                )
                .await
                {
                    Ok(Ok(n)) => n,
                    Ok(Err(e)) => return Err(classify_redis_error(&e)),
                    Err(_) => {
                        return Err(Failure::transient(REDIS_TIMEOUT_CODE, "xack timeout"));
                    }
                };
                let _ = acked;
                Ok(())
            }
        })
        .await
    }

    /// `XAUTOCLAIM` con cursor persistente (S1-P3-05): reclama huerfanos con
    /// `min_idle_ms > 2x job_timeout` y los procesa igual que los nuevos.
    async fn do_autoclaim(
        ctx: &JobContext<S, K>,
        conn: &MultiplexedConnection,
        cursor: &mut String,
        sem: &Arc<Semaphore>,
        jobs: &mut JoinSet<()>,
    ) -> Result<(), RunnerError> {
        let cfg = &ctx.cfg;
        let reply: StreamAutoClaimReply = Self::retry_redis(ctx, conn, |mut c| {
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

        for entry in reply.claimed {
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
            Self::spawn_job(ctx, sem, conn, jobs, entry.id, fields);
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
        ctx: &JobContext<S, K>,
        conn: &MultiplexedConnection,
        msg_id: &str,
    ) -> Result<u64, Failure> {
        let cfg = &ctx.cfg;
        Self::retry_redis(ctx, conn, |mut c| {
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
        .await
    }

    /// DLQ atomica (XADD + XACK en un script Lua) con Retry+CB: el script es
    /// idempotente, asi que reintentar no duplica entradas en la DLQ.
    #[allow(clippy::too_many_arguments)]
    async fn dlq_move(
        ctx: &JobContext<S, K>,
        conn: &MultiplexedConnection,
        msg_id: &str,
        item: &WorkItem,
        error_code: &str,
        error_message: &str,
        error_kind: ErrorKind,
    ) -> Result<bool, RunnerError> {
        let cfg = &ctx.cfg;
        let moved = Self::retry_redis(ctx, conn, |mut c| {
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
        Ok(moved)
    }

    async fn dlq_and_log(
        ctx: &JobContext<S, K>,
        conn: &MultiplexedConnection,
        msg_id: &str,
        item: &WorkItem,
        error_code: &str,
        error_message: &str,
        error_kind: ErrorKind,
    ) {
        match Self::dlq_move(
            ctx,
            conn,
            msg_id,
            item,
            error_code,
            error_message,
            error_kind,
        )
        .await
        {
            Ok(true) => eprintln!(
                "documento {} movido a DLQ ({error_code}: {error_message})",
                item.document_id
            ),
            Ok(false) => {
                eprintln!("mensaje {msg_id} ya no esta pendiente: DLQ omitida (idempotente)")
            }
            Err(e) => {
                eprintln!("DLQ fallo para {msg_id}: {e}; queda pendiente para reintento");
            }
        }
    }
}

/// Reloj del worker para `updated_at`/`completed_at`/`status_history.at`.
fn now_rfc3339() -> String {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_string())
}
