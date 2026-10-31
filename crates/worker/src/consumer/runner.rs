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
//! Apagado (S4-P3-04): al cancelar se dejan de leer mensajes nuevos y se
//! espera a los jobs en vuelo hasta `WORKER_SHUTDOWN_DRAIN_SECS`. Los que no
//! terminan se abortan y quedan pendientes en la PEL (otro worker los
//! reclama con `XAUTOCLAIM`); nunca se hace `XACK` de un job incompleto.
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
use tracing::{info, warn};

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
        // Apagado responsivo: el retry de conexion puede durar hasta 30s;
        // el token lo corta sin esperar al presupuesto de apagado.
        let mut conn = tokio::select! {
            _ = token.cancelled() => return Ok(()),
            r = Self::connect(ctx, &client) => r?,
        };
        match tokio::select! {
            _ = token.cancelled() => return Ok(()),
            r = timeout(REDIS_OP_TIMEOUT, group::ensure_group(&mut conn, &ctx.cfg)) => r,
        } {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(RunnerError::from(e)),
            Err(_) => return Err(RunnerError::Config("ensure_group timeout".to_string())),
        }

        let mut autoclaim_ticker = interval(ctx.cfg.xautoclaim_interval);
        autoclaim_ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut autoclaim_cursor = "0-0".to_string();
        // JoinSet de extracciones en vuelo; el apagado los drena con un
        // presupuesto acotado.
        let mut jobs: JoinSet<()> = JoinSet::new();

        // Gauges de cola en una conexion propia (DoD 3): no bloquean el
        // bucle de lectura ni consumen el bulkhead de extracciones.
        let metrics = tokio::spawn(Self::metrics_loop(
            ctx.clone(),
            conn.clone(),
            self.sem.clone(),
            token.clone(),
        ));

        loop {
            tokio::select! {
                _ = token.cancelled() => break,
                _ = autoclaim_ticker.tick() => {
                    if let Err(e) = Self::do_autoclaim(ctx, &conn, &mut autoclaim_cursor, &self.sem, &mut jobs).await {
                        warn!(
                            service = crate::telemetry::SERVICE,
                            msg = "xautoclaim fallo",
                            error = %e
                        );
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
                            warn!(
                                service = crate::telemetry::SERVICE,
                                msg = "lectura del stream fallo; reintentando",
                                error = %e
                            );
                            tokio::time::sleep(Duration::from_millis(500)).await;
                        }
                    }
                }
            }
        }
        let aborted = drain_jobs(&mut jobs, ctx.cfg.shutdown_drain).await;
        if aborted == 0 {
            info!(
                service = crate::telemetry::SERVICE,
                msg = "apagado: jobs en vuelo drenados",
                budget_secs = ctx.cfg.shutdown_drain.as_secs()
            );
        } else {
            metrics::counter!("worker_shutdown_aborted_total").increment(aborted as u64);
            warn!(
                service = crate::telemetry::SERVICE,
                msg = "apagado: jobs abortados; quedan en la PEL para XAUTOCLAIM",
                aborted,
                budget_secs = ctx.cfg.shutdown_drain.as_secs()
            );
        }
        let _ = metrics.await;
        Ok(())
    }

    /// Refresca gauges de cola y de estado interno cada
    /// `WORKER_METRICS_INTERVAL_SECS` (default 15s).
    async fn metrics_loop(
        ctx: JobContext<S, K>,
        conn: MultiplexedConnection,
        sem: Arc<Semaphore>,
        token: CancellationToken,
    ) {
        let mut ticker = interval(ctx.cfg.metrics_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = token.cancelled() => break,
                _ = ticker.tick() => Self::update_gauges(&ctx, &conn, &sem).await,
            }
        }
    }

    async fn update_gauges(
        ctx: &JobContext<S, K>,
        conn: &MultiplexedConnection,
        sem: &Arc<Semaphore>,
    ) {
        let in_flight = ctx
            .cfg
            .max_concurrency
            .saturating_sub(sem.available_permits());
        metrics::gauge!("worker_inflight").set(in_flight as f64);
        for (dependency, open) in [
            ("redis", ctx.redis_breaker.is_open()),
            ("mongo", ctx.mongo_breaker.is_open()),
            ("minio", ctx.pipeline.breaker().is_open()),
        ] {
            metrics::gauge!("worker_breaker_open", "dependency" => dependency).set(if open {
                1.0
            } else {
                0.0
            });
        }
        if let Some(depth) = Self::stream_depth(ctx, conn).await {
            metrics::gauge!("worker_stream_depth", "stream" => ctx.cfg.stream.clone())
                .set(depth as f64);
        }
        if let Some(pending) = Self::pel_pending(ctx, conn).await {
            metrics::gauge!("worker_pel_pending", "group" => ctx.cfg.group.clone())
                .set(pending as f64);
        }
        if let Some(stats) = Self::dlq_stats(ctx, conn).await {
            metrics::gauge!("worker_dlq_depth", "stream" => ctx.cfg.dlq_stream.clone())
                .set(stats.depth as f64);
            metrics::gauge!(
                "worker_dlq_oldest_age_seconds",
                "stream" => ctx.cfg.dlq_stream.clone()
            )
            .set(stats.oldest_age_secs.unwrap_or(0) as f64);
        }
        if let Some(dead) = Self::dead_consumers(ctx, conn).await {
            metrics::gauge!("worker_dead_consumers", "group" => ctx.cfg.group.clone())
                .set(dead.len() as f64);
            for consumer in dead {
                warn!(
                    service = crate::telemetry::SERVICE,
                    msg = "consumidor con pendientes sin senal de vida",
                    consumer = %consumer.name,
                    pending = consumer.pending,
                    idle_ms = consumer.idle,
                    group = %ctx.cfg.group
                );
            }
        }
    }

    async fn dlq_stats(
        ctx: &JobContext<S, K>,
        conn: &MultiplexedConnection,
    ) -> Option<dlq::DlqStats> {
        let mut c = conn.clone();
        let cfg = ctx.cfg.clone();
        match timeout(REDIS_OP_TIMEOUT, dlq::stats(&mut c, &cfg)).await {
            Ok(Ok(v)) => Some(v),
            _ => None,
        }
    }

    async fn dead_consumers(
        ctx: &JobContext<S, K>,
        conn: &MultiplexedConnection,
    ) -> Option<Vec<redis::streams::StreamInfoConsumer>> {
        let mut c = conn.clone();
        let cfg = ctx.cfg.clone();
        match timeout(REDIS_OP_TIMEOUT, group::dead_consumers(&mut c, &cfg)).await {
            Ok(Ok(v)) => Some(v),
            _ => None,
        }
    }

    async fn stream_depth(ctx: &JobContext<S, K>, conn: &MultiplexedConnection) -> Option<u64> {
        let mut c = conn.clone();
        let stream = ctx.cfg.stream.clone();
        match timeout(REDIS_OP_TIMEOUT, c.xlen::<_, u64>(&stream)).await {
            Ok(Ok(n)) => Some(n),
            _ => None,
        }
    }

    async fn pel_pending(ctx: &JobContext<S, K>, conn: &MultiplexedConnection) -> Option<u64> {
        let mut c = conn.clone();
        let cfg = ctx.cfg.clone();
        match timeout(REDIS_OP_TIMEOUT, group::pending_summary(&mut c, &cfg)).await {
            Ok(Ok(v)) => v.get("pending_count").and_then(serde_json::Value::as_u64),
            _ => None,
        }
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
                let item = best_effort_item(&fields);
                warn!(
                    service = crate::telemetry::SERVICE,
                    msg = "contrato invalido; va a la DLQ",
                    correlation_id = %item.correlation_id,
                    document_id = %item.document_id,
                    error = %issue
                );
                metrics::counter!("worker_documents_total", "outcome" => "rejected").increment(1);
                let (code, kind) = contract_cause(&issue);
                Self::dlq_and_log(&ctx, &conn, &msg_id, &item, code, &issue.to_string(), kind)
                    .await;
                return;
            }
        };
        if let Err(issue) = item.validate_schema() {
            warn!(
                service = crate::telemetry::SERVICE,
                msg = "schema invalido; va a la DLQ",
                correlation_id = %item.correlation_id,
                document_id = %item.document_id,
                error = %issue
            );
            metrics::counter!("worker_documents_total", "outcome" => "rejected").increment(1);
            let (code, kind) = contract_cause(&issue);
            Self::dlq_and_log(&ctx, &conn, &msg_id, &item, code, &issue.to_string(), kind).await;
            return;
        }

        // S2-P3-07: sin reintentar el trabajo si ya agoto sus intentos.
        let attempts = match Self::delivery_count(&ctx, &conn, &msg_id).await {
            Ok(a) => a,
            Err(e) => {
                warn!(
                    service = crate::telemetry::SERVICE,
                    msg = "no se pudo leer el delivery count; el mensaje queda pendiente",
                    correlation_id = %item.correlation_id,
                    document_id = %item.document_id,
                    msg_id = %msg_id,
                    error = %e
                );
                metrics::counter!("worker_documents_total", "outcome" => "deferred").increment(1);
                return;
            }
        };
        if attempts > cfg.max_attempts as u64 {
            let message = format!(
                "agoto {} intentos (delivery count {attempts})",
                cfg.max_attempts
            );
            warn!(
                service = crate::telemetry::SERVICE,
                msg = "max_attempts superado; va a la DLQ",
                correlation_id = %item.correlation_id,
                document_id = %item.document_id,
                attempts,
                max_attempts = cfg.max_attempts,
                error = %message
            );
            metrics::counter!("worker_documents_total", "outcome" => "failed").increment(1);
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
            Ok(true) => {
                metrics::counter!("worker_claim_total", "result" => "claimed").increment(1);
            }
            Ok(false) => {
                metrics::counter!("worker_claim_total", "result" => "not_claimable").increment(1);
                metrics::counter!("worker_documents_total", "outcome" => "duplicate").increment(1);
                info!(
                    service = crate::telemetry::SERVICE,
                    msg = "documento no reclamable; entrega duplicada, se ACK-ea",
                    correlation_id = %item.correlation_id,
                    document_id = %item.document_id
                );
                Self::ack_and_log(&ctx, &conn, &msg_id, &item).await;
                return;
            }
            Err(e) => {
                metrics::counter!("worker_claim_total", "result" => "error").increment(1);
                metrics::counter!("worker_documents_total", "outcome" => "deferred").increment(1);
                warn!(
                    service = crate::telemetry::SERVICE,
                    msg = "no se pudo reclamar en Mongo; el mensaje queda pendiente",
                    correlation_id = %item.correlation_id,
                    document_id = %item.document_id,
                    error = %e
                );
                return;
            }
        }

        // S2-P3-01/03: pipeline con bulkhead implicito (esta tarea tiene
        // permiso). Documento roto -> DLQ; infra -> pendiente.
        if let Err(f) = ctx.pipeline.process(&item).await {
            match f {
                Failure::Document { code, message } => {
                    metrics::counter!("worker_documents_total", "outcome" => "rejected")
                        .increment(1);
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
                    metrics::counter!("worker_documents_total", "outcome" => "deferred")
                        .increment(1);
                    warn!(
                        service = crate::telemetry::SERVICE,
                        msg = "fallo de infraestructura; el mensaje queda pendiente",
                        correlation_id = %item.correlation_id,
                        document_id = %item.document_id,
                        code = %code,
                        error = %message
                    );
                }
                Failure::CircuitOpen => {
                    metrics::counter!("worker_documents_total", "outcome" => "deferred")
                        .increment(1);
                    warn!(
                        service = crate::telemetry::SERVICE,
                        msg = "circuito de MinIO abierto; el mensaje queda pendiente",
                        correlation_id = %item.correlation_id,
                        document_id = %item.document_id
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
                metrics::counter!("worker_documents_total", "outcome" => "completed").increment(1);
                Self::ack_and_log(&ctx, &conn, &msg_id, &item).await;
            }
            Err(e) => {
                // Compensacion (S2-P3-09): borrar el .txt y dejar el documento
                // en PROCESSING para el reconciliador. El mensaje queda
                // pendiente y se reclamara de nuevo.
                metrics::counter!("worker_documents_total", "outcome" => "deferred").increment(1);
                warn!(
                    service = crate::telemetry::SERVICE,
                    msg = "el cierre en Mongo fallo; se compensa el .txt y queda pendiente",
                    correlation_id = %item.correlation_id,
                    document_id = %item.document_id,
                    error = %e
                );
                match ctx.pipeline.compensate(&item).await {
                    Ok(()) => {
                        metrics::counter!("worker_compensations_total", "result" => "ok")
                            .increment(1);
                        info!(
                            service = crate::telemetry::SERVICE,
                            msg = "txt compensado (0 huerfanos)",
                            correlation_id = %item.correlation_id,
                            document_id = %item.document_id
                        );
                    }
                    Err(ce) => {
                        metrics::counter!("worker_compensations_total", "result" => "error")
                            .increment(1);
                        warn!(
                            service = crate::telemetry::SERVICE,
                            msg = "la compensacion del txt fallo; revisar el bucket",
                            correlation_id = %item.correlation_id,
                            document_id = %item.document_id,
                            error = %ce
                        );
                    }
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
        item: &WorkItem,
    ) {
        match Self::ack(ctx, conn, msg_id).await {
            Ok(()) => info!(
                service = crate::telemetry::SERVICE,
                msg = "documento completado y ACK-eado",
                correlation_id = %item.correlation_id,
                document_id = %item.document_id
            ),
            Err(e) => warn!(
                service = crate::telemetry::SERVICE,
                msg = "procesado pero el XACK fallo; queda pendiente y se reprocesa (put idempotente)",
                correlation_id = %item.correlation_id,
                document_id = %item.document_id,
                error = %e
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
            info!(
                service = crate::telemetry::SERVICE,
                msg = "xautoclaim: mensajes ya no existen en el stream",
                deleted = reply.deleted_ids.len()
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
            Ok(true) => {
                metrics::counter!("worker_dlq_total", "cause" => error_code.to_string())
                    .increment(1);
                warn!(
                    service = crate::telemetry::SERVICE,
                    msg = "documento movido a la DLQ",
                    correlation_id = %item.correlation_id,
                    document_id = %item.document_id,
                    cause = %error_code,
                    error = %error_message
                );
            }
            Ok(false) => info!(
                service = crate::telemetry::SERVICE,
                msg = "mensaje ya no pendiente; DLQ omitida (idempotente)",
                msg_id = %msg_id,
                correlation_id = %item.correlation_id,
                document_id = %item.document_id
            ),
            Err(e) => warn!(
                service = crate::telemetry::SERVICE,
                msg = "DLQ fallo; el mensaje queda pendiente para reintento",
                msg_id = %msg_id,
                correlation_id = %item.correlation_id,
                document_id = %item.document_id,
                error = %e
            ),
        }
    }
}

/// Espera a que terminen los jobs en vuelo hasta `budget`; al agotarse los
/// aborta. Devuelve cuantos quedaron abortados. Los mensajes de los abortados
/// siguen pendientes en la PEL: no se ACK-ean ni se pierden.
pub(crate) async fn drain_jobs(jobs: &mut JoinSet<()>, budget: Duration) -> usize {
    let started = std::time::Instant::now();
    while !jobs.is_empty() {
        let remaining = budget.saturating_sub(started.elapsed());
        if remaining.is_zero() {
            while jobs.try_join_next().is_some() {}
            let aborted = jobs.len();
            jobs.abort_all();
            while jobs.join_next().await.is_some() {}
            return aborted;
        }
        match timeout(remaining, jobs.join_next()).await {
            Ok(Some(_)) => {}
            Ok(None) => return 0,
            Err(_) => {}
        }
    }
    0
}

/// Reloj del worker para `updated_at`/`completed_at`/`status_history.at`.
fn now_rfc3339() -> String {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::sleep;

    #[tokio::test]
    async fn drain_espera_los_cortos_y_aborta_los_largos() {
        let mut jobs: JoinSet<()> = JoinSet::new();
        jobs.spawn(async {
            sleep(Duration::from_millis(30)).await;
        });
        jobs.spawn(async {
            sleep(Duration::from_secs(60)).await;
        });

        let started = std::time::Instant::now();
        let aborted = drain_jobs(&mut jobs, Duration::from_millis(500)).await;

        assert_eq!(aborted, 1, "solo el job largo debe abortarse");
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "el drenaje no debe esperar al job largo"
        );
        assert!(jobs.is_empty(), "el JoinSet debe quedar consumido");
    }

    #[tokio::test]
    async fn drain_sin_presupuesto_aborta_todo() {
        let mut jobs: JoinSet<()> = JoinSet::new();
        jobs.spawn(async {
            sleep(Duration::from_millis(10)).await;
        });
        jobs.spawn(async {
            sleep(Duration::from_secs(60)).await;
        });

        let aborted = drain_jobs(&mut jobs, Duration::ZERO).await;
        assert_eq!(aborted, 2);
        assert!(jobs.is_empty());
    }

    #[tokio::test]
    async fn drain_con_todos_los_jobs_cortos_no_aborta() {
        let mut jobs: JoinSet<()> = JoinSet::new();
        for _ in 0..3 {
            jobs.spawn(async {
                sleep(Duration::from_millis(10)).await;
            });
        }
        let aborted = drain_jobs(&mut jobs, Duration::from_secs(1)).await;
        assert_eq!(aborted, 0);
        assert!(jobs.is_empty());
    }
}
