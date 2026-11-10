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
use std::sync::{Arc, Mutex};
use std::time::Duration;

use extraction_core::error_code::MAX_ATTEMPTS_EXCEEDED;
use extraction_core::{ErrorKind, WorkItem};
use redis::streams::{
    StreamAutoClaimOptions, StreamAutoClaimReply, StreamReadOptions, StreamReadReply,
};
use redis::{aio::MultiplexedConnection, AsyncCommands};
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;
use tokio::time::{interval, timeout, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::document_store::{mongo_breaker, DocumentStore};

use super::classify::{classify_redis_error, needs_reconnect};
use super::config::ConsumerConfig;
use super::dlq::{self, best_effort_item, contract_cause};
use super::group;
use super::pipeline::{ExtractionPipeline, ObjectSource, TextSink};
use super::redis_conn::{self, RedisClient};
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

/// Capacidad del canal reader -> bucle principal. Acotada a proposito: si el
/// bucle principal se atrasa, el reader aplica contrapresion en vez de acumular
/// trabajo en memoria.
const READ_QUEUE: usize = 32;
const REDIS_TIMEOUT_CODE: &str = "REDIS_TIMEOUT";

/// Conexion al master actual con re-resolucion ante failover (T-08).
///
/// Guarda el `MultiplexedConnection` vivo detras de un candado sobre un
/// unico `Arc<RedisClient>`. Cuando una operacion falla con el marcador
/// de failover (`REDIS_FAILOVER`: `READONLY`/`MASTERDOWN`), `run`
/// descarta la conexion vieja, re-resuelve el master via
/// `RedisClient::reset` y reconecta; el `with_retry_cb` envolvente
/// reintenta la operacion sobre la conexion nueva con backoff. Los loops
/// largos nunca crashean por un failover: es un error transitorio mas.
struct MasterConn {
    client: Arc<RedisClient>,
    conn: Mutex<MultiplexedConnection>,
    /// `true` para la conexion exclusiva del read bloqueante
    /// (`XREADGROUP BLOCK`): se reconecta con `connect_read`.
    read: bool,
}

impl MasterConn {
    /// Conexion inicial con Retry+CB de Redis: corta cuando el breaker
    /// decide o se agota el tiempo maximo de reintento (misma semantica
    /// que el `connect` anterior).
    async fn establish(
        client: Arc<RedisClient>,
        breaker: &CircuitBreaker,
        read: bool,
    ) -> Result<Self, Failure> {
        let conn = with_retry_cb(breaker, || Self::connect_once(&client, read)).await?;
        Ok(Self {
            client,
            conn: Mutex::new(conn),
            read,
        })
    }

    /// Una conexion al master vigente, con el mismo timeout duro y la
    /// misma clasificacion que la conexion inicial.
    async fn connect_once(
        client: &Arc<RedisClient>,
        read: bool,
    ) -> Result<MultiplexedConnection, Failure> {
        let attempt = if read {
            timeout(REDIS_OP_TIMEOUT, client.connect_read()).await
        } else {
            timeout(REDIS_OP_TIMEOUT, client.connect()).await
        };
        match attempt {
            Ok(Ok(c)) => Ok(c),
            Ok(Err(e)) => Err(classify_redis_error(&e)),
            Err(_) => Err(Failure::transient(
                REDIS_TIMEOUT_CODE,
                "redis connect timeout",
            )),
        }
    }

    /// Clon de la conexion actual (los clones comparten el mismo socket).
    fn get(&self) -> MultiplexedConnection {
        match self.conn.lock() {
            Ok(guard) => guard.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        }
    }

    /// Re-resuelve el master via la fabrica (`reset`, en hilo bloqueante:
    /// en Sentinel la fabrica hace I/O sincrona) y reemplaza la conexion
    /// vieja por una nueva al master vigente. Si la reconexion falla, el
    /// error sale hacia el `with_retry_cb` de `run`, que reintenta con
    /// backoff hasta el presupuesto maximo.
    async fn recover(&self) -> Result<(), Failure> {
        self.client
            .reset_async()
            .await
            .map_err(|e| classify_redis_error(&e))?;
        let fresh = Self::connect_once(&self.client, self.read).await?;
        match self.conn.lock() {
            Ok(mut guard) => *guard = fresh,
            Err(poisoned) => *poisoned.into_inner() = fresh,
        }
        Ok(())
    }

    /// Corre `op` con Retry+CB sobre la conexion actual. Si `op` falla con
    /// una falla que exige reemplazar la conexion ([`needs_reconnect`]:
    /// failover o socket muerto), la descarta, re-resuelve el master y
    /// reintenta la operacion una vez sobre la conexion nueva; si el
    /// reintento vuelve a fallar, el retry externo sigue con backoff. Los
    /// loops nunca crashean: es un error transitorio mas.
    async fn run<T, F, Fut>(&self, breaker: &CircuitBreaker, op: F) -> Result<T, Failure>
    where
        F: Fn(MultiplexedConnection) -> Fut,
        Fut: Future<Output = Result<T, Failure>>,
    {
        with_retry_cb(breaker, || async {
            match op(self.get()).await {
                Err(e) if needs_reconnect(&e) => {
                    warn!(
                        service = crate::telemetry::SERVICE,
                        msg = "conexion con el master inservible; re-resolviendo",
                        error = %e
                    );
                    self.recover().await?;
                    op(self.get()).await
                }
                other => other,
            }
        })
        .await
    }
}

/// Frecuencia de reevaluacion del modo degradado (S6-P3-03): barata (un
/// mutex local, sin red) y reacciona en cuanto vence el cooldown del
/// breaker para dejar pasar el probe de recuperacion.
const DEGRADED_PAUSE: Duration = Duration::from_secs(1);

/// Dependencia de procesamiento con su breaker abierto por cooldown, si la
/// hay (S6-P3-03). Redis queda fuera: sin Redis el read ya falla y reintentar
/// es el unico camino; MinIO y Mongo son los que queman mensajes si se
/// consumen degradados.
///
/// Usa `rejecting()` y no `is_open()`: en `HalfOpen` el probe debe pasar
/// para cerrar el breaker; pausar el consumo en half-open mataria la unica
/// via de recuperacion.
fn degraded_dependency(minio: &CircuitBreaker, mongo: &CircuitBreaker) -> Option<&'static str> {
    if minio.rejecting() {
        Some("minio")
    } else if mongo.rejecting() {
        Some("mongo")
    } else {
        None
    }
}

/// Contexto clonable que lleva cada tarea de procesamiento. La conexion se
/// pasa por parametro (`Arc<MasterConn>`, re-resoluble): se establece una
/// vez en `run()`. `Clone` es manual: los adaptadores `S`/`K` no son
/// clonables, pero viajan en `Arc`.
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
        // Config invalida falla aca, sin I/O y sin volcar credenciales (T-07).
        ctx.cfg.validate().map_err(RunnerError::Config)?;
        // T-08: UN solo cliente re-resoluble compartido por todas las
        // conexiones; ante failover cada MasterConn llama a `reset` y
        // reconecta contra el master vigente.
        let client =
            Arc::new(RedisClient::new(&ctx.cfg).map_err(|e| RunnerError::Config(e.to_string()))?);
        // Apagado responsivo: el retry de conexion puede durar hasta 30s;
        // el token lo corta sin esperar al presupuesto de apagado.
        let shared = tokio::select! {
            _ = token.cancelled() => return Ok(()),
            r = MasterConn::establish(client.clone(), &ctx.redis_breaker, false) => r?,
        };
        let shared = Arc::new(shared);
        let mut conn = shared.get();
        match tokio::select! {
            _ = token.cancelled() => return Ok(()),
            r = timeout(REDIS_OP_TIMEOUT, group::ensure_group(&mut conn, &ctx.cfg)) => r,
        } {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(RunnerError::from(e)),
            Err(_) => return Err(RunnerError::Config("ensure_group timeout".to_string())),
        }
        drop(conn);

        // El read bloqueante (XREADGROUP BLOCK) va por una conexion PROPIA.
        // Sobre una conexion multiplexada compartida, mientras el BLOCK espera,
        // cualquier otra operacion (gauges, XPENDING del delivery count,
        // XAUTOCLAIM) queda encolada detras en el mismo socket; si tarda mas
        // que REDIS_OP_TIMEOUT expira, se reintenta, agota el umbral del
        // breaker y el consumidor deja de leer. Medido: con BLOCK=5000 el
        // breaker abria a los ~15s y el worker moria.
        let reader_conn = tokio::select! {
            _ = token.cancelled() => return Ok(()),
            r = MasterConn::establish(client.clone(), &ctx.redis_breaker, true) => r?,
        };

        // JoinSet de extracciones en vuelo; el apagado los drena con un
        // presupuesto acotado.
        let mut jobs: JoinSet<()> = JoinSet::new();

        // Gauges de cola en una conexion propia (DoD 3): no bloquean el
        // bucle de lectura ni consumen el bulkhead de extracciones.
        let metrics = tokio::spawn(Self::metrics_loop(
            ctx.clone(),
            shared.clone(),
            self.sem.clone(),
            token.clone(),
        ));

        // El reader vive en su PROPIA tarea. Con el read dentro de un
        // `select!`, cualquier otro branch que gane la carrera dropea la
        // future en vuelo, y redis-rs NO cancela el comando del lado del
        // servidor (multiplexed_connection.rs:537-546): el mensaje ya
        // entregado por Redis queda en la PEL sin job y sin XACK. Medido: 1 de
        // 4 mensajes entregado y ACKeado sin procesar.
        let (tx, mut rx) = mpsc::channel::<Vec<(String, HashMap<String, String>)>>(READ_QUEUE);
        let reader = tokio::spawn(Self::reader_loop(
            ctx.clone(),
            Arc::new(reader_conn),
            tx.clone(),
            token.clone(),
        ));

        // XAUTOCLAIM fuera del camino del read, con su propia conexion y su
        // propio tick: era justamente el otro branch que cancelaba el read.
        let autoclaim_conn = tokio::select! {
            _ = token.cancelled() => return Ok(()),
            r = MasterConn::establish(client.clone(), &ctx.redis_breaker, false) => r?,
        };
        let autoclaim = tokio::spawn(Self::autoclaim_loop(
            ctx.clone(),
            Arc::new(autoclaim_conn),
            self.sem.clone(),
            token.clone(),
        ));

        loop {
            tokio::select! {
                _ = token.cancelled() => break,
                maybe = rx.recv() => {
                    match maybe {
                        Some(batch) => {
                            for (id, fields) in batch {
                                Self::spawn_job(ctx, &self.sem, &shared, &mut jobs, id, fields);
                            }
                        }
                        // El reader termino: no va a llegar mas trabajo.
                        None => break,
                    }
                }
                Some(_) = jobs.join_next() => {}
            }
        }
        let _ = reader.await;
        let _ = autoclaim.await;
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
    /// Lee del stream en su propia tarea.
    ///
    /// Es la pieza que evita la perdida: al no estar dentro de un `select!`,
    /// ningun otro branch puede cancelar un `XREADGROUP` en vuelo. Si la
    /// lectura falla, se registra y se reintenta con una pausa corta.
    /// Un failover re-resuelve el master dentro de `read_batch` (T-08); el
    /// token corta la pausa entre reintentos pero nunca un read en vuelo.
    async fn reader_loop(
        ctx: JobContext<S, K>,
        rc: Arc<MasterConn>,
        tx: mpsc::Sender<Vec<(String, HashMap<String, String>)>>,
        token: CancellationToken,
    ) {
        // S6-P3-03 (modo degradado consciente): mientras una dependencia de
        // procesamiento tenga su breaker abierto por cooldown, no se leen
        // mensajes nuevos. Quedan en el stream (sin reclamar, sin delivery
        // count) en vez de entregarse a jobs que fallarian igual y terminarian
        // quemados por MAX_ATTEMPTS.
        let mut degraded: Option<&'static str> = None;
        while !token.is_cancelled() {
            if let Some(dep) = degraded_dependency(&ctx.pipeline.breaker(), &ctx.mongo_breaker) {
                if degraded.is_none() {
                    warn!(
                        service = crate::telemetry::SERVICE,
                        msg = "modo degradado: consumo de mensajes nuevos pausado; quedan en el stream",
                        dependency = dep
                    );
                    degraded = Some(dep);
                }
                tokio::select! {
                    _ = token.cancelled() => break,
                    _ = tokio::time::sleep(DEGRADED_PAUSE) => {}
                }
                continue;
            }
            if let Some(dep) = degraded.take() {
                info!(
                    service = crate::telemetry::SERVICE,
                    msg = "modo degradado: consumo de mensajes nuevos reanudado",
                    dependency = dep
                );
            }
            match Self::read_batch(&ctx, &rc).await {
                Ok(batch) => {
                    if batch.is_empty() {
                        continue;
                    }
                    if tx.send(batch).await.is_err() {
                        // El bucle principal ya no escucha: apagado.
                        break;
                    }
                }
                Err(e) => {
                    warn!(
                        service = crate::telemetry::SERVICE,
                        msg = "lectura del stream fallo; reintentando",
                        error = %e
                    );
                    // El read en vuelo NUNCA se cancela con el token
                    // (cancelarlo ahi entrega mensajes sin procesar: defecto
                    // medido en T-04); el token corta la pausa entre
                    // reintentos.
                    tokio::select! {
                        _ = token.cancelled() => break,
                        _ = tokio::time::sleep(Duration::from_millis(500)) => {}
                    }
                }
            }
        }
    }

    /// Recupera mensajes huerfanos con `XAUTOCLAIM`, en su propia tarea: su
    /// tick ya no compite con el read, asi que no lo cancela. Mantiene su
    /// propio `JoinSet` para poder drenarlo en el apagado. Ante un failover
    /// la conexion se re-resuelve dentro de `do_autoclaim` (T-08); el
    /// token sigue cortando la rama en vuelo via el `select!`.
    async fn autoclaim_loop(
        ctx: JobContext<S, K>,
        rc: Arc<MasterConn>,
        sem: Arc<Semaphore>,
        token: CancellationToken,
    ) {
        let mut ticker = interval(ctx.cfg.xautoclaim_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut cursor = "0-0".to_string();
        let mut jobs: JoinSet<()> = JoinSet::new();
        // S6-P3-03: reclamar durante la caida solo haria crecer el delivery
        // count de mensajes sanos hasta MAX_ATTEMPTS y los quemaria en la DLQ
        // por un fallo de infraestructura. El reaper solo reanuda cuando el
        // breaker dejo de rechazar (el probe pudo pasar).
        let mut degraded = false;
        loop {
            tokio::select! {
                _ = token.cancelled() => break,
                _ = ticker.tick() => {
                    if degraded_dependency(&ctx.pipeline.breaker(), &ctx.mongo_breaker)
                        .is_some()
                    {
                        if !degraded {
                            degraded = true;
                            warn!(
                                service = crate::telemetry::SERVICE,
                                msg = "modo degradado: xautoclaim pausado para no quemar delivery counts",
                            );
                        }
                        continue;
                    }
                    if degraded {
                        degraded = false;
                        info!(
                            service = crate::telemetry::SERVICE,
                            msg = "modo degradado: xautoclaim reanudado",
                        );
                    }
                    if let Err(e) = Self::do_autoclaim(&ctx, &rc, &mut cursor, &sem, &mut jobs).await {
                        warn!(
                            service = crate::telemetry::SERVICE,
                            msg = "xautoclaim fallo",
                            error = %e
                        );
                    }
                }
                Some(_) = jobs.join_next() => {}
            }
        }
        let _ = drain_jobs(&mut jobs, ctx.cfg.shutdown_drain).await;
    }

    async fn metrics_loop(
        ctx: JobContext<S, K>,
        rc: Arc<MasterConn>,
        sem: Arc<Semaphore>,
        token: CancellationToken,
    ) {
        let mut ticker = interval(ctx.cfg.metrics_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = token.cancelled() => break,
                _ = ticker.tick() => Self::update_gauges(&ctx, &rc, &sem).await,
            }
        }
    }

    async fn update_gauges(ctx: &JobContext<S, K>, rc: &MasterConn, sem: &Arc<Semaphore>) {
        let in_flight = ctx
            .cfg
            .max_concurrency
            .saturating_sub(sem.available_permits());
        metrics::gauge!("worker_inflight").set(in_flight as f64);
        // Capacidad del bulkhead por instancia: la alerta de saturacion la
        // compara contra `worker_inflight` sin hardcodear el limite.
        metrics::gauge!("worker_bulkhead_capacity").set(ctx.cfg.max_concurrency as f64);
        // S6-P3-03: 1 mientras el consumo esta pausado por un breaker con
        // cooldown vigente (detalle de la dependencia en los logs y en
        // `worker_breaker_open`).
        let degraded = degraded_dependency(&ctx.pipeline.breaker(), &ctx.mongo_breaker).is_some();
        metrics::gauge!("worker_degraded").set(f64::from(degraded));
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
        if let Some(depth) = Self::stream_depth(ctx, rc).await {
            metrics::gauge!("worker_stream_depth", "stream" => ctx.cfg.stream.clone())
                .set(depth as f64);
        }
        if let Some(pending) = Self::pel_pending(ctx, rc).await {
            metrics::gauge!("worker_pel_pending", "group" => ctx.cfg.group.clone())
                .set(pending as f64);
        }
        if let Some(stats) = Self::dlq_stats(ctx, rc).await {
            metrics::gauge!("worker_dlq_depth", "stream" => ctx.cfg.dlq_stream.clone())
                .set(stats.depth as f64);
            metrics::gauge!(
                "worker_dlq_oldest_age_seconds",
                "stream" => ctx.cfg.dlq_stream.clone()
            )
            .set(stats.oldest_age_secs.unwrap_or(0) as f64);
        }
        if let Some(dead) = Self::dead_consumers(ctx, rc).await {
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

    /// Best-effort para los gauges: si el fallo fue un failover, re-resuelve
    /// el master para que la proxima vuelta use la conexion nueva. Los
    /// gauges siguen sin contar para el breaker ni reintentar (igual que
    /// antes): el tick siguiente es su backoff natural.
    async fn recover_on_failover(rc: &MasterConn, failover: bool) {
        if !failover {
            return;
        }
        warn!(
            service = crate::telemetry::SERVICE,
            msg = "failover de Redis en metricas; re-resolviendo el master"
        );
        if let Err(e) = rc.recover().await {
            warn!(
                service = crate::telemetry::SERVICE,
                msg = "la recuperacion del master fallo; se reintenta en el proximo tick",
                error = %e
            );
        }
    }

    async fn dlq_stats(ctx: &JobContext<S, K>, rc: &MasterConn) -> Option<dlq::DlqStats> {
        let mut c = rc.get();
        let cfg = ctx.cfg.clone();
        match timeout(REDIS_OP_TIMEOUT, dlq::stats(&mut c, &cfg)).await {
            Ok(Ok(v)) => Some(v),
            Ok(Err(e)) => {
                let failover = matches!(&e, dlq::DlqError::Redis(r) if redis_conn::is_failover(r));
                Self::recover_on_failover(rc, failover).await;
                None
            }
            Err(_) => None,
        }
    }

    async fn dead_consumers(
        ctx: &JobContext<S, K>,
        rc: &MasterConn,
    ) -> Option<Vec<redis::streams::StreamInfoConsumer>> {
        let mut c = rc.get();
        let cfg = ctx.cfg.clone();
        match timeout(REDIS_OP_TIMEOUT, group::dead_consumers(&mut c, &cfg)).await {
            Ok(Ok(v)) => Some(v),
            Ok(Err(e)) => {
                let failover =
                    matches!(&e, group::GroupError::Redis(r) if redis_conn::is_failover(r));
                Self::recover_on_failover(rc, failover).await;
                None
            }
            Err(_) => None,
        }
    }

    async fn stream_depth(ctx: &JobContext<S, K>, rc: &MasterConn) -> Option<u64> {
        let mut c = rc.get();
        let stream = ctx.cfg.stream.clone();
        match timeout(REDIS_OP_TIMEOUT, c.xlen::<_, u64>(&stream)).await {
            Ok(Ok(n)) => Some(n),
            Ok(Err(e)) => {
                Self::recover_on_failover(rc, redis_conn::is_failover(&e)).await;
                None
            }
            Err(_) => None,
        }
    }

    async fn pel_pending(ctx: &JobContext<S, K>, rc: &MasterConn) -> Option<u64> {
        let mut c = rc.get();
        let cfg = ctx.cfg.clone();
        match timeout(REDIS_OP_TIMEOUT, group::pending_summary(&mut c, &cfg)).await {
            Ok(Ok(v)) => v.get("pending_count").and_then(serde_json::Value::as_u64),
            Ok(Err(e)) => {
                let failover =
                    matches!(&e, group::GroupError::Redis(r) if redis_conn::is_failover(r));
                Self::recover_on_failover(rc, failover).await;
                None
            }
            Err(_) => None,
        }
    }

    async fn read_batch(
        ctx: &JobContext<S, K>,
        rc: &MasterConn,
    ) -> Result<Vec<(String, HashMap<String, String>)>, RunnerError> {
        let cfg = &ctx.cfg;
        let streams: StreamReadReply = rc
            .run(&ctx.redis_breaker, |mut c| async move {
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
    /// La conexion viaja como `Arc<MasterConn>`: cada escritura del job
    /// (delivery count, DLQ, ACK) pasa por `run`, que re-resuelve el
    /// master ante un failover (T-08).
    fn spawn_job(
        ctx: &JobContext<S, K>,
        sem: &Arc<Semaphore>,
        rc: &Arc<MasterConn>,
        jobs: &mut JoinSet<()>,
        msg_id: String,
        fields: HashMap<String, String>,
    ) {
        let ctx = ctx.clone();
        let rc = Arc::clone(rc);
        let sem = sem.clone();
        jobs.spawn(async move {
            let _permit: OwnedSemaphorePermit = match sem.acquire_owned().await {
                Ok(p) => p,
                Err(_) => return, // sem cerrado: apagado en curso
            };
            Self::run_job(ctx, rc, msg_id, fields).await;
        });
    }

    /// Politica F3 compartida por `XREADGROUP` y `XAUTOCLAIM`.
    async fn run_job(
        ctx: JobContext<S, K>,
        rc: Arc<MasterConn>,
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
                Self::dlq_and_log(&ctx, &rc, &msg_id, &item, code, &issue.to_string(), kind).await;
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
            Self::dlq_and_log(&ctx, &rc, &msg_id, &item, code, &issue.to_string(), kind).await;
            return;
        }

        // S2-P3-07: sin reintentar el trabajo si ya agoto sus intentos.
        let attempts = match Self::delivery_count(&ctx, &rc, &msg_id).await {
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
                &rc,
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
                info!(
                    service = crate::telemetry::SERVICE,
                    msg = "documento reclamado",
                    correlation_id = %item.correlation_id,
                    document_id = %item.document_id,
                    msg_id = %msg_id,
                    consumer = %cfg.consumer_name,
                    attempts,
                    min_idle_ms = cfg.min_idle_ms
                );
            }
            Ok(false) => {
                metrics::counter!("worker_claim_total", "result" => "not_claimable").increment(1);
                metrics::counter!("worker_documents_total", "outcome" => "duplicate").increment(1);
                info!(
                    service = crate::telemetry::SERVICE,
                    msg = "documento no reclamable; entrega duplicada, se ACK-ea",
                    correlation_id = %item.correlation_id,
                    document_id = %item.document_id,
                    msg_id = %msg_id,
                    consumer = %cfg.consumer_name,
                    attempts
                );
                Self::ack_and_log(&ctx, &rc, &msg_id, &item).await;
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
                        &rc,
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
                    Self::mark_retrying(&ctx, &item).await;
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
                    Self::mark_retrying(&ctx, &item).await;
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
                Self::ack_and_log(&ctx, &rc, &msg_id, &item).await;
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

    async fn ack_and_log(ctx: &JobContext<S, K>, rc: &MasterConn, msg_id: &str, item: &WorkItem) {
        match Self::ack(ctx, rc, msg_id).await {
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

    /// Marca `PROCESSING -> RETRYING` cuando un fallo transitorio difiere el
    /// trabajo. Es la transicion declarada por la que el documento pasa antes de
    /// que, agotados los reintentos, `RETRYING -> EXTRACTION_FAILED` sea legal
    /// (SPEC 4 y 5.4). Sin esto el documento queda en `PROCESSING` para siempre.
    /// Best-effort: si no matchea o falla, el mensaje sigue pendiente y el
    /// reconciliador cubre el caso.
    async fn mark_retrying(ctx: &JobContext<S, K>, item: &WorkItem) {
        match timeout(
            ctx.cfg.mongo_timeout,
            ctx.documents.mark_terminal(
                &item.document_id,
                extraction_core::status::DocumentStatus::Processing,
                extraction_core::status::DocumentStatus::Retrying,
                "transient_failure_deferred",
                &item.correlation_id,
                &now_rfc3339(),
            ),
        )
        .await
        {
            Ok(Ok(true)) => {}
            Ok(Ok(false)) => warn!(
                service = crate::telemetry::SERVICE,
                msg = "RETRYING no aplicado: el documento ya no estaba en PROCESSING",
                document_id = %item.document_id
            ),
            Ok(Err(e)) => warn!(
                service = crate::telemetry::SERVICE,
                msg = "no se pudo marcar RETRYING; el mensaje sigue pendiente",
                document_id = %item.document_id,
                error = %e
            ),
            Err(_) => warn!(
                service = crate::telemetry::SERVICE,
                msg = "timeout marcando RETRYING; el mensaje sigue pendiente",
                document_id = %item.document_id
            ),
        }
    }

    async fn ack(ctx: &JobContext<S, K>, rc: &MasterConn, msg_id: &str) -> Result<(), Failure> {
        let cfg = &ctx.cfg;
        rc.run(&ctx.redis_breaker, |mut c| {
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
        rc: &Arc<MasterConn>,
        cursor: &mut String,
        sem: &Arc<Semaphore>,
        jobs: &mut JoinSet<()>,
    ) -> Result<(), RunnerError> {
        let cfg = &ctx.cfg;
        let reply: StreamAutoClaimReply = rc
            .run(&ctx.redis_breaker, |mut c| {
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
            Self::spawn_job(ctx, sem, rc, jobs, entry.id, fields);
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
        rc: &MasterConn,
        msg_id: &str,
    ) -> Result<u64, Failure> {
        let cfg = &ctx.cfg;
        rc.run(&ctx.redis_breaker, |mut c| {
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
        rc: &MasterConn,
        msg_id: &str,
        item: &WorkItem,
        error_code: &str,
        error_message: &str,
        error_kind: ErrorKind,
    ) -> Result<bool, RunnerError> {
        let cfg = &ctx.cfg;
        let moved = rc
            .run(&ctx.redis_breaker, |mut c| {
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
        rc: &MasterConn,
        msg_id: &str,
        item: &WorkItem,
        error_code: &str,
        error_message: &str,
        error_kind: ErrorKind,
    ) {
        match Self::dlq_move(ctx, rc, msg_id, item, error_code, error_message, error_kind).await {
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

                // SPEC 5.4 y 6.3: la mudanza a la DLQ cierra el ciclo del
                // documento. `Business` (entrada no procesable) cierra desde
                // PROCESSING a REJECTED; `Transient` (reintentos agotados)
                // cierra desde RETRYING a EXTRACTION_FAILED, que dispara la
                // compensacion de la SAGA. Ambas son transiciones declaradas.
                let (expected, target) = match error_kind {
                    ErrorKind::Business => (
                        extraction_core::status::DocumentStatus::Processing,
                        extraction_core::status::DocumentStatus::Rejected,
                    ),
                    ErrorKind::Transient => (
                        extraction_core::status::DocumentStatus::Retrying,
                        extraction_core::status::DocumentStatus::ExtractionFailed,
                    ),
                };
                match timeout(
                    ctx.cfg.mongo_timeout,
                    ctx.documents.mark_terminal(
                        &item.document_id,
                        expected,
                        target,
                        error_code,
                        &item.correlation_id,
                        &now_rfc3339(),
                    ),
                )
                .await
                {
                    Ok(Ok(true)) => {}
                    Ok(Ok(false)) => warn!(
                        service = crate::telemetry::SERVICE,
                        msg = "estado terminal no aplicado: el documento ya no estaba en el estado esperado",
                        document_id = %item.document_id,
                        expected = expected.as_str(),
                        target = target.as_str()
                    ),
                    Ok(Err(e)) => warn!(
                        service = crate::telemetry::SERVICE,
                        msg = "no se pudo escribir el estado terminal; queda para el reconciliador",
                        document_id = %item.document_id,
                        error = %e
                    ),
                    Err(_) => warn!(
                        service = crate::telemetry::SERVICE,
                        msg = "timeout escribiendo el estado terminal; queda para el reconciliador",
                        document_id = %item.document_id
                    ),
                }
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

    /// S6-P3-03: el modo degradado refleja los breakers de procesamiento,
    /// con MinIO primero (es el caso de la tarea).
    #[test]
    fn modo_degradado_refleja_los_breakers_de_procesamiento() {
        let minio = CircuitBreaker::new(1, Duration::from_secs(60));
        let mongo = CircuitBreaker::new(1, Duration::from_secs(60));
        assert_eq!(degraded_dependency(&minio, &mongo), None);

        minio.on_failure();
        assert_eq!(degraded_dependency(&minio, &mongo), Some("minio"));

        mongo.on_failure();
        assert_eq!(
            degraded_dependency(&minio, &mongo),
            Some("minio"),
            "con ambos abiertos, minio es el que se reporta"
        );

        minio.on_success();
        assert_eq!(degraded_dependency(&minio, &mongo), Some("mongo"));
    }

    /// S6-P3-03: un breaker en half-open NO degrada: el probe tiene que
    /// pasar o el worker nunca recuperaria.
    #[test]
    fn half_open_no_cuenta_como_degradado() {
        let minio = CircuitBreaker::new(1, Duration::from_millis(30));
        let mongo = CircuitBreaker::new(1, Duration::from_secs(60));
        minio.on_failure();
        assert_eq!(degraded_dependency(&minio, &mongo), Some("minio"));

        std::thread::sleep(Duration::from_millis(40));
        assert!(minio.allow(), "vencido el cooldown, el probe pasa");
        assert_eq!(
            degraded_dependency(&minio, &mongo),
            None,
            "half-open no pausa el consumo"
        );
    }

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

    // --- T-08: re-resolucion del master ante failover ---

    /// Servidor RESP stub: cuenta aceptaciones y responde `+OK` de mas,
    /// suficiente para que `connect` (setup CLIENT SETINFO) complete.
    fn spawn_ok_server() -> (
        std::net::SocketAddr,
        std::sync::Arc<std::sync::atomic::AtomicUsize>,
    ) {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap_or_else(|e| panic!("bind server: {e}"));
        let addr = listener
            .local_addr()
            .unwrap_or_else(|e| panic!("local_addr: {e}"));
        let accepts = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = std::sync::Arc::clone(&accepts);
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { break };
                counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                std::thread::spawn(move || {
                    let mut buf = [0u8; 4096];
                    loop {
                        match stream.read(&mut buf) {
                            Ok(0) | Err(_) => break,
                            Ok(_) => {
                                let _ = stream.write_all(
                                    b"+OK\r\n+OK\r\n+OK\r\n+OK\r\n+OK\r\n+OK\r\n+OK\r\n+OK\r\n",
                                );
                            }
                        }
                    }
                });
            }
        });
        (addr, accepts)
    }

    fn direct_cfg(url: &str) -> ConsumerConfig {
        let _guard = crate::consumer::config::test_env::lock();
        std::env::remove_var("REDIS_SENTINEL_ADDRS");
        std::env::remove_var("REDIS_MASTER_NAME");
        std::env::set_var("REDIS_URL", url);
        ConsumerConfig::from_env()
    }

    fn readonly_failure() -> Failure {
        classify_redis_error(&redis::RedisError::from((
            redis::ErrorKind::Server(redis::ServerErrorKind::ReadOnly),
            "READONLY You can't write against a read only replica.",
        )))
    }

    #[tokio::test]
    async fn master_conn_reconecta_tras_fallo_de_failover() {
        let (addr, accepts) = spawn_ok_server();
        let cfg = direct_cfg(&format!("redis://{addr}/"));
        let client = std::sync::Arc::new(
            redis_conn::RedisClient::new(&cfg).unwrap_or_else(|e| panic!("client: {e}")),
        );
        let breaker = CircuitBreaker::default();
        let rc = MasterConn::establish(client, &breaker, false)
            .await
            .unwrap_or_else(|e| panic!("establish: {e}"));
        assert_eq!(
            accepts.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "la conexion inicial abre un socket"
        );

        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let result = {
            let calls = std::sync::Arc::clone(&calls);
            rc.run(&breaker, |_conn| {
                let n = calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                async move {
                    if n == 0 {
                        // Primer intento: el master viejo responde READONLY.
                        Err(readonly_failure())
                    } else {
                        Ok(7u8)
                    }
                }
            })
            .await
        };
        assert_eq!(result.ok(), Some(7), "tras re-resolver debe triunfar");
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "la operacion se reintenta sobre la conexion nueva"
        );
        assert_eq!(
            accepts.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "el failover debe haber descartado la conexion y abierto OTRA"
        );
    }

    #[tokio::test]
    async fn master_conn_sin_failover_no_reconecta() {
        let (addr, accepts) = spawn_ok_server();
        let cfg = direct_cfg(&format!("redis://{addr}/"));
        let client = std::sync::Arc::new(
            redis_conn::RedisClient::new(&cfg).unwrap_or_else(|e| panic!("client: {e}")),
        );
        let breaker = CircuitBreaker::default();
        let rc = MasterConn::establish(client, &breaker, false)
            .await
            .unwrap_or_else(|e| panic!("establish: {e}"));
        let before = accepts.load(std::sync::atomic::Ordering::SeqCst);

        let result = rc
            .run(&breaker, |_conn| async { Ok::<u8, Failure>(1) })
            .await;
        assert_eq!(result.ok(), Some(1));
        assert_eq!(
            accepts.load(std::sync::atomic::Ordering::SeqCst),
            before,
            "un camino sin failover no debe abrir conexiones nuevas"
        );
    }

    #[tokio::test]
    async fn master_conn_reconecta_si_el_socket_muere() {
        // Observado en el stack real de failover: el master viejo cierra el
        // socket (`broken pipe`) en vez de responder READONLY. El conn
        // multiplexado no se auto-repara: sin reconnect el worker queda
        // trabado en el breaker para siempre.
        let (addr, accepts) = spawn_ok_server();
        let cfg = direct_cfg(&format!("redis://{addr}/"));
        let client = std::sync::Arc::new(
            redis_conn::RedisClient::new(&cfg).unwrap_or_else(|e| panic!("client: {e}")),
        );
        let breaker = CircuitBreaker::default();
        let rc = MasterConn::establish(client, &breaker, false)
            .await
            .unwrap_or_else(|e| panic!("establish: {e}"));
        assert_eq!(accepts.load(std::sync::atomic::Ordering::SeqCst), 1);

        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let result = {
            let calls = std::sync::Arc::clone(&calls);
            rc.run(&breaker, |_conn| {
                let n = calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                async move {
                    if n == 0 {
                        Err(classify_redis_error(&redis::RedisError::from((
                            redis::ErrorKind::Io,
                            "broken pipe",
                        ))))
                    } else {
                        Ok(9u8)
                    }
                }
            })
            .await
        };
        assert_eq!(result.ok(), Some(9), "tras reconectar debe triunfar");
        assert_eq!(
            accepts.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "un socket muerto debe reemplazarse con una conexion nueva"
        );
    }
}
