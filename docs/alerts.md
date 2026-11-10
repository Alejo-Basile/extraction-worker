# Alertas operativas del extraction-worker (S6-P3-02)

Umbrales propuestos para las métricas de `/metrics` (Prometheus). Cada alerta
incluye el disparador, la acción de triage y la prueba provocada que la
ejercita en los tests de integración. Los umbrales son un punto de partida:
ajustar según el tráfico real del laboratorio antes de llevarlos a producción.

## Resumen

| Alerta | Expresión (PromQL) | Para | Severidad |
|---|---|---|---|
| `BreakerAbierto` | `max by (dependency) (worker_breaker_open) == 1` | 2m | critical |
| `ModoDegradado` | `worker_degraded == 1` | 2m | warning |
| `BulkheadSaturado` | `worker_inflight >= worker_bulkhead_capacity` | 5m | warning |
| `TasaDeErrorAlta` | `sum(rate(worker_documents_total{outcome=~"rejected\|deferred"}[5m])) / sum(rate(worker_documents_total[5m])) > 0.05` | 10m | warning |
| `DlqNoVacia` | `worker_dlq_depth > 0` | 5m | critical |
| `DlqEnvejece` | `worker_dlq_oldest_age_seconds > 3600` | 10m | warning |
| `ConsumidorSinAck` | `worker_pel_pending > 0 and worker_dead_consumers > 0` | 5m | critical |

## 1. `BreakerAbierto`

- **Disparador**: `worker_breaker_open{dependency=~"minio|mongo|redis"} == 1`
  sostenido. `dependency` sale del breaker real, no de un flag: en MinIO un
  cooldown de 30s tras 5 fallos consecutivos de infraestructura.
- **Acción**: identificar la dependencia; revisar disponibilidad del stack
  (`labs/docker-compose.yml`), logs `modo degradado` y el runbook de la
  dependencia. El worker se recupera solo al vencer el cooldown y pasar el
  probe; no reiniciarlo de entrada.
- **Prueba provocada**: `crates/worker/tests/degraded_it.rs` corta MinIO con un
  proxy TCP y verifica `worker_breaker_open{dependency="minio"} == 1` durante
  la caída y `== 0` tras la recuperación. Unitarios:
  `resilience::tests::breaker_reporta_estado_para_metricas`.

## 2. `ModoDegradado`

- **Disparador**: `worker_degraded == 1` sostenido (S6-P3-03). Es el estado en
  que el consumer **pausa** la lectura de mensajes nuevos y el `XAUTOCLAIM`
  para no quemar delivery counts de una dependencia caída.
- **Acción**: es la consecuencia esperada de `BreakerAbierto` sobre MinIO o
  Mongo. Verificar que el stream acumule lag (`worker_stream_depth`) sin que
  crezca `worker_dlq_depth`: eso confirma que el gate está protegiendo los
  mensajes. No escalar hasta que expire el cooldown (30s por defecto).
- **Prueba provocada**: `degraded_it`: con el modo en 1 los mensajes nuevos
  quedan `QUEUED` en Mongo, la PEL mantiene delivery count 1 y la DLQ queda
  vacía; al volver MinIO todo completa.

## 3. `BulkheadSaturado`

- **Disparador**: `worker_inflight` pegado al límite del bulkhead
  (`WORKER_MAX_CONCURRENCY`). Se documenta como alerta aunque el test de
  saturación lenta se recortó del alcance de #25 (Plan B): el bulkhead ya está
  cubierto de forma unitaria y el indicador es directo.
- **Acción**: si además hay `worker_degraded == 0` y `worker_stream_depth`
  creciendo, el worker está al límite: escalar réplicas o subir
  `WORKER_MAX_CONCURRENCY` con cuidado (cada permiso es una extracción
  concurrente). Si coincide con `ModoDegradado`, es la protección esperada, no
  una fuga de trabajos.
- **Prueba provocada**: `worker_inflight` se actualiza en cada refresco de
  gauges y se observa en `degraded_it` (vuelve a 0 tras completar el lote);
  el bulkhead en sí tiene cobertura unitaria en
  `consumer::pipeline` y `runner` (semáforo y `max_concurrency`).

## 4. `TasaDeErrorAlta`

- **Disparador**: proporción de `worker_documents_total{outcome="rejected"}`
  (documento) + `{outcome="deferred"}` (infraestructura) sobre el total, por
  encima del 5% durante 10m.
- **Acción**: separar por `outcome` y `cause` de `worker_dlq_total`. Muchos
  `rejected` con `PDF_CORRUPT`/`PDF_NO_TEXT` es un problema de origen
  (ver `docs/error-model.md`); muchos `deferred` apunta a infraestructura
  (correlacionar con `worker_breaker_open`).
- **Prueba provocada**: `degraded_it` genera `deferred` durante la caída y
  verifica que no terminen en DLQ; `tests/redis_it.rs` y `tests/minio_it.rs`
  cubren los caminos de documento e infraestructura por separado.

## 5. `DlqNoVacia` y `DlqEnvejece`

- **Disparador**: `worker_dlq_depth > 0` (5m) o
  `worker_dlq_oldest_age_seconds > 3600` (10m).
- **Acción**: inspeccionar con `--inspect-dlq` y reencolar con `--replay-dlq`
  (ver runbook / ADR de DLQ). Una DLQ que no se drena indica un problema
  sistemático, no un documento suelto.
- **Prueba provocada**: `tests/redis_it.rs` (movimiento atómico con causa y
  ACK), `tests/replay_it.rs` (replay idempotente y auditoría) y
  `docs/pdf-corpus.md` (corpus de PDFs hostiles que la alimentan).
  `degraded_it` verifica el complemento: una caída de infraestructura **no**
  debe incrementar `worker_dlq_depth` (`XLEN == 0` al final).

## 6. `ConsumidorSinAck`

- **Disparador**: `worker_pel_pending > 0` junto a
  `worker_dead_consumers > 0` sostenidos. Un consumidor con pendientes cuya
  última señal supera `WORKER_DEAD_CONSUMER_MS` está sin latido; el
  `XAUTOCLAIM` de los vivos debe reclamar sus mensajes.
- **Acción**: revisar si hay instancias muertas del worker sin apagado limpio
  (CrashLoop, OOM). Los mensajes huérfanos se recuperan solos vía
  `XAUTOCLAIM` en cuanto pase `min-idle-time`; no hace falta intervención
  manual salvo que `worker_pel_pending` no baje.
- **Prueba provocada**: `tests/replay_it.rs` y `tests/redis_it.rs` ejercitan
  la PEL y el autoclaim; los unitarios de `group::dead_consumers` cubren el
  umbral de antigüedad. `degraded_it` prueba que el `XAUTOCLAIM` no incrementa
  delivery counts mientras el consumer está degradado.

## Notas de implementación

- Los gauges se refrescan cada `WORKER_METRICS_INTERVAL_SECS` (15s por
  defecto). Los contadores son acumulativos: usar `rate()` en las alertas.
- `worker_breaker_open` cubre `redis`, `mongo` y `minio`; `worker_degraded`
  solo MinIO/Mongo (S6-P3-03: sin Redis el read ya falla y reintentar es el
  único camino).
- Ninguna métrica lleva `document_id`/`correlation_id` (cardinalidad); el
  detalle por documento va a logs.
