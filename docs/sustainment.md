# Test de sostenimiento (S5-P3-06)

Objetivo: comprobar que el worker procesa un volumen sostenido de documentos
sin fugas de memoria ni de descriptores, y que drena la cola sin dejar nada en
la DLQ.

## Método

`crates/worker/tests/sustainment_it.rs` (marcado `#[ignore]`) lanza el binario
real contra el stack de labs y procesa **1000 documentos** con tamaños mixtos
(1, 5 y 15 páginas, rotando). Los PDFs se generan en el momento (misma técnica
que el corpus de S5-P3-05), se siembran en MinIO y los documentos en Mongo como
`QUEUED`; luego se emiten 1000 mensajes al stream.

Durante la corrida se muestrea cada 250 ms:

| Señal | Fuente |
|---|---|
| RSS | `VmRSS` de `/proc/<pid>/status` |
| Descriptores abiertos | entradas de `/proc/<pid>/fd` |
| CPU (utime + stime) | `/proc/<pid>/stat` |
| `worker_inflight`, `worker_pel_pending`, `worker_stream_depth` | `/metrics` |

Configuración del worker en el test: `WORKER_BATCH_COUNT=10`,
`WORKER_MAX_CONCURRENCY=4`, `WORKER_METRICS_INTERVAL_SECS=1`.

## Criterios y resultado

El **primer cuarto de las muestras** define la línea base (calentamiento de
pools, buffers y caché de `pdf-extract`); lo que importa es el crecimiento
**después** de esa línea base. La ventana se define por muestras y no por
cantidad de completados porque en un host rápido los 1000 documentos pueden
terminar en ~10 s.

| Criterio | Umbral | Medido (labs local) |
|---|---|---|
| Documentos completados | 1000/1000 | 1000/1000 |
| Delta de RSS pico vs línea base | < 128 MiB | **3.2 MiB** |
| RSS final vs pico | estable | 53.3 MiB = 53.3 MiB |
| Delta de descriptores (base → final) | ±3 | **0** (59 → 59) |
| DLQ | vacía | `XLEN = 0` |
| PEL | drenada | `worker_pel_pending = 0` |

Referencia de la corrida: 1000 documentos en **14.4 s** de pared (54 muestras
cada 250 ms), con **44.1 s** de CPU acumulada (4 extracciones concurrentes).
RSS: base 50.1 MiB, pico 53.3 MiB.

La memoria se estabiliza tras el calentamiento y no vuelve a crecer con el
volumen: no se observó fuga. Los descriptores quedan constantes: no se filtran
sockets ni archivos temporales (los `TempPdf` se liberan por job).

## Calibración de bulkhead y `min_idle`

La matriz de bulkhead se corrio en el host con el mismo test de 1000 documentos,
variando `SUSTAINMENT_MAX_CONCURRENCY`:

```bash
SUSTAINMENT_MAX_CONCURRENCY=1 cargo test --all-features --test sustainment_it -- --ignored --nocapture
# repetir con 2, 4 y 8
```

| `MAX_CONCURRENCY` | Docs/s | Pared | CPU | Delta RSS | Delta FDs | p95 extracción | Máximo |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 16.0 | 62.6 s | 71.0 s | 2.0 MiB | 0 | 73.5 ms | 260.5 ms |
| 2 | 27.9 | 35.8 s | 72.5 s | 3.1 MiB | 0 | 84.2 ms | 349.7 ms |
| 4 | **44.8** | **22.3 s** | 72.4 s | 3.3 MiB | 0 | 102.6 ms | 136.9 ms |
| 8 | 57.5 | 17.4 s | 70.9 s | 4.2 MiB | +3 | 160.9 ms | 588.7 ms |

Una corrida de confirmación posterior con 4 dio 34.8 s de pared y p95 de
150.7 ms (máximo 513.8 ms); la variación del host no cambia la decisión.

**Decisión:** se mantiene `WORKER_MAX_CONCURRENCY=4`. Con 4 se duplica el
throughput respecto de 2 y se satura la CPU disponible; 8 solo mejora ~28% el
pared en el host, pero empeora p95 ~57% y lleva el delta de descriptores al
limite (±3). Bajo el limite de despliegue el beneficio marginal de 8 es menor.
La matriz se corrio en host; el valor elegido se valido ademas en el arnes
contenedorizado con `cpus=1.5`, `mem=768m` y `pids=200`:

| Validación Docker (4 PDFs / 412 páginas, `MAX_CONCURRENCY=4`) | Medido |
|---|---:|
| Documentos completados | 4/4 |
| `/readyz` 200 | 20/20 |
| `/readyz` p95 / máximo | 72.5 ms / 72.5 ms |
| `worker_pids_current` | 15 / 200 |
| `memory.peak` | 326,467,584 B (~311 MiB) |

**`WORKER_MIN_IDLE_MS`:** la fórmula vigente es
`WORKER_JOB_TIMEOUT_SECS * 2000 + 1000`; con 300 s da **601000 ms**. La
medición de extracción (p95 0.10–0.15 s, máximo <0.6 s) queda muy por debajo del
timeout duro de 300 s, por lo que el margen de 2x timeout + 1 s es seguro y se
mantiene. El arnés hostil usa el mismo valor 601000 y la variable
`WORKER_XAUTOCLAIM_INTERVAL_SECS` correcta.

## Reproducir

```bash
cd labs && cp .env.example .env && docker compose up -d
cd ..
cargo test --all-features --test sustainment_it -- --ignored --nocapture
```

El test imprime una línea `sostenimiento: docs=... rss_base=... ...` con las
medidas de la corrida y una línea `calibracion: concurrency=... p95=... max=...`
con el resumen de `worker_extraction_seconds`. Limpia sus propios streams,
documentos y objetos al terminar (ids únicos por corrida). Corre también en el
job `integration` de CI.

## Notas

- `worker_stream_depth` es `XLEN`: las entradas ACK-eadas permanecen en el
  stream hasta que se recorte (`XTRIM`), así que **no** es un indicador de lag.
  El drenaje se verifica con `worker_pel_pending` y con el conteo de
  `COMPLETED` en Mongo.
- En un runner de CI más lento el tiempo de pared sube, pero los criterios
  (delta de RSS y de descriptores) no dependen del throughput.
- Si se necesita un volumen mayor, subir `DOCS` en el test; el muestreo y los
  umbrales se mantienen.
