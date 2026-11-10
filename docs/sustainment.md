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

## Reproducir

```bash
cd labs && cp .env.example .env && docker compose up -d
cd ..
cargo test --all-features --test sustainment_it -- --ignored --nocapture
```

El test imprime una línea `sostenimiento: docs=... rss_base=... ...` con las
medidas de la corrida. Limpia sus propios streams, documentos y objetos al
terminar (ids únicos por corrida). Corre también en el job `integration` de CI.

## Notas

- `worker_stream_depth` es `XLEN`: las entradas ACK-eadas permanecen en el
  stream hasta que se recorte (`XTRIM`), así que **no** es un indicador de lag.
  El drenaje se verifica con `worker_pel_pending` y con el conteo de
  `COMPLETED` en Mongo.
- En un runner de CI más lento el tiempo de pared sube, pero los criterios
  (delta de RSS y de descriptores) no dependen del throughput.
- Si se necesita un volumen mayor, subir `DOCS` en el test; el muestreo y los
  umbrales se mantienen.
