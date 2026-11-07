# Modelo de errores del extraction-worker (S6-P3-04)

Este es el contrato operativo para triagear un documento fallido. La fuente de
verdad de los códigos de documento es `extraction_core::error_code`; la
clasificación `Document | Transient | Fatal | CircuitOpen` vive en
`crates/worker/src/consumer/resilience.rs`.

## Reglas

- **Documento (`Document` / `business`)**: el PDF o el mensaje es el problema.
  No se reintenta y no cuenta para el breaker. Va a la DLQ y el documento se
  cierra con transición condicional.
- **Infraestructura (`Transient`)**: red, timeout o dependencia caída. El
  mensaje queda pendiente en la PEL y `XAUTOCLAIM` lo reintentará.
- **Configuración (`Fatal`)**: credenciales, permisos o protocolo inesperado.
  No se reintenta ese intento; si el mensaje ya agotó intentos, va a la DLQ.
- **Circuito abierto (`CircuitOpen`)**: rechazo local sin tocar la red. El
  mensaje queda pendiente; el breaker se recupera solo.
- Toda transición a estado terminal es **condicional** y deja entrada en
  `status_history`. Una transición no declarada es un bug del worker y no se
  "corrige" escribiendo el estado.

## Destino del documento

| Causa | Transición Mongo | Objeto `.txt` |
|---|---|---|
| Éxito | `PROCESSING -> COMPLETED` | queda en `extracted-txt` |
| Error de documento / negocio | `PROCESSING -> REJECTED` | no se sube |
| Reintentos de infraestructura agotados | `PROCESSING -> RETRYING`, luego `RETRYING -> EXTRACTION_FAILED` | se compensa al ir a DLQ |
| `mark_terminal` inválido | no aplica | según estado |

## Códigos de documento (DLQ)

| Código | Significado | ¿Se reintenta? | Acción |
|---|---|---|---|
| `INVALID_CONTRACT` | El mensaje no cumple el contrato o trae campos extra | no | revisar el relay/producer |
| `UNSUPPORTED_SCHEMA_VERSION` | `schema_version` no es la soportada | no | actualizar el producer o el worker |
| `PDF_CORRUPT` | PDF no parseable, 0 bytes, no-PDF, fuente malformada o panic del parser | no | pedir re-subida o revisar el origen |
| `PDF_ENCRYPTED` | PDF cifrado y el worker no tiene contraseña | no | obtener contraseña o convertir a PDF plano |
| `PDF_NO_TEXT` | PDF sin capa de texto (escaneado) | no | OCR fuera del pipeline o marcar como no procesable |
| `PDF_TOO_LARGE` | Objeto supera `WORKER_MAX_PDF_BYTES` | no | subir versión reducida o ajustar límite |
| `PDF_TOO_MANY_PAGES` | PDF supera `WORKER_MAX_PAGES` | no | dividir el documento o ajustar límite |
| `PDF_OUTPUT_TOO_LARGE` | Texto extraído supera `WORKER_MAX_OUTPUT_BYTES` | no | revisar el PDF o ajustar límite |
| `MAX_ATTEMPTS_EXCEEDED` | Se agotó `WORKER_MAX_ATTEMPTS` por fallos de infraestructura | no (para ese intento) | revisar dependencia y usar replay de DLQ |

## Códigos de infraestructura

| Código | Dependencia | Clase | Acción |
|---|---|---|---|
| `JOB_TIMEOUT` | job completo | transient | verificar PDF hostil y CPU; ajustar `WORKER_JOB_TIMEOUT_SECS` si es sistémico |
| `TMP_CREATE` | disco local | transient | revisar espacio/permisos del tmpfs |
| `S3_CONFIG` | MinIO | fatal | corregir credenciales/endpoint |
| `S3_AUTH` | MinIO | fatal | corregir credenciales y permisos del bucket |
| `S3_IO` | MinIO | transient | verificar MinIO/red; deja mensaje pendiente |
| `S3_TIMEOUT` | MinIO | transient | verificar latencia; ajustar `S3_OP_TIMEOUT_MS` si es sistémico |
| `S3_TMP_FILE` | disco local | transient | revisar espacio y permisos |
| `EXTRACT_JOIN` | tarea de extracción | transient | panic o abort del bloqueo CPU-bound; revisar logs |
| `MONGO_CONFIG` | Mongo | fatal | corregir URI/configuración |
| `MONGO_AUTH` | Mongo | fatal | corregir credenciales y rol del worker |
| `MONGO_IO` | Mongo | transient | verificar réplica set/red; deja mensaje pendiente |
| `MONGO_TIMEOUT` | Mongo | transient | verificar latencia; ajustar `MONGO_TIMEOUT_MS` si es sistémico |
| `CONTRACT_TRANSITION` | reglas de estado | fatal | bug del worker o de la tabla de transiciones; revisar código |
| `REDIS_IO` / `REDIS_UNKNOWN` | Redis | transient | verificar Redis/red; deja mensaje pendiente |
| `REDIS_TIMEOUT` | Redis | transient | revisar latencia o `WORKER_BLOCK_MS` |
| `REDIS_CONFIG` | Redis | fatal | corregir URL/credenciales |
| `REDIS_PROTOCOL` | Redis | fatal | revisar versión y comandos soportados |
| `REDIS_SERVER` | Redis | transient o fatal según error del servidor | revisar clúster/permisos y el mensaje exacto |
| `DLQ_SERIALIZE` | serialización DLQ | fatal | bug del contrato; revisar `DlqMessage` |

## Operación

- Para inspeccionar los pendientes y la DLQ usar `--inspect-pel` y
  `--inspect-dlq`.
- Para reemitir un caso que se quiere reprocesar usar `--replay-dlq`; antes
  ajusta el estado Mongo a un estado reclamable con
  `PROCESSING -> RETRYING` (actor `operator`).
- No borrar objetos de `extracted-txt` a mano salvo con el flujo de
  compensación o el reconciliador.
