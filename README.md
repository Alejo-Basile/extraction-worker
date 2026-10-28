# extraction-worker

Extraction Worker Service — servicio en **Rust (Tokio + Actix-web)** del proyecto de migración a microservicios.

> Especificación arquitectónica (fuente única de verdad): [`infra/docs/SPEC.md`](https://github.com/Alejo-Basile/infra/blob/main/docs/SPEC.md) v2.1

## Qué hace este repo

- Consume mensajes del stream `stream:pdf-processing` (consumer group `extraction-workers`, `XREADGROUP`/`XAUTOCLAIM`, SPEC §5.4).
- Descarga el PDF desde MinIO, valida la cabecera `%PDF-` por rango (SPEC §9) y extrae el texto con los límites de parseo de SPEC §3.3 (timeout duro, topes de páginas/salida).
- Sube el `.txt` a MinIO y escribe el **estado terminal en Mongo** con transición condicional atómica (ADR-0006, SPEC §6.3).
- Maneja reintentos (contador = `XPENDING` delivery count) y mueve a la DLQ con `XACK` + `XADD` atómico (SPEC §5.4).
- Expone `/healthz`, `/readyz`, `/metrics` vía Actix-web (solo health y métricas, SPEC §3.3).

## Qué NO hace este repo

- **No tiene API pública ni CRUD de documentos**: eso es `doc-service/` (Go). Cero llamadas síncronas entre ambos (SPEC §1.3).
- **No define la cola ni la política de Redis/MinIO**: eso es configuración de `infra/`.
- **No contiene secretos reales**: solo `.env.example` con valores ficticios (repo público).

## Stack y estructura

- Rust (canal fijado en `rust-toolchain.toml`), Tokio, Actix-web, cliente Redis Sentinel-aware (SPEC §3.5), `pdf-extract`/`lopdf`.
- `Cargo.lock` **se commitea** (es aplicación, no librería).
- `docs/adr/`: ADRs **locales** del servicio. Las ADRs **globales** viven en `infra/docs/adr/`.

## Escaneo de secretos (gitleaks, S0-P1-04)

- **Local (primera capa):** instalar el hook de pre-commit **una vez** en este clone
  (gitleaks no tiene comando `install`; el hook es un `.git/hooks/pre-commit` que
  ejecuta `gitleaks git --staged`):

  ```bash
  printf '#!/usr/bin/env bash\nexec gitleaks git --staged --verbose\n' \
    > .git/hooks/pre-commit && chmod +x .git/hooks/pre-commit
  ```
- **CI (segunda capa):** el job `gitleaks` (`.github/workflows/gitleaks.yml`)
  escanea el diff de cada PR y cada push a `main`.

## Observabilidad

- **`/healthz`**: el proceso está vivo (liveness, sin tocar dependencias).
- **`/readyz`**: verifica dependencias reales en paralelo — `PING` a Redis,
  `head` de una clave sonda en MinIO y `PING` a Mongo. Responde `200` si las
  tres están OK y `503` con el detalle por dependencia.
- **`/metrics`**: exposición Prometheus. Etiquetas de baja cardinalidad
  (ruta/estado); nunca `document_id`, `correlation_id` ni `object_key`.
- La composición `Retry → Circuit Breaker → Timeout` por dependencia, sus
  umbrales y la taxonomía de fallos están en
  [`docs/adr/ADR-0007-composicion-resiliencia.md`](docs/adr/ADR-0007-composicion-resiliencia.md).

## Rollback / runbook

- **Rollback de una versión:** desplegar de nuevo la imagen/tag anterior del
  worker. El servicio es stateless; el estado vive en Redis, MinIO y Mongo, y
  las transiciones de Mongo son condicionales (ADR-0006), por lo que volver a
  una versión previa no corrompe documentos en curso.
- **Rollback de un cambio en `main`:** `git revert <sha-del-squash-merge>` y
  dejar que CI valide la reversión (la rama `main` está protegida; no se hace
  push directo).
- **Ante una dependencia caída:** revisar `/readyz` para identificar cuál
  falla. Con el breaker abierto el worker rechaza localmente y deja los
  mensajes pendientes en la PEL; al recuperarse la dependencia se reintentan
  sin intervención.
- **Mensajes trabados en la PEL:** `cargo run -- --inspect-pel` lista
  consumidores, antigüedad y cantidad de entregas pendientes.

## Gobernanza

- Rama `main` protegida por ruleset: push directo y force push denegados, PR obligatorio con CI en verde y al menos 1 aprobación del code owner (`@BenjaDiaZzZ`).
- Plantilla de PR obligatoria: *qué cambia · por qué · cómo se prueba · cómo se revierte*.
