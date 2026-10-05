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

- **Local (primera capa):** instalar el hook de pre-commit **una vez** en este clone:
  `gitleaks install` (el hook vive en `.git/hooks`, no se versiona).
- **CI (segunda capa):** el job `gitleaks` (`.github/workflows/gitleaks.yml`)
  escanea el diff de cada PR y cada push a `main`.

## Gobernanza

- Rama `main` protegida por ruleset: push directo y force push denegados, PR obligatorio con CI en verde y al menos 1 aprobación del code owner (`@BenjaDiaZzZ`).
- Plantilla de PR obligatoria: *qué cambia · por qué · cómo se prueba · cómo se revierte*.
