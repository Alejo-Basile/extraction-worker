# Reporte de seguridad y dependencias — extraction-worker (P3-A2)

- **Fecha de revisión:** 2026-11-16
- **Responsable:** equipo `extraction-worker` (cierre P3 / A2)
- **Próxima revisión:** 2027-02-16 o ante cualquier cambio de `Cargo.lock`,
  `deny.toml`, `actix-http` o de la imagen base.
- **Alcance:** worker de extracción y su imagen Docker. No incluye
  `doc-service`, `infra` ni `platform-services`.

## Resumen ejecutivo

| Área | Resultado |
|---|---|
| Vulnerabilidades críticas/altas | 0 |
| Vulnerabilidades medias | 0 |
| Vulnerabilidades bajas aceptadas | 1 (`RUSTSEC-2026-0258`, h2) |
| Advisories de mantenimiento aceptados | 3 (`backoff`, `instant`, `ttf-parser`) |
| Secretos detectados | 0 |
| Licencias no permitidas | 0 |
| CVEs `CRITICAL` con fix en la imagen | 0 (gate de CI) |
| Dependencias duplicadas | 21 nombres, solo warnings de `cargo deny` |

No hay hallazgos críticos sin mitigar. Las dependencias con advisories
informativos se aceptan con justificación y disparador de salida; la única
vulnerabilidad real (`h2`) es de severidad baja y la superficie expuesta del
worker es de solo lectura.

## Superficie y activos

- **HTTP:** `/healthz`, `/readyz` y `/metrics`, expuestos por Actix-web en el
  puerto interno. No hay API pública ni CRUD; no se procesan cuerpos de request.
- **Datos:** PDFs no confiables desde MinIO, texto extraído hacia
  `extracted-md`, estado en Mongo y mensajes en Redis Streams.
- **Secretos:** solo por entorno (`REDIS_URL`, `MONGO_WORKER_URI`, credenciales
  S3). El repo no contiene secretos reales; `.env.example` usa valores ficticios.
- **Contenedor:** usuario no-root `10001:10001`, healthcheck propio y límites de
  despliegue definidos en `infra`.

## Herramientas y comandos

| Control | Comando | Resultado (2026-11-16) |
|---|---|---|
| Advisories, bans y licencias | `cargo deny check bans licenses advisories` | `advisories ok, bans ok, licenses ok` |
| Vulnerabilidades Rust | `./scripts/security-audit.sh` | 0 vulnerabilidades, 0 warnings |
| Lints de seguridad | `cargo clippy --all-targets --all-features -- -D warnings` | limpio |
| Secretos | `gitleaks git .` (imagen oficial, historial completo) | 175 commits, `no leaks found` |
| Imagen | job `image-scan` de CI y Trivy local (`--severity CRITICAL --ignore-unfixed`) | 0 CRITICAL (local) / verde (CI) |

`scripts/security-audit.sh` toma la lista de advisories ignorados directamente de
`deny.toml`; `ci.yml` usa ese script. Así la lista canónica vive en un solo
archivo y no se desincroniza entre local y CI.

Para reproducir `cargo audit` en local cuando `cargo-deny` ya creó su base:

```bash
DB=$(find ~/.cargo/advisory-db -maxdepth 1 -type d -name 'advisory-db-*' | head -1)
./scripts/security-audit.sh --db "$DB" --no-fetch
```

## Hallazgos por severidad

### Baja — `RUSTSEC-2026-0258`: h2 unbounded empty DATA frames

- **Paquete:** `h2 0.3.27` (transitivo de `actix-http 3.20.3`).
- **Impacto:** un cliente HTTP/2 podría encolar frames `DATA` vacíos sin límite
  y agotar memoria si el stream no se drena. La advisory lo clasifica como
  **low severity**.
- **Estado:** aceptado temporalmente.
- **Justificación:** el worker usa `HttpServer::bind` de Actix-web, que crea el
  servicio con `.tcp()` (HTTP/1.x), no con `.listen_auto_h2c()` ni TLS/ALPN. El
  listener solo publica `GET`/`HEAD` de health/metrics y no procesa cuerpos, así
  que `h2 0.3.27` no está en el camino de request actual. El riesgo se reduce
  además limitando el acceso al puerto a la red interna del cluster.
- **Salida:** actualizar `actix-http` a una versión que dependa de `h2 >=
  0.4.16`; en ese momento retirar el ignore de `deny.toml`.
- **Disparador de revisión:** release de `actix-http` con h2 0.4, o antes si se
  habilita HTTP/2/TLS en el worker.

### Informativo — `RUSTSEC-2025-0012`: `backoff` sin mantenimiento

- **Paquete:** `backoff 0.4.0` (dependencia directa).
- **Impacto:** sin vulnerabilidad conocida; la crate no recibe correcciones.
- **Uso:** retroceso exponencial del reconnect de Redis. Está acotado y no
  procesa datos no confiables.
- **Mitigación:** reemplazar por `backon` o por un helper interno con la misma
  política en el próximo refactor del reconnect (A1 o tarea inmediata).
- **Disparador:** ese refactor o cualquier advisory de seguridad sobre `backoff`.

### Informativo — `RUSTSEC-2024-0384`: `instant` sin mantenimiento

- **Paquete:** `instant 0.1.13`, transitivo de `backoff`.
- **Impacto:** sin vulnerabilidad conocida; el autor recomienda `web-time`.
- **Mitigación:** desaparece al retirar `backoff`; no se agrega dependencia
  nueva solo para este reemplazo.

### Informativo — `RUSTSEC-2026-0192`: `ttf-parser` sin mantenimiento

- **Paquete:** `ttf-parser 0.25.1`, transitivo de `lopdf`/`pdf-extract`.
- **Impacto:** sin vulnerabilidad conocida. Se usa en el parseo de fuentes
  embebidas en PDFs.
- **Mitigación:** el worker ya aplica timeout duro y topes de páginas/salida;
  el corpus hostil (A3) cubre PDFs malformados. A mediano plazo, evaluar el
  aislamiento del parseo (ADR P3-FUTURE-01) o una versión de `lopdf` que migre
  a `skrifa`.
- **Disparador:** actualización de `lopdf`/`pdf-extract` o resultado del ADR de
  aislamiento.

## Advisories retirados de la lista de ignores

- `RUSTSEC-2026-0382` (`rustls-webpki`): **corregido**, no aceptado. Se actualizó
  `rustls-webpki 0.103.15 → 0.103.16` en `Cargo.lock`; el ignore se eliminó.
- `RUSTSEC-2026-0346`, `-0347`, `-0348`, `-0349` y `-0384`: ya no aplican al
  `Cargo.lock` actual. Se eliminaron para evitar supresiones obsoletas.

## Duplicados y licencias

`cargo deny` reporta 21 nombres de crate con más de una versión. Son warnings de
`[bans] multiple-versions = "warn"`, no vulnerabilidades; provienen de las
grafo transitivo de `mongodb`/`bson`, `backoff`, `actix-http`/`h2` y `rustls`:

```text
base64, block-buffer, bson, core-foundation, cpufeatures, crypto-common,
digest, h2, hashbrown, http, md-5, rand, rand_chacha, rand_core, r-efi,
sha2, syn, thiserror, thiserror-impl, windows-sys, getrandom
```

No hay licencias restrictivas. Todas las licencias del árbol están en la
allowlist de `deny.toml`. La única excepción es `CDLA-Permissive-2.0`, aplicada a
los datos de `webpki-root-certs` (raíces TLS de Mozilla distribuidas como datos,
no como código). `cargo deny` también avisa de entradas de la allowlist que el
árbol actual no usa; son warnings de higiene, no hallazgos de licencia.

## Plan de mitigación

| Acción | Prioridad | Responsable | Disparador |
|---|---|---|---|
| Retirar el ignore de h2 al actualizar `actix-http` | P2 | maintainers | release con h2 >= 0.4.16 |
| Reemplazar `backoff` por `backon` o helper interno | P2 | A1 / worker | próximo refactor del reconnect |
| Re-ejecutar `./scripts/security-audit.sh` y `cargo deny` | P0 | CI | cada PR que cambie `Cargo.lock` |
| Mantener `gitleaks` en pre-commit y CI | P1 | CI | cada PR/push |
| Escanear la imagen con Trivy (`CRITICAL`, `--ignore-unfixed`) | P1 | CI | cada build de imagen |
| Ampliar corpus hostil y evaluar aislamiento del parseo | P1/P2 | A3 / P3-FUTURE-01 | cierre P3 / ADR |

## Criterio de cierre A2

- Reporte publicado y enlazado desde `README.md`.
- `deny.toml` y `ci.yml` comparten la misma lista de ignores vía
  `scripts/security-audit.sh`.
- `cargo deny`, `cargo audit`, `clippy`, `gitleaks` y el escaneo de imagen en CI
  verdes.
- Sin vulnerabilidades críticas o altas sin mitigar.
