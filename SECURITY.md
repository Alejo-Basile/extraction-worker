# Política de Seguridad

## Reporte de vulnerabilidades

Este repositorio es público. Si encuentras una vulnerabilidad o un secreto expuesto:

1. **No** abras una issue pública.
2. Usa la pestaña **Security → Report a vulnerability** (GitHub Private Vulnerability Reporting), o contacta a los maintainers: `@BenjaDiaZzZ` (dueño) y `@Alejo-Basile` (plataforma).
3. Incluye: archivo/commit afectado, tipo de exposición y pasos para reproducir.

## Reglas permanentes

- Ningún secreto real entra al repo: solo `.env.example` con valores ficticios (SPEC §9).
- Si un secreto llega a un commit: revocar inmediatamente, purgar historial y rotar credenciales.
- Dockerfiles: imagen base con pin por digest, multi-stage build, `USER` no-root.

## Gate de imágenes (S9-X-04)

El job `image-scan` de `.github/workflows/ci.yml` construye y escanea **las dos** imágenes del
repositorio (`extraction-worker` y `ocr-worker`) con Trivy sobre `CRITICAL,HIGH` y `--exit-code 1`.
No se usa `--ignore-unfixed`, `continue-on-error` ni supresión implícita: cualquier CVE crítica o
alta nueva rompe el build.

Estado verificado el 2026-11-18 con Trivy (scan crudo, `--ignorefile /dev/null`):

| Imagen | Base | CRITICAL | HIGH | IDs aceptados |
|---|---|---|---|---|
| `extraction-worker:latest` | `debian:13.7-slim` | 0 | 106 | 27 |
| `ocr-worker` (digest C27) | `debian:13.7-slim` (Tesseract + Poppler) | 1 | 149 | 51 |

Las CVEs son de los paquetes del runtime Debian (tesseract, poppler, imagemagick, libssl) y de
binarios vendorizados, no del código del crate. Están aceptadas una por una en `.trivyignore`
(owner P1, review 2026-12-01, mitigación: contenedor no-root 10001:10001, rootfs read-only,
CapDrop=ALL, no-new-privileges, solo redes internas). Una CVE nueva no listada hace fallar el job.
El backlog de remediación es el upgrade de la base Debian y de sus paquetes, con fecha 2026-12-01.

`cargo audit` y `cargo deny check advisories` cubren las dependencias Rust; los `ignore` vigentes
de `deny.toml` siguen el mismo criterio (owner, fecha, mitigación).
