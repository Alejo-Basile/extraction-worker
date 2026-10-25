# Stack de desarrollo (F3a/F3b)

`docker compose` con MinIO (buckets del pipeline, seccion 11.4) y MongoDB 7
como Replica Set `rs0` (requisito del Change Stream del relay, S1-P2-06).

## Levantar

```bash
cd labs
cp .env.example .env          # credenciales de laboratorio (gitignored)
docker compose up -d
docker compose ps             # esperar healthchecks en verde
```

Servicios:

| Servicio | Puerto | Uso en F3a | Uso en F3b |
|---|---|---|---|
| `minio` | 9000 (API), 9001 (consola) | `raw-pdfs` (GET) y `extracted-txt` (PUT) | idem |
| `mongo` (rs0) | 27017 | preparado, sin uso | transicion condicional `PROCESSING -> COMPLETED` (S2-P3-08) |

`minio-init` y `mongo-init` son one-shot: crean buckets (`raw-pdfs`,
`extracted-txt`, anonimo deshabilitado) e inician el Replica Set.

## Variables que consume el worker

```bash
export S3_ENDPOINT=http://127.0.0.1:9000
export S3_ACCESS_KEY=worker-dev
export S3_SECRET_KEY=worker-dev-secret
# buckets con default: raw-pdfs / extracted-txt (S3_BUCKET_RAW, S3_BUCKET_TXT)
```

## Probar

```bash
# tests de integracion (#[ignore]) contra el stack levantado
cargo test --all-features -- --ignored
```

## Notas

- Credenciales de laboratorio, sin TLS ni ACL: **nunca** en produccion.
- MinIO retiro sus imagenes y binarios publicos (Docker Hub, quay, dl.min.io
  responden 410); el lab usa el build congelado `bitnamilegacy/minio`
  (Ultima via publica disponible). Los buckets se crean con
  `MINIO_DEFAULT_BUCKETS`; para inspeccionar:
  `docker run --rm --network extraction-worker-dev_default
  bitnamilegacy/minio-client:2025.7.21-debian-12-r3 /bin/bash -c
  "mc alias set chk http://minio:9000 worker-dev worker-dev-secret && mc ls chk"`.
- `.env` esta en `.gitignore` (`*.env`); solo se commitea `.env.example`.
- Volumenes nombrados (`minio-data`, `mongo-data`): los datos sobreviven a
  `docker compose down` (sin `-v`). Para reiniciar de cero:
  `docker compose down -v`.
