#!/usr/bin/env bash
# Arnés de carga hostil para S5-P1-01.
#
# Reproduce la corrida de contención del worker:
#   - 4 PDFs de ~25-28 MiB / 412 páginas (400 de texto + 12 de imagen),
#   - documentos QUEUED en Mongo, objetos en MinIO y mensajes en un stream aislado,
#   - worker con pids_limit=200, mem_limit=768m, cpus=1.5 y MAX_CONCURRENCY=4,
#   - sonda de /readyz cada 250 ms desde dentro del contenedor.
#
# Uso:
#   cd labs
#   ./hostile_load_harness.sh
#
# Variables:
#   WORKER_IMAGE            imagen del worker (default: ghcr.io/alejo-basile/extraction-worker:latest)
#   OUT_DIR                 directorio de salida (default: mktemp -d)
#   KEEP=1                  conserva el contenedor del worker para inspección
#   PDF_COUNT=4             cantidad de PDFs
#   TEXT_PAGES=400          páginas de texto por PDF
#   IMAGE_PAGES=12          páginas de imagen por PDF
#   LOAD_TIMEOUT_SECS=600   tope de espera de extracción
#   HOST_PORT=              puerto host opcional para /healthz y /metrics
#
# Requisitos: docker + compose v2, python3 con PIL, gs, curl, jq, pdfinfo.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
ENV_FILE="$SCRIPT_DIR/.env"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"
PROJECT_NAME="extraction-worker-dev"
NETWORK="${PROJECT_NAME}_default"
MC_BIN="/opt/bitnami/minio-client/bin/mc"

WORKER_IMAGE="${WORKER_IMAGE:-ghcr.io/alejo-basile/extraction-worker:latest}"
WORKER_NAME="${WORKER_NAME:-worker-hostile-load}"
PDF_COUNT="${PDF_COUNT:-4}"
TEXT_PAGES="${TEXT_PAGES:-400}"
IMAGE_PAGES="${IMAGE_PAGES:-12}"
IMAGE_W="${IMAGE_W:-1260}"
IMAGE_H="${IMAGE_H:-1780}"
IMAGE_QUALITY="${IMAGE_QUALITY:-92}"
READY_INTERVAL_SECS="${READY_INTERVAL_SECS:-0.25}"
LOAD_TIMEOUT_SECS="${LOAD_TIMEOUT_SECS:-600}"
HOST_PORT="${HOST_PORT:-}"
MAX_PDF_BYTES="${MAX_PDF_BYTES:-31457280}"
KEEP="${KEEP:-0}"

RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)-$$}"
STREAM="${STREAM:-stream:hostile-load-$RUN_ID}"
GROUP="${GROUP:-extraction-workers}"
DLQ_STREAM="${DLQ_STREAM:-$STREAM-dlq}"
OUT_DIR="${OUT_DIR:-$(mktemp -d "${TMPDIR:-/tmp}/hostile-load-$RUN_ID.XXXXXX")}"
PDF_DIR="$OUT_DIR/pdfs"
REPORT="$OUT_DIR/hostile-load-report.txt"

log() { printf '[hostile-load] %s\n' "$*"; }
fail() { printf '[hostile-load] ERROR: %s\n' "$*" >&2; exit 1; }

compose() { docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" "$@"; }
service_cid() { compose ps -q "$1" 2>/dev/null || true; }

cleanup() {
    docker exec "$WORKER_NAME" touch /tmp/readyz-stop >/dev/null 2>&1 || true
    if [ "$KEEP" != "1" ]; then
        docker rm -f "$WORKER_NAME" >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

require() {
    command -v "$1" >/dev/null 2>&1 || fail "falta $1"
}
for tool in docker python3 gs curl jq pdfinfo; do
    require "$tool"
done
docker compose version >/dev/null 2>&1 || fail "falta el plugin docker compose v2"
python3 -c 'import PIL' >/dev/null 2>&1 || fail "falta Pillow en python3"

# --- Dependencias del lab -----------------------------------------------------
if [ ! -f "$ENV_FILE" ]; then
    log "no existe $ENV_FILE; copiando .env.example"
    cp "$SCRIPT_DIR/.env.example" "$ENV_FILE"
fi
set -a
# shellcheck disable=SC1090
. "$ENV_FILE"
set +a
: "${MINIO_ROOT_USER:?falta MINIO_ROOT_USER en labs/.env}"
: "${MINIO_ROOT_PASSWORD:?falta MINIO_ROOT_PASSWORD en labs/.env}"
: "${MONGO_ROOT_PASSWORD:?falta MONGO_ROOT_PASSWORD en labs/.env}"
: "${MONGO_WORKER_PASSWORD:?falta MONGO_WORKER_PASSWORD en labs/.env}"

mkdir -p "$PDF_DIR" "$OUT_DIR"

log "run_id=$RUN_ID"
log "out_dir=$OUT_DIR"
log "levantando minio/redis/mongo del lab"
compose up -d minio redis mongo mongo-init >/dev/null

wait_healthy() {
    local svc="$1" timeout="${2:-120}" i=0 cid status
    while [ "$i" -lt "$timeout" ]; do
        cid="$(service_cid "$svc")"
        if [ -n "$cid" ]; then
            status="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$cid" 2>/dev/null || true)"
            [ "$status" = "healthy" ] && return 0
        fi
        sleep 1
        i=$((i + 1))
    done
    return 1
}

wait_mongo_init() {
    local timeout="${1:-120}" i=0 cid status code
    while [ "$i" -lt "$timeout" ]; do
        cid="$(compose ps -a -q mongo-init 2>/dev/null || true)"
        if [ -n "$cid" ]; then
            status="$(docker inspect -f '{{.State.Status}}' "$cid" 2>/dev/null || true)"
            if [ "$status" = "exited" ]; then
                code="$(docker inspect -f '{{.State.ExitCode}}' "$cid" 2>/dev/null || true)"
                [ "$code" = "0" ] && return 0
                return 1
            fi
        fi
        sleep 1
        i=$((i + 1))
    done
    return 1
}

wait_healthy minio 120 || fail "minio no quedó healthy"
wait_healthy redis 60 || fail "redis no quedó healthy"
wait_healthy mongo 120 || fail "mongo no quedó healthy"
wait_mongo_init 120 || fail "mongo-init no terminó con exit 0"
docker network inspect "$NETWORK" >/dev/null 2>&1 || fail "no existe la red $NETWORK"

MINIO_CID="$(service_cid minio)"
MONGO_CID="$(service_cid mongo)"
REDIS_CID="$(service_cid redis)"
[ -n "$MINIO_CID" ] || fail "no encontré el contenedor de MinIO"
[ -n "$MONGO_CID" ] || fail "no encontré el contenedor de Mongo"
[ -n "$REDIS_CID" ] || fail "no encontré el contenedor de Redis"

# --- PDFs hostiles ------------------------------------------------------------
log "generando $PDF_COUNT PDFs de $((TEXT_PAGES + IMAGE_PAGES)) páginas"
python3 - "$PDF_DIR" "$PDF_COUNT" "$TEXT_PAGES" "$IMAGE_PAGES" \
    "$IMAGE_W" "$IMAGE_H" "$IMAGE_QUALITY" "$MAX_PDF_BYTES" <<'PY'
import json
import os
import subprocess
import sys

from PIL import Image

outdir = sys.argv[1]
count = int(sys.argv[2])
text_pages = int(sys.argv[3])
image_pages = int(sys.argv[4])
width = int(sys.argv[5])
height = int(sys.argv[6])
quality = int(sys.argv[7])
max_bytes = int(sys.argv[8])

os.makedirs(outdir, exist_ok=True)
postscript = os.path.join(outdir, "text.ps")
with open(postscript, "w", encoding="ascii") as fh:
    fh.write("%!PS-Adobe-3.0\n")
    fh.write("/Helvetica findfont 9 scalefont setfont\n")
    for page in range(1, text_pages + 1):
        fh.write("%%Page: %d %d\n" % (page, page))
        y = 800
        for line in range(60):
            text = (
                "Pagina %d linea %d: texto denso de prueba para el corpus hostil del worker. "
                % (page, line)
            ) * 2
            text = text.replace("(", "[").replace(")", "]")
            fh.write("20 %d moveto (%s) show\n" % (y, text))
            y -= 12
        fh.write("showpage\n")
    fh.write("%%EOF\n")

text_pdf = os.path.join(outdir, "text.pdf")
subprocess.run(
    ["gs", "-q", "-dBATCH", "-dNOPAUSE", "-sDEVICE=pdfwrite",
     "-sOutputFile=" + text_pdf, postscript],
    check=True,
)

sizes = {}
for n in range(1, count + 1):
    images = [
        Image.frombytes("RGB", (width, height), os.urandom(width * height * 3))
        for _ in range(image_pages)
    ]
    image_pdf = os.path.join(outdir, "images-%d.pdf" % n)
    images[0].save(
        image_pdf,
        save_all=True,
        append_images=images[1:],
        resolution=300,
        quality=quality,
    )
    pdf = os.path.join(outdir, "pdf-%d.pdf" % n)
    subprocess.run(
        ["gs", "-q", "-dBATCH", "-dNOPAUSE", "-sDEVICE=pdfwrite",
         "-sOutputFile=" + pdf, text_pdf, image_pdf],
        check=True,
    )
    os.remove(image_pdf)
    size = os.path.getsize(pdf)
    if size > max_bytes:
        raise SystemExit("%s excede el tope de %d bytes: %d" % (pdf, max_bytes, size))
    sizes["pdf-%d.pdf" % n] = size
    print("pdf-%d.pdf %d bytes" % (n, size))

os.remove(postscript)
os.remove(text_pdf)
with open(os.path.join(outdir, "pdfs.json"), "w", encoding="utf-8") as fh:
    json.dump(sizes, fh, indent=2)
PY

for n in $(seq 1 "$PDF_COUNT"); do
    pages="$(pdfinfo "$PDF_DIR/pdf-$n.pdf" | awk '/^Pages:/{print $2}')"
    [ "$pages" = "$((TEXT_PAGES + IMAGE_PAGES))" ] \
        || fail "pdf-$n.pdf tiene $pages páginas, se esperaban $((TEXT_PAGES + IMAGE_PAGES))"
done

# --- Semilla: MinIO, Mongo, Redis --------------------------------------------
log "subiendo PDFs a MinIO"
docker exec "$MINIO_CID" "$MC_BIN" alias set hostile http://127.0.0.1:9000 \
    "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null
for n in $(seq 1 "$PDF_COUNT"); do
    id="hostile-$RUN_ID-$n"
    key="raw-pdfs/$id.pdf"
    docker cp "$PDF_DIR/pdf-$n.pdf" "$MINIO_CID:/tmp/$id.pdf" >/dev/null
    docker exec "$MINIO_CID" "$MC_BIN" cp "/tmp/$id.pdf" "hostile/raw-pdfs/$key" >/dev/null
    docker exec "$MINIO_CID" "$MC_BIN" stat "hostile/raw-pdfs/$key" >/dev/null
    docker exec -u 0 "$MINIO_CID" rm -f "/tmp/$id.pdf" >/dev/null 2>&1 || true
done

log "sembrando documentos QUEUED en Mongo"
SEED_JS="$OUT_DIR/seed.js"
{
    echo 'const app = db.getSiblingDB("app");'
    echo 'const ids = [];'
    for n in $(seq 1 "$PDF_COUNT"); do
        echo "ids.push(\"hostile-$RUN_ID-$n\");"
    done
    echo 'app.documents.deleteMany({_id: {$in: ids}});'
    echo 'app.documents.insertMany(ids.map((id) => ({'
    echo '  _id: id,'
    echo '  status: "QUEUED",'
    echo '  object_key: `raw-pdfs/${id}.pdf`,'
    echo '  correlation_id: id,'
    echo '  schema_version: 1,'
    echo '  status_history: [],'
    echo '  created_at: new Date(),'
    echo '})));'
    echo 'print("seeded " + ids.length);'
} > "$SEED_JS"
SEED_REMOTE="/tmp/hostile-seed-$RUN_ID.js"
docker cp "$SEED_JS" "$MONGO_CID:$SEED_REMOTE" >/dev/null
docker exec "$MONGO_CID" mongosh --host 127.0.0.1 -u root -p "$MONGO_ROOT_PASSWORD" \
    --authenticationDatabase admin app --quiet --file "$SEED_REMOTE" \
    || fail "no se pudieron sembrar los documentos"

log "creando stream y consumer group"
docker exec "$REDIS_CID" redis-cli XGROUP CREATE "$STREAM" "$GROUP" 0 MKSTREAM \
    >/dev/null 2>&1 || true

# --- Worker ------------------------------------------------------------------
log "levantando worker $WORKER_IMAGE"
docker image inspect "$WORKER_IMAGE" >/dev/null 2>&1 || docker pull "$WORKER_IMAGE"
WORKER_REVISION="$(docker image inspect "$WORKER_IMAGE" \
    --format '{{index .Config.Labels "org.opencontainers.image.revision"}}' 2>/dev/null || true)"
MONGO_PASS_ENC="$(python3 -c 'import sys, urllib.parse; print(urllib.parse.quote(sys.argv[1], safe=""))' "$MONGO_WORKER_PASSWORD")"
MONGO_URI="mongodb://worker:${MONGO_PASS_ENC}@mongo:27017/app?authSource=app&directConnection=true"

docker rm -f "$WORKER_NAME" >/dev/null 2>&1 || true
PORT_BIND=()
if [ -n "$HOST_PORT" ]; then
    PORT_BIND=(-p "127.0.0.1:${HOST_PORT}:8081")
fi
docker run -d --name "$WORKER_NAME" --network "$NETWORK" "${PORT_BIND[@]}" \
    --pids-limit 200 --memory 768m --cpus 1.5 \
    --read-only --tmpfs /tmp:size=256m,noexec,nosuid,nodev \
    -e HTTP_PORT=8081 \
    -e REDIS_URL="redis://redis:6379/0" \
    -e REDIS_STREAM="$STREAM" \
    -e REDIS_GROUP="$GROUP" \
    -e REDIS_DLQ_STREAM="$DLQ_STREAM" \
    -e WORKER_CONSUMER_NAME="hostile-$RUN_ID" \
    -e MONGO_WORKER_URI="$MONGO_URI" \
    -e MONGO_DB=app \
    -e MONGO_TIMEOUT_MS=3000 \
    -e S3_ENDPOINT=http://minio:9000 \
    -e S3_BUCKET_RAW=raw-pdfs \
    -e S3_BUCKET_TXT=extracted-txt \
    -e S3_ACCESS_KEY="$MINIO_ROOT_USER" \
    -e S3_SECRET_KEY="$MINIO_ROOT_PASSWORD" \
    -e S3_OP_TIMEOUT_MS=60000 \
    -e WORKER_JOB_TIMEOUT_SECS=300 \
    -e WORKER_MAX_CONCURRENCY=4 \
    -e WORKER_MIN_IDLE_MS=600001 \
    -e WORKER_DEAD_CONSUMER_MS=600001 \
    -e WORKER_MAX_ATTEMPTS=5 \
    -e WORKER_MAX_PAGES=2000 \
    -e WORKER_MAX_PDF_BYTES="$MAX_PDF_BYTES" \
    -e WORKER_MAX_OUTPUT_BYTES=52428800 \
    -e WORKER_BLOCK_MS=5000 \
    -e WORKER_XAUTCLAIM_INTERVAL_SECS=30 \
    -e WORKER_METRICS_INTERVAL_SECS=1 \
    -e WORKER_SHUTDOWN_DRAIN_SECS=4 \
    -e RUST_LOG=info \
    -e LOG_FORMAT=json \
    "$WORKER_IMAGE" >/dev/null

for i in $(seq 1 60); do
    if docker exec "$WORKER_NAME" curl -fsS http://127.0.0.1:8081/healthz >/dev/null 2>&1; then
        break
    fi
    if [ "$i" = "60" ]; then
        docker logs "$WORKER_NAME" >&2 || true
        fail "el worker no respondió /healthz en 60s"
    fi
    sleep 1
done
READYZ_CODE="$(docker exec "$WORKER_NAME" curl -s -o /dev/null -w '%{http_code}' \
    http://127.0.0.1:8081/readyz || true)"
[ "$READYZ_CODE" = "200" ] || {
    docker exec "$WORKER_NAME" curl -s http://127.0.0.1:8081/readyz >&2 || true
    fail "/readyz inicial devolvió $READYZ_CODE"
}

# --- Sonda /readyz desde dentro del contenedor -------------------------------
log "iniciando sonda /readyz cada ${READY_INTERVAL_SECS}s"
docker exec -d -e READY_INTERVAL_SECS="$READY_INTERVAL_SECS" "$WORKER_NAME" sh -c '
    rm -f /tmp/readyz-probe.log /tmp/readyz-stop
    while [ ! -f /tmp/readyz-stop ]; do
        ts=$(date +%s%3N 2>/dev/null || date +%s)
        res=$(curl -s --max-time 2 -o /dev/null -w "%{http_code} %{time_total}" \
            http://127.0.0.1:8081/readyz 2>/dev/null) || res="000 0"
        echo "$ts $res" >> /tmp/readyz-probe.log
        sleep "$READY_INTERVAL_SECS"
    done
'

# --- Carga -------------------------------------------------------------------
log "encolando $PDF_COUNT mensajes en $STREAM"
for n in $(seq 1 "$PDF_COUNT"); do
    id="hostile-$RUN_ID-$n"
    key="raw-pdfs/$id.pdf"
    docker exec "$REDIS_CID" redis-cli XADD "$STREAM" '*' \
        document_id "$id" object_key "$key" correlation_id "$id" \
        enqueued_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" schema_version 1 >/dev/null
done
XLEN="$(docker exec "$REDIS_CID" redis-cli XLEN "$STREAM")"
[ "$XLEN" = "$PDF_COUNT" ] || fail "XLEN=$XLEN, se esperaban $PDF_COUNT"

log "esperando saturación del bulkhead"
SATURATED=0
for _ in $(seq 1 60); do
    inflight="$(docker exec "$WORKER_NAME" curl -s http://127.0.0.1:8081/metrics \
        2>/dev/null | awk '$1=="worker_inflight"{print $2; exit}' || true)"
    if [ -n "$inflight" ] && [ "$inflight" != "0" ]; then
        SATURATED=1
        break
    fi
    sleep 0.5
done
[ "$SATURATED" = "1" ] || log "aviso: no se observó worker_inflight > 0; se captura igual"
sleep 2

docker exec "$WORKER_NAME" curl -s http://127.0.0.1:8081/metrics > "$OUT_DIR/metrics-during.txt" 2>/dev/null || true
docker inspect "$WORKER_NAME" > "$OUT_DIR/inspect.json" 2>/dev/null || true
docker exec "$WORKER_NAME" sh -c \
    'cat /sys/fs/cgroup/memory.peak 2>/dev/null || cat /sys/fs/cgroup/memory/memory.max_usage_in_bytes 2>/dev/null || true' \
    > "$OUT_DIR/memory-peak.txt" 2>/dev/null || true

# --- Espera de extracción ----------------------------------------------------
STATUS_JS="$OUT_DIR/status.js"
{
    echo 'const ids = [];'
    for n in $(seq 1 "$PDF_COUNT"); do
        echo "ids.push(\"hostile-$RUN_ID-$n\");"
    done
    echo 'const docs = db.getSiblingDB("app").documents.find({_id: {$in: ids}}).toArray();'
    echo 'const counts = {};'
    echo 'for (const d of docs) counts[d.status] = (counts[d.status] || 0) + 1;'
    echo 'print(JSON.stringify({statuses: docs.map((d) => [d._id, d.status]), counts}));'
} > "$STATUS_JS"
STATUS_REMOTE="/tmp/hostile-status-$RUN_ID.js"
docker cp "$STATUS_JS" "$MONGO_CID:$STATUS_REMOTE" >/dev/null

log "esperando extracción (tope ${LOAD_TIMEOUT_SECS}s)"
IDLE=0
for i in $(seq 1 "$LOAD_TIMEOUT_SECS"); do
    docker exec "$MONGO_CID" mongosh --host 127.0.0.1 -u root -p "$MONGO_ROOT_PASSWORD" \
        --authenticationDatabase admin app --quiet --file "$STATUS_REMOTE" \
        > "$OUT_DIR/status-latest.json" 2>/dev/null || true
    INFLIGHT="$(docker exec "$WORKER_NAME" curl -s http://127.0.0.1:8081/metrics \
        2>/dev/null | awk '$1=="worker_inflight"{print $2; exit}' || true)"
    COMPLETED="$(jq -r '.counts.COMPLETED // 0' "$OUT_DIR/status-latest.json" 2>/dev/null || echo 0)"
    printf 't=%ss inflight=%s completed=%s\n' "$i" "${INFLIGHT:-?}" "$COMPLETED" \
        | tee -a "$OUT_DIR/progress.log" >/dev/null
    RUNNING="$(docker inspect -f '{{.State.Running}}' "$WORKER_NAME" 2>/dev/null || echo false)"
    if [ "$RUNNING" != "true" ]; then
        log "el worker dejó de correr durante la extracción"
        break
    fi
    if [ "${INFLIGHT:-0}" = "0" ] && [ "$COMPLETED" -ge "$PDF_COUNT" ]; then
        break
    fi
    if [ "${INFLIGHT:-0}" = "0" ]; then
        IDLE=$((IDLE + 1))
    else
        IDLE=0
    fi
    if [ "$IDLE" -ge 10 ]; then
        break
    fi
    sleep 1
done

# --- Cierre y evidencia ------------------------------------------------------
log "deteniendo sonda y recolectando evidencia"
docker exec "$WORKER_NAME" touch /tmp/readyz-stop >/dev/null 2>&1 || true
sleep 1
docker exec "$WORKER_NAME" cat /tmp/readyz-probe.log > "$OUT_DIR/readyz-probe.log" 2>/dev/null || true
docker exec "$WORKER_NAME" curl -s http://127.0.0.1:8081/metrics > "$OUT_DIR/metrics-final.txt" 2>/dev/null || true
docker logs "$WORKER_NAME" > "$OUT_DIR/worker.log" 2>&1 || true
docker exec "$MONGO_CID" mongosh --host 127.0.0.1 -u root -p "$MONGO_ROOT_PASSWORD" \
    --authenticationDatabase admin app --quiet --file "$STATUS_REMOTE" \
    > "$OUT_DIR/status-final.json" 2>/dev/null || true

python3 - "$OUT_DIR/readyz-probe.log" "$OUT_DIR/readyz-stats.json" <<'PY'
import json
import sys

path, out = sys.argv[1], sys.argv[2]
samples = []
for line in open(path, encoding="utf-8", errors="replace"):
    parts = line.split()
    if len(parts) < 3:
        continue
    try:
        samples.append((parts[1], float(parts[2]) * 1000.0))
    except ValueError:
        continue
latencies = sorted(ms for _, ms in samples if ms >= 0)
stats = {
    "samples": len(samples),
    "ok_200": sum(1 for code, _ in samples if code == "200"),
    "errors": sum(1 for code, _ in samples if code != "200"),
    "mean_ms": round(sum(latencies) / len(latencies), 3) if latencies else None,
    "p95_ms": round(latencies[min(len(latencies) - 1, int(len(latencies) * 0.95))], 3) if latencies else None,
    "max_ms": round(max(latencies), 3) if latencies else None,
}
with open(out, "w", encoding="utf-8") as fh:
    json.dump(stats, fh, indent=2)
print(json.dumps(stats, indent=2))
PY

PIDS_CURRENT="$(awk '$1=="worker_pids_current"{print $2; exit}' "$OUT_DIR/metrics-during.txt" 2>/dev/null || true)"
PIDS_LIMIT="$(awk '$1=="worker_pids_limit"{print $2; exit}' "$OUT_DIR/metrics-during.txt" 2>/dev/null || true)"
INFLIGHT_DURING="$(awk '$1=="worker_inflight"{print $2; exit}' "$OUT_DIR/metrics-during.txt" 2>/dev/null || true)"
PIDS_LIMIT_DOCKER="$(jq -r '.[0].HostConfig.PidsLimit // empty' "$OUT_DIR/inspect.json" 2>/dev/null || true)"
MEMORY_PEAK="$(cat "$OUT_DIR/memory-peak.txt" 2>/dev/null || true)"
STATUS_COUNTS="$(jq -c '.counts' "$OUT_DIR/status-final.json" 2>/dev/null || echo '{}')"
READYZ_STATS="$(cat "$OUT_DIR/readyz-stats.json" 2>/dev/null || echo '{}')"
SIZES="$(cat "$PDF_DIR/pdfs.json" 2>/dev/null || echo '{}')"

READYZ_OK=0
READYZ_MAX="$(jq -r '.max_ms // empty' "$OUT_DIR/readyz-stats.json" 2>/dev/null || true)"
READYZ_ERRORS="$(jq -r '.errors // 1' "$OUT_DIR/readyz-stats.json" 2>/dev/null || echo 1)"
if [ "${READYZ_ERRORS:-1}" = "0" ] && [ -n "$READYZ_MAX" ] \
    && awk -v m="$READYZ_MAX" 'BEGIN{exit !(m < 200)}'; then
    READYZ_OK=1
fi
PIDS_OK=0
if [ "${PIDS_LIMIT:-}" = "200" ] && [ "${PIDS_LIMIT_DOCKER:-}" = "200" ]; then
    PIDS_OK=1
fi

{
    echo "# S5-P1-01 — hostil operativo (worker)"
    echo
    echo "- Fecha: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "- Run ID: $RUN_ID"
    echo "- Imagen: $WORKER_IMAGE"
    echo "- Revision OCI: ${WORKER_REVISION:-desconocida}"
    echo "- Stream/group: $STREAM / $GROUP"
    echo "- PDFs: $PDF_COUNT x $((TEXT_PAGES + IMAGE_PAGES)) páginas"
    echo "- Tamaños: $SIZES"
    echo "- Worker: pids_limit=200, mem_limit=768m, cpus=1.5, MAX_CONCURRENCY=4"
    echo
    echo "## /readyz durante saturación"
    echo '```json'
    echo "$READYZ_STATS"
    echo '```'
    echo
    echo "## PIDs"
    echo "- worker_pids_current: ${PIDS_CURRENT:-no disponible}"
    echo "- worker_pids_limit (metrics): ${PIDS_LIMIT:-no disponible}"
    echo "- HostConfig.PidsLimit: ${PIDS_LIMIT_DOCKER:-no disponible}"
    echo "- memory.peak: ${MEMORY_PEAK:-no disponible} bytes"
    echo "- worker_inflight durante la captura: ${INFLIGHT_DURING:-no disponible}"
    echo
    echo "## Documentos al cierre"
    echo '```json'
    echo "$STATUS_COUNTS"
    echo '```'
    echo
    echo "## Resultado"
    if [ "$PIDS_OK" = "1" ]; then
        echo "- pids_limit=200: PASS"
    else
        echo "- pids_limit=200: FAIL (metrics=${PIDS_LIMIT:-?}, docker=${PIDS_LIMIT_DOCKER:-?})"
    fi
    if [ "$READYZ_OK" = "1" ]; then
        echo "- /readyz 200 y max <200 ms: PASS"
    else
        echo "- /readyz 200 y max <200 ms: FAIL (errores=${READYZ_ERRORS:-?}, max=${READYZ_MAX:-?} ms)"
    fi
    if [ "$PIDS_OK" = "1" ] && [ "$READYZ_OK" = "1" ]; then
        echo "- OVERALL: PASS"
    else
        echo "- OVERALL: FAIL"
    fi
} > "$REPORT"

log "reporte: $REPORT"
cat "$REPORT"
