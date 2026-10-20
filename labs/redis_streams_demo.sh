#!/usr/bin/env bash
set -euo pipefail

# Lab: Redis Streams - XADD, XGROUP CREATEMKSTREAM, XREADGROUP, XACK, XPENDING, XAUTOCLAIM
# + efecto de NO hacer XACK (mensaje queda en PEL)
# Nota: por defecto usa claves con prefijo lab: para evitar borrar colas reales.
# Para usar el stream/grupo reales, establecer explícitamente LAB_USE_PROD=1
# y confirmar con LAB_CONFIRM_DESTRUCTIVE=1.

REDIS_HOST="${REDIS_HOST:-127.0.0.1}"
REDIS_PORT="${REDIS_PORT:-6379}"
if [[ "${LAB_USE_PROD:-0}" == "1" ]]; then
  STREAM="${STREAM:-stream:pdf-processing}"
  GROUP="${GROUP:-extraction-workers}"
else
  STREAM="${STREAM:-lab:stream:pdf-processing}"
  GROUP="${GROUP:-lab:extraction-workers}"
fi
CONSUMER1="${CONSUMER1:-consumer-lab-1}"
CONSUMER2="${CONSUMER2:-consumer-lab-2}"

REDIS_CLI="${REDIS_CLI:-redis-cli}"

echo "[LAB] Redis Streams demo (host=${REDIS_HOST}:${REDIS_PORT})"
echo "Stream=${STREAM} Group=${GROUP} Consumers=${CONSUMER1},${CONSUMER2}"
echo "---"

echo "[1] Limpiar estado previo (solo keys de este lab)"
if [[ "${LAB_USE_PROD:-0}" == "1" ]]; then
  if [[ "${LAB_CONFIRM_DESTRUCTIVE:-0}" != "1" ]]; then
    echo "ERROR: LAB_USE_PROD=1 requiere LAB_CONFIRM_DESTRUCTIVE=1 para limpiar el stream productivo" >&2
    exit 1
  fi
fi
$REDIS_CLI -h "$REDIS_HOST" -p "$REDIS_PORT" DEL "$STREAM" 2>/dev/null || true

echo "[2] Crear stream + consumer group (CREATEMKSTREAM) si no existe"
$REDIS_CLI -h "$REDIS_HOST" -p "$REDIS_PORT" XGROUP CREATE "$STREAM" "$GROUP" 0 MKSTREAM 2>&1 | tail -1

echo "[3] XADD: agregar mensajes"
ID1=$($REDIS_CLI -h "$REDIS_HOST" -p "$REDIS_PORT" XADD "$STREAM" * document_id 01J9Z object_key raw-pdfs/01J9Z.pdf)
echo "  ID1=${ID1}"
ID2=$($REDIS_CLI -h "$REDIS_HOST" -p "$REDIS_PORT" XADD "$STREAM" * document_id 01JA object_key raw-pdfs/01JA.pdf)
echo "  ID2=${ID2}"

echo "[4] XREADGROUP: leer mensajes con consumer ${CONSUMER1} (nuevo)"
$REDIS_CLI -h "$REDIS_HOST" -p "$REDIS_PORT" XREADGROUP GROUP "$GROUP" "$CONSUMER1" COUNT 2 STREAMS "$STREAM" >

echo "[5] XPENDING: ver PEL para ${GROUP}"
$REDIS_CLI -h "$REDIS_HOST" -p "$REDIS_PORT" XPENDING "$STREAM" "$GROUP"

echo "[6] XACK: ackear primer mensaje"
$REDIS_CLI -h "$REDIS_HOST" -p "$REDIS_PORT" XACK "$STREAM" "$GROUP" "$ID1"

echo "[7] XPENDING tras XACK"
$REDIS_CLI -h "$REDIS_HOST" -p "$REDIS_PORT" XPENDING "$STREAM" "$GROUP"

echo "[8] NO hacer XACK del segundo mensaje (efecto: queda en PEL)"
$REDIS_CLI -h "$REDIS_HOST" -p "$REDIS_PORT" XPENDING "$STREAM" "$GROUP" - + 10

echo "[9] XAUTOCLAIM: otro consumer reclama mensajes idle (simula worker caido)"
$REDIS_CLI -h "$REDIS_HOST" -p "$REDIS_PORT" XAUTOCLAIM "$STREAM" "$GROUP" "$CONSUMER2" 0 "$ID2"

echo "[10] XPENDING final"
$REDIS_CLI -h "$REDIS_HOST" -p "$REDIS_PORT" XPENDING "$STREAM" "$GROUP" - + 10

echo "[LAB] Fin. Mensajes sin ACK permanecen en Pending Entries List (PEL)."
