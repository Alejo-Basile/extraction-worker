#!/bin/sh
# =============================================================================
# extraction-worker — entrypoint de contenedor.
#
# SPEC 9 y S2-P1-01: los secretos no viajan en texto plano en el compose
# commiteado. Llegan como archivos en /run/secrets (Docker secrets) y este
# script los materializa en las variables que el worker ya lee, con el mismo
# contrato *_FILE que usa el resto del stack.
#
# El worker todavia no lee *_FILE por si mismo (gap de codigo, no de
# empaquetado); mientras tanto la traduccion vive aca para que el secreto nunca
# aparezca en `docker inspect` ni en el compose del repositorio.
#
# Solo se arman las variables que faltan: si el operador ya definio REDIS_URL o
# MONGO_WORKER_URI explicitamente, se respetan (util en labs locales).
# =============================================================================
set -eu

read_file_secret() {
    # $1 = nombre de la variable *_FILE. Exporta su base con el valor leido.
    # Falla fuerte si el archivo no se puede leer o queda vacio: arrancar con un
    # secreto vacio es peor que no arrancar.
    eval "secret_path=\${$1:-}"
    [ -n "${secret_path}" ] || return 0

    if [ ! -r "${secret_path}" ]; then
        echo "entrypoint: no puedo leer ${1}=${secret_path}" >&2
        exit 1
    fi

    secret_name=${1%_FILE}
    secret_value=$(cat "${secret_path}")
    if [ -z "${secret_value}" ]; then
        echo "entrypoint: ${1} esta vacio (${secret_path})" >&2
        exit 1
    fi
    export "${secret_name}=${secret_value}"
    unset secret_path secret_name secret_value
}

read_file_secret S3_ACCESS_KEY_FILE
read_file_secret S3_SECRET_KEY_FILE

# Percent-encoding de todos los bytes (RFC 3986 2.1). Es legal en el userinfo
# de una URI y evita depender de que la password sea alfanumerica: los secretos
# del stack llevan caracteres especiales y el driver de Mongo rechaza la URI
# con "password must be URL encoded".
urlencode() {
    printf '%s' "$1" | od -An -v -tx1 | tr ' ' '\n' | while IFS= read -r byte; do
        if [ -n "${byte}" ]; then
            printf '%%%s' "${byte}"
        fi
    done
}

# El worker lee una URL completa, no una password suelta: se arma aca para no
# escribir el secreto en el compose. `$(cat ...)` recorta el newline final del
# archivo, que no forma parte del secreto.
if [ -z "${REDIS_URL:-}" ] && [ -n "${REDIS_PASSWORD_FILE:-}" ]; then
    redis_password=$(urlencode "$(cat "${REDIS_PASSWORD_FILE}")")
    export REDIS_URL="redis://:${redis_password}@${REDIS_HOST:-redis-streams}:${REDIS_PORT:-6379}/${REDIS_DB:-0}"
    unset redis_password
fi

bind="?authSource=${MONGO_DB:-app}"
if [ -z "${MONGO_WORKER_URI:-}" ] && [ -n "${MONGO_WORKER_PASSWORD_FILE:-}" ]; then
    mongo_password=$(urlencode "$(cat "${MONGO_WORKER_PASSWORD_FILE}")")
    export MONGO_WORKER_URI="mongodb://${MONGO_WORKER_USER:-worker}:${mongo_password}@${MONGO_HOST:-mongo}:27017/${MONGO_DB:-app}${bind}"
    unset mongo_password
fi
unset bind

exec "$@"
