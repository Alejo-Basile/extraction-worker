# syntax=docker/dockerfile:1
# extraction-worker (Rust 1.90) — imagen de produccion multi-stage, no-root.
#
# S5-P4-01: sin este artefacto no se pueden medir tres tareas P0 de la misma
# fase, porque las tres necesitan el worker corriendo como contenedor:
#   - S5-P1-01 (recursos y contencion del PDF hostil): mem_limit, pids_limit y
#     ulimits solo existen y solo se auditan sobre un contenedor.
#   - S5-P1-03 (/readyz con el bulkhead saturado): la sonda tiene que estar
#     escuchando bajo carga real.
#   - S6-P1-01 criterio (b): el cliente Rust tiene que reconectar solo tras un
#     failover de Sentinel; sin contenedor no hay failover que ensayar.
#
# La toolchain sale de rust-toolchain.toml (1.90.0), igual que la CI: build
# reproducible local y remoto.

# --- Build --------------------------------------------------------------------
FROM rust:1.90-bookworm AS build
WORKDIR /src

# pdf-extract, mongodb y object_store compilan crates con build scripts: sin un
# linker y pkg-config el build muere en una imagen slim.
RUN apt-get update \
    && apt-get install -y --no-install-recommends pkg-config build-essential \
    && rm -rf /var/lib/apt/lists/*

# Cache de dependencias por capas: el lockfile y los manifiestos van antes que
# el codigo, asi un cambio de fuente no invalida la descarga de crates.
# Cargo.lock NO se ignora en este repo (es aplicacion, no libreria).
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

# Los cache mounts evitan recompilar 13 minutos de dependencias en cada build.
# `target/` es un cache mount (no queda en la capa), por eso el binario se copia
# a /src, que si queda.
RUN --mount=type=cache,target=/src/target \
    --mount=type=cache,target=/usr/local/cargo/registry \
    cargo build --release --locked --bin extraction-worker \
    && cp /src/target/release/extraction-worker /src/extraction-worker

# --- Runtime ------------------------------------------------------------------
FROM debian:bookworm-slim

# ca-certificates: TLS hacia dependencias con https si algun dia se habilita.
# curl: lo usa el HEALTHCHECK; debian-slim no trae cliente HTTP.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 worker \
    && useradd --uid 10001 --gid 10001 --no-create-home --home-dir /nonexistent \
        --shell /usr/sbin/nologin worker

COPY --from=build /src/extraction-worker /usr/local/bin/extraction-worker
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod 0555 /entrypoint.sh

# SPEC 3.2 y 9: usuario no-root explicito (S1-P1-15). El worker solo escribe en
# /tmp para el PDF descargado, asi que el contenedor puede correr con
# `read_only: true` + tmpfs acotado desde compose.
USER 10001:10001

# HTTP de salud y metricas (SPEC 3.3): nunca se publica al host; lo consume
# Prometheus por la red interna.
EXPOSE 8081

HEALTHCHECK --interval=10s --timeout=3s --retries=6 --start-period=15s \
    CMD curl -fsS "http://127.0.0.1:${HTTP_PORT:-8081}/healthz" || exit 1

ENTRYPOINT ["/entrypoint.sh"]
CMD ["/usr/local/bin/extraction-worker"]
