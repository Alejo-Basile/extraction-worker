# Lab: Redis Streams (S0-P3-03)

Este laboratorio demuestra operaciones básicas sobre Redis Streams y el efecto de no hacer `XACK`.

## Operaciones demostradas
- `XADD` - Encola mensajes en el stream
- `XGROUP CREATEMKSTREAM` - Crea consumer group
- `XREADGROUP` - Lee mensajes para un consumer (nunca se reentregan a quien ya los tiene)
- `XACK` - Confirma procesamiento exitoso (saca mensaje del PEL)
- `XPENDING` - Inspecciona Pending Entries List (PEL)
- `XAUTOCLAIM` - Reclama mensajes huérfanos (worker caido)

## Efecto clave
- Si **NO** se hace `XACK`, el mensaje queda en el **PEL** (Pending Entries List). Puede ser reclamado por otro consumer con `XAUTOCLAIM` o seguir pendiente.
- Si **SÍ** se hace `XACK`, el mensaje deja de estar pendiente.

## Requisitos
- Redis corriendo en `127.0.0.1:6379` (o configurar `REDIS_HOST`/`REDIS_PORT`)
- `redis-cli` instalado (configurable vía `REDIS_CLI`)

## Ejecutar
```bash
cd labs
REDIS_HOST=127.0.0.1 REDIS_PORT=6379 ./redis_streams_demo.sh
# o si redis-cli está en otro path
REDIS_CLI=/usr/bin/redis-cli ./redis_streams_demo.sh
```

## Notas
- Usa `XPENDING` para contar reintentos/pendientes (el contador es el delivery count de Redis, NO un campo del mensaje).
- `XAUTOCLAIM` requiere `min-idle-time` adecuado: debe ser >= 2× `job_timeout` para evitar reintentos paralelos (ver SDD 5.4).
