//! Clasificacion de errores de Redis (S2-P3-02).
//!
//! Mapeo por tipo (`redis::ErrorKind`/`ServerErrorKind`), no por texto:
//! - `Io`/`Client` -> Transient (red, conexion).
//! - `AuthenticationFailed`/`InvalidClientConfig` -> Fatal (config/auth).
//! - `Parse`/`UnexpectedReturnType`/`Extension` -> Fatal (protocolo inesperado).
//! - `Server(BusyLoading|TryAgain|ClusterDown|ResponseError)` -> Transient
//!   (condicion runtime: NOGROUP, BUSYGROUP, etc.).
//! - `Server(MasterDown|ReadOnly)` -> Transient con marcador
//!   [`FAILOVER_CODE`] (T-08): el master cambio y hay que re-resolverlo.
//! - `Server(NoScript|CrossSlot|NoPerm|ExecAbort)` -> Fatal.
//!
//! Un error de Redis nunca es `Document`: si Redis falla, el problema no es
//! el documento y no se le manda a la DLQ por un error de infraestructura.

use redis::{ErrorKind, RedisError, ServerErrorKind};

use super::resilience::Failure;

/// Codigo de `Failure` para errores que indican failover (`READONLY` o
/// `MASTERDOWN`). Sigue siendo `Transient` (se reintenta y cuenta para el
/// breaker como antes); el codigo propio permite detectar el failover en
/// cualquier punto que ya perdio el `RedisError` crudo y re-resolver el
/// master (T-08).
pub const FAILOVER_CODE: &str = "REDIS_FAILOVER";

/// Codigo de `Failure` para errores de socket (`Io`/`Client`).
pub const IO_CODE: &str = "REDIS_IO";

/// `true` si la falla clasificada es un failover de Sentinel: el caller
/// debe descartar la conexion, `RedisClient::reset()` y reconectar.
pub fn is_failover_failure(f: &Failure) -> bool {
    matches!(f, Failure::Transient { code, .. } if code == FAILOVER_CODE)
}

/// `true` si la operacion necesita REEMPLAZAR la conexion (T-08), no solo
/// reintentar sobre el mismo socket:
/// - failover (`REDIS_FAILOVER`): el master cambio, la IP vieja es ahora
///   una replica que responde `READONLY` en escrituras;
/// - socket muerto (`REDIS_IO`): observado en el stack real de failover
///   (`broken pipe` cuando el master viejo cierra a los clientes).
///   `MultiplexedConnection` no se auto-repara: sin reconnect cada retry
///   falla igual y el breaker se abre sin recuperacion posible.
pub fn needs_reconnect(f: &Failure) -> bool {
    is_failover_failure(f) || matches!(f, Failure::Transient { code, .. } if code == IO_CODE)
}

pub fn classify_redis_error(e: &RedisError) -> Failure {
    let message = e.to_string();
    match e.kind() {
        ErrorKind::Io | ErrorKind::Client => Failure::transient(IO_CODE, message),
        ErrorKind::AuthenticationFailed | ErrorKind::InvalidClientConfig => {
            Failure::fatal("REDIS_CONFIG", message)
        }
        ErrorKind::Parse | ErrorKind::UnexpectedReturnType | ErrorKind::Extension => {
            Failure::fatal("REDIS_PROTOCOL", message)
        }
        ErrorKind::Server(sk) => match sk {
            // El master cambio (T-08): transitorio con marcador propio.
            ServerErrorKind::ReadOnly | ServerErrorKind::MasterDown => {
                Failure::transient(FAILOVER_CODE, message)
            }
            ServerErrorKind::BusyLoading
            | ServerErrorKind::TryAgain
            | ServerErrorKind::ClusterDown
            | ServerErrorKind::ResponseError
            | ServerErrorKind::Moved
            | ServerErrorKind::Ask => Failure::transient("REDIS_SERVER", message),
            ServerErrorKind::NoScript
            | ServerErrorKind::CrossSlot
            | ServerErrorKind::NoPerm
            | ServerErrorKind::ExecAbort
            | ServerErrorKind::NotBusy
            | ServerErrorKind::NoSub => Failure::fatal("REDIS_SERVER", message),
            _ => Failure::transient("REDIS_SERVER", message),
        },
        // Errores sin clasificar: por defecto reintentables (SDD 7.1: el
        // criterio de abandono es el tiempo maximo transcurrido).
        _ => Failure::transient("REDIS_UNKNOWN", message),
    }
}

/// Error de documento (contrato invalido, schema no soportada, PDF corrupto):
/// va directo a DLQ sin reintentos ni breaker.
pub fn document_failure(code: &str, message: impl Into<String>) -> Failure {
    Failure::Document {
        code: code.to_string(),
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kind_of(f: &Failure) -> &'static str {
        match f {
            Failure::Document { .. } => "document",
            Failure::Transient { .. } => "transient",
            Failure::Fatal { .. } => "fatal",
            Failure::CircuitOpen => "circuit-open",
        }
    }

    #[test]
    fn io_y_client_son_transitorios() {
        let e = RedisError::from((ErrorKind::Io, "connection refused"));
        assert_eq!(kind_of(&classify_redis_error(&e)), "transient");
        let e = RedisError::from((ErrorKind::Client, "client bug"));
        assert_eq!(kind_of(&classify_redis_error(&e)), "transient");
    }

    #[test]
    fn auth_y_config_son_fatales() {
        let e = RedisError::from((ErrorKind::AuthenticationFailed, "WRONGPASS"));
        assert_eq!(kind_of(&classify_redis_error(&e)), "fatal");
        let e = RedisError::from((ErrorKind::InvalidClientConfig, "bad url"));
        assert_eq!(kind_of(&classify_redis_error(&e)), "fatal");
    }

    #[test]
    fn protocolo_inesperado_es_fatal() {
        let e = RedisError::from((ErrorKind::Parse, "bad resp"));
        assert_eq!(kind_of(&classify_redis_error(&e)), "fatal");
        let e = RedisError::from((ErrorKind::UnexpectedReturnType, "bad type"));
        assert_eq!(kind_of(&classify_redis_error(&e)), "fatal");
    }

    #[test]
    fn errores_de_servidor_reintentables_son_transitorios() {
        for sk in [
            ServerErrorKind::BusyLoading,
            ServerErrorKind::TryAgain,
            ServerErrorKind::ClusterDown,
            ServerErrorKind::MasterDown,
            ServerErrorKind::ReadOnly,
            ServerErrorKind::ResponseError,
        ] {
            let e = RedisError::from((ErrorKind::Server(sk), "server said no"));
            assert_eq!(
                kind_of(&classify_redis_error(&e)),
                "transient",
                "{sk:?} debe ser transient"
            );
        }
    }

    #[test]
    fn errores_de_servidor_de_configuracion_son_fatales() {
        for sk in [
            ServerErrorKind::NoScript,
            ServerErrorKind::CrossSlot,
            ServerErrorKind::NoPerm,
        ] {
            let e = RedisError::from((ErrorKind::Server(sk), "server refused"));
            assert_eq!(
                kind_of(&classify_redis_error(&e)),
                "fatal",
                "{sk:?} debe ser fatal"
            );
        }
    }

    #[test]
    fn redis_nunca_clasifica_como_documento() {
        let e = RedisError::from((ErrorKind::Server(ServerErrorKind::ResponseError), "NOGROUP"));
        assert_ne!(kind_of(&classify_redis_error(&e)), "document");
    }

    // --- T-08: el failover lleva un marcador propio y sigue transitorio ---

    #[test]
    fn failover_de_master_lleva_marcador_transitorio() {
        for sk in [ServerErrorKind::ReadOnly, ServerErrorKind::MasterDown] {
            let e = RedisError::from((ErrorKind::Server(sk), "el master cambio"));
            let f = classify_redis_error(&e);
            assert_eq!(kind_of(&f), "transient", "{sk:?} sigue siendo transient");
            assert!(
                is_failover_failure(&f),
                "{sk:?} debe llevar el marcador {FAILOVER_CODE}: {f}"
            );
        }
    }

    #[test]
    fn marcador_de_failover_no_aplica_a_otros_transitorios() {
        let io = classify_redis_error(&RedisError::from((ErrorKind::Io, "connection refused")));
        assert_eq!(kind_of(&io), "transient");
        assert!(!is_failover_failure(&io), "Io no es failover");

        let resp = classify_redis_error(&RedisError::from((
            ErrorKind::Server(ServerErrorKind::ResponseError),
            "NOGROUP",
        )));
        assert_eq!(kind_of(&resp), "transient");
        assert!(!is_failover_failure(&resp), "NOGROUP no es failover");
    }

    // --- T-08: cuando reconectar ---

    #[test]
    fn needs_reconnect_cubre_failover_y_socket_muerto() {
        // Failover (READONLY/MASTERDOWN): el master cambio.
        let failover = classify_redis_error(&RedisError::from((
            ErrorKind::Server(ServerErrorKind::ReadOnly),
            "READONLY",
        )));
        assert!(needs_reconnect(&failover), "READONLY requiere reconectar");

        // Socket muerto (broken pipe al degradar el master): el
        // MultiplexedConnection no se auto-repara, hay que reemplazarlo.
        let broken = classify_redis_error(&RedisError::from((ErrorKind::Io, "broken pipe")));
        assert!(needs_reconnect(&broken), "Io requiere reconectar");

        // El resto conserva el comportamiento anterior (retry sobre la
        // misma conexion): timeouts, NOGROUP, fatales y documentos.
        for f in [
            Failure::transient(REDIS_TIMEOUT_CODE_TEST, "xack timeout"),
            classify_redis_error(&RedisError::from((
                ErrorKind::Server(ServerErrorKind::ResponseError),
                "NOGROUP",
            ))),
            Failure::fatal("REDIS_CONFIG", "auth"),
            Failure::Document {
                code: "X".to_string(),
                message: "y".to_string(),
            },
        ] {
            assert!(!needs_reconnect(&f), "no debe reconectar por: {f}");
        }
    }

    const REDIS_TIMEOUT_CODE_TEST: &str = "REDIS_TIMEOUT";
}
