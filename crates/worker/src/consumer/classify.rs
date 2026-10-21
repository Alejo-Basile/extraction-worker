//! Clasificacion de errores de Redis (S2-P3-02).
//!
//! Mapeo por tipo (`redis::ErrorKind`/`ServerErrorKind`), no por texto:
//! - `Io`/`Client` -> Transient (red, conexion).
//! - `AuthenticationFailed`/`InvalidClientConfig` -> Fatal (config/auth).
//! - `Parse`/`UnexpectedReturnType`/`Extension` -> Fatal (protocolo inesperado).
//! - `Server(BusyLoading|TryAgain|ClusterDown|MasterDown|ReadOnly)` -> Transient.
//! - `Server(ResponseError)` -> Transient (condicion runtime: NOGROUP,
//!   BUSYGROUP, etc.).
//! - `Server(NoScript|CrossSlot|NoPerm|ExecAbort)` -> Fatal.
//!
//! Un error de Redis nunca es `Document`: si Redis falla, el problema no es
//! el documento y no se le manda a la DLQ por un error de infraestructura.

use redis::{ErrorKind, RedisError, ServerErrorKind};

use super::resilience::Failure;

pub fn classify_redis_error(e: &RedisError) -> Failure {
    let message = e.to_string();
    match e.kind() {
        ErrorKind::Io | ErrorKind::Client => Failure::transient("REDIS_IO", message),
        ErrorKind::AuthenticationFailed | ErrorKind::InvalidClientConfig => {
            Failure::fatal("REDIS_CONFIG", message)
        }
        ErrorKind::Parse | ErrorKind::UnexpectedReturnType | ErrorKind::Extension => {
            Failure::fatal("REDIS_PROTOCOL", message)
        }
        ErrorKind::Server(sk) => match sk {
            ServerErrorKind::BusyLoading
            | ServerErrorKind::TryAgain
            | ServerErrorKind::ClusterDown
            | ServerErrorKind::MasterDown
            | ServerErrorKind::ReadOnly
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
}
