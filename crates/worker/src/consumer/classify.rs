//! Clasificación de errores (S2-P3-02).
//! Transient: reintentable (red, timeout, 503, conexión rechazada)
//! Business: no reintentable (PDF corrupto/cifrado, contrato inválido, schema no soportado)

use super::resilience::{ClassifiedError, ErrorKind};

#[allow(dead_code)]
pub fn classify_redis_error(e: &redis::RedisError) -> ClassifiedError {
    let msg = e.to_string();
    // Errores transitorios típicos
    if msg.contains("Connection")
        || msg.contains("timeout")
        || msg.contains("broken pipe")
        || msg.contains("connection refused")
        || msg.contains("try again")
    {
        return ClassifiedError {
            kind: ErrorKind::Transient,
            message: msg,
        };
    }
    // Por defecto transitorio para llamadas de red
    ClassifiedError {
        kind: ErrorKind::Transient,
        message: msg,
    }
}

#[allow(dead_code)]
pub fn classify_contract_error() -> ClassifiedError {
    ClassifiedError {
        kind: ErrorKind::Business,
        message: "invalid_contract".to_string(),
    }
}

#[allow(dead_code)]
pub fn classify_extraction_business(code: &str) -> ClassifiedError {
    // PDF corrupto, cifrado, sin texto extraíble -> Business
    ClassifiedError {
        kind: ErrorKind::Business,
        message: format!("extraction_business:{code}"),
    }
}

#[allow(dead_code)]
pub fn classify_extraction_transient(msg: &str) -> ClassifiedError {
    ClassifiedError {
        kind: ErrorKind::Transient,
        message: msg.to_string(),
    }
}
