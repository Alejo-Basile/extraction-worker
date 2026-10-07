//! Dominio del extraction-worker.
//!
//! Libreria de dominio pura (S0-P3-01): sin dependencias de infraestructura.

use serde::Deserialize;

/// Version del esquema del contrato JSON (seccion 11.3).
pub const SUPPORTED_SCHEMA_VERSION: u32 = 1;

/// Mensaje de trabajo del stream `stream:pdf-processing`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkItem {
    pub document_id: String,
    pub object_key: String,
    pub correlation_id: String,
    pub enqueued_at: String,
    pub schema_version: u32,
}

/// Error de validacion del contrato del mensaje.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContractError {
    /// `schema_version` distinta de la soportada.
    UnsupportedSchemaVersion(u32),
}

impl WorkItem {
    /// Valida la version del esquema. Llamar tras la deserializacion.
    pub fn validate_schema(&self) -> Result<(), ContractError> {
        if self.schema_version == SUPPORTED_SCHEMA_VERSION {
            Ok(())
        } else {
            Err(ContractError::UnsupportedSchemaVersion(self.schema_version))
        }
    }
}

/// Clasificacion de errores (SDD seccion 7, `S2-P3-02`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    Transient,
    Business,
}

/// Error del puerto de extraccion.
#[derive(Debug)]
pub struct ExtractorError {
    /// Codigo de error estable (ej. "PDF_CORRUPT", "PDF_ENCRYPTED").
    pub code: String,
    pub message: String,
    pub kind: ErrorKind,
}

impl std::fmt::Display for ExtractorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for ExtractorError {}

/// Puerto de extraccion de texto (S0-P3-06).
pub trait TextExtractor: Send + Sync {
    fn extract(&self, pdf: &[u8]) -> Result<String, ExtractorError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_work_item_json() -> String {
        serde_json::json!({
            "document_id": "01J9Z8QK3M7X2V0N4P6R8T1Y0B",
            "object_key": "raw-pdfs/01J9Z8QK3M7X2V0N4P6R8T1Y0B.pdf",
            "correlation_id": "01J9Z8QK3M7X2V0N4P6R8T1Y0B",
            "enqueued_at": "2026-11-30T10:15:03Z",
            "schema_version": 1
        })
        .to_string()
    }

    #[test]
    fn deserializa_mensaje_valido_y_acepta_schema_v1() {
        let parsed: Result<WorkItem, _> = serde_json::from_str(&valid_work_item_json());
        let item = parsed.unwrap_or_else(|e| panic!("mensaje valido debe deserializar: {e}"));
        assert_eq!(item.validate_schema(), Ok(()));
        assert_eq!(item.document_id, "01J9Z8QK3M7X2V0N4P6R8T1Y0B");
    }

    #[test]
    fn rechaza_campos_desconocidos() {
        let bad = valid_work_item_json().replace("document_id", "doc_id");
        let result = serde_json::from_str::<WorkItem>(&bad);
        assert!(result.is_err(), "un campo inesperado debe fallar");
    }

    #[test]
    fn rechaza_schema_version_no_soportada() {
        let bad = valid_work_item_json().replace("\"schema_version\":1", "\"schema_version\":2");
        let item: WorkItem = match serde_json::from_str(&bad) {
            Ok(i) => i,
            Err(e) => panic!("deserializacion debe pasar: {e}"),
        };
        assert_eq!(
            item.validate_schema(),
            Err(ContractError::UnsupportedSchemaVersion(2))
        );
    }

    #[test]
    fn el_mensaje_no_lleva_attempts() {
        let with_attempts = valid_work_item_json().replace(
            "\"schema_version\":1",
            "\"attempts\":3,\"schema_version\":1",
        );
        let result = serde_json::from_str::<WorkItem>(&with_attempts);
        assert!(result.is_err(), "attempts no forma parte del contrato");
    }
}

pub mod fake;
