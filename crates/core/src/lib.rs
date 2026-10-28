//! Dominio del extraction-worker.
//!
//! Libreria de dominio pura (S0-P3-01): sin dependencias de infraestructura
//! (nada de Redis, MinIO, Mongo, Actix). Contiene:
//! - `WorkItem`: el mensaje de la cola, contrato JSON compartido con doc-service
//!   (pasos-iniciales.md seccion 11.3, propiedad compartida de P2 y P3).
//! - `DlqMessage`: el mensaje de la DLQ con el diagnostico del fallo (seccion 11.3).
//! - `TextExtractor` y su error: puerto de extraccion de texto (los adaptadores
//!   reales llegan en `S2-P3-01`; un fake entra en `S0-P3-06`).
//! - `ErrorKind`: clasificacion transitorio/negocio (`S2-P3-02`).

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

pub mod fake;
pub mod status;

/// Version del esquema del contrato JSON (seccion 11.3).
/// Un worker que no entiende la version NO procesa el mensaje: va a la DLQ
/// con `error_code: UNSUPPORTED_SCHEMA_VERSION`.
pub const SUPPORTED_SCHEMA_VERSION: u32 = 1;

/// Codigos de error estables para la DLQ (seccion 11.3).
pub mod error_code {
    /// El mensaje no deserializa contra el contrato (campo faltante,
    /// desconocido o de tipo incorrecto).
    pub const INVALID_CONTRACT: &str = "INVALID_CONTRACT";
    /// `schema_version` del mensaje no es la soportada por este worker.
    pub const UNSUPPORTED_SCHEMA_VERSION: &str = "UNSUPPORTED_SCHEMA_VERSION";
    /// El mensaje agoto `WORKER_MAX_ATTEMPTS` entregas sin procesarse.
    pub const MAX_ATTEMPTS_EXCEEDED: &str = "MAX_ATTEMPTS_EXCEEDED";
    /// El PDF no se pudo parsear (xref roto, estructura invalida).
    pub const PDF_CORRUPT: &str = "PDF_CORRUPT";
    /// El PDF esta cifrado y el worker no tiene contrasenia.
    pub const PDF_ENCRYPTED: &str = "PDF_ENCRYPTED";
    /// El PDF se parseo pero no tiene texto extraible (escaneado).
    pub const PDF_NO_TEXT: &str = "PDF_NO_TEXT";
    /// El objeto supera `WORKER_MAX_PDF_BYTES` (defensa en profundidad).
    pub const PDF_TOO_LARGE: &str = "PDF_TOO_LARGE";
    /// El PDF supera `WORKER_MAX_PAGES` (defensa ante PDF hostil).
    pub const PDF_TOO_MANY_PAGES: &str = "PDF_TOO_MANY_PAGES";
    /// El texto extraido supera `WORKER_MAX_OUTPUT_BYTES`.
    pub const PDF_OUTPUT_TOO_LARGE: &str = "PDF_OUTPUT_TOO_LARGE";
}

/// Mensaje en la DLQ (seccion 11.3). Anade el diagnostico al mensaje original;
/// el numero de intentos viene de Redis (`XPENDING`), no del mensaje.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DlqMessage {
    pub document_id: String,
    pub object_key: String,
    pub correlation_id: String,
    pub enqueued_at: String,
    pub schema_version: u32,
    /// Delivery count leido de `XPENDING` al momento de mover a la DLQ.
    pub delivery_count: u64,
    /// RFC 3339 del momento del fallo.
    pub failed_at: String,
    /// Codigo estable: ver [`error_code`].
    pub error_code: String,
    pub error_message: String,
    /// `business` (el documento es el problema) o `transient` (se agotaron
    /// los reintentos de infraestructura).
    pub error_kind: ErrorKind,
}

/// Mensaje de trabajo del stream `stream:pdf-processing`.
///
/// Provisto por el relay de doc-service. Reglas del contrato:
/// - El binario nunca viaja por la cola, solo metadatos.
/// - NO lleva `attempts`: el conteo de intentos es el delivery count de
///   `XPENDING` (SPEC seccion 5.4).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkItem {
    /// ULID del documento; tambien es el `_id` en Mongo y la base del
    /// `object_key` en MinIO (idempotencia por disenio).
    pub document_id: String,
    /// Siempre `raw-pdfs/<document_id>.pdf`.
    pub object_key: String,
    pub correlation_id: String,
    /// RFC 3339 (ej. "2026-11-30T10:15:03Z"); sin parseo estricto en S0.
    pub enqueued_at: String,
    pub schema_version: u32,
}

/// Error de validacion del contrato del mensaje.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContractError {
    /// `schema_version` distinta de la soportada.
    UnsupportedSchemaVersion(u32),
    /// El mensaje no cumple el contrato (campo faltante, desconocido o
    /// de tipo incorrecto).
    Invalid(String),
}

impl std::fmt::Display for ContractError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ContractError::UnsupportedSchemaVersion(v) => {
                write!(f, "schema_version no soportada: {v}")
            }
            ContractError::Invalid(m) => write!(f, "contrato invalido: {m}"),
        }
    }
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

    /// Parsea el mensaje a partir de los campos crudos del stream.
    ///
    /// El relay de doc-service (P2, aun sin implementar) puede XADD de dos
    /// formas; se aceptan ambas hasta que P2 fije la representacion:
    /// - un unico campo `payload` con el JSON del contrato;
    /// - campos planos (uno por campo del contrato; en ese caso Redis entrega
    ///   `schema_version` como texto y se convierte aqui).
    ///
    /// En cualquier caso se aplica el contrato estricto: campos desconocidos o
    /// tipos no convertibles son un error visible, no se ignora nada.
    pub fn from_stream_fields(fields: &HashMap<String, String>) -> Result<WorkItem, ContractError> {
        if let (Some(payload), 1) = (fields.get("payload"), fields.len()) {
            return serde_json::from_str::<WorkItem>(payload).map_err(|e| {
                ContractError::Invalid(format!("payload no es un mensaje valido: {e}"))
            });
        }

        const CONTRACT_FIELDS: [&str; 5] = [
            "document_id",
            "object_key",
            "correlation_id",
            "enqueued_at",
            "schema_version",
        ];
        for key in fields.keys() {
            if !CONTRACT_FIELDS.contains(&key.as_str()) {
                return Err(ContractError::Invalid(format!("campo desconocido: {key}")));
            }
        }

        let get = |name: &str| -> Result<String, ContractError> {
            fields
                .get(name)
                .cloned()
                .ok_or_else(|| ContractError::Invalid(format!("falta el campo {name}")))
        };
        let raw_schema_version = get("schema_version")?;
        let schema_version = raw_schema_version.parse::<u32>().map_err(|_| {
            ContractError::Invalid(format!(
                "schema_version no es un entero: {raw_schema_version}"
            ))
        })?;
        Ok(WorkItem {
            document_id: get("document_id")?,
            object_key: get("object_key")?,
            correlation_id: get("correlation_id")?,
            enqueued_at: get("enqueued_at")?,
            schema_version,
        })
    }
}

/// Clasificacion de errores (SDD seccion 7, `S2-P3-02`):
/// - `Transient`: timeout de red, 5xx, conexion rechazada -> se reintenta.
/// - `Business`: PDF cifrado, corrupto, sin texto extraible -> no se reintenta,
///   va directo a su destino (DLQ o REJECTED segun el caso).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
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

/// Puerto de extraccion de texto (S0-P3-06, S2-P3-01).
///
/// Sincrono a proposito: la extraccion de PDF es CPU-bound; el adaptador
/// concreto decidira como ejecutarla (spawn_blocking, ver SDD 3.3). Recibe la
/// ruta del PDF ya descargado a un archivo temporal (S2-P3-01): el binario
/// nunca se mantiene completo en la memoria del worker.
pub trait TextExtractor: Send + Sync {
    fn extract(&self, pdf_path: &std::path::Path) -> Result<String, ExtractorError>;
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

    fn valid_work_item() -> WorkItem {
        WorkItem {
            document_id: "01J9Z8QK3M7X2V0N4P6R8T1Y0B".to_string(),
            object_key: "raw-pdfs/01J9Z8QK3M7X2V0N4P6R8T1Y0B.pdf".to_string(),
            correlation_id: "01J9Z8QK3M7X2V0N4P6R8T1Y0B".to_string(),
            enqueued_at: "2026-11-30T10:15:03Z".to_string(),
            schema_version: 1,
        }
    }

    fn from_fields_expect(fields: &[(&str, &str)]) -> Result<WorkItem, ContractError> {
        let map: HashMap<String, String> = fields
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        WorkItem::from_stream_fields(&map)
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

    #[test]
    fn parsea_campos_planos_del_stream() {
        let fields = [
            ("document_id", "01J9Z8QK3M7X2V0N4P6R8T1Y0B"),
            ("object_key", "raw-pdfs/01J9Z8QK3M7X2V0N4P6R8T1Y0B.pdf"),
            ("correlation_id", "01J9Z8QK3M7X2V0N4P6R8T1Y0B"),
            ("enqueued_at", "2026-11-30T10:15:03Z"),
            ("schema_version", "1"),
        ];
        let item = from_fields_expect(&fields).unwrap_or_else(|e| panic!("debe parsear: {e}"));
        assert_eq!(item, valid_work_item());
    }

    #[test]
    fn parsea_payload_json_unico() {
        let payload = valid_work_item_json();
        let fields = [("payload", payload.as_str())];
        let item = from_fields_expect(&fields).unwrap_or_else(|e| panic!("debe parsear: {e}"));
        assert_eq!(item, valid_work_item());
    }

    #[test]
    fn rechaza_campo_desconocido_en_campos_planos() {
        let fields = [
            ("document_id", "01J9Z8QK3M7X2V0N4P6R8T1Y0B"),
            ("object_key", "raw-pdfs/x.pdf"),
            ("correlation_id", "01J9Z8QK3M7X2V0N4P6R8T1Y0B"),
            ("enqueued_at", "2026-11-30T10:15:03Z"),
            ("schema_version", "1"),
            ("attempts", "3"),
        ];
        match from_fields_expect(&fields) {
            Err(ContractError::Invalid(m)) => {
                assert!(m.contains("campo desconocido"), "mensaje: {m}")
            }
            other => panic!("debe rechazar campo desconocido, obtuvo: {other:?}"),
        }
    }

    #[test]
    fn rechaza_schema_version_no_numerica_en_campos_planos() {
        let fields = [
            ("document_id", "01J9Z8QK3M7X2V0N4P6R8T1Y0B"),
            ("object_key", "raw-pdfs/x.pdf"),
            ("correlation_id", "01J9Z8QK3M7X2V0N4P6R8T1Y0B"),
            ("enqueued_at", "2026-11-30T10:15:03Z"),
            ("schema_version", "uno"),
        ];
        match from_fields_expect(&fields) {
            Err(ContractError::Invalid(m)) => {
                assert!(m.contains("schema_version"), "mensaje: {m}")
            }
            other => panic!("debe rechazar schema_version no numerica, obtuvo: {other:?}"),
        }
    }

    #[test]
    fn rechaza_campos_faltantes() {
        let fields = [("document_id", "01J9Z8QK3M7X2V0N4P6R8T1Y0B")];
        match from_fields_expect(&fields) {
            Err(ContractError::Invalid(m)) => assert!(m.contains("falta"), "mensaje: {m}"),
            other => panic!("debe rechazar mensaje incompleto, obtuvo: {other:?}"),
        }
    }

    #[test]
    fn dlq_message_serializa_error_kind_en_minusculas() {
        let msg = DlqMessage {
            document_id: "01J9Z8QK3M7X2V0N4P6R8T1Y0B".to_string(),
            object_key: "raw-pdfs/01J9Z8QK3M7X2V0N4P6R8T1Y0B.pdf".to_string(),
            correlation_id: "01J9Z8QK3M7X2V0N4P6R8T1Y0B".to_string(),
            enqueued_at: "2026-11-30T10:15:03Z".to_string(),
            schema_version: 1,
            delivery_count: 5,
            failed_at: "2026-11-30T10:17:44Z".to_string(),
            error_code: "PDF_CORRUPT".to_string(),
            error_message: "Failed to parse xref table".to_string(),
            error_kind: ErrorKind::Business,
        };
        let json = serde_json::to_value(&msg).unwrap_or_else(|e| panic!("debe serializar: {e}"));
        match json.get("error_kind") {
            Some(serde_json::Value::String(s)) => assert_eq!(s, "business"),
            other => panic!("error_kind debe ser string minuscula, obtuvo: {other:?}"),
        }
        let back: DlqMessage =
            serde_json::from_value(json).unwrap_or_else(|e| panic!("debe deserializar: {e}"));
        assert_eq!(back, msg);
    }

    #[test]
    fn dlq_message_rechaza_campos_desconocidos() {
        let json = serde_json::json!({
            "document_id": "x",
            "object_key": "raw-pdfs/x.pdf",
            "correlation_id": "x",
            "enqueued_at": "2026-11-30T10:15:03Z",
            "schema_version": 1,
            "delivery_count": 1,
            "failed_at": "2026-11-30T10:17:44Z",
            "error_code": "INVALID_CONTRACT",
            "error_message": "m",
            "error_kind": "business",
            "extra": "no"
        });
        let result = serde_json::from_value::<DlqMessage>(json);
        assert!(result.is_err(), "campo desconocido debe fallar en la DLQ");
    }
}
