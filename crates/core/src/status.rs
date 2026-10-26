//! Estados del documento y tabla de transiciones (seccion 11.1).
//!
//! Contrato compartido con doc-service (P2): los estados terminales son
//! `COMPLETED`, `FAILED`, `REJECTED` y `UPLOAD_EXPIRED`; toda transicion se
//! ejecuta con filtro de estado esperado y deja entrada en `status_history`.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum DocumentStatus {
    PendingUpload,
    Uploaded,
    Queued,
    Processing,
    Retrying,
    ExtractionFailed,
    Compensating,
    Failed,
    Completed,
    Rejected,
    UploadExpired,
}

impl DocumentStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            DocumentStatus::PendingUpload => "PENDING_UPLOAD",
            DocumentStatus::Uploaded => "UPLOADED",
            DocumentStatus::Queued => "QUEUED",
            DocumentStatus::Processing => "PROCESSING",
            DocumentStatus::Retrying => "RETRYING",
            DocumentStatus::ExtractionFailed => "EXTRACTION_FAILED",
            DocumentStatus::Compensating => "COMPENSATING",
            DocumentStatus::Failed => "FAILED",
            DocumentStatus::Completed => "COMPLETED",
            DocumentStatus::Rejected => "REJECTED",
            DocumentStatus::UploadExpired => "UPLOAD_EXPIRED",
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            DocumentStatus::Completed
                | DocumentStatus::Failed
                | DocumentStatus::Rejected
                | DocumentStatus::UploadExpired
        )
    }

    /// Tabla de transiciones declaradas (seccion 11.1). Una transicion no
    /// declarada se rechaza y se reporta como bug, nunca se "corrige".
    pub fn can_transition_to(self, next: DocumentStatus) -> bool {
        use DocumentStatus::*;
        matches!(
            (self, next),
            (PendingUpload, Uploaded)
                | (PendingUpload, UploadExpired)
                | (Uploaded, Queued)
                | (Queued, Processing)
                | (Processing, Completed)
                | (Processing, Retrying)
                | (Processing, Rejected)
                | (Retrying, Processing)
                | (Retrying, ExtractionFailed)
                | (ExtractionFailed, Compensating)
                | (Compensating, Failed)
        )
    }

    /// Estados de origen validos para el reclamo del worker (XREADGROUP):
    /// encolado nuevo o reintento.
    pub fn claimable_from() -> [DocumentStatus; 2] {
        [DocumentStatus::Queued, DocumentStatus::Retrying]
    }
}

/// Entrada de `status_history` (seccion 11.1, regla 3): estado resultante,
/// fecha, actor y `correlation_id`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatusEvent {
    pub status: DocumentStatus,
    /// RFC 3339 del momento de la transicion.
    pub at: String,
    /// `worker` | `doc-service` | `reconciler` | `client` | `minio-webhook`.
    pub actor: String,
    pub correlation_id: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminales_no_salen_de_si_mismos() {
        for st in [
            DocumentStatus::Completed,
            DocumentStatus::Failed,
            DocumentStatus::Rejected,
            DocumentStatus::UploadExpired,
        ] {
            assert!(
                !st.can_transition_to(DocumentStatus::Processing),
                "de {st:?} no debe salir nada"
            );
        }
    }

    #[test]
    fn camino_feliz_es_valido() {
        assert!(DocumentStatus::PendingUpload.can_transition_to(DocumentStatus::Uploaded));
        assert!(DocumentStatus::Uploaded.can_transition_to(DocumentStatus::Queued));
        assert!(DocumentStatus::Queued.can_transition_to(DocumentStatus::Processing));
        assert!(DocumentStatus::Processing.can_transition_to(DocumentStatus::Completed));
    }

    #[test]
    fn reintento_y_compensacion_son_validos() {
        assert!(DocumentStatus::Processing.can_transition_to(DocumentStatus::Retrying));
        assert!(DocumentStatus::Retrying.can_transition_to(DocumentStatus::Processing));
        assert!(DocumentStatus::Retrying.can_transition_to(DocumentStatus::ExtractionFailed));
        assert!(DocumentStatus::ExtractionFailed.can_transition_to(DocumentStatus::Compensating));
        assert!(DocumentStatus::Compensating.can_transition_to(DocumentStatus::Failed));
        assert!(DocumentStatus::Processing.can_transition_to(DocumentStatus::Rejected));
    }

    #[test]
    fn saltos_no_declarados_se_rechazan() {
        assert!(!DocumentStatus::Queued.can_transition_to(DocumentStatus::Completed));
        assert!(!DocumentStatus::Processing.can_transition_to(DocumentStatus::Failed));
        assert!(!DocumentStatus::PendingUpload.can_transition_to(DocumentStatus::Completed));
        assert!(!DocumentStatus::Processing.can_transition_to(DocumentStatus::ExtractionFailed));
    }

    #[test]
    fn serde_usa_mayusculas_como_en_mongo() {
        let json = serde_json::to_value(DocumentStatus::Processing)
            .unwrap_or_else(|e| panic!("debe serializar: {e}"));
        assert_eq!(json, serde_json::Value::String("PROCESSING".to_string()));
    }

    #[test]
    fn claim_acepta_queued_y_retrying() {
        assert_eq!(
            DocumentStatus::claimable_from(),
            [DocumentStatus::Queued, DocumentStatus::Retrying]
        );
    }
}
