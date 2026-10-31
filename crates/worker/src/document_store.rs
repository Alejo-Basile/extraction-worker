//! Cliente Mongo del worker con rol acotado (ADR-0006, S2-P3-08/09).
//!
//! El worker usa `MONGO_WORKER_URI`: un usuario con permiso **unicamente** de
//! `update` sobre la coleccion `documents` (sin insert, sin admin). Todas las
//! escrituras son `updateOne` **condicionales** por estado esperado y dejan
//! entrada en `status_history` (§11.1 reglas 2 y 3). Transiciones:
//!
//! - Reclamo: `QUEUED|RETRYING -> PROCESSING` al tomar el mensaje.
//! - Cierre: `PROCESSING -> COMPLETED` con `txt_ref` y `completed_at`.
//!   Si el update no matchea (otro actor movio el estado), el worker lo
//!   detecta y no pisa el estado ajeno.
//!
//! Si el cierre falla tras reintentos, la compensacion (S2-P3-09) borra el
//! `.txt` de MinIO y el documento queda en `PROCESSING` para el reconciliador.

use std::sync::Arc;
use std::time::Duration;

use extraction_core::status::DocumentStatus;
use mongodb::bson::{doc, Document as BsonDocument};
use mongodb::options::ClientOptions;
use mongodb::{Client, Collection};

use crate::consumer::resilience::{CircuitBreaker, Failure};

pub const WORKER_ACTOR: &str = "worker";
pub const OPERATOR_ACTOR: &str = "operator";
const COLLECTION_DOCUMENTS: &str = "documents";

#[derive(Debug, Clone)]
pub struct MongoConfig {
    pub uri: String,
    pub db: String,
    pub timeout: Duration,
}

pub struct DocumentStore {
    client: Client,
    documents: Collection<BsonDocument>,
    timeout: Duration,
}

impl DocumentStore {
    pub async fn connect(cfg: &MongoConfig) -> Result<Self, Failure> {
        let mut opts = match ClientOptions::parse(&cfg.uri).await {
            Ok(o) => o,
            Err(e) => return Err(Failure::fatal("MONGO_CONFIG", e.to_string())),
        };
        opts.server_selection_timeout = Some(cfg.timeout);
        opts.connect_timeout = Some(cfg.timeout);
        let client = Client::with_options(opts)
            .map_err(|e| Failure::fatal("MONGO_CONFIG", e.to_string()))?;
        let documents = client.database(&cfg.db).collection(COLLECTION_DOCUMENTS);
        Ok(Self {
            client,
            documents,
            timeout: cfg.timeout,
        })
    }

    /// PING para `/readyz`.
    pub async fn ping(&self) -> Result<(), Failure> {
        match tokio::time::timeout(
            self.timeout,
            self.client.database("admin").run_command(doc! {"ping": 1}),
        )
        .await
        {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(e)) => Err(Failure::transient("MONGO_IO", e.to_string())),
            Err(_) => Err(Failure::transient("MONGO_TIMEOUT", "ping timeout")),
        }
    }

    /// Coleccion `documents` para verificaciones del rol en tests (el rol
    /// del worker no permite ni leer ni insertar; eso lo demuestran los
    /// tests de integracion).
    pub fn collection(&self) -> &Collection<BsonDocument> {
        &self.documents
    }

    /// `QUEUED|RETRYING -> PROCESSING` (reclamo del mensaje).
    ///
    /// Devuelve `Ok(true)` si el documento estaba reclambiable. `Ok(false)`
    /// significa que otro actor ya movio el estado: el mensaje no se procesa
    /// (idempotencia ante entregas duplicadas).
    pub async fn claim(
        &self,
        document_id: &str,
        correlation_id: &str,
        now_rfc3339: &str,
    ) -> Result<bool, Failure> {
        let history = status_event(DocumentStatus::Processing, now_rfc3339, correlation_id);
        let filter = doc! {
            "_id": document_id,
            "status": doc! {"$in": DocumentStatus::claimable_from().iter().map(|s| s.as_str()).collect::<Vec<_>>()},
        };
        let update = doc! {
            "$set": {"status": DocumentStatus::Processing.as_str(), "updated_at": now_rfc3339},
            "$push": {"status_history": history},
        };
        let res = self
            .documents
            .update_one(filter, update)
            .await
            .map_err(mongo_failure)?;
        Ok(res.matched_count > 0)
    }

    /// `PROCESSING -> COMPLETED` con `txt_ref` (paso 5, transicion condicional).
    ///
    /// Devuelve `Ok(true)` si cerro, `Ok(false)` si el documento ya no estaba
    /// en `PROCESSING` (el estado de otro actor no se pisa).
    pub async fn complete(
        &self,
        document_id: &str,
        txt_ref: &str,
        correlation_id: &str,
        now_rfc3339: &str,
    ) -> Result<bool, Failure> {
        let history = status_event(DocumentStatus::Completed, now_rfc3339, correlation_id);
        let filter = doc! {
            "_id": document_id,
            "status": DocumentStatus::Processing.as_str(),
        };
        let update = doc! {
            "$set": {
                "status": DocumentStatus::Completed.as_str(),
                "txt_ref": txt_ref,
                "completed_at": now_rfc3339,
                "updated_at": now_rfc3339,
            },
            "$push": {"status_history": history},
        };
        let res = self
            .documents
            .update_one(filter, update)
            .await
            .map_err(mongo_failure)?;
        Ok(res.matched_count > 0)
    }

    /// Prepara un documento para replay de la DLQ (S4-P3-01):
    /// `PROCESSING -> RETRYING` (transicion declarada) o confirma que ya
    /// esta en un estado reclamable (`QUEUED`/`RETRYING`). Devuelve `false`
    /// si el documento esta en un estado terminal/desconocido: el replay
    /// automatico no lo toca.
    pub async fn prepare_replay(
        &self,
        document_id: &str,
        correlation_id: &str,
        now_rfc3339: &str,
    ) -> Result<bool, Failure> {
        let history = status_event_with_actor(
            DocumentStatus::Retrying,
            now_rfc3339,
            correlation_id,
            OPERATOR_ACTOR,
        );
        let res = self
            .documents
            .update_one(
                doc! {
                    "_id": document_id,
                    "status": DocumentStatus::Processing.as_str(),
                },
                doc! {
                    "$set": {
                        "status": DocumentStatus::Retrying.as_str(),
                        "updated_at": now_rfc3339,
                    },
                    "$push": {"status_history": history},
                },
            )
            .await
            .map_err(mongo_failure)?;
        if res.matched_count > 0 {
            return Ok(true);
        }

        let claimable: Vec<&str> = DocumentStatus::claimable_from()
            .iter()
            .map(DocumentStatus::as_str)
            .collect();
        let res = self
            .documents
            .update_one(
                doc! {
                    "_id": document_id,
                    "status": {"$in": claimable},
                },
                doc! {"$set": {"updated_at": now_rfc3339}},
            )
            .await
            .map_err(mongo_failure)?;
        Ok(res.matched_count > 0)
    }

    fn breaker() -> Arc<CircuitBreaker> {
        Arc::new(CircuitBreaker::default())
    }
}

/// CB propio de Mongo (S5-P3-01: un breaker por dependencia).
pub fn mongo_breaker() -> Arc<CircuitBreaker> {
    DocumentStore::breaker()
}

fn mongo_failure(e: mongodb::error::Error) -> Failure {
    // Auth/config falla siempre; el resto (red, selection timeout) es
    // reintentable y deja el mensaje pendiente.
    let message = e.to_string();
    if message.contains("Authentication failed") || message.contains("authSource") {
        Failure::fatal("MONGO_AUTH", message)
    } else {
        Failure::transient("MONGO_IO", message)
    }
}

/// Entrada de `status_history` con los mismos campos que `StatusEvent`
/// (status, at, actor, correlation_id).
fn status_event(status: DocumentStatus, now_rfc3339: &str, correlation_id: &str) -> BsonDocument {
    status_event_with_actor(status, now_rfc3339, correlation_id, WORKER_ACTOR)
}

fn status_event_with_actor(
    status: DocumentStatus,
    now_rfc3339: &str,
    correlation_id: &str,
    actor: &str,
) -> BsonDocument {
    doc! {
        "status": status.as_str(),
        "at": now_rfc3339,
        "actor": actor,
        "correlation_id": correlation_id,
    }
}
