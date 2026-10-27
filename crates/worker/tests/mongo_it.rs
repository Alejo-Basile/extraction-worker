//! Tests de integracion contra Mongo real (S2-P3-08/09, ADR-0006).
//!
//! Requieren el stack de labs (mongod con auth, rol del worker solo update):
//! `cd labs && cp .env.example .env && docker compose up -d`
//! `cargo test --all-features -- --ignored`
//!
//! El worker usa su usuario acotado (`MONGO_WORKER_URI`); el sembrado y la
//! verificacion usan el root del laboratorio (`MONGO_TEST_ROOT_URI`), porque
//! el rol del worker no puede leer.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use extraction_core::status::DocumentStatus;
use extraction_worker::consumer::resilience::Failure;
use extraction_worker::document_store::{DocumentStore, MongoConfig};
use mongodb::bson::{doc, Document as BsonDocument};
use mongodb::options::ClientOptions;
use mongodb::Client;

fn worker_config() -> MongoConfig {
    MongoConfig {
        uri: std::env::var("MONGO_WORKER_URI").unwrap_or_else(|_| {
            "mongodb://worker:worker-dev-secret@127.0.0.1:27017/app?authSource=app&directConnection=true"
                .to_string()
        }),
        db: "app".to_string(),
        timeout: Duration::from_secs(3),
    }
}

fn root_config() -> MongoConfig {
    MongoConfig {
        uri: std::env::var("MONGO_TEST_ROOT_URI").unwrap_or_else(|_| {
            "mongodb://root:worker-dev-root@127.0.0.1:27017/?directConnection=true".to_string()
        }),
        db: "app".to_string(),
        timeout: Duration::from_secs(3),
    }
}

fn dead_config() -> MongoConfig {
    MongoConfig {
        uri: "mongodb://127.0.0.1:1/?directConnection=true&serverSelectionTimeoutMS=100"
            .to_string(),
        db: "app".to_string(),
        timeout: Duration::from_millis(200),
    }
}

fn must<T, E: std::fmt::Display>(r: Result<T, E>) -> T {
    match r {
        Ok(v) => v,
        Err(e) => panic!("fallo inesperado: {e}"),
    }
}

fn now_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

fn new_id(tag: &str) -> String {
    format!("it-f3b-{tag}-{}-{}", std::process::id(), now_millis())
}

async fn root_client() -> Client {
    let cfg = root_config();
    let opts = must(ClientOptions::parse(&cfg.uri).await);
    must(Client::with_options(opts))
}

async fn seed(conn: &Client, id: &str, status: &str) {
    let documents = conn.database("app").collection::<BsonDocument>("documents");
    must(
        documents
            .insert_one(doc! {
                "_id": id,
                "status": status,
                "object_key": format!("raw-pdfs/{id}.pdf"),
                "correlation_id": id,
                "schema_version": 1,
                "status_history": [],
            })
            .await,
    );
}

async fn read_document(conn: &Client, id: &str) -> BsonDocument {
    let documents = conn.database("app").collection::<BsonDocument>("documents");
    must(documents.find_one(doc! {"_id": id}).await)
        .unwrap_or_else(|| panic!("el documento {id} debe existir"))
}

async fn delete_document(conn: &Client, id: &str) {
    let documents = conn.database("app").collection::<BsonDocument>("documents");
    let _ = documents.delete_one(doc! {"_id": id}).await;
}

const NOW: &str = "2026-11-30T10:15:03Z";

#[tokio::test]
#[ignore]
async fn claim_transiciona_a_processing_con_historia() {
    let root = root_client().await;
    let store = must(DocumentStore::connect(&worker_config()).await);
    let id = new_id("claim");

    seed(&root, &id, "QUEUED").await;
    let claimed = must(store.claim(&id, &id, NOW).await);
    assert!(claimed, "un documento QUEUED debe poder reclamarse");

    let after = read_document(&root, &id).await;
    assert_eq!(after.get_str("status").ok(), Some("PROCESSING"));
    let history = after
        .get_array("status_history")
        .unwrap_or_else(|e| panic!("debe haber status_history: {e}"));
    assert_eq!(history.len(), 1, "una entrada por la transicion");
    let entry = match history[0].as_document() {
        Some(d) => d,
        None => panic!("la entrada de historia debe ser un documento"),
    };
    assert_eq!(entry.get_str("status").ok(), Some("PROCESSING"));
    assert_eq!(entry.get_str("actor").ok(), Some("worker"));
    assert_eq!(entry.get_str("correlation_id").ok(), Some(id.as_str()));
    assert_eq!(entry.get_str("at").ok(), Some(NOW));

    delete_document(&root, &id).await;
}

#[tokio::test]
#[ignore]
async fn claim_no_matchea_con_estado_no_reclambiable() {
    let root = root_client().await;
    let store = must(DocumentStore::connect(&worker_config()).await);
    let id = new_id("nomatch");

    seed(&root, &id, "UPLOADED").await;
    let claimed = must(store.claim(&id, &id, NOW).await);
    assert!(!claimed, "UPLOADED no es reclambiable");
    let after = read_document(&root, &id).await;
    assert_eq!(after.get_str("status").ok(), Some("UPLOADED"));

    delete_document(&root, &id).await;
}

#[tokio::test]
#[ignore]
async fn complete_cierra_condicional_y_es_idempotente() {
    let root = root_client().await;
    let store = must(DocumentStore::connect(&worker_config()).await);
    let id = new_id("complete");

    seed(&root, &id, "QUEUED").await;
    assert!(must(store.claim(&id, &id, NOW).await));

    let txt_ref = format!("{id}.txt");
    let closed = must(store.complete(&id, &txt_ref, &id, NOW).await);
    assert!(closed, "el cierre debe matchear PROCESSING");

    let after = read_document(&root, &id).await;
    assert_eq!(after.get_str("status").ok(), Some("COMPLETED"));
    assert_eq!(after.get_str("txt_ref").ok(), Some(txt_ref.as_str()));
    assert_eq!(after.get_str("completed_at").ok(), Some(NOW));
    let history = after
        .get_array("status_history")
        .unwrap_or_else(|e| panic!("debe haber status_history: {e}"));
    assert_eq!(history.len(), 2, "claim + complete");

    // Repetir el cierre: no matchea y no pisa nada (S2-P3-08).
    let again = must(store.complete(&id, &txt_ref, &id, NOW).await);
    assert!(!again, "el segundo cierre no debe matchear");
    let after = read_document(&root, &id).await;
    assert_eq!(after.get_str("status").ok(), Some("COMPLETED"));
    let history = after
        .get_array("status_history")
        .unwrap_or_else(|e| panic!("debe haber status_history: {e}"));
    assert_eq!(history.len(), 2, "sin entradas duplicadas");

    delete_document(&root, &id).await;
}

#[tokio::test]
#[ignore]
async fn rol_del_worker_no_permite_insert() {
    let store = must(DocumentStore::connect(&worker_config()).await);
    let documents = store.collection();
    let res = documents
        .insert_one(doc! {"_id": new_id("noinsert"), "status": "QUEUED"})
        .await;
    match res {
        Err(e) => {
            let msg = e.to_string();
            assert!(
                msg.contains("Unauthorized") || msg.contains("not authorized"),
                "el rol acotado debe rechazar insert: {msg}"
            );
        }
        Ok(_) => panic!("el rol del worker no debe permitir insert (ADR-0006)"),
    }
}

#[tokio::test]
#[ignore]
async fn mongo_caido_en_el_cierre_da_error_transitorio() {
    // Simulacion del caos 4 (S4-P4-04): Mongo no responde cuando el worker
    // intenta cerrar. El error debe ser transitorio para dejar el mensaje
    // pendiente y que la compensacion borre el .txt.
    let store_res = DocumentStore::connect(&dead_config()).await;
    let store = match store_res {
        Ok(s) => s,
        Err(_) => panic!("connect no debe contactar al servidor todavia"),
    };
    match store
        .complete("it-f3b-caos", "it-f3b-caos.txt", "corr", NOW)
        .await
    {
        Err(Failure::Transient { .. }) => {}
        Err(Failure::CircuitOpen) => {}
        other => panic!("debe ser un fallo transitorio/circuito, obtuvo: {other:?}"),
    }
}

#[tokio::test]
#[ignore]
async fn readyz_del_stack_completo_responde_ok() {
    // /readyz real contra el stack de labs: Redis + MinIO + Mongo.
    let readiness = must(
        extraction_worker::http::Readiness::new(
            "redis://127.0.0.1:6379/",
            must(extraction_worker::objects::ObjectStoreSource::new(
                &extraction_worker::objects::S3Config {
                    endpoint: "http://127.0.0.1:9000".to_string(),
                    bucket_raw: "raw-pdfs".to_string(),
                    bucket_txt: "extracted-txt".to_string(),
                    access_key: "worker-dev".to_string(),
                    secret_key: "worker-dev-secret".to_string(),
                },
            )),
            Arc::new(must(DocumentStore::connect(&worker_config()).await)),
        )
        .map_err(|e| e.to_string()),
    );
    let checks = readiness.check_all().await;
    for (name, res) in checks {
        match res {
            Ok(()) => {}
            Err(e) => panic!("check {name} fallo contra el stack: {e}"),
        }
    }
}

#[allow(dead_code)]
fn _status_usado(_s: DocumentStatus) {}
