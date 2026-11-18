//! Registro y desregistro de replicas del ocr-worker en Consul.
//!
//! S11-P1-C18: cada replica se registra en el agente local (no en el
//! servidor) con ID `<servicio>-<hostname>` y se desregistra en el apagado
//! ordenado. El registro es no bloqueante (retry con backoff) y Consul NO es
//! dependencia de arranque (plan S11 §1).

use std::net::{ToSocketAddrs, UdpSocket};
use std::path::PathBuf;
use std::time::Duration;

use serde::Serialize;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

const DEFAULT_REGISTRATION_ADDR: &str = "https://consul-agent:8501";
const DEFAULT_SERVICE_NAME: &str = "ocr-worker";
const DEFAULT_AGENT_PORT: u16 = 8501;
const REGISTER_TIMEOUT: Duration = Duration::from_secs(5);
const RETRY_INITIAL: Duration = Duration::from_secs(5);
const RETRY_MAX: Duration = Duration::from_secs(60);

#[derive(Clone, Debug)]
pub struct ConsulRegConfig {
    pub base_url: String,
    pub service_name: String,
    pub disabled: bool,
    pub ca_cert_path: Option<PathBuf>,
    pub token_path: Option<PathBuf>,
}

impl ConsulRegConfig {
    pub fn from_env() -> Self {
        let disabled = std::env::var("CONSUL_REGISTRATION_DISABLED")
            .ok()
            .map(|v| v == "true" || v == "1")
            .unwrap_or(false);
        let base_url = std::env::var("CONSUL_REGISTRATION_ADDR")
            .unwrap_or_else(|_| DEFAULT_REGISTRATION_ADDR.to_string());
        let service_name =
            std::env::var("SERVICE_NAME").unwrap_or_else(|_| DEFAULT_SERVICE_NAME.to_string());
        let ca_cert_path = std::env::var("CONSUL_CACERT").ok().map(PathBuf::from);
        let token_path = std::env::var("CONSUL_TOKEN_FILE")
            .ok()
            .or_else(|| std::env::var("CONSUL_HTTP_TOKEN_FILE").ok())
            .map(PathBuf::from);
        Self {
            base_url,
            service_name,
            disabled,
            ca_cert_path,
            token_path,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Registration {
    pub id: String,
    pub name: String,
    pub address: String,
    pub port: u16,
}

#[derive(Serialize)]
struct CheckPayload {
    #[serde(rename = "HTTP")]
    http: String,
    #[serde(rename = "Interval")]
    interval: String,
    #[serde(rename = "Timeout")]
    timeout: String,
    #[serde(rename = "DeregisterCriticalServiceAfter")]
    deregister_critical_service_after: String,
}

#[derive(Serialize)]
struct RegisterPayload {
    #[serde(rename = "ID")]
    id: String,
    #[serde(rename = "Name")]
    name: String,
    #[serde(rename = "Address")]
    address: String,
    #[serde(rename = "Port")]
    port: u16,
    #[serde(rename = "Tags")]
    tags: Vec<String>,
    #[serde(rename = "Check")]
    check: CheckPayload,
}

pub struct ConsulRegistrar {
    cfg: ConsulRegConfig,
    http: reqwest::Client,
    reg: Registration,
}

impl ConsulRegistrar {
    pub fn new(cfg: ConsulRegConfig, hostname: String, address: String, port: u16) -> Self {
        let http = build_http_client(cfg.ca_cert_path.as_ref());
        let reg = Registration {
            id: format!("{}-{}", cfg.service_name, hostname),
            name: format!("svc-{}", cfg.service_name),
            address,
            port,
        };
        Self { cfg, http, reg }
    }

    pub fn registration(&self) -> &Registration {
        &self.reg
    }

    pub fn is_disabled(&self) -> bool {
        self.cfg.disabled
    }

    fn token(&self) -> Option<String> {
        self.cfg
            .token_path
            .as_ref()
            .and_then(|p| std::fs::read_to_string(p).ok())
            .map(|s| s.trim().to_string())
    }

    fn payload(&self) -> RegisterPayload {
        RegisterPayload {
            id: self.reg.id.clone(),
            name: self.reg.name.clone(),
            address: self.reg.address.clone(),
            port: self.reg.port,
            tags: vec![
                "http".to_string(),
                "s11".to_string(),
                self.cfg.service_name.clone(),
            ],
            check: CheckPayload {
                http: format!("http://{}:{}/healthz", self.reg.address, self.reg.port),
                interval: "10s".to_string(),
                timeout: "3s".to_string(),
                deregister_critical_service_after: "1m".to_string(),
            },
        }
    }

    pub async fn register(&self) -> Result<(), String> {
        if self.cfg.disabled {
            return Ok(());
        }
        let url = format!(
            "{}/v1/agent/service/register",
            self.cfg.base_url.trim_end_matches('/')
        );
        let body = serde_json::to_string(&self.payload()).map_err(|e| {
            registration_failed(format!("registro en consul: serializar payload: {e}"))
        })?;
        let mut req = self.http.put(&url).timeout(REGISTER_TIMEOUT);
        if let Some(tok) = self.token() {
            req = req.header("X-Consul-Token", tok);
        }
        let resp = req
            .body(body)
            .send()
            .await
            .map_err(|e| registration_failed(format!("registro en consul: {e}")))?;
        if !resp.status().is_success() {
            return Err(registration_failed(format!(
                "registro en consul: status {}",
                resp.status()
            )));
        }
        info!(
            service = "ocr-worker",
            msg = "registrado en consul",
            id = %self.reg.id,
            address = %self.reg.address,
            port = self.reg.port
        );
        Ok(())
    }

    pub async fn deregister(&self) -> Result<(), String> {
        if self.cfg.disabled {
            return Ok(());
        }
        let id = percent_encode(&self.reg.id);
        let url = format!(
            "{}/v1/agent/service/deregister/{}",
            self.cfg.base_url.trim_end_matches('/'),
            id
        );
        let mut req = self.http.put(&url);
        if let Some(tok) = self.token() {
            req = req.header("X-Consul-Token", tok);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| registration_failed(format!("desregistro de consul: {e}")))?;
        if !resp.status().is_success() {
            return Err(registration_failed(format!(
                "desregistro de consul: status {}",
                resp.status()
            )));
        }
        info!(
            service = "ocr-worker",
            msg = "desregistrado de consul",
            id = %self.reg.id
        );
        Ok(())
    }

    pub async fn register_with_retry(&self, cancel: CancellationToken) {
        if self.cfg.disabled {
            return;
        }
        let mut delay = RETRY_INITIAL;
        loop {
            if cancel.is_cancelled() {
                return;
            }
            match self.register().await {
                Ok(()) => return,
                Err(e) => {
                    warn!(
                        service = "ocr-worker",
                        msg = "registro en consul fallido; el servicio sigue operando y se reintentara",
                        id = %self.reg.id,
                        address = %self.reg.address,
                        error = %e,
                        retry_in = delay.as_secs()
                    );
                    tokio::select! {
                        _ = cancel.cancelled() => return,
                        _ = tokio::time::sleep(delay) => {}
                    }
                    delay = (delay * 2).min(RETRY_MAX);
                }
            }
        }
    }
}

/// Construye el cliente HTTP con la CA de Consul. Un fallo de lectura o de
/// parseo de la CA no aborta el arranque: se loguea y se sigue con el almacen
/// de confianza del sistema (el registro fallara y se reintentara).
fn build_http_client(ca_cert_path: Option<&PathBuf>) -> reqwest::Client {
    let mut builder = reqwest::Client::builder();
    if let Some(path) = ca_cert_path {
        match std::fs::read(path) {
            Ok(bytes) => match reqwest::Certificate::from_pem(&bytes) {
                Ok(cert) => builder = builder.add_root_certificate(cert),
                Err(e) => warn!(
                    service = "ocr-worker",
                    msg = "CA de consul invalida; se usa el almacen del sistema",
                    error = %e
                ),
            },
            Err(e) => warn!(
                service = "ocr-worker",
                msg = "no se pudo leer la CA de consul; se usa el almacen del sistema",
                error = %e
            ),
        }
    }
    builder.build().unwrap_or_else(|e| {
        warn!(
            service = "ocr-worker",
            msg = "no se pudo construir el cliente HTTP de consul",
            error = %e
        );
        reqwest::Client::new()
    })
}

/// Incrementa la metrica de fallos y devuelve el motivo tal cual.
fn registration_failed(reason: String) -> String {
    metrics::counter!("worker_ocr_consul_registration_failed_total").increment(1);
    reason
}

pub fn percent_encode(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for b in input.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => {
                out.push('%');
                out.push_str(&format!("{b:02X}"));
            }
        }
    }
    out
}

/// Separa host y puerto de la direccion del agente (`https://host:8501`).
fn split_host_port(base_url: &str) -> (String, u16) {
    let without_scheme = base_url
        .trim_start_matches("https://")
        .trim_start_matches("http://");
    let authority = without_scheme.split('/').next().unwrap_or(without_scheme);
    match authority.rsplit_once(':') {
        Some((host, port)) => match port.parse::<u16>() {
            Ok(port) => (host.trim_matches(['[', ']']).to_string(), port),
            Err(_) => (authority.to_string(), DEFAULT_AGENT_PORT),
        },
        None => (authority.to_string(), DEFAULT_AGENT_PORT),
    }
}

/// Deriva la IPv4 del contenedor sobre la red `internal` haciendo un UDP
/// connect (sin enviar paquetes) al agente ya resuelto por DNS.
///
/// C14 midio que el DNS embebido de Docker no resuelve el hostname corto del
/// contenedor para otros contenedores: publicar el hostname deja el check
/// critico para siempre. El fallback al hostname solo existe para no quedar
/// sin Address cuando no hay red.
pub fn resolve_address(base_url: &str) -> String {
    let (host, port) = split_host_port(base_url);
    if let Ok(addrs) = (host.as_str(), port).to_socket_addrs() {
        for addr in addrs {
            let Ok(sock) = UdpSocket::bind("0.0.0.0:0") else {
                continue;
            };
            if sock.connect(addr).is_err() {
                continue;
            }
            let Ok(local) = sock.local_addr() else {
                continue;
            };
            if !local.ip().is_loopback() {
                return local.ip().to_string();
            }
        }
    }
    hostname::get()
        .ok()
        .and_then(|h| h.into_string().ok())
        .unwrap_or_else(|| "localhost".to_string())
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use super::*;

    fn test_cfg(base_url: String) -> ConsulRegConfig {
        ConsulRegConfig {
            base_url,
            service_name: DEFAULT_SERVICE_NAME.to_string(),
            disabled: false,
            ca_cert_path: None,
            token_path: None,
        }
    }

    fn registrar(base_url: String) -> ConsulRegistrar {
        ConsulRegistrar::new(
            test_cfg(base_url),
            "replica-1".to_string(),
            "172.24.0.5".to_string(),
            8081,
        )
    }

    /// Arranca un stub HTTP de un solo request y devuelve su URL y el bytes
    /// capturado (request completo, cabeceras incluidas).
    fn http_stub(status: u16) -> (String, Arc<Mutex<Vec<u8>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap_or_else(|e| panic!("bind: {e}"));
        let addr = listener
            .local_addr()
            .unwrap_or_else(|e| panic!("local_addr: {e}"));
        let captured = Arc::new(Mutex::new(Vec::new()));
        let sink = captured.clone();
        std::thread::spawn(move || {
            let Ok((mut stream, _)) = listener.accept() else {
                return;
            };
            let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
            let mut buf = Vec::new();
            let mut chunk = [0u8; 1024];
            loop {
                match stream.read(&mut chunk) {
                    Ok(0) => break,
                    Ok(n) => {
                        buf.extend_from_slice(&chunk[..n]);
                        if request_complete(&buf) {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
            if let Ok(mut guard) = sink.lock() {
                *guard = buf;
            }
            let resp =
                format!("HTTP/1.1 {status} Stub\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
            let _ = stream.write_all(resp.as_bytes());
        });
        (format!("http://{addr}"), captured)
    }

    fn header_end(buf: &[u8]) -> Option<usize> {
        buf.windows(4).position(|w| w == b"\r\n\r\n")
    }

    fn request_complete(buf: &[u8]) -> bool {
        let Some(end) = header_end(buf) else {
            return false;
        };
        let headers = String::from_utf8_lossy(&buf[..end]).to_lowercase();
        let mut content_length = 0usize;
        for line in headers.lines() {
            if let Some(value) = line.strip_prefix("content-length:") {
                content_length = value.trim().parse().unwrap_or(0);
            }
        }
        buf.len() >= end + 4 + content_length
    }

    fn request_text(buf: &[u8]) -> String {
        String::from_utf8_lossy(buf).to_string()
    }

    fn request_body(buf: &[u8]) -> Vec<u8> {
        match header_end(buf) {
            Some(end) => buf[end + 4..].to_vec(),
            None => Vec::new(),
        }
    }

    #[test]
    fn service_id_y_nombre_derivan_del_hostname() {
        let r = registrar("https://consul-agent:8501".to_string());
        assert_eq!(r.registration().id, "ocr-worker-replica-1");
        assert_eq!(r.registration().name, "svc-ocr-worker");
    }

    #[test]
    fn service_name_alternativo_cambia_id_y_nombre() {
        let mut cfg = test_cfg("https://consul-agent:8501".to_string());
        cfg.service_name = "custom".to_string();
        let r = ConsulRegistrar::new(cfg, "abc".to_string(), "172.24.0.5".to_string(), 8081);
        assert_eq!(r.registration().id, "custom-abc");
        assert_eq!(r.registration().name, "svc-custom");
        assert_eq!(r.payload().tags[2], "custom");
    }

    #[test]
    fn payload_cumple_el_contrato() {
        let r = registrar("https://consul-agent:8501".to_string());
        let payload = r.payload();
        assert_eq!(payload.id, "ocr-worker-replica-1");
        assert_eq!(payload.name, "svc-ocr-worker");
        assert_eq!(payload.address, "172.24.0.5");
        assert_eq!(payload.port, 8081);
        assert_eq!(
            payload.tags,
            vec![
                "http".to_string(),
                "s11".to_string(),
                "ocr-worker".to_string(),
            ]
        );
        assert_eq!(payload.check.http, "http://172.24.0.5:8081/healthz");
        assert_eq!(payload.check.interval, "10s");
        assert_eq!(payload.check.timeout, "3s");
        assert_eq!(payload.check.deregister_critical_service_after, "1m");
    }

    #[tokio::test]
    async fn register_hace_put_al_agente() {
        let (url, captured) = http_stub(200);
        registrar(url)
            .register()
            .await
            .unwrap_or_else(|e| panic!("register: {e}"));
        let raw = captured.lock().map(|g| g.clone()).unwrap_or_default();
        let text = request_text(&raw);
        assert!(
            text.starts_with("PUT /v1/agent/service/register "),
            "linea de request inesperada: {text}"
        );
        let body: serde_json::Value = serde_json::from_slice(&request_body(&raw))
            .unwrap_or_else(|e| panic!("body JSON invalido: {e}"));
        assert_eq!(body["ID"], "ocr-worker-replica-1");
        assert_eq!(body["Name"], "svc-ocr-worker");
        assert_eq!(body["Check"]["HTTP"], "http://172.24.0.5:8081/healthz");
    }

    #[tokio::test]
    async fn deregister_hace_put_con_el_id() {
        let (url, captured) = http_stub(200);
        registrar(url)
            .deregister()
            .await
            .unwrap_or_else(|e| panic!("deregister: {e}"));
        let raw = captured.lock().map(|g| g.clone()).unwrap_or_default();
        let text = request_text(&raw);
        assert!(
            text.starts_with("PUT /v1/agent/service/deregister/ocr-worker-replica-1 "),
            "linea de request inesperada: {text}"
        );
    }

    #[tokio::test]
    async fn disabled_no_hace_http() {
        let mut cfg = test_cfg("http://127.0.0.1:1".to_string());
        cfg.disabled = true;
        let r = ConsulRegistrar::new(cfg, "replica-1".to_string(), "172.24.0.5".to_string(), 8081);
        assert!(r.is_disabled());
        assert!(r.register().await.is_ok());
        assert!(r.deregister().await.is_ok());
    }

    #[tokio::test]
    async fn registro_no_2xx_devuelve_error() {
        let (url, _captured) = http_stub(500);
        let err = match registrar(url).register().await {
            Ok(()) => panic!("se esperaba Err, no Ok"),
            Err(e) => e,
        };
        assert!(err.contains("status 500"), "error inesperado: {err}");
    }

    #[test]
    fn percent_encode_escapa_caracteres_reservados() {
        assert_eq!(percent_encode("a b/c"), "a%20b%2Fc");
        assert_eq!(percent_encode("abc-123_~."), "abc-123_~.");
    }

    #[test]
    fn split_host_port_tolera_esquema_y_puerto() {
        assert_eq!(
            split_host_port("https://consul-agent:8501"),
            ("consul-agent".to_string(), 8501)
        );
        assert_eq!(
            split_host_port("https://consul-agent"),
            ("consul-agent".to_string(), DEFAULT_AGENT_PORT)
        );
    }
}
