use crate::error::ConfigKvError;
use base64::Engine;
use serde::Deserialize;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct ClientSettings {
    pub base_url: String,
    pub ca_cert_path: Option<PathBuf>,
    pub token_path: Option<PathBuf>,
    pub prefix: String,
    pub kv_disabled: bool,
    pub cache_path: PathBuf,
}

impl ClientSettings {
    pub fn from_env() -> Self {
        let kv_disabled = std::env::var("CONSUL_KV_DISABLED")
            .ok()
            .map(|v| v == "true" || v == "1")
            .unwrap_or(false);
        let base_url =
            std::env::var("CONSUL_HTTP_ADDR").unwrap_or_else(|_| "https://consul:8501".to_string());
        let ca_cert_path = std::env::var("CONSUL_CACERT").ok().map(PathBuf::from);
        let token_path = std::env::var("CONSUL_TOKEN_FILE")
            .ok()
            .or_else(|| std::env::var("CONSUL_HTTP_TOKEN_FILE").ok())
            .map(PathBuf::from);
        let prefix = std::env::var("CONSUL_KV_PREFIX")
            .unwrap_or_else(|_| "config/extraction-worker/".to_string());
        let cache_path = std::env::var("CONSUL_KV_CACHE_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("/var/lib/worker/config-cache.json"));
        Self {
            base_url,
            ca_cert_path,
            token_path,
            prefix,
            kv_disabled,
            cache_path,
        }
    }
}

#[derive(Clone, Debug)]
pub struct KvEntry {
    pub key: String,
    pub value: String,
}

impl KvEntry {
    pub fn decode_value(&self) -> Result<String, ConfigKvError> {
        let b = base64::engine::general_purpose::STANDARD
            .decode(self.value.trim())
            .or_else(|_| base64::engine::general_purpose::STANDARD_NO_PAD.decode(self.value.trim()))
            .map_err(|e| ConfigKvError::Decode(e.to_string()))?;
        String::from_utf8(b).map_err(|e| ConfigKvError::Decode(e.to_string()))
    }
}

#[derive(Clone, Debug)]
pub struct KvPage {
    pub entries: Vec<KvEntry>,
    pub index: u64,
}

#[derive(Deserialize)]
struct RawEntry {
    #[serde(rename = "Key")]
    key: Option<String>,
    #[serde(rename = "Value")]
    value: Option<String>,
}

pub struct Client {
    settings: ClientSettings,
    http: reqwest::Client,
}

impl Client {
    pub fn new(settings: ClientSettings) -> Result<Self, ConfigKvError> {
        // CONSUL_CACERT: CA is loaded from a file path, never inlined in config or env.
        let mut builder = reqwest::Client::builder();
        if let Some(path) = &settings.ca_cert_path {
            let bytes =
                std::fs::read(path).map_err(|e| ConfigKvError::InvalidEnv(e.to_string()))?;
            if !bytes
                .windows(27)
                .any(|w| w == b"-----BEGIN CERTIFICATE-----")
            {
                return Err(ConfigKvError::Decode("not a pem".into()));
            }
            let cert = reqwest::Certificate::from_pem(&bytes)
                .map_err(|e| ConfigKvError::Decode(e.to_string()))?;
            builder = builder.add_root_certificate(cert);
        }
        let http = builder
            .build()
            .map_err(|e| ConfigKvError::Transport(e.to_string()))?;
        Ok(Self { settings, http })
    }
    pub fn settings(&self) -> &ClientSettings {
        &self.settings
    }
    fn token(&self) -> Option<String> {
        self.settings
            .token_path
            .as_ref()
            .and_then(|p| std::fs::read_to_string(p).ok())
            .map(|s| s.trim().to_string())
    }
    fn trimmed_prefix(&self) -> String {
        self.settings.prefix.trim_matches('/').to_string()
    }
    pub async fn get_recurse(&self, index: Option<u64>) -> Result<KvPage, ConfigKvError> {
        let url = format!(
            "{}/v1/kv/{}",
            self.settings.base_url.trim_end_matches('/'),
            self.trimmed_prefix()
        );
        let mut req = self.http.get(&url).query(&[("recurse", "")]);
        if let Some(i) = index {
            req = req.query(&[("index", i.to_string().as_str())]);
        }
        req = req.query(&[("wait", "5m")]);
        if let Some(t) = self.token() {
            req = req.header("X-Consul-Token", t);
        }
        let resp = req
            .timeout(Duration::from_secs(360))
            .send()
            .await
            .map_err(|e| ConfigKvError::Transport(e.to_string()))?;
        let status = resp.status().as_u16();
        if status != 200 {
            return Err(ConfigKvError::HttpStatus(status));
        }
        let hdr_index: u64 = resp
            .headers()
            .get("X-Consul-Index")
            .and_then(|h| h.to_str().ok())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let body: Vec<RawEntry> = resp
            .json()
            .await
            .map_err(|e| ConfigKvError::Decode(e.to_string()))?;
        let mut entries = Vec::new();
        for r in body {
            entries.push(KvEntry {
                key: r.key.unwrap_or_default(),
                value: r.value.unwrap_or_default(),
            });
        }
        Ok(KvPage {
            entries,
            index: hdr_index,
        })
    }
    pub async fn get_once(&self) -> Result<KvPage, ConfigKvError> {
        let url = format!(
            "{}/v1/kv/{}",
            self.settings.base_url.trim_end_matches('/'),
            self.trimmed_prefix()
        );
        let mut req = self.http.get(&url).query(&[("recurse", "")]);
        if let Some(t) = self.token() {
            req = req.header("X-Consul-Token", t);
        }
        let resp = req
            .timeout(Duration::from_secs(2))
            .send()
            .await
            .map_err(|e| ConfigKvError::Transport(e.to_string()))?;
        let status = resp.status().as_u16();
        if status != 200 {
            return Err(ConfigKvError::HttpStatus(status));
        }
        let hdr_index: u64 = resp
            .headers()
            .get("X-Consul-Index")
            .and_then(|h| h.to_str().ok())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let body: Vec<RawEntry> = resp
            .json()
            .await
            .map_err(|e| ConfigKvError::Decode(e.to_string()))?;
        let mut entries = Vec::new();
        for r in body {
            entries.push(KvEntry {
                key: r.key.unwrap_or_default(),
                value: r.value.unwrap_or_default(),
            });
        }
        Ok(KvPage {
            entries,
            index: hdr_index,
        })
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    #[tokio::test]
    async fn ca_path_errors() {
        let mut s = ClientSettings::from_env();
        s.ca_cert_path = Some(std::path::PathBuf::from("/no/such/ca.pem"));
        assert!(matches!(Client::new(s), Err(ConfigKvError::InvalidEnv(_))));
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("ca.pem");
        std::fs::write(&p, b"not a pem").unwrap();
        let s2 = ClientSettings {
            base_url: "https://consul:8501".into(),
            ca_cert_path: Some(p),
            token_path: None,
            prefix: "config/extraction-worker/".into(),
            kv_disabled: false,
            cache_path: std::path::PathBuf::from("/tmp/c.json"),
        };
        assert!(matches!(Client::new(s2), Err(ConfigKvError::Decode(_))));
        let s3 = ClientSettings::from_env();
        assert!(Client::new(s3).is_ok());
    }
    #[tokio::test]
    async fn mock_http() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        // server task
        let handle = tokio::spawn(async move {
            // first request
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 4096];
            let n = sock.read(&mut buf).await.unwrap();
            let req = String::from_utf8_lossy(&buf[..n]).to_string();
            // capture
            // respond
            let body = r#"[{"Key":"config/extraction-worker/max_pages","Value":"MjAwMA==","CreateIndex":1,"ModifyIndex":7}]"#;
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nX-Consul-Index: 7\r\n\r\n{}",
                body.len(),
                body
            );
            sock.write_all(resp.as_bytes()).await.unwrap();
            // second request for non-2xx
            let (mut sock2, _) = listener.accept().await.unwrap();
            let mut buf2 = vec![0u8; 4096];
            let _ = sock2.read(&mut buf2).await.unwrap();
            let resp2 = "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n";
            sock2.write_all(resp2.as_bytes()).await.unwrap();
            req
        });
        // client with index 42
        let mut settings = ClientSettings::from_env();
        settings.base_url = format!("http://{}", addr);
        settings.kv_disabled = false;
        let client = Client::new(settings).unwrap();
        let page = client.get_recurse(Some(42)).await.unwrap();
        assert_eq!(page.index, 7);
        assert_eq!(page.entries.len(), 1);
        assert_eq!(page.entries[0].decode_value().unwrap(), "2000");
        // non-2xx
        let mut settings2 = ClientSettings::from_env();
        settings2.base_url = format!("http://{}", addr);
        let client2 = Client::new(settings2).unwrap();
        let r = client2.get_recurse(Some(1)).await;
        assert!(matches!(r, Err(ConfigKvError::HttpStatus(500))));
        let req = handle.await.unwrap();
        assert!(req.contains("recurse"));
        assert!(req.contains("wait=5m"));
        assert!(req.contains("index=42"));
        // connection refused
        let mut settings3 = ClientSettings::from_env();
        settings3.base_url = "http://127.0.0.1:1".to_string();
        let client3 = Client::new(settings3).unwrap();
        assert!(matches!(
            client3.get_recurse(None).await,
            Err(ConfigKvError::Transport(_))
        ));
    }
}
