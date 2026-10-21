use std::time::Duration;

use extraction_core::{ContractError, WorkItem};
use redis::{aio::MultiplexedConnection, AsyncCommands};
use tokio_util::sync::CancellationToken;

use super::config::ConsumerConfig;
use super::group;

#[derive(thiserror::Error, Debug)]
pub enum RunnerError {
    #[error("config: {0}")]
    Config(String),
    #[error("redis: {0}")]
    Redis(#[from] redis::RedisError),
    #[error("group: {0}")]
    Group(#[from] group::GroupError),
}

pub struct ConsumerRunner {
    cfg: ConsumerConfig,
}

impl ConsumerRunner {
    pub fn new(cfg: ConsumerConfig) -> Self {
        Self { cfg }
    }

    pub async fn run(&self, token: CancellationToken) -> Result<(), RunnerError> {
        let client = redis::Client::open(self.cfg.redis_url.as_str())
            .map_err(|e| RunnerError::Config(e.to_string()))?;
        let mut conn = client.get_multiplexed_async_connection().await?;

        group::ensure_group(&mut conn, &self.cfg).await?;

        loop {
            tokio::select! {
                _ = token.cancelled() => {
                    // S1-P3-03: apagado con cancelacion
                    break;
                }
                res = self.read_one(&mut conn) => {
                    match res {
                        Ok(None) => continue,
                        Ok(Some((id, item))) => {
                            let _ = (id, item);
                            // procesamiento se implementara en F2/F3
                        }
                        Err(e) => {
                            eprintln!("consumer error: {e}");
                            tokio::time::sleep(Duration::from_millis(500)).await;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn read_one(
        &self,
        conn: &mut MultiplexedConnection,
    ) -> Result<Option<(String, WorkItem)>, RunnerError> {
        // S1-P3-02: XREADGROUP COUNT 1 BLOCK 5000
        let opts = redis::streams::StreamReadOptions::default()
            .group(&self.cfg.group, &self.cfg.consumer_name)
            .count(self.cfg.count)
            .block(self.cfg.block_ms as usize);

        let keys = vec![self.cfg.stream.clone()];
        let streams: redis::streams::StreamReadReply = conn
            .xread_options(&keys, &vec![">".to_string()], &opts)
            .await?;

        if streams.keys.is_empty() {
            return Ok(None);
        }
        let key = &streams.keys[0];
        if key.ids.is_empty() {
            return Ok(None);
        }
        let id = key.ids[0].id.clone();
        let map = &key.ids[0].map;

        // extraer campos
        let mut fields = std::collections::HashMap::new();
        for (k, v) in map.iter() {
            if let redis::Value::BulkString(b) = v {
                if let Ok(s) = String::from_utf8(b.clone()) {
                    fields.insert(k.clone(), s);
                }
            }
        }

        // S1-P3-01: validar contrato
        let val = serde_json::Value::Object(
            fields
                .into_iter()
                .map(|(k, v)| (k, serde_json::Value::String(v)))
                .collect(),
        );
        match serde_json::from_value::<WorkItem>(val) {
            Ok(item) => {
                if let Err(ContractError::UnsupportedSchemaVersion(v)) = item.validate_schema() {
                    eprintln!("schema_version no soportada: {v} -> DLQ (pendiente S2-P3-07)");
                    // TODO: DLQ atomica
                    return Ok(None);
                }
                Ok(Some((id, item)))
            }
            Err(e) => {
                eprintln!("mensaje invalido segun contrato -> DLQ (pendiente S2-P3-07): {e}");
                // TODO: DLQ atomica con XADD+XACK
                Ok(None)
            }
        }
    }
}
