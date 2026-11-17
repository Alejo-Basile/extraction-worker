use crate::config::OcrConfig;
#[allow(unused_imports)]
use crate::pipeline::OcrPipeline;
#[allow(unused_imports)]
use extraction_core::WorkItem;
use metrics::{counter, histogram};
use redis::{aio, streams::{StreamReadOptions, StreamReadReply}};
#[allow(unused_imports)]
use serde_json;
use tracing::{error, info, warn};
#[allow(unused_imports)]
use std::time::Instant;

#[allow(dead_code)]
pub struct OcrConsumer {
    cfg: OcrConfig,
}

impl OcrConsumer {
    pub fn new(cfg: OcrConfig) -> Self {
        Self { cfg }
    }

    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        info!("OCR consumer starting, stream={}", self.cfg.redis_ocr_stream);
        
        let client = redis::Client::open(&self.cfg.redis_url)?;
        let mut con = aio::MultiplexedConnection::new(client.get_connection().await?).await?;
        
        // Asegurar grupo
        let _: redis::RedisResult<()> = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(&self.cfg.redis_ocr_stream)
            .arg(&self.cfg.redis_ocr_group)
            .arg("$")
            .arg("MKSTREAM")
            .query_async(&mut con)
            .await;
        
        let consumer_name = std::env::var("HOSTNAME").unwrap_or_else(|_| "ocr-worker-1".into());
        
        loop {
            let reply: StreamReadReply = redis::cmd("XREADGROUP")
                .arg("GROUP")
                .arg(&self.cfg.redis_ocr_group)
                .arg(&consumer_name)
                .arg("COUNT")
                .arg(self.cfg.consumer_count)
                .arg("BLOCK")
                .arg(self.cfg.consumer_block_ms)
                .arg("STREAMS")
                .arg(&self.cfg.redis_ocr_stream)
                .arg(">")
                .query_async(&mut con)
                .await?;
            
            counter!("worker_ocr_poll_total").increment(1);
            
            for stream in &reply.keys {
                for message in &stream.ids {
                    let msg_id = message.id.clone();
                    let payload = message.map.get("payload")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    
                    let start = Instant::now();
                    match serde_json::from_str::<WorkItem>(payload) {
                        Ok(item) => {
                            info!("Procesando documento {}", item.document_id);
                            let mut pipeline = OcrPipeline::new(self.cfg.clone());
                            match pipeline.run(&item).await {
                                Ok(_) => {
                                    counter!("worker_ocr_processed_total").increment(1);
                                    histogram!("worker_ocr_duration_seconds").record(start.elapsed().as_secs_f64());
                                    let _: redis::RedisResult<()> = redis::cmd("XACK")
                                        .arg(&self.cfg.redis_ocr_stream)
                                        .arg(&self.cfg.redis_ocr_group)
                                        .arg(&msg_id)
                                        .query_async(&mut con)
                                        .await;
                                }
                                Err(e) => {
                                    counter!("worker_ocr_errors_total").increment(1);
                                    error!("Error OCR para {}: {}", item.document_id, e);
                                    // Envío a DLQ
                                    let _: redis::RedisResult<()> = redis::cmd("XADD")
                                        .arg(&self.cfg.redis_ocr_dlq_stream)
                                        .arg("*")
                                        .arg("original_id")
                                        .arg(&msg_id)
                                        .arg("document_id")
                                        .arg(&item.document_id)
                                        .arg("error")
                                        .arg(&e)
                                        .query_async(&mut con)
                                        .await;
                                    let _: redis::RedisResult<()> = redis::cmd("XACK")
                                        .arg(&self.cfg.redis_ocr_stream)
                                        .arg(&self.cfg.redis_ocr_group)
                                        .arg(&msg_id)
                                        .query_async(&mut con)
                                        .await;
                                }
                            }
                        }
                        Err(e) => {
                            warn!("Payload inválido: {}", e);
                            let _: redis::RedisResult<()> = redis::cmd("XACK")
                                .arg(&self.cfg.redis_ocr_stream)
                                .arg(&self.cfg.redis_ocr_group)
                                .arg(&msg_id)
                                .query_async(&mut con)
                                .await;
                        }
                    }
                }
            }
        }
    }
}
