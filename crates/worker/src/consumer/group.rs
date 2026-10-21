use redis::{aio::MultiplexedConnection, RedisResult};

use super::config::ConsumerConfig;

#[derive(thiserror::Error, Debug)]
pub enum GroupError {
    #[error("redis: {0}")]
    Redis(#[from] redis::RedisError),
}

pub async fn ensure_group(
    conn: &mut MultiplexedConnection,
    cfg: &ConsumerConfig,
) -> Result<(), GroupError> {
    // S1-P3-02: XGROUP CREATE con MKSTREAM
    let res: RedisResult<()> = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg(&cfg.stream)
        .arg(&cfg.group)
        .arg("0")
        .arg("MKSTREAM")
        .query_async(conn)
        .await;
    match res {
        Ok(_) => Ok(()),
        Err(e) => {
            let msg = e.to_string();
            if msg.contains("BUSYGROUP") {
                Ok(())
            } else {
                Err(GroupError::Redis(e))
            }
        }
    }
}
