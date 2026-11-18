use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigKvError {
    #[error("transport error: {0}")]
    Transport(String),
    #[error("decode error: {0}")]
    Decode(String),
    #[error("http status {0}")]
    HttpStatus(u16),
    #[error("invalid environment: {0}")]
    InvalidEnv(String),
    #[error("cache io error: {0}")]
    CacheIo(String),
}
