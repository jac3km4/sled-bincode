use thiserror::Error;

pub type Result<A, E = Error> = std::result::Result<A, E>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    SledError(#[from] sled::Error),
    #[error("decode error: {0}")]
    DecodeError(bincode::error::DecodeError),
    #[error("encode error: {0}")]
    EncodeError(bincode::error::EncodeError),
}
