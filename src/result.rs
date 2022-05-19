use thiserror::Error;

pub type Result<A, E = Error> = std::result::Result<A, E>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    SledError(#[from] sled::Error),
    #[error("{0}")]
    DecodeError(bincode::error::DecodeError),
    #[error("{0}")]
    EncodeError(bincode::error::EncodeError),
}

#[derive(Debug, Error)]
pub enum TransactionError {
    #[error("{0}")]
    Unabortable(#[from] sled::transaction::UnabortableTransactionError),
    #[error("{0}")]
    EncodeError(bincode::error::EncodeError),
}
