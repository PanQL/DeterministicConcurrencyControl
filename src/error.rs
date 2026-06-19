use crate::model::{BatchId, ShardId, TxId};
use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid path: {0}")]
    InvalidPath(String),
    #[error("invalid proto: {0}")]
    InvalidProto(String),
    #[error("invalid batch: {0}")]
    InvalidBatch(String),
    #[error("batch too large: {size} > {max}")]
    BatchTooLarge { size: usize, max: usize },
    #[error("redb error: {0}")]
    Redb(#[from] redb::Error),
    #[error("redb database error: {0}")]
    RedbDatabase(#[from] redb::DatabaseError),
    #[error("redb table error: {0}")]
    RedbTable(#[from] redb::TableError),
    #[error("redb transaction error: {0}")]
    RedbTransaction(#[from] redb::TransactionError),
    #[error("redb commit error: {0}")]
    RedbCommit(#[from] redb::CommitError),
    #[error("redb storage error: {0}")]
    RedbStorage(#[from] redb::StorageError),
    #[error("bincode error: {0}")]
    Bincode(#[from] bincode::Error),
    #[error("tonic transport error: {0}")]
    TonicTransport(#[from] tonic::transport::Error),
    #[error("tonic status: {0}")]
    TonicStatus(#[from] tonic::Status),
    #[error("channel closed: {0}")]
    ChannelClosed(String),
    #[error("task join error: {0}")]
    TaskJoin(String),
    #[error("lock invariant violation: {0}")]
    LockInvariant(String),
    #[error("speculation failed: {0}")]
    SpeculationFailed(String),
    #[error("missing peer shard {0}")]
    MissingPeer(ShardId),
    #[error("missing batch {0}")]
    MissingBatch(BatchId),
    #[error("missing transaction {0}")]
    MissingTx(TxId),
    #[error("checker failure: {0}")]
    Checker(String),
}

impl From<Error> for tonic::Status {
    fn from(value: Error) -> Self {
        match value {
            Error::InvalidPath(_)
            | Error::InvalidProto(_)
            | Error::InvalidBatch(_)
            | Error::BatchTooLarge { .. } => tonic::Status::invalid_argument(value.to_string()),
            _ => tonic::Status::internal(value.to_string()),
        }
    }
}
