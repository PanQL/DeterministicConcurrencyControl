use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fmt;

pub type TxId = u64;
pub type BatchId = u64;
pub type ShardId = u64;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct Key(String);

impl Key {
    pub fn new(path: impl Into<String>) -> Result<Self> {
        let path = path.into();
        validate_path(&path)?;
        Ok(Self(path))
    }

    pub fn root() -> Self {
        Self("/".to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn parent(&self) -> Result<Self> {
        if self.0 == "/" {
            return Err(Error::InvalidPath("root has no parent".to_string()));
        }
        let idx = self
            .0
            .rfind('/')
            .ok_or_else(|| Error::InvalidPath(self.0.clone()))?;
        if idx == 0 {
            Ok(Self::root())
        } else {
            Ok(Self(self.0[..idx].to_string()))
        }
    }
}

impl fmt::Display for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl TryFrom<String> for Key {
    type Error = Error;

    fn try_from(value: String) -> Result<Self> {
        Key::new(value)
    }
}

impl From<&Key> for String {
    fn from(value: &Key) -> Self {
        value.0.clone()
    }
}

fn validate_path(path: &str) -> Result<()> {
    if path.is_empty() || !path.starts_with('/') {
        return Err(Error::InvalidPath(path.to_string()));
    }
    if path == "/" {
        return Ok(());
    }
    if path.ends_with('/') {
        return Err(Error::InvalidPath(path.to_string()));
    }
    for segment in path.split('/').skip(1) {
        if segment.is_empty() || segment == "." || segment == ".." {
            return Err(Error::InvalidPath(path.to_string()));
        }
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Inode {
    pub kind: NodeKind,
    pub child_count: u64,
}

impl Inode {
    pub fn file() -> Self {
        Self {
            kind: NodeKind::File,
            child_count: 0,
        }
    }

    pub fn directory(child_count: u64) -> Self {
        Self {
            kind: NodeKind::Directory,
            child_count,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum NodeKind {
    File,
    Directory,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FsOp {
    Create { path: Key },
    Mkdir { path: Key },
    Unlink { path: Key },
    Rmdir { path: Key },
    Rename { src: Key, dst: Key },
    Stat { path: Key },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Batch {
    pub batch_id: BatchId,
    pub txs: Vec<OrderedTx>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderedTx {
    pub tx_id: TxId,
    pub batch_index: u32,
    pub op: FsOp,
    pub read_set: BTreeSet<Key>,
    pub write_set: BTreeSet<Key>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum TxResult {
    Ok,
    NotFound,
    AlreadyExists,
    NotDirectory,
    DirectoryNotEmpty,
    Invalid,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReadValue {
    Present(Inode),
    Missing,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WriteOp {
    Put { key: Key, value: Inode },
    Delete { key: Key },
}

impl WriteOp {
    pub fn key(&self) -> &Key {
        match self {
            WriteOp::Put { key, .. } | WriteOp::Delete { key } => key,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TxExecutionOutput {
    pub result: TxResult,
    pub writes: Vec<WriteOp>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TxResultRecord {
    pub tx_id: TxId,
    pub shard_id: ShardId,
    pub result: TxResult,
}

pub fn parent_key(path: &Key) -> Result<Key> {
    path.parent()
}
