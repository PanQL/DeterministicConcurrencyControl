use crate::error::{Error, Result};
use crate::model::{
    parent_key, FsOp, Inode, Key, NodeKind, OrderedTx, ReadValue, TxExecutionOutput, TxResult,
    WriteOp,
};
use crate::router::ShardLayout;
use std::collections::{BTreeMap, BTreeSet};

pub fn derive_read_write_set(op: &FsOp) -> Result<(BTreeSet<Key>, BTreeSet<Key>)> {
    let mut read = BTreeSet::new();
    let mut write = BTreeSet::new();
    match op {
        FsOp::Mkdir { path } if path.as_str() == "/" => {
            read.insert(Key::root());
            write.insert(Key::root());
        }
        FsOp::Create { path } | FsOp::Unlink { path } | FsOp::Rmdir { path }
            if path.as_str() == "/" =>
        {
            read.insert(Key::root());
            write.insert(Key::root());
        }
        FsOp::Mkdir { path }
        | FsOp::Create { path }
        | FsOp::Unlink { path }
        | FsOp::Rmdir { path } => {
            let parent = parent_key(path)?;
            read.insert(parent.clone());
            read.insert(path.clone());
            write.insert(parent);
            write.insert(path.clone());
        }
        FsOp::Rename { src, dst } => {
            if src == dst || src.as_str() == "/" || dst.as_str() == "/" {
                read.insert(src.clone());
                read.insert(dst.clone());
                write = read.clone();
            } else {
                read.insert(parent_key(src)?);
                read.insert(parent_key(dst)?);
                read.insert(src.clone());
                read.insert(dst.clone());
                write = read.clone();
            }
        }
        FsOp::Stat { path } => {
            read.insert(path.clone());
        }
    }
    Ok((read, write))
}

pub fn validate_sets(tx: &OrderedTx) -> Result<()> {
    let (read, write) = derive_read_write_set(&tx.op)?;
    if read != tx.read_set {
        return Err(Error::InvalidBatch(format!(
            "tx {} read_set mismatch: expected {:?}, got {:?}",
            tx.tx_id, read, tx.read_set
        )));
    }
    if write != tx.write_set {
        return Err(Error::InvalidBatch(format!(
            "tx {} write_set mismatch: expected {:?}, got {:?}",
            tx.tx_id, write, tx.write_set
        )));
    }
    Ok(())
}

pub fn execute_deterministic(
    tx: &OrderedTx,
    full_reads: &BTreeMap<Key, ReadValue>,
) -> TxExecutionOutput {
    let output = match &tx.op {
        FsOp::Mkdir { path } => exec_mkdir(path, full_reads),
        FsOp::Create { path } => exec_create(path, full_reads),
        FsOp::Stat { path } => exec_stat(path, full_reads),
        FsOp::Unlink { path } => exec_unlink(path, full_reads),
        FsOp::Rmdir { path } => exec_rmdir(path, full_reads),
        FsOp::Rename { src, dst } => exec_rename(src, dst, full_reads),
    };
    output.unwrap_or_else(|_| TxExecutionOutput {
        result: TxResult::Invalid,
        writes: Vec::new(),
    })
}

pub fn filter_local_writes(
    writes: Vec<WriteOp>,
    local_shard: u64,
    layout: &ShardLayout,
) -> Vec<WriteOp> {
    writes
        .into_iter()
        .filter(|write| layout.shard_for_key(write.key()) == local_shard)
        .collect()
}

fn exec_mkdir(path: &Key, reads: &BTreeMap<Key, ReadValue>) -> Result<TxExecutionOutput> {
    if path.as_str() == "/" {
        return match read_key(reads, path)? {
            Some(_) => Ok(no_writes(TxResult::AlreadyExists)),
            None => Ok(with_writes(
                TxResult::Ok,
                vec![WriteOp::Put {
                    key: path.clone(),
                    value: Inode::directory(0),
                }],
            )),
        };
    }
    let parent = parent_key(path)?;
    let parent_inode = match read_key(reads, &parent)? {
        Some(inode) => inode,
        None => return Ok(no_writes(TxResult::NotFound)),
    };
    if parent_inode.kind != NodeKind::Directory {
        return Ok(no_writes(TxResult::NotDirectory));
    }
    if read_key(reads, path)?.is_some() {
        return Ok(no_writes(TxResult::AlreadyExists));
    }
    let mut new_parent = parent_inode.clone();
    new_parent.child_count += 1;
    Ok(with_writes(
        TxResult::Ok,
        vec![
            WriteOp::Put {
                key: parent,
                value: new_parent,
            },
            WriteOp::Put {
                key: path.clone(),
                value: Inode::directory(0),
            },
        ],
    ))
}

fn exec_create(path: &Key, reads: &BTreeMap<Key, ReadValue>) -> Result<TxExecutionOutput> {
    if path.as_str() == "/" {
        return Ok(no_writes(TxResult::Invalid));
    }
    let parent = parent_key(path)?;
    let parent_inode = match read_key(reads, &parent)? {
        Some(inode) => inode,
        None => return Ok(no_writes(TxResult::NotFound)),
    };
    if parent_inode.kind != NodeKind::Directory {
        return Ok(no_writes(TxResult::NotDirectory));
    }
    if read_key(reads, path)?.is_some() {
        return Ok(no_writes(TxResult::AlreadyExists));
    }
    let mut new_parent = parent_inode.clone();
    new_parent.child_count += 1;
    Ok(with_writes(
        TxResult::Ok,
        vec![
            WriteOp::Put {
                key: parent,
                value: new_parent,
            },
            WriteOp::Put {
                key: path.clone(),
                value: Inode::file(),
            },
        ],
    ))
}

fn exec_stat(path: &Key, reads: &BTreeMap<Key, ReadValue>) -> Result<TxExecutionOutput> {
    Ok(match read_key(reads, path)? {
        Some(_) => no_writes(TxResult::Ok),
        None => no_writes(TxResult::NotFound),
    })
}

fn exec_unlink(path: &Key, reads: &BTreeMap<Key, ReadValue>) -> Result<TxExecutionOutput> {
    if path.as_str() == "/" {
        return Ok(no_writes(TxResult::Invalid));
    }
    let parent = parent_key(path)?;
    let parent_inode = match read_key(reads, &parent)? {
        Some(inode) => inode,
        None => return Ok(no_writes(TxResult::NotFound)),
    };
    if parent_inode.kind != NodeKind::Directory {
        return Ok(no_writes(TxResult::NotDirectory));
    }
    let target = match read_key(reads, path)? {
        Some(inode) => inode,
        None => return Ok(no_writes(TxResult::NotFound)),
    };
    if target.kind == NodeKind::Directory {
        return Ok(no_writes(TxResult::Invalid));
    }
    let mut new_parent = parent_inode.clone();
    new_parent.child_count = new_parent.child_count.saturating_sub(1);
    Ok(with_writes(
        TxResult::Ok,
        vec![
            WriteOp::Put {
                key: parent,
                value: new_parent,
            },
            WriteOp::Delete { key: path.clone() },
        ],
    ))
}

fn exec_rmdir(path: &Key, reads: &BTreeMap<Key, ReadValue>) -> Result<TxExecutionOutput> {
    if path.as_str() == "/" {
        return Ok(no_writes(TxResult::Invalid));
    }
    let parent = parent_key(path)?;
    let parent_inode = match read_key(reads, &parent)? {
        Some(inode) => inode,
        None => return Ok(no_writes(TxResult::NotFound)),
    };
    if parent_inode.kind != NodeKind::Directory {
        return Ok(no_writes(TxResult::NotDirectory));
    }
    let target = match read_key(reads, path)? {
        Some(inode) => inode,
        None => return Ok(no_writes(TxResult::NotFound)),
    };
    if target.kind != NodeKind::Directory {
        return Ok(no_writes(TxResult::NotDirectory));
    }
    if target.child_count > 0 {
        return Ok(no_writes(TxResult::DirectoryNotEmpty));
    }
    let mut new_parent = parent_inode.clone();
    new_parent.child_count = new_parent.child_count.saturating_sub(1);
    Ok(with_writes(
        TxResult::Ok,
        vec![
            WriteOp::Put {
                key: parent,
                value: new_parent,
            },
            WriteOp::Delete { key: path.clone() },
        ],
    ))
}

fn exec_rename(
    src: &Key,
    dst: &Key,
    reads: &BTreeMap<Key, ReadValue>,
) -> Result<TxExecutionOutput> {
    if src == dst || src.as_str() == "/" || dst.as_str() == "/" {
        return Ok(no_writes(TxResult::Invalid));
    }
    let src_parent = parent_key(src)?;
    let dst_parent = parent_key(dst)?;
    let src_parent_inode = match read_key(reads, &src_parent)? {
        Some(inode) => inode,
        None => return Ok(no_writes(TxResult::NotFound)),
    };
    let dst_parent_inode = match read_key(reads, &dst_parent)? {
        Some(inode) => inode,
        None => return Ok(no_writes(TxResult::NotFound)),
    };
    if src_parent_inode.kind != NodeKind::Directory || dst_parent_inode.kind != NodeKind::Directory
    {
        return Ok(no_writes(TxResult::NotDirectory));
    }
    let src_inode = match read_key(reads, src)? {
        Some(inode) => inode,
        None => return Ok(no_writes(TxResult::NotFound)),
    };
    if read_key(reads, dst)?.is_some() {
        return Ok(no_writes(TxResult::AlreadyExists));
    }
    if src_inode.kind == NodeKind::Directory && src_inode.child_count > 0 {
        return Ok(no_writes(TxResult::DirectoryNotEmpty));
    }
    let mut writes = vec![
        WriteOp::Delete { key: src.clone() },
        WriteOp::Put {
            key: dst.clone(),
            value: src_inode.clone(),
        },
    ];
    if src_parent != dst_parent {
        let mut new_src_parent = src_parent_inode.clone();
        new_src_parent.child_count = new_src_parent.child_count.saturating_sub(1);
        let mut new_dst_parent = dst_parent_inode.clone();
        new_dst_parent.child_count += 1;
        writes.push(WriteOp::Put {
            key: src_parent,
            value: new_src_parent,
        });
        writes.push(WriteOp::Put {
            key: dst_parent,
            value: new_dst_parent,
        });
    }
    Ok(with_writes(TxResult::Ok, writes))
}

fn read_key<'a>(reads: &'a BTreeMap<Key, ReadValue>, key: &Key) -> Result<Option<&'a Inode>> {
    match reads.get(key) {
        Some(ReadValue::Present(inode)) => Ok(Some(inode)),
        Some(ReadValue::Missing) => Ok(None),
        None => Err(Error::InvalidBatch(format!(
            "missing read value for key {}",
            key
        ))),
    }
}

fn no_writes(result: TxResult) -> TxExecutionOutput {
    TxExecutionOutput {
        result,
        writes: Vec::new(),
    }
}

fn with_writes(result: TxResult, writes: Vec<WriteOp>) -> TxExecutionOutput {
    TxExecutionOutput { result, writes }
}
