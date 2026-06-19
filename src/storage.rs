use crate::error::Result;
use crate::model::{Inode, Key, ReadValue, WriteOp};
use crate::scc::{DeltaOp, InodeIntegerField, TxDelta};
use redb::{backends::InMemoryBackend, Database, ReadableDatabase, ReadableTable, TableDefinition};
use std::collections::{BTreeMap, BTreeSet};

const INODE_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("inodes");

pub struct RedbInMemoryInodeStore {
    db: Database,
}

impl RedbInMemoryInodeStore {
    pub fn new() -> Result<Self> {
        let db = Database::builder().create_with_backend(InMemoryBackend::new())?;
        let write = db.begin_write()?;
        {
            let _table = write.open_table(INODE_TABLE)?;
        }
        write.commit()?;
        Ok(Self { db })
    }

    pub fn read_many(&self, keys: &BTreeSet<Key>) -> Result<BTreeMap<Key, ReadValue>> {
        let read = self.db.begin_read()?;
        let table = read.open_table(INODE_TABLE)?;
        let mut out = BTreeMap::new();
        for key in keys {
            match table.get(key.as_str())? {
                Some(value) => {
                    let inode: Inode = bincode::deserialize(value.value())?;
                    out.insert(key.clone(), ReadValue::Present(inode));
                }
                None => {
                    out.insert(key.clone(), ReadValue::Missing);
                }
            }
        }
        Ok(out)
    }

    pub fn apply_writes_atomically(&self, writes: &[WriteOp]) -> Result<()> {
        let write = self.db.begin_write()?;
        {
            let mut table = write.open_table(INODE_TABLE)?;
            for op in writes {
                match op {
                    WriteOp::Put { key, value } => {
                        let bytes = bincode::serialize(value)?;
                        table.insert(key.as_str(), bytes.as_slice())?;
                    }
                    WriteOp::Delete { key } => {
                        table.remove(key.as_str())?;
                    }
                }
            }
        }
        write.commit()?;
        Ok(())
    }

    pub fn apply_delta_atomically(&self, delta: &TxDelta) -> Result<()> {
        let write = self.db.begin_write()?;
        {
            let mut table = write.open_table(INODE_TABLE)?;
            for op in &delta.ops {
                match op {
                    DeltaOp::Put { key, value } => {
                        let bytes = bincode::serialize(value)?;
                        table.insert(key.as_str(), bytes.as_slice())?;
                    }
                    DeltaOp::Delete { key } => {
                        table.remove(key.as_str())?;
                    }
                    DeltaOp::AddIntegerField {
                        key,
                        field: InodeIntegerField::ChildCount,
                        delta,
                    } => {
                        let mut inode: Inode = {
                            let current = table.get(key.as_str())?.ok_or_else(|| {
                                crate::error::Error::InvalidBatch(format!(
                                    "cannot apply child_count delta to missing {}",
                                    key
                                ))
                            })?;
                            bincode::deserialize(current.value())?
                        };
                        if inode.kind != crate::model::NodeKind::Directory {
                            return Err(crate::error::Error::InvalidBatch(format!(
                                "cannot apply child_count delta to non-directory {}",
                                key
                            )));
                        }
                        inode.child_count = apply_signed_delta(inode.child_count, *delta)
                            .ok_or_else(|| {
                                crate::error::Error::InvalidBatch(format!(
                                    "child_count delta underflow for {}",
                                    key
                                ))
                            })?;
                        let bytes = bincode::serialize(&inode)?;
                        table.insert(key.as_str(), bytes.as_slice())?;
                    }
                }
            }
        }
        write.commit()?;
        Ok(())
    }

    pub fn dump(&self) -> Result<BTreeMap<Key, Inode>> {
        let read = self.db.begin_read()?;
        let table = read.open_table(INODE_TABLE)?;
        let mut out = BTreeMap::new();
        for item in table.iter()? {
            let (key, value) = item?;
            let key = Key::new(key.value().to_string())?;
            let inode: Inode = bincode::deserialize(value.value())?;
            out.insert(key, inode);
        }
        Ok(out)
    }
}

fn apply_signed_delta(value: u64, delta: i64) -> Option<u64> {
    if delta >= 0 {
        value.checked_add(delta as u64)
    } else {
        value.checked_sub(delta.unsigned_abs())
    }
}
