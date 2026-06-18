use crate::error::Result;
use crate::model::{Inode, Key, ReadValue, WriteOp};
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
