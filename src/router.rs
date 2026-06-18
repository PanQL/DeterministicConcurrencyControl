use crate::model::{Key, OrderedTx, ShardId};
use std::collections::BTreeSet;

const FNV_OFFSET_BASIS: u64 = 14_695_981_039_346_656_037;
const FNV_PRIME: u64 = 1_099_511_628_211;

#[derive(Clone, Debug)]
pub struct ShardLayout {
    pub shard_count: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Participants {
    pub all: BTreeSet<ShardId>,
    pub active: BTreeSet<ShardId>,
    pub passive: BTreeSet<ShardId>,
}

impl ShardLayout {
    pub fn new(shard_count: u64) -> Self {
        assert!(shard_count > 0);
        Self { shard_count }
    }

    pub fn shard_for_key(&self, key: &Key) -> ShardId {
        stable_hash(key.as_str().as_bytes()) % self.shard_count
    }

    pub fn participants_for_sets(
        &self,
        read_set: &BTreeSet<Key>,
        write_set: &BTreeSet<Key>,
    ) -> Participants {
        let mut all = BTreeSet::new();
        for key in read_set.iter().chain(write_set.iter()) {
            all.insert(self.shard_for_key(key));
        }
        let mut active = BTreeSet::new();
        if write_set.is_empty() {
            if let Some(shard) = self.read_only_coordinator(read_set) {
                active.insert(shard);
            }
        } else {
            for key in write_set {
                active.insert(self.shard_for_key(key));
            }
        }
        let passive = all.difference(&active).copied().collect();
        Participants {
            all,
            active,
            passive,
        }
    }

    pub fn participants(&self, tx: &OrderedTx) -> Participants {
        self.participants_for_sets(&tx.read_set, &tx.write_set)
    }

    pub fn result_shard_for_sets(
        &self,
        read_set: &BTreeSet<Key>,
        write_set: &BTreeSet<Key>,
    ) -> Option<ShardId> {
        self.participants_for_sets(read_set, write_set)
            .active
            .iter()
            .next()
            .copied()
    }

    pub fn result_shard(&self, tx: &OrderedTx) -> Option<ShardId> {
        self.result_shard_for_sets(&tx.read_set, &tx.write_set)
    }

    pub fn read_only_coordinator(&self, read_set: &BTreeSet<Key>) -> Option<ShardId> {
        read_set.iter().next().map(|key| self.shard_for_key(key))
    }

    pub fn local_read_keys(&self, tx: &OrderedTx, shard_id: ShardId) -> BTreeSet<Key> {
        tx.read_set
            .iter()
            .filter(|key| self.shard_for_key(key) == shard_id)
            .cloned()
            .collect()
    }

    pub fn local_write_keys(&self, tx: &OrderedTx, shard_id: ShardId) -> BTreeSet<Key> {
        tx.write_set
            .iter()
            .filter(|key| self.shard_for_key(key) == shard_id)
            .cloned()
            .collect()
    }

    pub fn local_lock_keys(&self, tx: &OrderedTx, shard_id: ShardId) -> BTreeSet<Key> {
        tx.read_set
            .iter()
            .chain(tx.write_set.iter())
            .filter(|key| self.shard_for_key(key) == shard_id)
            .cloned()
            .collect()
    }
}

pub fn stable_hash(bytes: &[u8]) -> u64 {
    let mut hash = FNV_OFFSET_BASIS;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}
