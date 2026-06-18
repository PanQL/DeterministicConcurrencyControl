use crate::error::{Error, Result};
use crate::executor::execute_deterministic;
use crate::model::{
    Batch, Inode, Key, ReadValue, ShardId, TxId, TxResult, TxResultRecord, WriteOp,
};
use crate::router::ShardLayout;
use std::collections::{BTreeMap, BTreeSet};

pub fn reference_execute_batches(batches: &[Batch]) -> BTreeMap<Key, Inode> {
    let mut state = BTreeMap::new();
    for batch in batches {
        for tx in &batch.txs {
            let reads = tx
                .read_set
                .iter()
                .map(|key| {
                    let value = state
                        .get(key)
                        .cloned()
                        .map(ReadValue::Present)
                        .unwrap_or(ReadValue::Missing);
                    (key.clone(), value)
                })
                .collect();
            let output = execute_deterministic(tx, &reads);
            apply_writes(&mut state, output.writes);
        }
    }
    state
}

pub fn merge_shard_states(
    layout: &ShardLayout,
    shard_states: Vec<(ShardId, BTreeMap<Key, Inode>)>,
) -> Result<BTreeMap<Key, Inode>> {
    let mut merged = BTreeMap::new();
    for (shard_id, state) in shard_states {
        for (key, inode) in state {
            let owner = layout.shard_for_key(&key);
            if owner != shard_id {
                return Err(Error::Checker(format!(
                    "key {} found on shard {}, but owner is shard {}",
                    key, shard_id, owner
                )));
            }
            if merged.insert(key.clone(), inode).is_some() {
                return Err(Error::Checker(format!(
                    "key {} appears in more than one shard dump",
                    key
                )));
            }
        }
    }
    Ok(merged)
}

pub fn assert_state_matches(
    expected: &BTreeMap<Key, Inode>,
    actual: &BTreeMap<Key, Inode>,
) -> Result<()> {
    if expected != actual {
        return Err(Error::Checker(format!(
            "final state mismatch: expected {:?}, got {:?}",
            expected, actual
        )));
    }
    Ok(())
}

pub fn assert_tx_results_consistent(
    layout: &ShardLayout,
    batches: &[Batch],
    records: &[TxResultRecord],
) -> Result<()> {
    let mut by_tx: BTreeMap<TxId, BTreeMap<ShardId, TxResult>> = BTreeMap::new();
    for record in records {
        if by_tx
            .entry(record.tx_id)
            .or_default()
            .insert(record.shard_id, record.result)
            .is_some()
        {
            return Err(Error::Checker(format!(
                "duplicate result for tx {} on shard {}",
                record.tx_id, record.shard_id
            )));
        }
    }

    for batch in batches {
        for tx in &batch.txs {
            let participants = layout.participants(tx);
            let actual = by_tx.get(&tx.tx_id).ok_or_else(|| {
                Error::Checker(format!("missing all active results for tx {}", tx.tx_id))
            })?;
            let actual_shards: BTreeSet<ShardId> = actual.keys().copied().collect();
            if actual_shards != participants.active {
                return Err(Error::Checker(format!(
                    "tx {} active result shards mismatch: expected {:?}, got {:?}",
                    tx.tx_id, participants.active, actual_shards
                )));
            }
            let mut values = actual.values();
            let first = values
                .next()
                .ok_or_else(|| Error::Checker(format!("empty result set for tx {}", tx.tx_id)))?;
            if values.any(|value| value != first) {
                return Err(Error::Checker(format!(
                    "active result disagreement for tx {}: {:?}",
                    tx.tx_id, actual
                )));
            }
        }
    }
    Ok(())
}

fn apply_writes(state: &mut BTreeMap<Key, Inode>, writes: Vec<WriteOp>) {
    for write in writes {
        match write {
            WriteOp::Put { key, value } => {
                state.insert(key, value);
            }
            WriteOp::Delete { key } => {
                state.remove(&key);
            }
        }
    }
}
