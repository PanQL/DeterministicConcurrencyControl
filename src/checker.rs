use crate::error::{Error, Result};
use crate::executor::execute_deterministic;
use crate::model::{
    Batch, BatchId, Inode, Key, ReadValue, SccReorderRecord, ShardId, TxId, TxResult,
    TxResultRecord, WriteOp,
};
use crate::router::ShardLayout;
use std::collections::{BTreeMap, BTreeSet};

pub fn reference_execute_batches(batches: &[Batch]) -> BTreeMap<Key, Inode> {
    let mut state = BTreeMap::new();
    for batch in batches {
        for tx in &batch.txs {
            apply_reference_tx(&mut state, tx);
        }
    }
    state
}

pub fn reference_execute_scc_reordered_batches(
    batches: &[Batch],
    reorders: &BTreeMap<BatchId, SccReorderRecord>,
) -> Result<BTreeMap<Key, Inode>> {
    let mut state = BTreeMap::new();
    for batch in batches {
        let reorder = reorders.get(&batch.batch_id).ok_or_else(|| {
            Error::Checker(format!(
                "missing SCC reorder record for batch {}",
                batch.batch_id
            ))
        })?;
        validate_reorder_record(batch, reorder)?;
        for index in reorder
            .speculative_success_indices
            .iter()
            .chain(reorder.fallback_indices.iter())
        {
            apply_reference_tx(&mut state, &batch.txs[*index]);
        }
    }
    Ok(state)
}

pub fn assert_scc_reorders_consistent(
    batches: &[Batch],
    shard_reorders: Vec<(ShardId, Vec<SccReorderRecord>)>,
) -> Result<BTreeMap<BatchId, SccReorderRecord>> {
    let mut batch_by_id = BTreeMap::new();
    for batch in batches {
        if batch_by_id.insert(batch.batch_id, batch).is_some() {
            return Err(Error::Checker(format!(
                "duplicate SCC batch id {} in reference batches",
                batch.batch_id
            )));
        }
    }
    let expected_batch_ids: BTreeSet<BatchId> = batch_by_id.keys().copied().collect();
    let mut fallback_by_batch: BTreeMap<BatchId, BTreeSet<usize>> = BTreeMap::new();

    for (shard_id, records) in shard_reorders {
        let mut by_batch = BTreeMap::new();
        for record in records {
            let batch = batch_by_id.get(&record.batch_id).ok_or_else(|| {
                Error::Checker(format!(
                    "shard {} reported SCC reorder for unknown batch {}",
                    shard_id, record.batch_id
                ))
            })?;
            validate_reorder_record(batch, &record)?;
            fallback_by_batch
                .entry(record.batch_id)
                .or_default()
                .extend(record.fallback_indices.iter().copied());
            if by_batch.insert(record.batch_id, record).is_some() {
                return Err(Error::Checker(format!(
                    "shard {} reported duplicate SCC reorder for a batch",
                    shard_id
                )));
            }
        }
        let actual_batch_ids: BTreeSet<BatchId> = by_batch.keys().copied().collect();
        if actual_batch_ids != expected_batch_ids {
            return Err(Error::Checker(format!(
                "shard {} SCC reorder batches mismatch: expected {:?}, got {:?}",
                shard_id, expected_batch_ids, actual_batch_ids
            )));
        }
    }

    let mut reference_reorders = BTreeMap::new();
    for batch in batches {
        let fallback_set = fallback_by_batch
            .remove(&batch.batch_id)
            .unwrap_or_default();
        let all_indices: BTreeSet<usize> = (0..batch.txs.len()).collect();
        if !fallback_set.is_subset(&all_indices) {
            return Err(Error::Checker(format!(
                "SCC fallback union for batch {} contains index outside batch length {}: {:?}",
                batch.batch_id,
                batch.txs.len(),
                fallback_set
            )));
        }
        reference_reorders.insert(
            batch.batch_id,
            SccReorderRecord {
                batch_id: batch.batch_id,
                speculative_success_indices: all_indices
                    .difference(&fallback_set)
                    .copied()
                    .collect(),
                fallback_indices: fallback_set.into_iter().collect(),
            },
        );
    }

    if !fallback_by_batch.is_empty() {
        return Err(Error::Checker(format!(
            "SCC fallback union contains unknown batches: {:?}",
            fallback_by_batch.keys().collect::<Vec<_>>()
        )));
    }

    Ok(reference_reorders)
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

fn validate_reorder_record(batch: &Batch, reorder: &SccReorderRecord) -> Result<()> {
    if reorder.batch_id != batch.batch_id {
        return Err(Error::Checker(format!(
            "SCC reorder batch id mismatch: record {}, batch {}",
            reorder.batch_id, batch.batch_id
        )));
    }
    let batch_len = batch.txs.len();
    let mut seen = BTreeSet::new();
    for index in reorder
        .speculative_success_indices
        .iter()
        .chain(reorder.fallback_indices.iter())
    {
        if *index >= batch_len {
            return Err(Error::Checker(format!(
                "SCC reorder for batch {} contains out-of-range index {} for len {}",
                batch.batch_id, index, batch_len
            )));
        }
        if !seen.insert(*index) {
            return Err(Error::Checker(format!(
                "SCC reorder for batch {} contains duplicate index {}",
                batch.batch_id, index
            )));
        }
    }
    let expected: BTreeSet<usize> = (0..batch_len).collect();
    if seen != expected {
        return Err(Error::Checker(format!(
            "SCC reorder for batch {} does not cover every tx index: expected {:?}, got {:?}",
            batch.batch_id, expected, seen
        )));
    }
    Ok(())
}

fn apply_reference_tx(state: &mut BTreeMap<Key, Inode>, tx: &crate::model::OrderedTx) {
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
    apply_writes(state, output.writes);
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
