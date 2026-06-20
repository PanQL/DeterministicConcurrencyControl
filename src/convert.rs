use crate::error::{Error, Result};
use crate::model::{
    Batch, FsOp, Inode, Key, LocalReadStatus, NodeKind, OrderedTx, ReadPhase, ReadValue,
    SccReorderRecord, SchedulerProfileCounters, SchedulerProfileRecord, SchedulerProfileScheduler,
    SchedulerProfileTimings, TxResult, TxResultRecord, WorkerStageStats, WriteOp,
};
use crate::proto::pb;
use std::collections::{BTreeMap, BTreeSet};

pub fn batch_from_proto(value: pb::Batch) -> Result<Batch> {
    let mut txs = Vec::with_capacity(value.txs.len());
    for tx in value.txs {
        txs.push(ordered_tx_from_proto(tx)?);
    }
    Ok(Batch {
        batch_id: value.batch_id,
        txs,
    })
}

pub fn batch_to_proto(value: &Batch) -> pb::Batch {
    pb::Batch {
        batch_id: value.batch_id,
        txs: value.txs.iter().map(ordered_tx_to_proto).collect(),
    }
}

pub fn ordered_tx_from_proto(value: pb::OrderedTx) -> Result<OrderedTx> {
    let read_set = keys_from_proto(value.read_set, "read_set")?;
    let write_set = keys_from_proto(value.write_set, "write_set")?;
    Ok(OrderedTx {
        tx_id: value.tx_id,
        batch_index: value.batch_index,
        op: fs_op_from_proto(value.op)?,
        read_set,
        write_set,
    })
}

pub fn ordered_tx_to_proto(value: &OrderedTx) -> pb::OrderedTx {
    pb::OrderedTx {
        tx_id: value.tx_id,
        batch_index: value.batch_index,
        op: Some(fs_op_to_proto(&value.op)),
        read_set: value.read_set.iter().map(String::from).collect(),
        write_set: value.write_set.iter().map(String::from).collect(),
    }
}

pub fn fs_op_from_proto(value: Option<pb::FsOp>) -> Result<FsOp> {
    let value = value.ok_or_else(|| Error::InvalidProto("missing FsOp".to_string()))?;
    let op = value
        .op
        .ok_or_else(|| Error::InvalidProto("missing FsOp oneof".to_string()))?;
    Ok(match op {
        pb::fs_op::Op::Create(create) => FsOp::Create {
            path: Key::new(create.path)?,
        },
        pb::fs_op::Op::Mkdir(mkdir) => FsOp::Mkdir {
            path: Key::new(mkdir.path)?,
        },
        pb::fs_op::Op::Unlink(unlink) => FsOp::Unlink {
            path: Key::new(unlink.path)?,
        },
        pb::fs_op::Op::Rmdir(rmdir) => FsOp::Rmdir {
            path: Key::new(rmdir.path)?,
        },
        pb::fs_op::Op::Rename(rename) => FsOp::Rename {
            src: Key::new(rename.src)?,
            dst: Key::new(rename.dst)?,
        },
        pb::fs_op::Op::Stat(stat) => FsOp::Stat {
            path: Key::new(stat.path)?,
        },
    })
}

pub fn fs_op_to_proto(value: &FsOp) -> pb::FsOp {
    let op = match value {
        FsOp::Create { path } => pb::fs_op::Op::Create(pb::Create {
            path: String::from(path),
        }),
        FsOp::Mkdir { path } => pb::fs_op::Op::Mkdir(pb::Mkdir {
            path: String::from(path),
        }),
        FsOp::Unlink { path } => pb::fs_op::Op::Unlink(pb::Unlink {
            path: String::from(path),
        }),
        FsOp::Rmdir { path } => pb::fs_op::Op::Rmdir(pb::Rmdir {
            path: String::from(path),
        }),
        FsOp::Rename { src, dst } => pb::fs_op::Op::Rename(pb::Rename {
            src: String::from(src),
            dst: String::from(dst),
        }),
        FsOp::Stat { path } => pb::fs_op::Op::Stat(pb::Stat {
            path: String::from(path),
        }),
    };
    pb::FsOp { op: Some(op) }
}

pub fn inode_from_proto(value: Option<pb::Inode>) -> Result<Inode> {
    let value = value.ok_or_else(|| Error::InvalidProto("missing inode".to_string()))?;
    Ok(Inode {
        kind: node_kind_from_i32(value.kind)?,
        child_count: value.child_count,
    })
}

pub fn inode_to_proto(value: &Inode) -> pb::Inode {
    pb::Inode {
        kind: node_kind_to_i32(value.kind),
        child_count: value.child_count,
    }
}

pub fn read_entries_from_proto(entries: Vec<pb::ReadEntry>) -> Result<BTreeMap<Key, ReadValue>> {
    let mut reads = BTreeMap::new();
    for entry in entries {
        let key = Key::new(entry.key)?;
        let value = match entry
            .value
            .ok_or_else(|| Error::InvalidProto("missing read value".to_string()))?
        {
            pb::read_entry::Value::Inode(inode) => {
                ReadValue::Present(inode_from_proto(Some(inode))?)
            }
            pb::read_entry::Value::Missing(_) => ReadValue::Missing,
        };
        if reads.insert(key.clone(), value).is_some() {
            return Err(Error::InvalidProto(format!("duplicate read key {}", key)));
        }
    }
    Ok(reads)
}

pub fn read_entries_to_proto(reads: &BTreeMap<Key, ReadValue>) -> Vec<pb::ReadEntry> {
    reads
        .iter()
        .map(|(key, value)| {
            let value = match value {
                ReadValue::Present(inode) => pb::read_entry::Value::Inode(inode_to_proto(inode)),
                ReadValue::Missing => pb::read_entry::Value::Missing(pb::Missing {}),
            };
            pb::ReadEntry {
                key: String::from(key),
                value: Some(value),
            }
        })
        .collect()
}

pub fn read_entry_from_proto(entry: Option<pb::ReadEntry>) -> Result<(Key, ReadValue)> {
    let entry = entry.ok_or_else(|| Error::InvalidProto("missing read entry".to_string()))?;
    let key = Key::new(entry.key)?;
    let value = match entry
        .value
        .ok_or_else(|| Error::InvalidProto("missing read value".to_string()))?
    {
        pb::read_entry::Value::Inode(inode) => ReadValue::Present(inode_from_proto(Some(inode))?),
        pb::read_entry::Value::Missing(_) => ReadValue::Missing,
    };
    Ok((key, value))
}

pub fn read_entry_to_proto(key: &Key, value: &ReadValue) -> pb::ReadEntry {
    let value = match value {
        ReadValue::Present(inode) => pb::read_entry::Value::Inode(inode_to_proto(inode)),
        ReadValue::Missing => pb::read_entry::Value::Missing(pb::Missing {}),
    };
    pb::ReadEntry {
        key: String::from(key),
        value: Some(value),
    }
}

pub fn write_entries_from_proto(entries: Vec<pb::WriteEntry>) -> Result<Vec<WriteOp>> {
    entries.into_iter().map(write_entry_from_proto).collect()
}

pub fn write_entries_to_proto(writes: &[WriteOp]) -> Vec<pb::WriteEntry> {
    writes.iter().map(write_entry_to_proto).collect()
}

fn write_entry_from_proto(entry: pb::WriteEntry) -> Result<WriteOp> {
    let op = entry
        .op
        .ok_or_else(|| Error::InvalidProto("missing write op".to_string()))?;
    Ok(match op {
        pb::write_entry::Op::Put(put) => WriteOp::Put {
            key: Key::new(put.key)?,
            value: inode_from_proto(put.inode)?,
        },
        pb::write_entry::Op::Delete(delete) => WriteOp::Delete {
            key: Key::new(delete.key)?,
        },
    })
}

fn write_entry_to_proto(write: &WriteOp) -> pb::WriteEntry {
    let op = match write {
        WriteOp::Put { key, value } => pb::write_entry::Op::Put(pb::PutEntry {
            key: String::from(key),
            inode: Some(inode_to_proto(value)),
        }),
        WriteOp::Delete { key } => pb::write_entry::Op::Delete(pb::DeleteEntry {
            key: String::from(key),
        }),
    };
    pb::WriteEntry { op: Some(op) }
}

pub fn inode_entries_to_proto(entries: &BTreeMap<Key, Inode>) -> Vec<pb::InodeEntry> {
    entries
        .iter()
        .map(|(key, inode)| pb::InodeEntry {
            key: String::from(key),
            inode: Some(inode_to_proto(inode)),
        })
        .collect()
}

pub fn inode_entries_from_proto(entries: Vec<pb::InodeEntry>) -> Result<BTreeMap<Key, Inode>> {
    let mut out = BTreeMap::new();
    for entry in entries {
        let key = Key::new(entry.key)?;
        let inode = inode_from_proto(entry.inode)?;
        if out.insert(key.clone(), inode).is_some() {
            return Err(Error::InvalidProto(format!("duplicate inode key {}", key)));
        }
    }
    Ok(out)
}

pub fn tx_result_record_to_proto(value: &TxResultRecord) -> pb::TxResultRecord {
    pb::TxResultRecord {
        tx_id: value.tx_id,
        shard_id: value.shard_id,
        result: tx_result_to_i32(value.result),
    }
}

pub fn tx_result_record_from_proto(value: pb::TxResultRecord) -> Result<TxResultRecord> {
    Ok(TxResultRecord {
        tx_id: value.tx_id,
        shard_id: value.shard_id,
        result: tx_result_from_i32(value.result)?,
    })
}

pub fn tx_result_records_to_proto(values: &[TxResultRecord]) -> Vec<pb::TxResultRecord> {
    values.iter().map(tx_result_record_to_proto).collect()
}

pub fn tx_result_records_from_proto(
    values: Vec<pb::TxResultRecord>,
) -> Result<Vec<TxResultRecord>> {
    values
        .into_iter()
        .map(tx_result_record_from_proto)
        .collect()
}

pub fn scc_reorder_record_to_proto(value: &SccReorderRecord) -> pb::SccReorderRecord {
    pb::SccReorderRecord {
        batch_id: value.batch_id,
        speculative_success_indices: value
            .speculative_success_indices
            .iter()
            .map(|index| *index as u32)
            .collect(),
        fallback_indices: value
            .fallback_indices
            .iter()
            .map(|index| *index as u32)
            .collect(),
    }
}

pub fn scc_reorder_record_from_proto(value: pb::SccReorderRecord) -> SccReorderRecord {
    SccReorderRecord {
        batch_id: value.batch_id,
        speculative_success_indices: value
            .speculative_success_indices
            .into_iter()
            .map(|index| index as usize)
            .collect(),
        fallback_indices: value
            .fallback_indices
            .into_iter()
            .map(|index| index as usize)
            .collect(),
    }
}

pub fn scc_reorder_records_to_proto(values: &[SccReorderRecord]) -> Vec<pb::SccReorderRecord> {
    values.iter().map(scc_reorder_record_to_proto).collect()
}

pub fn scc_reorder_records_from_proto(values: Vec<pb::SccReorderRecord>) -> Vec<SccReorderRecord> {
    values
        .into_iter()
        .map(scc_reorder_record_from_proto)
        .collect()
}

pub fn scheduler_profile_record_to_proto(
    value: &SchedulerProfileRecord,
) -> pb::SchedulerProfileRecord {
    pb::SchedulerProfileRecord {
        scheduler: scheduler_profile_scheduler_to_i32(value.scheduler),
        batch_id: value.batch_id,
        shard_id: value.shard_id,
        counters: Some(scheduler_profile_counters_to_proto(&value.counters)),
        timings: Some(scheduler_profile_timings_to_proto(&value.timings)),
    }
}

pub fn scheduler_profile_record_from_proto(
    value: pb::SchedulerProfileRecord,
) -> Result<SchedulerProfileRecord> {
    Ok(SchedulerProfileRecord {
        scheduler: scheduler_profile_scheduler_from_i32(value.scheduler)?,
        batch_id: value.batch_id,
        shard_id: value.shard_id,
        counters: scheduler_profile_counters_from_proto(value.counters.unwrap_or_default()),
        timings: scheduler_profile_timings_from_proto(value.timings.unwrap_or_default()),
    })
}

pub fn scheduler_profile_records_to_proto(
    values: &[SchedulerProfileRecord],
) -> Vec<pb::SchedulerProfileRecord> {
    values
        .iter()
        .map(scheduler_profile_record_to_proto)
        .collect()
}

pub fn scheduler_profile_records_from_proto(
    values: Vec<pb::SchedulerProfileRecord>,
) -> Result<Vec<SchedulerProfileRecord>> {
    values
        .into_iter()
        .map(scheduler_profile_record_from_proto)
        .collect()
}

fn scheduler_profile_counters_to_proto(
    value: &SchedulerProfileCounters,
) -> pb::SchedulerProfileCounters {
    pb::SchedulerProfileCounters {
        tx_count: value.tx_count,
        relevant_tx_count: value.relevant_tx_count,
        active_tx_count: value.active_tx_count,
        passive_tx_count: value.passive_tx_count,
        non_participant_tx_count: value.non_participant_tx_count,
        local_read_key_count: value.local_read_key_count,
        local_write_key_count: value.local_write_key_count,
        remote_read_messages_sent: value.remote_read_messages_sent,
        remote_read_messages_received: value.remote_read_messages_received,
        result_records_produced: value.result_records_produced,
        lock_key_count: value.lock_key_count,
        plan_pair_count: value.plan_pair_count,
        effect_edge_count: value.effect_edge_count,
        condition_edge_count: value.condition_edge_count,
        condition_skipped_count: value.condition_skipped_count,
        speculative_success_count: value.speculative_success_count,
        local_failed_count: value.local_failed_count,
        global_failed_count: value.global_failed_count,
        fallback_tx_count: value.fallback_tx_count,
        delta_op_count: value.delta_op_count,
        completion_reports_sent: value.completion_reports_sent,
        completion_reports_received: value.completion_reports_received,
    }
}

fn scheduler_profile_counters_from_proto(
    value: pb::SchedulerProfileCounters,
) -> SchedulerProfileCounters {
    SchedulerProfileCounters {
        tx_count: value.tx_count,
        relevant_tx_count: value.relevant_tx_count,
        active_tx_count: value.active_tx_count,
        passive_tx_count: value.passive_tx_count,
        non_participant_tx_count: value.non_participant_tx_count,
        local_read_key_count: value.local_read_key_count,
        local_write_key_count: value.local_write_key_count,
        remote_read_messages_sent: value.remote_read_messages_sent,
        remote_read_messages_received: value.remote_read_messages_received,
        result_records_produced: value.result_records_produced,
        lock_key_count: value.lock_key_count,
        plan_pair_count: value.plan_pair_count,
        effect_edge_count: value.effect_edge_count,
        condition_edge_count: value.condition_edge_count,
        condition_skipped_count: value.condition_skipped_count,
        speculative_success_count: value.speculative_success_count,
        local_failed_count: value.local_failed_count,
        global_failed_count: value.global_failed_count,
        fallback_tx_count: value.fallback_tx_count,
        delta_op_count: value.delta_op_count,
        completion_reports_sent: value.completion_reports_sent,
        completion_reports_received: value.completion_reports_received,
    }
}

fn scheduler_profile_timings_to_proto(
    value: &SchedulerProfileTimings,
) -> pb::SchedulerProfileTimings {
    pb::SchedulerProfileTimings {
        total_ns: value.total_ns,
        cleanup_ns: value.cleanup_ns,
        validate_ns: value.validate_ns,
        result_registry_ns: value.result_registry_ns,
        lock_wait_sum_ns: value.lock_wait_sum_ns,
        lock_wait_max_ns: value.lock_wait_max_ns,
        local_read_ns: value.local_read_ns,
        remote_read_send_ns: value.remote_read_send_ns,
        remote_read_collect_ns: value.remote_read_collect_ns,
        execute_apply_ns: value.execute_apply_ns,
        result_mark_ns: value.result_mark_ns,
        outcome_collect_release_ns: value.outcome_collect_release_ns,
        plan_build_ns: value.plan_build_ns,
        dag_setup_ns: value.dag_setup_ns,
        base_read_ns: value.base_read_ns,
        mailbox_spawn_ns: value.mailbox_spawn_ns,
        completion_publish_ns: value.completion_publish_ns,
        completion_collect_ns: value.completion_collect_ns,
        record_reorder_ns: value.record_reorder_ns,
        install_successes_ns: value.install_successes_ns,
        fallback_ns: value.fallback_ns,
        scc_effect_wait: Some(worker_stage_stats_to_proto(&value.scc_effect_wait)),
        scc_effect_materialize: Some(worker_stage_stats_to_proto(&value.scc_effect_materialize)),
        scc_effect_send: Some(worker_stage_stats_to_proto(&value.scc_effect_send)),
        scc_effect_collect: Some(worker_stage_stats_to_proto(&value.scc_effect_collect)),
        scc_execute: Some(worker_stage_stats_to_proto(&value.scc_execute)),
        scc_delta_build: Some(worker_stage_stats_to_proto(&value.scc_delta_build)),
        scc_condition_wait: Some(worker_stage_stats_to_proto(&value.scc_condition_wait)),
        scc_condition_materialize: Some(worker_stage_stats_to_proto(
            &value.scc_condition_materialize,
        )),
        scc_condition_send: Some(worker_stage_stats_to_proto(&value.scc_condition_send)),
        scc_condition_collect: Some(worker_stage_stats_to_proto(&value.scc_condition_collect)),
        scc_condition_check: Some(worker_stage_stats_to_proto(&value.scc_condition_check)),
        scc_commit: Some(worker_stage_stats_to_proto(&value.scc_commit)),
    }
}

fn scheduler_profile_timings_from_proto(
    value: pb::SchedulerProfileTimings,
) -> SchedulerProfileTimings {
    SchedulerProfileTimings {
        total_ns: value.total_ns,
        cleanup_ns: value.cleanup_ns,
        validate_ns: value.validate_ns,
        result_registry_ns: value.result_registry_ns,
        lock_wait_sum_ns: value.lock_wait_sum_ns,
        lock_wait_max_ns: value.lock_wait_max_ns,
        local_read_ns: value.local_read_ns,
        remote_read_send_ns: value.remote_read_send_ns,
        remote_read_collect_ns: value.remote_read_collect_ns,
        execute_apply_ns: value.execute_apply_ns,
        result_mark_ns: value.result_mark_ns,
        outcome_collect_release_ns: value.outcome_collect_release_ns,
        plan_build_ns: value.plan_build_ns,
        dag_setup_ns: value.dag_setup_ns,
        base_read_ns: value.base_read_ns,
        mailbox_spawn_ns: value.mailbox_spawn_ns,
        completion_publish_ns: value.completion_publish_ns,
        completion_collect_ns: value.completion_collect_ns,
        record_reorder_ns: value.record_reorder_ns,
        install_successes_ns: value.install_successes_ns,
        fallback_ns: value.fallback_ns,
        scc_effect_wait: worker_stage_stats_from_proto(value.scc_effect_wait),
        scc_effect_materialize: worker_stage_stats_from_proto(value.scc_effect_materialize),
        scc_effect_send: worker_stage_stats_from_proto(value.scc_effect_send),
        scc_effect_collect: worker_stage_stats_from_proto(value.scc_effect_collect),
        scc_execute: worker_stage_stats_from_proto(value.scc_execute),
        scc_delta_build: worker_stage_stats_from_proto(value.scc_delta_build),
        scc_condition_wait: worker_stage_stats_from_proto(value.scc_condition_wait),
        scc_condition_materialize: worker_stage_stats_from_proto(value.scc_condition_materialize),
        scc_condition_send: worker_stage_stats_from_proto(value.scc_condition_send),
        scc_condition_collect: worker_stage_stats_from_proto(value.scc_condition_collect),
        scc_condition_check: worker_stage_stats_from_proto(value.scc_condition_check),
        scc_commit: worker_stage_stats_from_proto(value.scc_commit),
    }
}

fn worker_stage_stats_to_proto(value: &WorkerStageStats) -> pb::WorkerStageStats {
    pb::WorkerStageStats {
        sum_ns: value.sum_ns,
        max_ns: value.max_ns,
    }
}

fn worker_stage_stats_from_proto(value: Option<pb::WorkerStageStats>) -> WorkerStageStats {
    let value = value.unwrap_or_default();
    WorkerStageStats {
        sum_ns: value.sum_ns,
        max_ns: value.max_ns,
    }
}

pub fn scheduler_profile_scheduler_to_i32(value: SchedulerProfileScheduler) -> i32 {
    match value {
        SchedulerProfileScheduler::CalvinLocking => 1,
        SchedulerProfileScheduler::SccOnline => 2,
        SchedulerProfileScheduler::Aria => 3,
    }
}

pub fn scheduler_profile_scheduler_from_i32(value: i32) -> Result<SchedulerProfileScheduler> {
    Ok(match value {
        1 => SchedulerProfileScheduler::CalvinLocking,
        2 => SchedulerProfileScheduler::SccOnline,
        3 => SchedulerProfileScheduler::Aria,
        _ => {
            return Err(Error::InvalidProto(format!(
                "invalid SchedulerProfileScheduler {}",
                value
            )))
        }
    })
}

pub fn tx_result_to_i32(value: TxResult) -> i32 {
    match value {
        TxResult::Ok => 1,
        TxResult::NotFound => 2,
        TxResult::AlreadyExists => 3,
        TxResult::NotDirectory => 4,
        TxResult::DirectoryNotEmpty => 5,
        TxResult::Invalid => 6,
    }
}

pub fn tx_result_from_i32(value: i32) -> Result<TxResult> {
    Ok(match value {
        1 => TxResult::Ok,
        2 => TxResult::NotFound,
        3 => TxResult::AlreadyExists,
        4 => TxResult::NotDirectory,
        5 => TxResult::DirectoryNotEmpty,
        6 => TxResult::Invalid,
        _ => return Err(Error::InvalidProto(format!("invalid TxResult {}", value))),
    })
}

pub fn read_phase_to_i32(value: ReadPhase) -> i32 {
    match value {
        ReadPhase::Calvin => 1,
        ReadPhase::SccEffect => 2,
        ReadPhase::SccCondition => 3,
        ReadPhase::AriaFallback => 4,
    }
}

pub fn read_phase_from_i32(value: i32) -> Result<ReadPhase> {
    Ok(match value {
        1 => ReadPhase::Calvin,
        2 => ReadPhase::SccEffect,
        3 => ReadPhase::SccCondition,
        4 => ReadPhase::AriaFallback,
        _ => return Err(Error::InvalidProto(format!("invalid ReadPhase {}", value))),
    })
}

pub fn local_read_status_to_i32(value: LocalReadStatus) -> i32 {
    match value {
        LocalReadStatus::Ok => 1,
        LocalReadStatus::SpeculationFailed => 2,
    }
}

pub fn local_read_status_from_i32(value: i32) -> Result<LocalReadStatus> {
    Ok(match value {
        1 => LocalReadStatus::Ok,
        2 => LocalReadStatus::SpeculationFailed,
        _ => {
            return Err(Error::InvalidProto(format!(
                "invalid LocalReadStatus {}",
                value
            )))
        }
    })
}

fn node_kind_to_i32(value: NodeKind) -> i32 {
    match value {
        NodeKind::File => 1,
        NodeKind::Directory => 2,
    }
}

fn node_kind_from_i32(value: i32) -> Result<NodeKind> {
    Ok(match value {
        1 => NodeKind::File,
        2 => NodeKind::Directory,
        _ => return Err(Error::InvalidProto(format!("invalid NodeKind {}", value))),
    })
}

fn keys_from_proto(values: Vec<String>, field: &str) -> Result<BTreeSet<Key>> {
    let mut out = BTreeSet::new();
    for value in values {
        let key = Key::new(value)?;
        if !out.insert(key.clone()) {
            return Err(Error::InvalidProto(format!(
                "duplicate key {} in {}",
                key, field
            )));
        }
    }
    Ok(out)
}
