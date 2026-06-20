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

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum ReadPhase {
    Calvin,
    SccEffect,
    SccCondition,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocalReadStatus {
    Ok,
    SpeculationFailed,
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SccReorderRecord {
    pub batch_id: BatchId,
    pub speculative_success_indices: Vec<usize>,
    pub fallback_indices: Vec<usize>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SchedulerProfileScheduler {
    CalvinLocking,
    SccOnline,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct WorkerStageStats {
    pub sum_ns: u64,
    pub max_ns: u64,
}

impl WorkerStageStats {
    pub fn add_sample(&mut self, ns: u64) {
        self.sum_ns = self.sum_ns.saturating_add(ns);
        self.max_ns = self.max_ns.max(ns);
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SchedulerProfileCounters {
    pub tx_count: u64,
    pub relevant_tx_count: u64,
    pub active_tx_count: u64,
    pub passive_tx_count: u64,
    pub non_participant_tx_count: u64,
    pub local_read_key_count: u64,
    pub local_write_key_count: u64,
    pub remote_read_messages_sent: u64,
    pub remote_read_messages_received: u64,
    pub result_records_produced: u64,
    pub lock_key_count: u64,
    pub plan_pair_count: u64,
    pub effect_edge_count: u64,
    pub condition_edge_count: u64,
    pub condition_skipped_count: u64,
    pub speculative_success_count: u64,
    pub local_failed_count: u64,
    pub global_failed_count: u64,
    pub fallback_tx_count: u64,
    pub delta_op_count: u64,
    pub completion_reports_sent: u64,
    pub completion_reports_received: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SchedulerProfileTimings {
    pub total_ns: u64,
    pub cleanup_ns: u64,
    pub validate_ns: u64,
    pub result_registry_ns: u64,
    pub lock_wait_sum_ns: u64,
    pub lock_wait_max_ns: u64,
    pub local_read_ns: u64,
    pub remote_read_send_ns: u64,
    pub remote_read_collect_ns: u64,
    pub execute_apply_ns: u64,
    pub result_mark_ns: u64,
    pub outcome_collect_release_ns: u64,
    pub plan_build_ns: u64,
    pub dag_setup_ns: u64,
    pub base_read_ns: u64,
    pub mailbox_spawn_ns: u64,
    pub completion_publish_ns: u64,
    pub completion_collect_ns: u64,
    pub record_reorder_ns: u64,
    pub install_successes_ns: u64,
    pub fallback_ns: u64,
    pub scc_effect_wait: WorkerStageStats,
    pub scc_effect_materialize: WorkerStageStats,
    pub scc_effect_send: WorkerStageStats,
    pub scc_effect_collect: WorkerStageStats,
    pub scc_execute: WorkerStageStats,
    pub scc_delta_build: WorkerStageStats,
    pub scc_condition_wait: WorkerStageStats,
    pub scc_condition_materialize: WorkerStageStats,
    pub scc_condition_send: WorkerStageStats,
    pub scc_condition_collect: WorkerStageStats,
    pub scc_condition_check: WorkerStageStats,
    pub scc_commit: WorkerStageStats,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchedulerProfileRecord {
    pub scheduler: SchedulerProfileScheduler,
    pub batch_id: BatchId,
    pub shard_id: ShardId,
    pub counters: SchedulerProfileCounters,
    pub timings: SchedulerProfileTimings,
}

pub fn parent_key(path: &Key) -> Result<Key> {
    path.parent()
}
