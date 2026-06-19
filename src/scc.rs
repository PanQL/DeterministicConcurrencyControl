use crate::error::{Error, Result};
use crate::model::{
    Batch, FsOp, Inode, Key, NodeKind, OrderedTx, ReadValue, TxExecutionOutput, TxResult, WriteOp,
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use tokio::sync::{oneshot, watch};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum PathId {
    CreateSuccess,
    MkdirSuccess,
    UnlinkSuccess,
    RmdirSuccess,
    RenameSuccess,
    StatSuccess,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct ConflictMask(u64);

impl ConflictMask {
    pub const SAME_TARGET: Self = Self(1 << 0);
    pub const SAME_PARENT_DIFFERENT_TARGET: Self = Self((1 << 1) | (1 << 2));
    pub const LHS_TARGET_IS_RHS_PARENT: Self = Self(1 << 3);
    pub const ANCESTOR_DESCENDANT: Self = Self(1 << 4);
    pub const ROOT_INVOLVED: Self = Self(1 << 5);
    pub const INDEPENDENT_SUBTREE: Self = Self(1 << 6);
    pub const RENAME_INVOLVED: Self = Self(1 << 7);

    pub fn bits(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct SemanticKey {
    pub path1: PathId,
    pub path2: PathId,
    pub conflict_mask: ConflictMask,
}

#[derive(Clone, Debug)]
pub struct SemanticTable {
    entries: BTreeMap<SemanticKey, bool>,
    default_conflict: bool,
}

impl SemanticTable {
    pub fn has_conflict(&self, key: &SemanticKey) -> bool {
        self.entries
            .get(key)
            .copied()
            .unwrap_or(self.default_conflict)
    }
}

#[derive(Clone, Debug)]
pub struct SemanticTables {
    pub effect: SemanticTable,
    pub condition: SemanticTable,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeltaOp {
    Put {
        key: Key,
        value: Inode,
    },
    Delete {
        key: Key,
    },
    AddIntegerField {
        key: Key,
        field: InodeIntegerField,
        delta: i64,
    },
}

impl DeltaOp {
    pub fn key(&self) -> &Key {
        match self {
            DeltaOp::Put { key, .. }
            | DeltaOp::Delete { key }
            | DeltaOp::AddIntegerField { key, .. } => key,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InodeIntegerField {
    ChildCount,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TxDelta {
    pub ops: Vec<DeltaOp>,
}

#[derive(Clone, Debug)]
pub struct SccBatchPlan {
    pub effect: SemanticDag,
    pub condition: SemanticDag,
    pub tx_plans: Vec<SccTxPlan>,
}

#[derive(Clone, Debug)]
pub struct SemanticDag {
    pub nodes: Vec<DagNode>,
}

#[derive(Clone, Debug)]
pub struct DagNode {
    pub successors: BTreeSet<usize>,
    #[cfg(debug_assertions)]
    pub predecessors: BTreeSet<usize>,
}

pub struct SccDagRuntime {
    effect: DagRuntime,
    condition: DagRuntime,
}

pub struct DagRuntime {
    nodes: Vec<DagRuntimeNode>,
}

struct DagRuntimeNode {
    indegree: usize,
    successors: BTreeSet<usize>,
    ready_tx: Option<oneshot::Sender<()>>,
    finished: bool,
}

pub struct TxDagWaiters {
    pub effect_ready: oneshot::Receiver<()>,
    pub condition_ready: oneshot::Receiver<()>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CommitSlotState {
    Pending,
    NoOp,
    Delta(Arc<TxDelta>),
    Failed,
}

pub struct CommitSequence {
    slots: Vec<CommitSlotCell>,
}

struct CommitSlotCell {
    state_tx: watch::Sender<CommitSlotState>,
}

impl CommitSequence {
    pub fn new(batch_len: usize) -> Self {
        let slots = (0..batch_len)
            .map(|_| {
                let (state_tx, _) = watch::channel(CommitSlotState::Pending);
                CommitSlotCell { state_tx }
            })
            .collect();
        Self { slots }
    }

    pub async fn wait_terminal(&self, index: usize) -> Result<CommitSlotState> {
        let slot = self.slot(index)?;
        let mut rx = slot.state_tx.subscribe();
        loop {
            let state = rx.borrow().clone();
            if !matches!(state, CommitSlotState::Pending) {
                return Ok(state);
            }
            rx.changed().await.map_err(|_| {
                Error::ChannelClosed(format!("commit slot {index} watch channel closed"))
            })?;
        }
    }

    pub fn set_terminal_once(&self, index: usize, state: CommitSlotState) -> Result<()> {
        if matches!(state, CommitSlotState::Pending) {
            return Err(Error::InvalidBatch(format!(
                "commit slot {index} cannot be set back to Pending"
            )));
        }
        let slot = self.slot(index)?;
        if !matches!(*slot.state_tx.borrow(), CommitSlotState::Pending) {
            return Err(Error::InvalidBatch(format!(
                "commit slot {index} terminal state was already set"
            )));
        }
        slot.state_tx.send_replace(state);
        Ok(())
    }

    pub fn terminal_snapshot(&self) -> Vec<CommitSlotState> {
        self.slots
            .iter()
            .map(|slot| slot.state_tx.borrow().clone())
            .collect()
    }

    fn slot(&self, index: usize) -> Result<&CommitSlotCell> {
        self.slots.get(index).ok_or_else(|| {
            Error::InvalidBatch(format!(
                "commit slot index {index} out of range {}",
                self.slots.len()
            ))
        })
    }
}

#[derive(Clone, Debug)]
pub struct SccTxPlan {
    pub predicted_path: PathId,
    pub effect_max_pred_index: Option<usize>,
    pub condition_max_pred_index: Option<usize>,
}

impl SccTxPlan {
    pub fn effect_prefix_covers_condition(&self) -> bool {
        match (self.effect_max_pred_index, self.condition_max_pred_index) {
            (_, None) => true,
            (Some(effect), Some(condition)) => effect >= condition,
            (None, Some(_)) => false,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SccPhase {
    Effect,
    Condition,
}

pub fn predicted_path(op: &FsOp) -> PathId {
    match op {
        FsOp::Create { .. } => PathId::CreateSuccess,
        FsOp::Mkdir { .. } => PathId::MkdirSuccess,
        FsOp::Unlink { .. } => PathId::UnlinkSuccess,
        FsOp::Rmdir { .. } => PathId::RmdirSuccess,
        FsOp::Rename { .. } => PathId::RenameSuccess,
        FsOp::Stat { .. } => PathId::StatSuccess,
    }
}

pub fn classify_actual_path(tx: &OrderedTx, output: &TxExecutionOutput) -> Option<PathId> {
    if output.result != TxResult::Ok {
        return None;
    }
    Some(predicted_path(&tx.op))
}

pub fn semantic_tables() -> SemanticTables {
    let mut effect = SemanticTable {
        entries: BTreeMap::new(),
        default_conflict: true,
    };
    let mut condition = SemanticTable {
        entries: BTreeMap::new(),
        default_conflict: true,
    };

    let namespace_mutations = [
        PathId::CreateSuccess,
        PathId::MkdirSuccess,
        PathId::UnlinkSuccess,
        PathId::RmdirSuccess,
    ];
    for lhs in namespace_mutations {
        for rhs in namespace_mutations {
            insert_no_conflict(
                &mut effect,
                lhs,
                rhs,
                ConflictMask::SAME_PARENT_DIFFERENT_TARGET,
            );
            insert_no_conflict(
                &mut condition,
                lhs,
                rhs,
                ConflictMask::SAME_PARENT_DIFFERENT_TARGET,
            );
        }
    }

    for mask in [
        ConflictMask::SAME_TARGET,
        ConflictMask::SAME_PARENT_DIFFERENT_TARGET,
        ConflictMask::LHS_TARGET_IS_RHS_PARENT,
        ConflictMask::ANCESTOR_DESCENDANT,
    ] {
        insert_no_conflict(&mut effect, PathId::StatSuccess, PathId::StatSuccess, mask);
        insert_no_conflict(
            &mut condition,
            PathId::StatSuccess,
            PathId::StatSuccess,
            mask,
        );
    }

    for mutation in namespace_mutations {
        for (lhs, rhs) in [
            (mutation, PathId::StatSuccess),
            (PathId::StatSuccess, mutation),
        ] {
            insert_no_conflict(
                &mut effect,
                lhs,
                rhs,
                ConflictMask::SAME_PARENT_DIFFERENT_TARGET,
            );
            insert_no_conflict(
                &mut condition,
                lhs,
                rhs,
                ConflictMask::SAME_PARENT_DIFFERENT_TARGET,
            );
        }
    }

    let non_rename_successes = [
        PathId::CreateSuccess,
        PathId::MkdirSuccess,
        PathId::UnlinkSuccess,
        PathId::RmdirSuccess,
        PathId::StatSuccess,
    ];
    for lhs in non_rename_successes {
        for rhs in non_rename_successes {
            insert_no_conflict(&mut effect, lhs, rhs, ConflictMask::INDEPENDENT_SUBTREE);
            insert_no_conflict(&mut condition, lhs, rhs, ConflictMask::INDEPENDENT_SUBTREE);
        }
    }

    SemanticTables { effect, condition }
}

pub fn build_scc_batch_plan(batch: &Batch) -> Result<SccBatchPlan> {
    let tables = semantic_tables();
    let len = batch.txs.len();
    let mut effect = SemanticDag::new(len);
    let mut condition = SemanticDag::new(len);
    let mut tx_plans: Vec<SccTxPlan> = batch
        .txs
        .iter()
        .map(|tx| SccTxPlan {
            predicted_path: predicted_path(&tx.op),
            effect_max_pred_index: None,
            condition_max_pred_index: None,
        })
        .collect();

    for i in 0..len {
        for j in (i + 1)..len {
            let mask = conflict_mask(&batch.txs[i].op, &batch.txs[j].op)?;
            let key = SemanticKey {
                path1: tx_plans[i].predicted_path,
                path2: tx_plans[j].predicted_path,
                conflict_mask: mask,
            };
            if tables.effect.has_conflict(&key) {
                effect.add_edge(i, j)?;
                update_max_pred(&mut tx_plans[j].effect_max_pred_index, i);
            }
            if tables.condition.has_conflict(&key) {
                condition.add_edge(i, j)?;
                update_max_pred(&mut tx_plans[j].condition_max_pred_index, i);
            }
        }
    }

    Ok(SccBatchPlan {
        effect,
        condition,
        tx_plans,
    })
}

impl SemanticDag {
    fn new(len: usize) -> Self {
        Self {
            nodes: (0..len).map(|_| DagNode::new()).collect(),
        }
    }

    fn add_edge(&mut self, from: usize, to: usize) -> Result<()> {
        if from >= self.nodes.len() || to >= self.nodes.len() {
            return Err(Error::InvalidBatch(format!(
                "DAG edge {from}->{to} out of range {}",
                self.nodes.len()
            )));
        }
        if from >= to {
            return Err(Error::InvalidBatch(format!(
                "DAG edge {from}->{to} violates batch order"
            )));
        }
        self.nodes[from].successors.insert(to);
        #[cfg(debug_assertions)]
        {
            self.nodes[to].predecessors.insert(from);
        }
        Ok(())
    }
}

impl DagNode {
    fn new() -> Self {
        Self {
            successors: BTreeSet::new(),
            #[cfg(debug_assertions)]
            predecessors: BTreeSet::new(),
        }
    }
}

impl SccDagRuntime {
    pub fn new(plan: &SccBatchPlan) -> (Self, Vec<TxDagWaiters>) {
        let (effect, effect_rx) = DagRuntime::new(&plan.effect);
        let (condition, condition_rx) = DagRuntime::new(&plan.condition);
        let waiters = effect_rx
            .into_iter()
            .zip(condition_rx)
            .map(|(effect_ready, condition_ready)| TxDagWaiters {
                effect_ready,
                condition_ready,
            })
            .collect();
        (Self { effect, condition }, waiters)
    }

    pub fn start(&mut self) -> Result<()> {
        self.effect.start()?;
        self.condition.start()
    }

    pub fn finish_vertex(&mut self, tx_index: usize) -> Result<()> {
        self.effect.finish_vertex(tx_index)?;
        self.condition.finish_vertex(tx_index)
    }
}

impl DagRuntime {
    fn new(dag: &SemanticDag) -> (Self, Vec<oneshot::Receiver<()>>) {
        let mut indegrees = vec![0usize; dag.nodes.len()];
        for node in &dag.nodes {
            for successor in &node.successors {
                indegrees[*successor] += 1;
            }
        }

        let mut receivers = Vec::with_capacity(dag.nodes.len());
        let nodes = dag
            .nodes
            .iter()
            .enumerate()
            .map(|(index, node)| {
                let (ready_tx, ready_rx) = oneshot::channel();
                receivers.push(ready_rx);
                DagRuntimeNode {
                    indegree: indegrees[index],
                    successors: node.successors.clone(),
                    ready_tx: Some(ready_tx),
                    finished: false,
                }
            })
            .collect();
        (Self { nodes }, receivers)
    }

    fn start(&mut self) -> Result<()> {
        let ready: Vec<usize> = self
            .nodes
            .iter()
            .enumerate()
            .filter_map(|(index, node)| (node.indegree == 0).then_some(index))
            .collect();
        for index in ready {
            self.notify_ready(index)?;
        }
        Ok(())
    }

    fn finish_vertex(&mut self, tx_index: usize) -> Result<()> {
        let node = self.nodes.get_mut(tx_index).ok_or_else(|| {
            Error::InvalidBatch(format!("DAG vertex index {tx_index} out of range"))
        })?;
        if node.finished {
            return Err(Error::InvalidBatch(format!(
                "DAG vertex {tx_index} already finished"
            )));
        }
        node.finished = true;
        let successors: Vec<usize> = node.successors.iter().copied().collect();

        for successor in successors {
            let successor_node = self.nodes.get_mut(successor).ok_or_else(|| {
                Error::InvalidBatch(format!("DAG successor index {successor} out of range"))
            })?;
            if successor_node.indegree == 0 {
                return Err(Error::InvalidBatch(format!(
                    "DAG successor {successor} indegree underflow"
                )));
            }
            successor_node.indegree -= 1;
            if successor_node.indegree == 0 {
                self.notify_ready(successor)?;
            }
        }
        Ok(())
    }

    fn notify_ready(&mut self, tx_index: usize) -> Result<()> {
        let node = self.nodes.get_mut(tx_index).ok_or_else(|| {
            Error::InvalidBatch(format!("DAG ready index {tx_index} out of range"))
        })?;
        let Some(sender) = node.ready_tx.take() else {
            return Ok(());
        };
        let _ = sender.send(());
        Ok(())
    }
}

fn update_max_pred(slot: &mut Option<usize>, pred: usize) {
    *slot = Some(slot.map_or(pred, |current| current.max(pred)));
}

fn insert_no_conflict(table: &mut SemanticTable, lhs: PathId, rhs: PathId, mask: ConflictMask) {
    table.entries.insert(
        SemanticKey {
            path1: lhs,
            path2: rhs,
            conflict_mask: mask,
        },
        false,
    );
}

pub fn conflict_mask(lhs: &FsOp, rhs: &FsOp) -> Result<ConflictMask> {
    if matches!(lhs, FsOp::Rename { .. }) || matches!(rhs, FsOp::Rename { .. }) {
        return Ok(ConflictMask::RENAME_INVOLVED);
    }

    let lhs_target = primary_target(lhs);
    let rhs_target = primary_target(rhs);
    if lhs_target.as_str() == "/" || rhs_target.as_str() == "/" {
        return Ok(ConflictMask::ROOT_INVOLVED);
    }
    if lhs_target == rhs_target {
        return Ok(ConflictMask::SAME_TARGET);
    }

    let lhs_parent = lhs_target.parent()?;
    let rhs_parent = rhs_target.parent()?;
    if lhs_target == &rhs_parent {
        return Ok(ConflictMask::LHS_TARGET_IS_RHS_PARENT);
    }
    if is_strict_ancestor(lhs_target, rhs_target) || is_strict_ancestor(rhs_target, lhs_target) {
        return Ok(ConflictMask::ANCESTOR_DESCENDANT);
    }
    if lhs_parent == rhs_parent {
        return Ok(ConflictMask::SAME_PARENT_DIFFERENT_TARGET);
    }
    Ok(ConflictMask::INDEPENDENT_SUBTREE)
}

pub fn output_to_delta(
    tx: &OrderedTx,
    reads: &BTreeMap<Key, ReadValue>,
    output: TxExecutionOutput,
) -> Result<TxDelta> {
    if output.result != TxResult::Ok {
        return Err(Error::InvalidBatch(format!(
            "tx {} cannot convert non-Ok result {:?} to delta",
            tx.tx_id, output.result
        )));
    }
    let mut new_values: BTreeMap<Key, Option<Inode>> = tx
        .write_set
        .iter()
        .map(|key| Ok((key.clone(), read_value_to_option(reads.get(key))?)))
        .collect::<Result<_>>()?;

    for write in output.writes {
        if !tx.write_set.contains(write.key()) {
            return Err(Error::InvalidBatch(format!(
                "tx {} output contains key {} outside write_set",
                tx.tx_id,
                write.key()
            )));
        }
        match write {
            WriteOp::Put { key, value } => {
                new_values.insert(key, Some(value));
            }
            WriteOp::Delete { key } => {
                new_values.insert(key, None);
            }
        }
    }

    let mut ops = Vec::new();
    for key in &tx.write_set {
        let old = read_value_to_option(reads.get(key))?;
        let new = new_values.remove(key).flatten();
        append_delta_op(&mut ops, key.clone(), old, new)?;
    }
    Ok(TxDelta { ops })
}

pub fn apply_delta_to_state(state: &mut BTreeMap<Key, Inode>, delta: &TxDelta) -> Result<()> {
    for op in &delta.ops {
        match op {
            DeltaOp::Put { key, value } => {
                state.insert(key.clone(), value.clone());
            }
            DeltaOp::Delete { key } => {
                state.remove(key);
            }
            DeltaOp::AddIntegerField {
                key,
                field: InodeIntegerField::ChildCount,
                delta,
            } => {
                let inode = state.get_mut(key).ok_or_else(|| {
                    Error::InvalidBatch(format!(
                        "cannot apply child_count delta to missing {}",
                        key
                    ))
                })?;
                if inode.kind != NodeKind::Directory {
                    return Err(Error::InvalidBatch(format!(
                        "cannot apply child_count delta to non-directory {}",
                        key
                    )));
                }
                let next = apply_signed_delta(inode.child_count, *delta).ok_or_else(|| {
                    Error::InvalidBatch(format!("child_count delta underflow for {}", key))
                })?;
                inode.child_count = next;
            }
        }
    }
    Ok(())
}

pub async fn materialized_local_read(
    tx_plan: &SccTxPlan,
    phase: SccPhase,
    local_read_keys: &BTreeSet<Key>,
    local_write_keys_by_tx: &[BTreeSet<Key>],
    base_read_cache: &BTreeMap<Key, ReadValue>,
    commit_seq: &CommitSequence,
) -> Result<BTreeMap<Key, ReadValue>> {
    let mut materialized = project_base_reads(base_read_cache, local_read_keys)?;
    let Some(max_pred_index) = (match phase {
        SccPhase::Effect => tx_plan.effect_max_pred_index,
        SccPhase::Condition => tx_plan.condition_max_pred_index,
    }) else {
        return Ok(materialized);
    };

    let mut state = read_values_to_state(&materialized);
    for index in 0..=max_pred_index {
        let slot = commit_seq.wait_terminal(index).await?;
        match slot {
            CommitSlotState::Pending => unreachable!("wait_terminal never returns Pending"),
            CommitSlotState::Failed => {
                return Err(Error::SpeculationFailed(format!(
                    "materialized read observed failed predecessor slot {index}"
                )))
            }
            CommitSlotState::NoOp => {}
            CommitSlotState::Delta(delta) => {
                let writes = local_write_keys_by_tx.get(index).ok_or_else(|| {
                    Error::InvalidBatch(format!("missing local write keys for tx index {index}"))
                })?;
                if !writes.is_disjoint(local_read_keys) {
                    let projected_delta = filter_delta_to_keys(&delta, local_read_keys);
                    apply_delta_to_state(&mut state, &projected_delta)?;
                    materialized = state_to_read_values(local_read_keys, &state);
                }
            }
        }
    }
    Ok(materialized)
}

pub fn filter_delta_to_keys(delta: &TxDelta, keys: &BTreeSet<Key>) -> TxDelta {
    TxDelta {
        ops: delta
            .ops
            .iter()
            .filter(|op| keys.contains(op.key()))
            .cloned()
            .collect(),
    }
}

fn append_delta_op(
    ops: &mut Vec<DeltaOp>,
    key: Key,
    old: Option<Inode>,
    new: Option<Inode>,
) -> Result<()> {
    match (old, new) {
        (None, None) => {}
        (None, Some(value)) => ops.push(DeltaOp::Put { key, value }),
        (Some(_), None) => ops.push(DeltaOp::Delete { key }),
        (Some(old), Some(new)) if old == new => {}
        (Some(old), Some(new))
            if old.kind == NodeKind::Directory && new.kind == NodeKind::Directory =>
        {
            let mut normalized_new = new.clone();
            normalized_new.child_count = old.child_count;
            if normalized_new == old {
                let delta = new.child_count as i128 - old.child_count as i128;
                if delta != 0 {
                    let delta = i64::try_from(delta).map_err(|_| {
                        Error::InvalidBatch(format!(
                            "child_count delta out of range for key {}",
                            key
                        ))
                    })?;
                    ops.push(DeltaOp::AddIntegerField {
                        key,
                        field: InodeIntegerField::ChildCount,
                        delta,
                    });
                }
            } else {
                ops.push(DeltaOp::Put { key, value: new });
            }
        }
        (Some(_), Some(value)) => ops.push(DeltaOp::Put { key, value }),
    }
    Ok(())
}

fn read_value_to_option(value: Option<&ReadValue>) -> Result<Option<Inode>> {
    match value {
        Some(ReadValue::Present(inode)) => Ok(Some(inode.clone())),
        Some(ReadValue::Missing) => Ok(None),
        None => Err(Error::InvalidBatch(
            "missing read value for delta conversion".to_string(),
        )),
    }
}

fn project_base_reads(
    base_read_cache: &BTreeMap<Key, ReadValue>,
    local_read_keys: &BTreeSet<Key>,
) -> Result<BTreeMap<Key, ReadValue>> {
    let mut out = BTreeMap::new();
    for key in local_read_keys {
        let value = base_read_cache.get(key).ok_or_else(|| {
            Error::InvalidBatch(format!("base read cache missing local read key {}", key))
        })?;
        out.insert(key.clone(), value.clone());
    }
    Ok(out)
}

fn read_values_to_state(reads: &BTreeMap<Key, ReadValue>) -> BTreeMap<Key, Inode> {
    reads
        .iter()
        .filter_map(|(key, value)| match value {
            ReadValue::Present(inode) => Some((key.clone(), inode.clone())),
            ReadValue::Missing => None,
        })
        .collect()
}

fn state_to_read_values(
    local_read_keys: &BTreeSet<Key>,
    state: &BTreeMap<Key, Inode>,
) -> BTreeMap<Key, ReadValue> {
    local_read_keys
        .iter()
        .map(|key| {
            let value = state
                .get(key)
                .cloned()
                .map(ReadValue::Present)
                .unwrap_or(ReadValue::Missing);
            (key.clone(), value)
        })
        .collect()
}

fn read_value_to_option_for_key(reads: &BTreeMap<Key, ReadValue>, key: &Key) -> Option<Inode> {
    match reads.get(key) {
        Some(ReadValue::Present(inode)) => Some(inode.clone()),
        Some(ReadValue::Missing) | None => None,
    }
}

fn primary_target(op: &FsOp) -> &Key {
    match op {
        FsOp::Create { path }
        | FsOp::Mkdir { path }
        | FsOp::Unlink { path }
        | FsOp::Rmdir { path }
        | FsOp::Stat { path } => path,
        FsOp::Rename { src, .. } => src,
    }
}

fn is_strict_ancestor(ancestor: &Key, descendant: &Key) -> bool {
    let ancestor = ancestor.as_str();
    let descendant = descendant.as_str();
    if ancestor == "/" {
        return descendant != "/";
    }
    descendant
        .strip_prefix(ancestor)
        .is_some_and(|suffix| suffix.starts_with('/'))
}

fn apply_signed_delta(value: u64, delta: i64) -> Option<u64> {
    if delta >= 0 {
        value.checked_add(delta as u64)
    } else {
        value.checked_sub(delta.unsigned_abs())
    }
}

pub fn check_success_path_condition(
    tx: &OrderedTx,
    predicted: PathId,
    reads: &BTreeMap<Key, ReadValue>,
) -> Result<bool> {
    if predicted != predicted_path(&tx.op) {
        return Ok(false);
    }
    Ok(match &tx.op {
        FsOp::Create { path } => check_create_like(path, reads, NodeKind::File)?,
        FsOp::Mkdir { path } => check_create_like(path, reads, NodeKind::Directory)?,
        FsOp::Unlink { path } => check_unlink_success(path, reads)?,
        FsOp::Rmdir { path } => check_rmdir_success(path, reads)?,
        FsOp::Rename { src, dst } => check_rename_success(src, dst, reads)?,
        FsOp::Stat { path } => read_value_to_option_for_key(reads, path).is_some(),
    })
}

fn check_create_like(
    path: &Key,
    reads: &BTreeMap<Key, ReadValue>,
    _kind: NodeKind,
) -> Result<bool> {
    if path.as_str() == "/" {
        return Ok(matches!(reads.get(path), Some(ReadValue::Missing)));
    }
    let parent = path.parent()?;
    let Some(parent_inode) = read_value_to_option_for_key(reads, &parent) else {
        return Ok(false);
    };
    Ok(parent_inode.kind == NodeKind::Directory
        && read_value_to_option_for_key(reads, path).is_none())
}

fn check_unlink_success(path: &Key, reads: &BTreeMap<Key, ReadValue>) -> Result<bool> {
    if path.as_str() == "/" {
        return Ok(false);
    }
    let parent = path.parent()?;
    let Some(parent_inode) = read_value_to_option_for_key(reads, &parent) else {
        return Ok(false);
    };
    let Some(target) = read_value_to_option_for_key(reads, path) else {
        return Ok(false);
    };
    Ok(parent_inode.kind == NodeKind::Directory && target.kind == NodeKind::File)
}

fn check_rmdir_success(path: &Key, reads: &BTreeMap<Key, ReadValue>) -> Result<bool> {
    if path.as_str() == "/" {
        return Ok(false);
    }
    let parent = path.parent()?;
    let Some(parent_inode) = read_value_to_option_for_key(reads, &parent) else {
        return Ok(false);
    };
    let Some(target) = read_value_to_option_for_key(reads, path) else {
        return Ok(false);
    };
    Ok(parent_inode.kind == NodeKind::Directory
        && target.kind == NodeKind::Directory
        && target.child_count == 0)
}

fn check_rename_success(src: &Key, dst: &Key, reads: &BTreeMap<Key, ReadValue>) -> Result<bool> {
    if src == dst || src.as_str() == "/" || dst.as_str() == "/" {
        return Ok(false);
    }
    if is_strict_ancestor(src, dst) {
        return Ok(false);
    }
    let src_parent = src.parent()?;
    let dst_parent = dst.parent()?;
    let Some(src_parent_inode) = read_value_to_option_for_key(reads, &src_parent) else {
        return Ok(false);
    };
    let Some(dst_parent_inode) = read_value_to_option_for_key(reads, &dst_parent) else {
        return Ok(false);
    };
    let Some(src_inode) = read_value_to_option_for_key(reads, src) else {
        return Ok(false);
    };
    Ok(src_parent_inode.kind == NodeKind::Directory
        && dst_parent_inode.kind == NodeKind::Directory
        && read_value_to_option_for_key(reads, dst).is_none()
        && (src_inode.kind == NodeKind::File
            || (src_inode.kind == NodeKind::Directory && src_inode.child_count == 0)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{BatchId, TxId};

    fn key(path: &str) -> Key {
        Key::new(path).unwrap()
    }

    fn create_tx(tx_id: TxId, path: &str) -> OrderedTx {
        let path = key(path);
        let parent = path.parent().unwrap();
        OrderedTx {
            tx_id,
            batch_index: (tx_id - 1) as u32,
            op: FsOp::Create { path: path.clone() },
            read_set: [parent.clone(), path.clone()].into_iter().collect(),
            write_set: [parent, path].into_iter().collect(),
        }
    }

    fn mkdir_tx(tx_id: TxId, path: &str) -> OrderedTx {
        let path = key(path);
        let mut read_set = BTreeSet::new();
        let mut write_set = BTreeSet::new();
        if path.as_str() == "/" {
            read_set.insert(path.clone());
            write_set.insert(path.clone());
        } else {
            let parent = path.parent().unwrap();
            read_set.insert(parent.clone());
            read_set.insert(path.clone());
            write_set.insert(parent);
            write_set.insert(path.clone());
        }
        OrderedTx {
            tx_id,
            batch_index: (tx_id - 1) as u32,
            op: FsOp::Mkdir { path },
            read_set,
            write_set,
        }
    }

    #[test]
    fn same_parent_different_name_create_has_no_conflict() {
        let tables = semantic_tables();
        let mask = conflict_mask(
            &FsOp::Create { path: key("/d/a") },
            &FsOp::Create { path: key("/d/b") },
        )
        .unwrap();
        assert_eq!(mask, ConflictMask::SAME_PARENT_DIFFERENT_TARGET);
        let key = SemanticKey {
            path1: PathId::CreateSuccess,
            path2: PathId::CreateSuccess,
            conflict_mask: mask,
        };
        assert!(!tables.effect.has_conflict(&key));
        assert!(!tables.condition.has_conflict(&key));
    }

    #[test]
    fn parent_create_before_child_create_conflicts() {
        let tables = semantic_tables();
        let mask = conflict_mask(
            &FsOp::Mkdir { path: key("/d") },
            &FsOp::Create { path: key("/d/a") },
        )
        .unwrap();
        assert_eq!(mask, ConflictMask::LHS_TARGET_IS_RHS_PARENT);
        let key = SemanticKey {
            path1: PathId::MkdirSuccess,
            path2: PathId::CreateSuccess,
            conflict_mask: mask,
        };
        assert!(tables.effect.has_conflict(&key));
        assert!(tables.condition.has_conflict(&key));
    }

    #[test]
    fn batch_plan_omits_same_parent_different_target_edges() {
        let batch = Batch {
            batch_id: 1,
            txs: vec![create_tx(1, "/d/a"), create_tx(2, "/d/b")],
        };
        let plan = build_scc_batch_plan(&batch).unwrap();
        assert!(plan.effect.nodes[0].successors.is_empty());
        assert!(plan.condition.nodes[0].successors.is_empty());
        assert_eq!(plan.tx_plans[1].effect_max_pred_index, None);
        assert_eq!(plan.tx_plans[1].condition_max_pred_index, None);
    }

    #[test]
    fn batch_plan_adds_parent_before_child_edges_and_prefix_bounds() {
        let batch = Batch {
            batch_id: 1,
            txs: vec![mkdir_tx(1, "/d"), create_tx(2, "/d/a")],
        };
        let plan = build_scc_batch_plan(&batch).unwrap();
        assert!(plan.effect.nodes[0].successors.contains(&1));
        assert!(plan.condition.nodes[0].successors.contains(&1));
        assert_eq!(plan.tx_plans[1].effect_max_pred_index, Some(0));
        assert_eq!(plan.tx_plans[1].condition_max_pred_index, Some(0));
    }

    #[test]
    fn effect_prefix_cover_determines_condition_skip() {
        let mut plan = SccTxPlan {
            predicted_path: PathId::CreateSuccess,
            effect_max_pred_index: None,
            condition_max_pred_index: None,
        };
        assert!(plan.effect_prefix_covers_condition());

        plan.effect_max_pred_index = Some(2);
        assert!(plan.effect_prefix_covers_condition());

        plan.condition_max_pred_index = Some(1);
        assert!(plan.effect_prefix_covers_condition());

        plan.condition_max_pred_index = Some(2);
        assert!(plan.effect_prefix_covers_condition());

        plan.condition_max_pred_index = Some(3);
        assert!(!plan.effect_prefix_covers_condition());

        plan.effect_max_pred_index = None;
        plan.condition_max_pred_index = Some(0);
        assert!(!plan.effect_prefix_covers_condition());
    }

    #[test]
    fn delta_create_same_parent_merges_child_count() {
        let tx = create_tx(1, "/d/a");
        let mut reads = BTreeMap::new();
        reads.insert(key("/d"), ReadValue::Present(Inode::directory(0)));
        reads.insert(key("/d/a"), ReadValue::Missing);
        let output = TxExecutionOutput {
            result: TxResult::Ok,
            writes: vec![
                WriteOp::Put {
                    key: key("/d"),
                    value: Inode::directory(1),
                },
                WriteOp::Put {
                    key: key("/d/a"),
                    value: Inode::file(),
                },
            ],
        };
        let delta = output_to_delta(&tx, &reads, output).unwrap();
        assert_eq!(
            delta.ops,
            vec![
                DeltaOp::AddIntegerField {
                    key: key("/d"),
                    field: InodeIntegerField::ChildCount,
                    delta: 1,
                },
                DeltaOp::Put {
                    key: key("/d/a"),
                    value: Inode::file(),
                },
            ]
        );

        let mut state = BTreeMap::from([(key("/d"), Inode::directory(41))]);
        apply_delta_to_state(&mut state, &delta).unwrap();
        assert_eq!(state.get(&key("/d")).unwrap().child_count, 42);
        assert_eq!(state.get(&key("/d/a")).unwrap().kind, NodeKind::File);
    }

    #[tokio::test]
    async fn materialized_read_prefix_failed_slot_forces_failure_without_write_intersection() {
        let commit_seq = CommitSequence::new(2);
        commit_seq
            .set_terminal_once(0, CommitSlotState::Failed)
            .unwrap();
        let tx_plan = SccTxPlan {
            predicted_path: PathId::CreateSuccess,
            effect_max_pred_index: Some(0),
            condition_max_pred_index: None,
        };
        let local_read_keys = BTreeSet::from([key("/unrelated")]);
        let base = BTreeMap::from([(key("/unrelated"), ReadValue::Missing)]);
        let writes_by_tx = vec![BTreeSet::from([key("/d/a")])];
        let result = materialized_local_read(
            &tx_plan,
            SccPhase::Effect,
            &local_read_keys,
            &writes_by_tx,
            &base,
            &commit_seq,
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn materialized_read_applies_only_intersecting_deltas() {
        let commit_seq = CommitSequence::new(2);
        let delta = TxDelta {
            ops: vec![DeltaOp::AddIntegerField {
                key: key("/d"),
                field: InodeIntegerField::ChildCount,
                delta: 1,
            }],
        };
        commit_seq
            .set_terminal_once(0, CommitSlotState::Delta(Arc::new(delta)))
            .unwrap();
        let tx_plan = SccTxPlan {
            predicted_path: PathId::CreateSuccess,
            effect_max_pred_index: Some(0),
            condition_max_pred_index: None,
        };
        let local_read_keys = BTreeSet::from([key("/d")]);
        let base = BTreeMap::from([(key("/d"), ReadValue::Present(Inode::directory(0)))]);
        let writes_by_tx = vec![BTreeSet::from([key("/d")])];
        let result = materialized_local_read(
            &tx_plan,
            SccPhase::Effect,
            &local_read_keys,
            &writes_by_tx,
            &base,
            &commit_seq,
        )
        .await
        .unwrap();
        assert_eq!(
            result.get(&key("/d")),
            Some(&ReadValue::Present(Inode::directory(1)))
        );
    }

    #[tokio::test]
    async fn dag_runtime_notifies_initial_and_successor_ready() {
        let batch = Batch {
            batch_id: 1,
            txs: vec![
                mkdir_tx(1, "/d"),
                create_tx(2, "/d/a"),
                create_tx(3, "/other/a"),
            ],
        };
        let plan = build_scc_batch_plan(&batch).unwrap();
        let (mut runtime, mut waiters) = SccDagRuntime::new(&plan);
        runtime.start().unwrap();

        assert!(waiters[0].effect_ready.try_recv().is_ok());
        assert!(waiters[2].effect_ready.try_recv().is_ok());
        assert!(waiters[1].effect_ready.try_recv().is_err());

        runtime.finish_vertex(0).unwrap();
        assert!(waiters[1].effect_ready.try_recv().is_ok());
    }

    #[allow(dead_code)]
    fn _batch_id(_: BatchId) {}
}
