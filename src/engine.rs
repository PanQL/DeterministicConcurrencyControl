use crate::convert::{
    batch_to_proto, local_read_status_to_i32, read_entries_to_proto, read_phase_to_i32,
    tx_result_to_i32, write_entries_to_proto,
};
use crate::error::{Error, Result};
use crate::executor::{
    derive_read_write_set, execute_deterministic, filter_local_writes, validate_sets,
};
use crate::lock::{LockGrant, LockTable};
use crate::model::{
    Batch, BatchId, FsOp, Inode, Key, LocalReadStatus, OrderedTx, ReadPhase, ReadValue,
    SccReorderRecord, SchedulerProfileCounters, SchedulerProfileRecord, SchedulerProfileScheduler,
    SchedulerProfileTimings, ShardId, TxId, TxResult, TxResultRecord, WorkerStageStats,
};
use crate::proto::pb;
use crate::router::{Participants, ShardLayout};
use crate::scc::{
    build_scc_batch_plan, check_success_path_condition, classify_actual_path, filter_delta_to_keys,
    materialized_local_read, output_to_delta, CommitSequence, CommitSlotState, SccDagRuntime,
    SccPhase, SccTxPlan, SemanticDag, TxDagWaiters, TxDelta,
};
use crate::storage::RedbInMemoryInodeStore;
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot, watch, Mutex, Notify};

const BATCH_QUEUE_CAPACITY: usize = 16;
const DEFAULT_MAILBOX_CAPACITY: usize = 1024;
const SEQUENCER_COMMAND_CAPACITY: usize = 1024;
const DEFAULT_BATCH_FLUSH_INTERVAL: Duration = Duration::from_millis(1);
const SCHEDULER_PROFILE_ENV: &str = "CALVINFS_SCHED_PROFILE";

fn scheduler_profile_enabled_from_env() -> bool {
    env::var_os(SCHEDULER_PROFILE_ENV)
        .and_then(|value| value.into_string().ok())
        .is_some_and(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SchedulerKind {
    #[default]
    CalvinLocking,
    SccOnline,
    Aria,
}

#[derive(Clone, Debug)]
pub struct ShardConfig {
    pub node_id: String,
    pub shard_id: ShardId,
    pub shard_count: u64,
    pub peer_endpoints: BTreeMap<ShardId, String>,
    pub scheduler: SchedulerKind,
}

#[derive(Clone)]
pub struct ShardRuntime {
    core: Arc<ShardCore>,
    batch_tx: mpsc::Sender<BatchJob>,
}

struct ShardCore {
    node_id: String,
    shard_id: ShardId,
    layout: ShardLayout,
    store: Arc<RedbInMemoryInodeStore>,
    peer_endpoints: Arc<BTreeMap<ShardId, String>>,
    mailboxes: ReadResultMailboxRegistry,
    aria_batches: AriaBatchRegistry,
    client_results: TxResultRegistry,
    scc_reorders: Arc<Mutex<BTreeMap<BatchId, SccReorderRecord>>>,
    scheduler_profiles: Arc<Mutex<BTreeMap<(BatchId, ShardId), SchedulerProfileRecord>>>,
    scheduler_profile_enabled: bool,
    scheduler: SchedulerKind,
}

pub struct BatchExecutionSummary {
    pub batch_id: BatchId,
    pub shard_id: ShardId,
    pub tx_results: Vec<TxResultRecord>,
}

struct ProfiledBatchExecution {
    summary: BatchExecutionSummary,
    profile: Option<SchedulerProfileRecord>,
}

#[derive(Clone, Debug)]
pub struct AriaStageOutcome {
    pub batch_id: BatchId,
    pub tx_index: usize,
    pub tx_id: TxId,
    pub from_shard: ShardId,
    pub result: TxResult,
    pub writes: Vec<crate::model::WriteOp>,
    pub is_result_shard: bool,
}

#[derive(Clone, Debug)]
struct AriaStagedOutcome {
    tx_index: usize,
    tx_id: TxId,
    result: Option<TxResult>,
    local_writes: Vec<crate::model::WriteOp>,
    is_result_shard: bool,
}

#[derive(Default)]
struct AriaBatchState {
    staged_outcomes: BTreeMap<usize, AriaStagedOutcome>,
    readers_by_key: BTreeMap<Key, BTreeSet<usize>>,
    writers_by_key: BTreeMap<Key, BTreeSet<usize>>,
    execution_done_reports: BTreeSet<ShardId>,
    failure_reports: BTreeMap<ShardId, BTreeSet<usize>>,
}

struct AriaBatchEntry {
    state: Mutex<AriaBatchState>,
    notify: Notify,
}

#[derive(Clone, Default)]
struct AriaBatchRegistry {
    inner: Arc<Mutex<BTreeMap<BatchId, Arc<AriaBatchEntry>>>>,
}

#[derive(Default)]
struct CalvinWorkerProfile {
    lock_wait_ns: u64,
    local_read_ns: u64,
    remote_read_send_ns: u64,
    remote_read_collect_ns: u64,
    execute_apply_ns: u64,
    result_mark_ns: u64,
    remote_sent: u64,
    remote_received: u64,
}

#[derive(Default)]
struct SccWorkerProfile {
    effect_wait_ns: u64,
    effect_materialize_ns: u64,
    effect_send_ns: u64,
    effect_collect_ns: u64,
    execute_ns: u64,
    delta_build_ns: u64,
    condition_wait_ns: u64,
    condition_materialize_ns: u64,
    condition_send_ns: u64,
    condition_collect_ns: u64,
    condition_check_ns: u64,
    commit_ns: u64,
    remote_sent: u64,
    remote_received: u64,
    condition_skipped: bool,
    delta_op_count: u64,
}

struct BatchJob {
    batch: Batch,
    respond_to: oneshot::Sender<Result<BatchExecutionSummary>>,
}

impl ShardRuntime {
    pub fn new(config: ShardConfig) -> Result<Self> {
        let (batch_tx, batch_rx) = mpsc::channel(BATCH_QUEUE_CAPACITY);
        let core = Arc::new(ShardCore {
            node_id: config.node_id,
            shard_id: config.shard_id,
            layout: ShardLayout::new(config.shard_count),
            store: Arc::new(RedbInMemoryInodeStore::new()?),
            peer_endpoints: Arc::new(config.peer_endpoints),
            mailboxes: ReadResultMailboxRegistry::new(),
            aria_batches: AriaBatchRegistry::new(),
            client_results: TxResultRegistry::new(),
            scc_reorders: Arc::new(Mutex::new(BTreeMap::new())),
            scheduler_profiles: Arc::new(Mutex::new(BTreeMap::new())),
            scheduler_profile_enabled: scheduler_profile_enabled_from_env(),
            scheduler: config.scheduler,
        });
        tokio::spawn(run_batch_executor(core.clone(), batch_rx));
        Ok(Self { core, batch_tx })
    }

    pub fn node_id(&self) -> &str {
        &self.core.node_id
    }

    pub fn shard_id(&self) -> ShardId {
        self.core.shard_id
    }

    pub async fn execute_batch(&self, batch: Batch) -> Result<BatchExecutionSummary> {
        let (respond_to, response_rx) = oneshot::channel();
        self.batch_tx
            .send(BatchJob { batch, respond_to })
            .await
            .map_err(|_| Error::ChannelClosed("shard batch executor is closed".to_string()))?;
        response_rx.await.map_err(|_| {
            Error::ChannelClosed("shard batch response channel is closed".to_string())
        })?
    }

    pub async fn route_local_read_result(&self, result: LocalReadResult) -> Result<()> {
        self.core.mailboxes.route(result).await
    }

    pub async fn aria_read_snapshot(
        &self,
        batch_id: BatchId,
        tx_index: usize,
        tx_id: TxId,
        from_shard: ShardId,
        key: Key,
    ) -> Result<ReadValue> {
        self.core
            .aria_batches
            .read_snapshot(
                self.core.clone(),
                batch_id,
                tx_index,
                tx_id,
                from_shard,
                key,
            )
            .await
    }

    pub async fn route_aria_stage_outcome(&self, outcome: AriaStageOutcome) -> Result<()> {
        self.core
            .aria_batches
            .stage_outcome(self.core.clone(), outcome)
            .await
    }

    pub async fn report_aria_execution_done(
        &self,
        batch_id: BatchId,
        from_shard: ShardId,
    ) -> Result<()> {
        self.core
            .aria_batches
            .record_execution_done(batch_id, from_shard, self.core.layout.shard_count)
            .await
    }

    pub async fn report_aria_local_failures(
        &self,
        batch_id: BatchId,
        from_shard: ShardId,
        failed_indices: BTreeSet<usize>,
    ) -> Result<()> {
        self.core
            .aria_batches
            .record_local_failures(
                batch_id,
                from_shard,
                failed_indices,
                self.core.layout.shard_count,
            )
            .await
    }

    pub async fn get_tx_result(&self, tx_id: TxId) -> Result<ClientTxResult> {
        self.core.client_results.wait(tx_id).await
    }

    pub fn dump_state(&self) -> Result<BTreeMap<Key, Inode>> {
        self.core.store.dump()
    }

    pub async fn dump_scc_reorders(&self) -> Vec<SccReorderRecord> {
        self.core
            .scc_reorders
            .lock()
            .await
            .values()
            .cloned()
            .collect()
    }

    pub async fn dump_scheduler_profiles(&self) -> Vec<SchedulerProfileRecord> {
        self.core
            .scheduler_profiles
            .lock()
            .await
            .values()
            .cloned()
            .collect()
    }
}

async fn run_batch_executor(core: Arc<ShardCore>, mut batch_rx: mpsc::Receiver<BatchJob>) {
    while let Some(job) = batch_rx.recv().await {
        let response = execute_batch_on_shard(core.clone(), job.batch).await;
        let _ = job.respond_to.send(response);
    }
}

async fn execute_batch_on_shard(
    core: Arc<ShardCore>,
    batch: Batch,
) -> Result<BatchExecutionSummary> {
    let profile_started = Instant::now();
    let mut result = match core.scheduler {
        SchedulerKind::CalvinLocking => {
            execute_calvin_batch(
                core.clone(),
                batch.clone(),
                core.scheduler_profile_enabled,
                SchedulerProfileScheduler::CalvinLocking,
            )
            .await
        }
        SchedulerKind::SccOnline => {
            execute_scc_batch(core.clone(), batch.clone(), core.scheduler_profile_enabled).await
        }
        SchedulerKind::Aria => {
            execute_aria_batch(core.clone(), batch.clone(), core.scheduler_profile_enabled).await
        }
    };
    let cleanup_started = Instant::now();
    core.mailboxes.cleanup_batch(batch.batch_id).await;
    core.aria_batches.cleanup_batch(batch.batch_id).await;
    let cleanup_ns = elapsed_ns(cleanup_started);

    if let Ok(profiled) = &mut result {
        if let Some(profile) = &mut profiled.profile {
            profile.timings.cleanup_ns = cleanup_ns;
            profile.timings.total_ns = elapsed_ns(profile_started);
            let mut profiles = core.scheduler_profiles.lock().await;
            profiles.insert((profile.batch_id, profile.shard_id), profile.clone());
        }
    }

    result.map(|profiled| profiled.summary)
}

async fn execute_calvin_batch(
    core: Arc<ShardCore>,
    batch: Batch,
    profile_enabled: bool,
    profile_scheduler: SchedulerProfileScheduler,
) -> Result<ProfiledBatchExecution> {
    let mut profile =
        profile_enabled.then(|| new_scheduler_profile(profile_scheduler, &batch, &core));
    let started = Instant::now();
    validate_batch_order(&batch)?;
    add_timing(&mut profile, |timings| {
        timings.validate_ns = timings.validate_ns.saturating_add(elapsed_ns(started));
    });

    let mut lock_table = LockTable::new();
    let mut relevant_count = 0usize;
    let (outcome_tx, mut outcome_rx) = mpsc::channel(batch.txs.len().max(1));

    for tx in &batch.txs {
        let validate_started = Instant::now();
        validate_sets(tx)?;
        add_timing(&mut profile, |timings| {
            timings.validate_ns = timings
                .validate_ns
                .saturating_add(elapsed_ns(validate_started));
        });
        let result_shard = core
            .layout
            .result_shard(tx)
            .ok_or_else(|| Error::InvalidBatch(format!("tx {} has no result shard", tx.tx_id)))?;
        let registry_started = Instant::now();
        if result_shard == core.shard_id {
            core.client_results.ensure_pending(tx.tx_id).await;
        } else {
            core.client_results.mark_not_responsible(tx.tx_id).await;
        }
        add_timing(&mut profile, |timings| {
            timings.result_registry_ns = timings
                .result_registry_ns
                .saturating_add(elapsed_ns(registry_started));
        });

        let local_keys = core.layout.local_lock_keys(tx, core.shard_id);
        if let Some(profile) = &mut profile {
            let local_read_keys = core.layout.local_read_keys(tx, core.shard_id);
            let local_write_keys = core.layout.local_write_keys(tx, core.shard_id);
            profile.counters.local_read_key_count = profile
                .counters
                .local_read_key_count
                .saturating_add(usize_to_u64(local_read_keys.len()));
            profile.counters.local_write_key_count = profile
                .counters
                .local_write_key_count
                .saturating_add(usize_to_u64(local_write_keys.len()));
            profile.counters.lock_key_count = profile
                .counters
                .lock_key_count
                .saturating_add(usize_to_u64(local_keys.len()));
        }
        if local_keys.is_empty() {
            add_counter(&mut profile, |counters| {
                counters.non_participant_tx_count =
                    counters.non_participant_tx_count.saturating_add(1);
            });
            continue;
        }

        relevant_count += 1;
        add_counter(&mut profile, |counters| {
            counters.relevant_tx_count = counters.relevant_tx_count.saturating_add(1);
        });
        let (grant_tx, grant_rx) = mpsc::channel(local_keys.len().max(1));
        lock_table.enqueue(tx.tx_id, &local_keys, grant_tx);

        let participants = core.layout.participants(tx);
        add_participant_counters(&mut profile, &participants, core.shard_id);
        let remote_rx = if participants.active.contains(&core.shard_id) {
            Some(
                core.mailboxes
                    .receiver(
                        batch.batch_id,
                        tx.tx_id,
                        ReadPhase::Calvin,
                        participants.all.len().max(1),
                    )
                    .await?,
            )
        } else {
            None
        };

        spawn_tx_worker(TxWorker {
            core: core.clone(),
            batch_id: batch.batch_id,
            tx: tx.clone(),
            participants,
            result_shard,
            local_keys,
            grant_rx,
            remote_rx,
            profile_enabled,
            outcome_tx: outcome_tx.clone(),
        });
    }
    drop(outcome_tx);

    lock_table.grant_initial_heads().await?;

    let mut tx_results = Vec::new();
    for _ in 0..relevant_count {
        let collect_started = Instant::now();
        let outcome = outcome_rx.recv().await.ok_or_else(|| {
            Error::ChannelClosed("worker outcome channel closed before batch finished".to_string())
        })?;
        let tx_id = outcome.tx_id;
        lock_table
            .release_and_grant_next(tx_id, &outcome.local_keys)
            .await?;
        add_timing(&mut profile, |timings| {
            timings.outcome_collect_release_ns = timings
                .outcome_collect_release_ns
                .saturating_add(elapsed_ns(collect_started));
        });
        match outcome.result? {
            WorkerCompletion::Active(record, worker_profile) => {
                merge_calvin_worker_profile(&mut profile, worker_profile);
                tx_results.push(record);
            }
            WorkerCompletion::Passive(worker_profile) => {
                merge_calvin_worker_profile(&mut profile, worker_profile);
            }
        }
    }

    tx_results.sort_by_key(|record| (record.tx_id, record.shard_id));
    if let Some(profile) = &mut profile {
        profile.counters.result_records_produced = usize_to_u64(tx_results.len());
    }
    Ok(ProfiledBatchExecution {
        summary: BatchExecutionSummary {
            batch_id: batch.batch_id,
            shard_id: core.shard_id,
            tx_results,
        },
        profile,
    })
}

async fn execute_scc_batch(
    core: Arc<ShardCore>,
    batch: Batch,
    profile_enabled: bool,
) -> Result<ProfiledBatchExecution> {
    let mut profile = profile_enabled.then(|| {
        let mut profile =
            new_scheduler_profile(SchedulerProfileScheduler::SccOnline, &batch, &core);
        profile.counters.plan_pair_count = plan_pair_count(batch.txs.len());
        profile
    });

    let validate_started = Instant::now();
    validate_batch_order(&batch)?;
    add_timing(&mut profile, |timings| {
        timings.validate_ns = timings
            .validate_ns
            .saturating_add(elapsed_ns(validate_started));
    });
    for tx in &batch.txs {
        let validate_started = Instant::now();
        validate_sets(tx)?;
        add_timing(&mut profile, |timings| {
            timings.validate_ns = timings
                .validate_ns
                .saturating_add(elapsed_ns(validate_started));
        });
        let result_shard = core
            .layout
            .result_shard(tx)
            .ok_or_else(|| Error::InvalidBatch(format!("tx {} has no result shard", tx.tx_id)))?;
        let registry_started = Instant::now();
        if result_shard == core.shard_id {
            core.client_results.ensure_pending(tx.tx_id).await;
        } else {
            core.client_results.mark_not_responsible(tx.tx_id).await;
        }
        add_timing(&mut profile, |timings| {
            timings.result_registry_ns = timings
                .result_registry_ns
                .saturating_add(elapsed_ns(registry_started));
        });
    }

    let plan_started = Instant::now();
    let plan = Arc::new(build_scc_batch_plan(&batch)?);
    add_timing(&mut profile, |timings| {
        timings.plan_build_ns = elapsed_ns(plan_started);
    });
    if let Some(profile) = &mut profile {
        profile.counters.effect_edge_count = dag_edge_count(&plan.effect);
        profile.counters.condition_edge_count = dag_edge_count(&plan.condition);
    }

    let dag_setup_started = Instant::now();
    let commit_seq = Arc::new(CommitSequence::new(batch.txs.len()));
    let (mut dag_runtime, waiters) = SccDagRuntime::new(&plan);
    let mut waiters_by_tx: Vec<Option<TxDagWaiters>> = waiters.into_iter().map(Some).collect();
    add_timing(&mut profile, |timings| {
        timings.dag_setup_ns = elapsed_ns(dag_setup_started);
    });

    let local_write_keys_by_tx: Arc<Vec<BTreeSet<Key>>> = Arc::new(
        batch
            .txs
            .iter()
            .map(|tx| core.layout.local_write_keys(tx, core.shard_id))
            .collect(),
    );
    let local_read_keys_by_tx: Vec<BTreeSet<Key>> = batch
        .txs
        .iter()
        .map(|tx| core.layout.local_read_keys(tx, core.shard_id))
        .collect();
    let local_base_read_keys = local_read_keys_by_tx
        .iter()
        .flat_map(|keys| keys.iter().cloned())
        .collect();
    if let Some(profile) = &mut profile {
        profile.counters.local_read_key_count = local_read_keys_by_tx
            .iter()
            .map(|keys| usize_to_u64(keys.len()))
            .fold(0u64, u64::saturating_add);
        profile.counters.local_write_key_count = local_write_keys_by_tx
            .iter()
            .map(|keys| usize_to_u64(keys.len()))
            .fold(0u64, u64::saturating_add);
    }
    let base_read_started = Instant::now();
    let base_read_cache = Arc::new(core.store.read_many(&local_base_read_keys)?);
    add_timing(&mut profile, |timings| {
        timings.base_read_ns = elapsed_ns(base_read_started);
    });

    let (outcome_tx, mut outcome_rx) = mpsc::channel(batch.txs.len().max(1));
    let mut relevant_count = 0usize;
    let mut non_participant_indices = Vec::new();

    for (tx_index, tx) in batch.txs.iter().enumerate() {
        let local_read_keys = local_read_keys_by_tx[tx_index].clone();
        let local_write_keys = local_write_keys_by_tx[tx_index].clone();
        if local_read_keys.is_empty() && local_write_keys.is_empty() {
            non_participant_indices.push(tx_index);
            add_counter(&mut profile, |counters| {
                counters.non_participant_tx_count =
                    counters.non_participant_tx_count.saturating_add(1);
            });
            continue;
        }

        relevant_count += 1;
        add_counter(&mut profile, |counters| {
            counters.relevant_tx_count = counters.relevant_tx_count.saturating_add(1);
        });
        let participants = core.layout.participants(tx);
        add_participant_counters(&mut profile, &participants, core.shard_id);
        let mailbox_started = Instant::now();
        let effect_rx = core
            .mailboxes
            .receiver(
                batch.batch_id,
                tx.tx_id,
                ReadPhase::SccEffect,
                participants.all.len().max(1),
            )
            .await?;
        let condition_rx = core
            .mailboxes
            .receiver(
                batch.batch_id,
                tx.tx_id,
                ReadPhase::SccCondition,
                participants.all.len().max(1),
            )
            .await?;
        add_timing(&mut profile, |timings| {
            timings.mailbox_spawn_ns = timings
                .mailbox_spawn_ns
                .saturating_add(elapsed_ns(mailbox_started));
        });
        let result_shard = core
            .layout
            .result_shard(tx)
            .ok_or_else(|| Error::InvalidBatch(format!("tx {} has no result shard", tx.tx_id)))?;
        spawn_scc_worker(SccWorker {
            core: core.clone(),
            batch_id: batch.batch_id,
            tx_index,
            tx: tx.clone(),
            tx_plan: plan.tx_plans[tx_index].clone(),
            participants,
            result_shard,
            local_read_keys,
            local_write_keys,
            local_write_keys_by_tx: local_write_keys_by_tx.clone(),
            base_read_cache: base_read_cache.clone(),
            commit_seq: commit_seq.clone(),
            waiters: waiters_by_tx[tx_index].take().ok_or_else(|| {
                Error::InvalidBatch(format!("missing SCC waiters for tx index {tx_index}"))
            })?,
            effect_rx,
            condition_rx,
            profile: profile_enabled.then(SccWorkerProfile::default),
            outcome_tx: outcome_tx.clone(),
        });
    }
    drop(outcome_tx);

    let dag_setup_started = Instant::now();
    dag_runtime.start()?;
    for tx_index in non_participant_indices {
        commit_seq.set_terminal_once(tx_index, CommitSlotState::NoOp)?;
        dag_runtime.finish_vertex(tx_index)?;
    }
    add_timing(&mut profile, |timings| {
        timings.dag_setup_ns = timings
            .dag_setup_ns
            .saturating_add(elapsed_ns(dag_setup_started));
    });

    let mut speculative_records = BTreeMap::new();
    for _ in 0..relevant_count {
        let outcome = outcome_rx.recv().await.ok_or_else(|| {
            Error::ChannelClosed(
                "SCC worker outcome channel closed before batch finished".to_string(),
            )
        })?;
        let completion = outcome.result?;
        dag_runtime.finish_vertex(outcome.tx_index)?;
        merge_scc_worker_profile(&mut profile, completion.profile);
        if let Some(record) = completion.record {
            speculative_records.insert(outcome.tx_index, record);
        }
    }
    let snapshot = commit_seq.terminal_snapshot();
    let failed_indices = failed_indices_from_snapshot(&snapshot);

    let record_started = Instant::now();
    record_scc_reorder(
        core.clone(),
        batch.batch_id,
        batch.txs.len(),
        &failed_indices,
    )
    .await?;
    add_timing(&mut profile, |timings| {
        timings.record_reorder_ns = elapsed_ns(record_started);
    });
    if let Some(profile) = &mut profile {
        profile.counters.local_failed_count = usize_to_u64(failed_indices.len());
        profile.counters.global_failed_count = usize_to_u64(failed_indices.len());
    }

    let mut tx_results = Vec::new();
    for (tx_index, record) in speculative_records {
        if failed_indices.contains(&tx_index) {
            continue;
        }
        tx_results.push(record);
    }

    let install_started = Instant::now();
    install_scc_successes(core.clone(), &snapshot, &failed_indices)?;
    add_timing(&mut profile, |timings| {
        timings.install_successes_ns = elapsed_ns(install_started);
    });

    if !failed_indices.is_empty() {
        let fallback_batch = fallback_batch_from_failed_indices(&batch, &failed_indices);
        add_counter(&mut profile, |counters| {
            counters.fallback_tx_count = usize_to_u64(fallback_batch.txs.len());
        });
        let fallback_started = Instant::now();
        let fallback_execution = execute_calvin_batch(
            core.clone(),
            fallback_batch,
            false,
            SchedulerProfileScheduler::CalvinLocking,
        )
        .await?;
        add_timing(&mut profile, |timings| {
            timings.fallback_ns = elapsed_ns(fallback_started);
        });
        tx_results.extend(fallback_execution.summary.tx_results);
    }

    tx_results.sort_by_key(|record| (record.tx_id, record.shard_id));
    if let Some(profile) = &mut profile {
        profile.counters.speculative_success_count =
            usize_to_u64(batch.txs.len().saturating_sub(failed_indices.len()));
        profile.counters.result_records_produced = usize_to_u64(tx_results.len());
    }
    Ok(ProfiledBatchExecution {
        summary: BatchExecutionSummary {
            batch_id: batch.batch_id,
            shard_id: core.shard_id,
            tx_results,
        },
        profile,
    })
}

async fn execute_aria_batch(
    core: Arc<ShardCore>,
    batch: Batch,
    profile_enabled: bool,
) -> Result<ProfiledBatchExecution> {
    let mut profile = profile_enabled
        .then(|| new_scheduler_profile(SchedulerProfileScheduler::Aria, &batch, &core));

    let validate_started = Instant::now();
    validate_batch_order(&batch)?;
    add_timing(&mut profile, |timings| {
        timings.validate_ns = timings
            .validate_ns
            .saturating_add(elapsed_ns(validate_started));
    });

    for tx in &batch.txs {
        let result_shard = aria_result_shard(tx.tx_id, core.layout.shard_count);
        let registry_started = Instant::now();
        if result_shard == core.shard_id {
            core.client_results.ensure_pending(tx.tx_id).await;
        } else {
            core.client_results.mark_not_responsible(tx.tx_id).await;
        }
        add_timing(&mut profile, |timings| {
            timings.result_registry_ns = timings
                .result_registry_ns
                .saturating_add(elapsed_ns(registry_started));
        });
    }

    let (worker_tx, mut worker_rx) = mpsc::channel(batch.txs.len().max(1));
    let mut coordinator_count = 0usize;
    for (tx_index, tx) in batch.txs.iter().enumerate() {
        let result_shard = aria_result_shard(tx.tx_id, core.layout.shard_count);
        if result_shard != core.shard_id {
            continue;
        }
        coordinator_count += 1;
        add_counter(&mut profile, |counters| {
            counters.relevant_tx_count = counters.relevant_tx_count.saturating_add(1);
            counters.active_tx_count = counters.active_tx_count.saturating_add(1);
        });
        let worker_tx = worker_tx.clone();
        let core = core.clone();
        let tx = tx.clone();
        tokio::spawn(async move {
            let result = run_aria_coordinator(core, batch.batch_id, tx_index, tx).await;
            let _ = worker_tx.send(result).await;
        });
    }
    drop(worker_tx);

    for _ in 0..coordinator_count {
        let result = worker_rx.recv().await.ok_or_else(|| {
            Error::ChannelClosed(
                "Aria coordinator channel closed before batch finished".to_string(),
            )
        })?;
        result?;
    }

    let publish_started = Instant::now();
    broadcast_aria_execution_done(core.clone(), batch.batch_id).await?;
    add_timing(&mut profile, |timings| {
        timings.completion_publish_ns = timings
            .completion_publish_ns
            .saturating_add(elapsed_ns(publish_started));
    });
    add_counter(&mut profile, |counters| {
        counters.completion_reports_sent = counters.completion_reports_sent.saturating_add(1);
    });

    let collect_started = Instant::now();
    core.aria_batches
        .wait_execution_done(batch.batch_id, core.layout.shard_count as usize)
        .await?;
    add_timing(&mut profile, |timings| {
        timings.completion_collect_ns = timings
            .completion_collect_ns
            .saturating_add(elapsed_ns(collect_started));
    });
    add_counter(&mut profile, |counters| {
        counters.completion_reports_received = core.layout.shard_count;
    });

    let local_failed_indices = core.aria_batches.local_failed_indices(batch.batch_id).await;
    if let Some(profile) = &mut profile {
        profile.counters.local_failed_count = usize_to_u64(local_failed_indices.len());
    }
    let publish_started = Instant::now();
    broadcast_aria_local_failures(core.clone(), batch.batch_id, &local_failed_indices).await?;
    add_timing(&mut profile, |timings| {
        timings.completion_publish_ns = timings
            .completion_publish_ns
            .saturating_add(elapsed_ns(publish_started));
    });

    let collect_started = Instant::now();
    let global_failed_indices = core
        .aria_batches
        .wait_failure_reports(batch.batch_id, core.layout.shard_count as usize)
        .await?;
    add_timing(&mut profile, |timings| {
        timings.completion_collect_ns = timings
            .completion_collect_ns
            .saturating_add(elapsed_ns(collect_started));
    });
    if let Some(profile) = &mut profile {
        profile.counters.global_failed_count = usize_to_u64(global_failed_indices.len());
    }

    let record_started = Instant::now();
    record_batch_reorder(
        core.clone(),
        batch.batch_id,
        batch.txs.len(),
        &global_failed_indices,
    )
    .await?;
    add_timing(&mut profile, |timings| {
        timings.record_reorder_ns = elapsed_ns(record_started);
    });

    let install_started = Instant::now();
    let mut tx_results =
        install_aria_successes(core.clone(), batch.batch_id, &global_failed_indices).await?;
    add_timing(&mut profile, |timings| {
        timings.install_successes_ns = elapsed_ns(install_started);
    });

    if !global_failed_indices.is_empty() {
        let fallback_batch =
            fallback_batch_from_failed_indices_with_derived_sets(&batch, &global_failed_indices)?;
        add_counter(&mut profile, |counters| {
            counters.fallback_tx_count = usize_to_u64(fallback_batch.txs.len());
        });
        let fallback_started = Instant::now();
        let fallback_execution =
            execute_aria_fallback_batch(core.clone(), fallback_batch, profile_enabled).await?;
        add_timing(&mut profile, |timings| {
            timings.fallback_ns = elapsed_ns(fallback_started);
        });
        tx_results.extend(fallback_execution.summary.tx_results);
    }

    tx_results.sort_by_key(|record| (record.tx_id, record.shard_id));
    if let Some(profile) = &mut profile {
        profile.counters.speculative_success_count =
            usize_to_u64(batch.txs.len().saturating_sub(global_failed_indices.len()));
        profile.counters.result_records_produced = usize_to_u64(tx_results.len());
    }

    Ok(ProfiledBatchExecution {
        summary: BatchExecutionSummary {
            batch_id: batch.batch_id,
            shard_id: core.shard_id,
            tx_results,
        },
        profile,
    })
}

async fn run_aria_coordinator(
    core: Arc<ShardCore>,
    batch_id: BatchId,
    tx_index: usize,
    tx: OrderedTx,
) -> Result<()> {
    let read_keys = aria_read_keys_for_tx(&tx)?;
    let mut full_reads = BTreeMap::new();
    for key in read_keys {
        let owner = core.layout.shard_for_key(&key);
        let value = if owner == core.shard_id {
            core.aria_batches
                .read_snapshot(
                    core.clone(),
                    batch_id,
                    tx_index,
                    tx.tx_id,
                    core.shard_id,
                    key.clone(),
                )
                .await?
        } else {
            aria_read_snapshot_remote(
                core.clone(),
                batch_id,
                tx_index,
                tx.tx_id,
                owner,
                key.clone(),
            )
            .await?
        };
        if full_reads.insert(key.clone(), value).is_some() {
            return Err(Error::InvalidBatch(format!(
                "duplicate Aria full read key {} for tx {}",
                key, tx.tx_id
            )));
        }
    }

    let output = execute_deterministic(&tx, &full_reads);
    let result = output.result;
    let result_shard = aria_result_shard(tx.tx_id, core.layout.shard_count);
    let mut writes_by_shard: BTreeMap<ShardId, Vec<crate::model::WriteOp>> = BTreeMap::new();
    for write in output.writes {
        let owner = core.layout.shard_for_key(write.key());
        writes_by_shard.entry(owner).or_default().push(write);
    }
    let mut targets: BTreeSet<ShardId> = writes_by_shard.keys().copied().collect();
    targets.insert(result_shard);

    for target in targets {
        let writes = writes_by_shard.remove(&target).unwrap_or_default();
        let outcome = AriaStageOutcome {
            batch_id,
            tx_index,
            tx_id: tx.tx_id,
            from_shard: core.shard_id,
            result,
            writes,
            is_result_shard: target == result_shard,
        };
        if target == core.shard_id {
            core.aria_batches
                .stage_outcome(core.clone(), outcome)
                .await?;
        } else {
            send_aria_stage_outcome(core.clone(), target, outcome).await?;
        }
    }

    Ok(())
}

fn validate_batch_order(batch: &Batch) -> Result<()> {
    let mut seen = BTreeSet::new();
    for (index, tx) in batch.txs.iter().enumerate() {
        if tx.batch_index as usize != index {
            return Err(Error::InvalidBatch(format!(
                "tx {} has batch_index {}, expected {}",
                tx.tx_id, tx.batch_index, index
            )));
        }
        if !seen.insert(tx.tx_id) {
            return Err(Error::InvalidBatch(format!(
                "duplicate tx_id {} in batch {}",
                tx.tx_id, batch.batch_id
            )));
        }
    }
    Ok(())
}

fn aria_result_shard(tx_id: TxId, shard_count: ShardId) -> ShardId {
    tx_id % shard_count
}

fn aria_read_keys_for_tx(tx: &OrderedTx) -> Result<BTreeSet<Key>> {
    derive_read_write_set(&tx.op).map(|(read_set, _)| read_set)
}

fn tx_with_derived_sets(tx: &OrderedTx) -> Result<OrderedTx> {
    let (read_set, write_set) = derive_read_write_set(&tx.op)?;
    let mut tx = tx.clone();
    tx.read_set = read_set;
    tx.write_set = write_set;
    Ok(tx)
}

async fn aria_read_snapshot_remote(
    core: Arc<ShardCore>,
    batch_id: BatchId,
    tx_index: usize,
    tx_id: TxId,
    target: ShardId,
    key: Key,
) -> Result<ReadValue> {
    let endpoint = core
        .peer_endpoints
        .get(&target)
        .ok_or(Error::MissingPeer(target))?
        .clone();
    let mut client = pb::shard_client::ShardClient::connect(endpoint).await?;
    let response = client
        .aria_read_snapshot(pb::AriaReadSnapshotRequest {
            batch_id,
            tx_index: tx_index as u32,
            tx_id,
            from_shard: core.shard_id,
            key: String::from(&key),
        })
        .await?
        .into_inner();
    let (returned_key, value) = crate::convert::read_entry_from_proto(response.read)?;
    if returned_key != key {
        return Err(Error::InvalidBatch(format!(
            "Aria read for tx {} requested key {}, got {}",
            tx_id, key, returned_key
        )));
    }
    Ok(value)
}

async fn send_aria_stage_outcome(
    core: Arc<ShardCore>,
    target: ShardId,
    outcome: AriaStageOutcome,
) -> Result<()> {
    let endpoint = core
        .peer_endpoints
        .get(&target)
        .ok_or(Error::MissingPeer(target))?
        .clone();
    let mut client = pb::shard_client::ShardClient::connect(endpoint).await?;
    client
        .aria_stage_outcome(pb::AriaStageOutcomeRequest {
            batch_id: outcome.batch_id,
            tx_index: outcome.tx_index as u32,
            tx_id: outcome.tx_id,
            from_shard: outcome.from_shard,
            result: tx_result_to_i32(outcome.result),
            writes: write_entries_to_proto(&outcome.writes),
            is_result_shard: outcome.is_result_shard,
        })
        .await?;
    Ok(())
}

async fn broadcast_aria_execution_done(core: Arc<ShardCore>, batch_id: BatchId) -> Result<()> {
    for target in 0..core.layout.shard_count {
        if target == core.shard_id {
            core.aria_batches
                .record_execution_done(batch_id, core.shard_id, core.layout.shard_count)
                .await?;
            continue;
        }
        let endpoint = core
            .peer_endpoints
            .get(&target)
            .ok_or(Error::MissingPeer(target))?
            .clone();
        let mut client = pb::shard_client::ShardClient::connect(endpoint).await?;
        client
            .report_aria_execution_done(pb::AriaExecutionDoneRequest {
                batch_id,
                from_shard: core.shard_id,
            })
            .await?;
    }
    Ok(())
}

async fn broadcast_aria_local_failures(
    core: Arc<ShardCore>,
    batch_id: BatchId,
    failed_indices: &BTreeSet<usize>,
) -> Result<()> {
    let failed_indices: Vec<u32> = failed_indices.iter().map(|index| *index as u32).collect();
    for target in 0..core.layout.shard_count {
        if target == core.shard_id {
            core.aria_batches
                .record_local_failures(
                    batch_id,
                    core.shard_id,
                    failed_indices.iter().map(|index| *index as usize).collect(),
                    core.layout.shard_count,
                )
                .await?;
            continue;
        }
        let endpoint = core
            .peer_endpoints
            .get(&target)
            .ok_or(Error::MissingPeer(target))?
            .clone();
        let mut client = pb::shard_client::ShardClient::connect(endpoint).await?;
        client
            .report_aria_local_failures(pb::AriaLocalFailuresRequest {
                batch_id,
                from_shard: core.shard_id,
                failed_indices: failed_indices.clone(),
            })
            .await?;
    }
    Ok(())
}

async fn install_aria_successes(
    core: Arc<ShardCore>,
    batch_id: BatchId,
    failed_indices: &BTreeSet<usize>,
) -> Result<Vec<TxResultRecord>> {
    let mut outcomes = core.aria_batches.staged_outcomes(batch_id).await;
    outcomes.sort_by_key(|outcome| outcome.tx_index);
    let mut records = Vec::new();
    for outcome in outcomes {
        if failed_indices.contains(&outcome.tx_index) {
            continue;
        }
        core.store.apply_writes_atomically(&outcome.local_writes)?;
        if outcome.is_result_shard {
            let result = outcome.result.ok_or_else(|| {
                Error::InvalidBatch(format!(
                    "Aria result shard outcome for tx {} has no result",
                    outcome.tx_id
                ))
            })?;
            core.client_results.mark_ready(outcome.tx_id, result).await;
            records.push(TxResultRecord {
                tx_id: outcome.tx_id,
                shard_id: core.shard_id,
                result,
            });
        }
    }
    Ok(records)
}

async fn execute_aria_fallback_batch(
    core: Arc<ShardCore>,
    batch: Batch,
    _profile_enabled: bool,
) -> Result<ProfiledBatchExecution> {
    validate_batch_order(&batch)?;
    let mut lock_table = LockTable::new();
    let (outcome_tx, mut outcome_rx) = mpsc::channel(batch.txs.len().max(1));
    let mut relevant_count = 0usize;

    for tx in &batch.txs {
        let tx = tx_with_derived_sets(tx)?;
        let local_lock_keys = core.layout.local_lock_keys(&tx, core.shard_id);
        let local_read_keys = core.layout.local_read_keys(&tx, core.shard_id);
        let local_write_keys = core.layout.local_write_keys(&tx, core.shard_id);
        let result_shard = aria_result_shard(tx.tx_id, core.layout.shard_count);
        let active = aria_fallback_active_shards(&core.layout, &tx);
        let read_sources = owner_shards_for_keys(&core.layout, &tx.read_set);

        if local_lock_keys.is_empty()
            && !active.contains(&core.shard_id)
            && !read_sources.contains(&core.shard_id)
        {
            continue;
        }

        relevant_count += 1;
        let (grant_tx, grant_rx) = mpsc::channel(local_lock_keys.len().max(1));
        if !local_lock_keys.is_empty() {
            lock_table.enqueue(tx.tx_id, &local_lock_keys, grant_tx);
        }
        let remote_rx = if active.contains(&core.shard_id) {
            Some(
                core.mailboxes
                    .receiver(
                        batch.batch_id,
                        tx.tx_id,
                        ReadPhase::AriaFallback,
                        read_sources.len().max(1),
                    )
                    .await?,
            )
        } else {
            None
        };
        spawn_aria_fallback_worker(AriaFallbackWorker {
            core: core.clone(),
            batch_id: batch.batch_id,
            tx,
            result_shard,
            active,
            read_sources,
            local_lock_keys,
            local_read_keys,
            local_write_keys,
            grant_rx,
            remote_rx,
            outcome_tx: outcome_tx.clone(),
        });
    }
    drop(outcome_tx);

    lock_table.grant_initial_heads().await?;

    let mut tx_results = Vec::new();
    let mut active_results: BTreeMap<TxId, TxResult> = BTreeMap::new();
    for _ in 0..relevant_count {
        let outcome = outcome_rx.recv().await.ok_or_else(|| {
            Error::ChannelClosed("Aria fallback outcome channel closed".to_string())
        })?;
        lock_table
            .release_and_grant_next(outcome.tx_id, &outcome.local_lock_keys)
            .await?;
        match outcome.result? {
            AriaFallbackCompletion::Active { result, record } => {
                if let Some(previous) = active_results.insert(outcome.tx_id, result) {
                    if previous != result {
                        return Err(Error::InvalidBatch(format!(
                            "Aria fallback active executors disagree for tx {}: {:?} vs {:?}",
                            outcome.tx_id, previous, result
                        )));
                    }
                }
                if let Some(record) = record {
                    tx_results.push(record);
                }
            }
            AriaFallbackCompletion::Passive => {}
        }
    }
    tx_results.sort_by_key(|record| (record.tx_id, record.shard_id));
    Ok(ProfiledBatchExecution {
        summary: BatchExecutionSummary {
            batch_id: batch.batch_id,
            shard_id: core.shard_id,
            tx_results,
        },
        profile: None,
    })
}

fn aria_fallback_active_shards(layout: &ShardLayout, tx: &OrderedTx) -> BTreeSet<ShardId> {
    let mut active = owner_shards_for_keys(layout, &tx.write_set);
    active.insert(aria_result_shard(tx.tx_id, layout.shard_count));
    active
}

fn owner_shards_for_keys(layout: &ShardLayout, keys: &BTreeSet<Key>) -> BTreeSet<ShardId> {
    keys.iter().map(|key| layout.shard_for_key(key)).collect()
}

fn fallback_batch_from_failed_indices_with_derived_sets(
    batch: &Batch,
    failed_indices: &BTreeSet<usize>,
) -> Result<Batch> {
    let mut txs = Vec::with_capacity(failed_indices.len());
    for (fallback_index, original_index) in failed_indices.iter().enumerate() {
        let mut tx = tx_with_derived_sets(&batch.txs[*original_index])?;
        tx.batch_index = fallback_index as u32;
        txs.push(tx);
    }
    Ok(Batch {
        batch_id: batch.batch_id,
        txs,
    })
}

struct AriaFallbackWorker {
    core: Arc<ShardCore>,
    batch_id: BatchId,
    tx: OrderedTx,
    result_shard: ShardId,
    active: BTreeSet<ShardId>,
    read_sources: BTreeSet<ShardId>,
    local_lock_keys: BTreeSet<Key>,
    local_read_keys: BTreeSet<Key>,
    local_write_keys: BTreeSet<Key>,
    grant_rx: mpsc::Receiver<LockGrant>,
    remote_rx: Option<mpsc::Receiver<LocalReadResult>>,
    outcome_tx: mpsc::Sender<AriaFallbackOutcome>,
}

struct AriaFallbackOutcome {
    tx_id: TxId,
    local_lock_keys: BTreeSet<Key>,
    result: Result<AriaFallbackCompletion>,
}

enum AriaFallbackCompletion {
    Active {
        result: TxResult,
        record: Option<TxResultRecord>,
    },
    Passive,
}

fn spawn_aria_fallback_worker(worker: AriaFallbackWorker) {
    tokio::spawn(async move {
        let tx_id = worker.tx.tx_id;
        let local_lock_keys = worker.local_lock_keys.clone();
        let outcome_tx = worker.outcome_tx.clone();
        let result = worker.run().await;
        let _ = outcome_tx
            .send(AriaFallbackOutcome {
                tx_id,
                local_lock_keys,
                result,
            })
            .await;
    });
}

impl AriaFallbackWorker {
    async fn run(mut self) -> Result<AriaFallbackCompletion> {
        self.wait_for_lock_grants().await?;
        let local_reads = self.core.store.read_many(&self.local_read_keys)?;
        if self.read_sources.contains(&self.core.shard_id) {
            self.send_local_reads(&local_reads).await?;
        }
        if !self.active.contains(&self.core.shard_id) {
            return Ok(AriaFallbackCompletion::Passive);
        }
        let full_reads = self.collect_full_reads(local_reads).await?;
        let output = execute_deterministic(&self.tx, &full_reads);
        let result = output.result;
        let local_writes =
            filter_local_writes(output.writes, self.core.shard_id, &self.core.layout);
        self.validate_local_writes(&local_writes)?;
        self.core.store.apply_writes_atomically(&local_writes)?;
        let record = if self.result_shard == self.core.shard_id {
            self.core
                .client_results
                .mark_ready(self.tx.tx_id, result)
                .await;
            Some(TxResultRecord {
                tx_id: self.tx.tx_id,
                shard_id: self.core.shard_id,
                result,
            })
        } else {
            None
        };
        Ok(AriaFallbackCompletion::Active { result, record })
    }

    async fn wait_for_lock_grants(&mut self) -> Result<()> {
        let mut granted = BTreeSet::new();
        while granted.len() < self.local_lock_keys.len() {
            let grant = self.grant_rx.recv().await.ok_or_else(|| {
                Error::ChannelClosed(format!(
                    "Aria fallback lock grant receiver closed for tx {}",
                    self.tx.tx_id
                ))
            })?;
            if !self.local_lock_keys.contains(&grant.key) {
                return Err(Error::LockInvariant(format!(
                    "Aria fallback tx {} got unexpected lock grant for key {}",
                    self.tx.tx_id, grant.key
                )));
            }
            if !granted.insert(grant.key.clone()) {
                return Err(Error::LockInvariant(format!(
                    "Aria fallback tx {} got duplicate lock grant for key {}",
                    self.tx.tx_id, grant.key
                )));
            }
        }
        Ok(())
    }

    async fn send_local_reads(&self, local_reads: &BTreeMap<Key, ReadValue>) -> Result<usize> {
        let mut sent = 0usize;
        for target in &self.active {
            if *target == self.core.shard_id {
                continue;
            }
            let endpoint = self
                .core
                .peer_endpoints
                .get(target)
                .ok_or(Error::MissingPeer(*target))?
                .clone();
            let request = pb::LocalReadResultRequest {
                batch_id: self.batch_id,
                tx_id: self.tx.tx_id,
                from_shard: self.core.shard_id,
                reads: read_entries_to_proto(local_reads),
                phase: read_phase_to_i32(ReadPhase::AriaFallback),
                status: local_read_status_to_i32(LocalReadStatus::Ok),
            };
            let mut client = pb::shard_client::ShardClient::connect(endpoint).await?;
            client.local_read_result(request).await?;
            sent += 1;
        }
        Ok(sent)
    }

    async fn collect_full_reads(
        &mut self,
        local_reads: BTreeMap<Key, ReadValue>,
    ) -> Result<BTreeMap<Key, ReadValue>> {
        let mut full_reads = local_reads;
        let expected_remote: BTreeSet<ShardId> = self
            .read_sources
            .iter()
            .copied()
            .filter(|shard| *shard != self.core.shard_id)
            .collect();
        let remote_rx = self.remote_rx.as_mut().ok_or_else(|| {
            Error::InvalidBatch(format!(
                "Aria fallback active tx {} has no remote read mailbox",
                self.tx.tx_id
            ))
        })?;
        let mut received = BTreeSet::new();
        while received.len() < expected_remote.len() {
            let msg = remote_rx.recv().await.ok_or_else(|| {
                Error::ChannelClosed(format!(
                    "Aria fallback remote read mailbox closed for tx {}",
                    self.tx.tx_id
                ))
            })?;
            if msg.batch_id != self.batch_id
                || msg.tx_id != self.tx.tx_id
                || msg.phase != ReadPhase::AriaFallback
            {
                return Err(Error::InvalidBatch(format!(
                    "Aria fallback read result routed to wrong target for tx {}",
                    self.tx.tx_id
                )));
            }
            if !expected_remote.contains(&msg.from_shard) {
                return Err(Error::InvalidBatch(format!(
                    "unexpected Aria fallback read result for tx {} from shard {}",
                    self.tx.tx_id, msg.from_shard
                )));
            }
            if !received.insert(msg.from_shard) {
                return Err(Error::InvalidBatch(format!(
                    "duplicate Aria fallback read result for tx {} from shard {}",
                    self.tx.tx_id, msg.from_shard
                )));
            }
            if msg.status != LocalReadStatus::Ok {
                return Err(Error::InvalidBatch(format!(
                    "Aria fallback read result for tx {} from shard {} has status {:?}",
                    self.tx.tx_id, msg.from_shard, msg.status
                )));
            }
            let expected_keys = self.core.layout.local_read_keys(&self.tx, msg.from_shard);
            let actual_keys: BTreeSet<Key> = msg.reads.keys().cloned().collect();
            if actual_keys != expected_keys {
                return Err(Error::InvalidBatch(format!(
                    "Aria fallback tx {} read keys from shard {} mismatch: expected {:?}, got {:?}",
                    self.tx.tx_id, msg.from_shard, expected_keys, actual_keys
                )));
            }
            for (key, value) in msg.reads {
                if full_reads.insert(key.clone(), value).is_some() {
                    return Err(Error::InvalidBatch(format!(
                        "duplicate Aria fallback full read key {} for tx {}",
                        key, self.tx.tx_id
                    )));
                }
            }
        }
        let full_keys: BTreeSet<Key> = full_reads.keys().cloned().collect();
        if full_keys != self.tx.read_set {
            return Err(Error::InvalidBatch(format!(
                "Aria fallback tx {} full read set mismatch: expected {:?}, got {:?}",
                self.tx.tx_id, self.tx.read_set, full_keys
            )));
        }
        Ok(full_reads)
    }

    fn validate_local_writes(&self, writes: &[crate::model::WriteOp]) -> Result<()> {
        for write in writes {
            let key = write.key();
            if !self.local_write_keys.contains(key) {
                return Err(Error::InvalidBatch(format!(
                    "Aria fallback tx {} tried to write unexpected key {} on shard {}",
                    self.tx.tx_id, key, self.core.shard_id
                )));
            }
        }
        Ok(())
    }
}

fn new_scheduler_profile(
    scheduler: SchedulerProfileScheduler,
    batch: &Batch,
    core: &ShardCore,
) -> SchedulerProfileRecord {
    SchedulerProfileRecord {
        scheduler,
        batch_id: batch.batch_id,
        shard_id: core.shard_id,
        counters: SchedulerProfileCounters {
            tx_count: usize_to_u64(batch.txs.len()),
            ..SchedulerProfileCounters::default()
        },
        timings: SchedulerProfileTimings::default(),
    }
}

fn add_counter(
    profile: &mut Option<SchedulerProfileRecord>,
    update: impl FnOnce(&mut SchedulerProfileCounters),
) {
    if let Some(profile) = profile {
        update(&mut profile.counters);
    }
}

fn add_timing(
    profile: &mut Option<SchedulerProfileRecord>,
    update: impl FnOnce(&mut SchedulerProfileTimings),
) {
    if let Some(profile) = profile {
        update(&mut profile.timings);
    }
}

fn add_participant_counters(
    profile: &mut Option<SchedulerProfileRecord>,
    participants: &Participants,
    shard_id: ShardId,
) {
    add_counter(profile, |counters| {
        if participants.active.contains(&shard_id) {
            counters.active_tx_count = counters.active_tx_count.saturating_add(1);
        } else {
            counters.passive_tx_count = counters.passive_tx_count.saturating_add(1);
        }
    });
}

fn merge_calvin_worker_profile(
    profile: &mut Option<SchedulerProfileRecord>,
    worker_profile: Option<CalvinWorkerProfile>,
) {
    let Some(worker_profile) = worker_profile else {
        return;
    };
    let Some(profile) = profile else {
        return;
    };
    profile.timings.lock_wait_sum_ns = profile
        .timings
        .lock_wait_sum_ns
        .saturating_add(worker_profile.lock_wait_ns);
    profile.timings.lock_wait_max_ns = profile
        .timings
        .lock_wait_max_ns
        .max(worker_profile.lock_wait_ns);
    profile.timings.local_read_ns = profile
        .timings
        .local_read_ns
        .saturating_add(worker_profile.local_read_ns);
    profile.timings.remote_read_send_ns = profile
        .timings
        .remote_read_send_ns
        .saturating_add(worker_profile.remote_read_send_ns);
    profile.timings.remote_read_collect_ns = profile
        .timings
        .remote_read_collect_ns
        .saturating_add(worker_profile.remote_read_collect_ns);
    profile.timings.execute_apply_ns = profile
        .timings
        .execute_apply_ns
        .saturating_add(worker_profile.execute_apply_ns);
    profile.timings.result_mark_ns = profile
        .timings
        .result_mark_ns
        .saturating_add(worker_profile.result_mark_ns);
    profile.counters.remote_read_messages_sent = profile
        .counters
        .remote_read_messages_sent
        .saturating_add(worker_profile.remote_sent);
    profile.counters.remote_read_messages_received = profile
        .counters
        .remote_read_messages_received
        .saturating_add(worker_profile.remote_received);
}

fn merge_scc_worker_profile(
    profile: &mut Option<SchedulerProfileRecord>,
    worker_profile: Option<SccWorkerProfile>,
) {
    let Some(worker_profile) = worker_profile else {
        return;
    };
    let Some(profile) = profile else {
        return;
    };
    add_stage_sample(
        &mut profile.timings.scc_effect_wait,
        worker_profile.effect_wait_ns,
    );
    add_stage_sample(
        &mut profile.timings.scc_effect_materialize,
        worker_profile.effect_materialize_ns,
    );
    add_stage_sample(
        &mut profile.timings.scc_effect_send,
        worker_profile.effect_send_ns,
    );
    add_stage_sample(
        &mut profile.timings.scc_effect_collect,
        worker_profile.effect_collect_ns,
    );
    add_stage_sample(&mut profile.timings.scc_execute, worker_profile.execute_ns);
    add_stage_sample(
        &mut profile.timings.scc_delta_build,
        worker_profile.delta_build_ns,
    );
    add_stage_sample(
        &mut profile.timings.scc_condition_wait,
        worker_profile.condition_wait_ns,
    );
    add_stage_sample(
        &mut profile.timings.scc_condition_materialize,
        worker_profile.condition_materialize_ns,
    );
    add_stage_sample(
        &mut profile.timings.scc_condition_send,
        worker_profile.condition_send_ns,
    );
    add_stage_sample(
        &mut profile.timings.scc_condition_collect,
        worker_profile.condition_collect_ns,
    );
    add_stage_sample(
        &mut profile.timings.scc_condition_check,
        worker_profile.condition_check_ns,
    );
    add_stage_sample(&mut profile.timings.scc_commit, worker_profile.commit_ns);
    if worker_profile.condition_skipped {
        profile.counters.condition_skipped_count =
            profile.counters.condition_skipped_count.saturating_add(1);
    }
    profile.counters.delta_op_count = profile
        .counters
        .delta_op_count
        .saturating_add(worker_profile.delta_op_count);
    profile.counters.remote_read_messages_sent = profile
        .counters
        .remote_read_messages_sent
        .saturating_add(worker_profile.remote_sent);
    profile.counters.remote_read_messages_received = profile
        .counters
        .remote_read_messages_received
        .saturating_add(worker_profile.remote_received);
}

fn add_stage_sample(stats: &mut WorkerStageStats, ns: u64) {
    if ns > 0 {
        stats.add_sample(ns);
    }
}

fn dag_edge_count(dag: &SemanticDag) -> u64 {
    dag.nodes
        .iter()
        .map(|node| usize_to_u64(node.successors.len()))
        .fold(0u64, u64::saturating_add)
}

fn plan_pair_count(tx_count: usize) -> u64 {
    let tx_count = usize_to_u64(tx_count);
    tx_count.saturating_mul(tx_count.saturating_sub(1)) / 2
}

fn elapsed_ns(started: Instant) -> u64 {
    let nanos = started.elapsed().as_nanos();
    (nanos.min(u128::from(u64::MAX)) as u64).max(1)
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

struct TxWorker {
    core: Arc<ShardCore>,
    batch_id: BatchId,
    tx: OrderedTx,
    participants: Participants,
    result_shard: ShardId,
    local_keys: BTreeSet<Key>,
    grant_rx: mpsc::Receiver<LockGrant>,
    remote_rx: Option<mpsc::Receiver<LocalReadResult>>,
    profile_enabled: bool,
    outcome_tx: mpsc::Sender<WorkerOutcome>,
}

struct WorkerOutcome {
    tx_id: TxId,
    local_keys: BTreeSet<Key>,
    result: Result<WorkerCompletion>,
}

enum WorkerCompletion {
    Active(TxResultRecord, Option<CalvinWorkerProfile>),
    Passive(Option<CalvinWorkerProfile>),
}

fn spawn_tx_worker(worker: TxWorker) {
    tokio::spawn(async move {
        let tx_id = worker.tx.tx_id;
        let local_keys = worker.local_keys.clone();
        let outcome_tx = worker.outcome_tx.clone();
        let result = worker.run().await;
        let _ = outcome_tx
            .send(WorkerOutcome {
                tx_id,
                local_keys,
                result,
            })
            .await;
    });
}

impl TxWorker {
    async fn run(mut self) -> Result<WorkerCompletion> {
        let mut profile = self.profile_enabled.then(CalvinWorkerProfile::default);
        let started = Instant::now();
        self.wait_for_lock_grants().await?;
        if let Some(profile) = &mut profile {
            profile.lock_wait_ns = elapsed_ns(started);
        }

        let local_read_keys = self
            .core
            .layout
            .local_read_keys(&self.tx, self.core.shard_id);
        let started = Instant::now();
        let local_reads = self.core.store.read_many(&local_read_keys)?;
        if let Some(profile) = &mut profile {
            profile.local_read_ns = elapsed_ns(started);
        }

        let started = Instant::now();
        let sent = self.send_local_reads(&local_reads).await?;
        if let Some(profile) = &mut profile {
            profile.remote_read_send_ns = elapsed_ns(started);
            profile.remote_sent = usize_to_u64(sent);
        }

        if !self.participants.active.contains(&self.core.shard_id) {
            return Ok(WorkerCompletion::Passive(profile));
        }

        let started = Instant::now();
        let (full_reads, received) = self.collect_full_reads(local_reads).await?;
        if let Some(profile) = &mut profile {
            profile.remote_read_collect_ns = elapsed_ns(started);
            profile.remote_received = usize_to_u64(received);
        }

        let started = Instant::now();
        let output = execute_deterministic(&self.tx, &full_reads);
        let result = output.result;
        let local_writes =
            filter_local_writes(output.writes, self.core.shard_id, &self.core.layout);
        self.validate_local_writes(&local_writes)?;
        self.core.store.apply_writes_atomically(&local_writes)?;
        if let Some(profile) = &mut profile {
            profile.execute_apply_ns = elapsed_ns(started);
        }
        if self.result_shard == self.core.shard_id {
            let started = Instant::now();
            self.core
                .client_results
                .mark_ready(self.tx.tx_id, result)
                .await;
            if let Some(profile) = &mut profile {
                profile.result_mark_ns = elapsed_ns(started);
            }
        }

        Ok(WorkerCompletion::Active(
            TxResultRecord {
                tx_id: self.tx.tx_id,
                shard_id: self.core.shard_id,
                result,
            },
            profile,
        ))
    }

    async fn wait_for_lock_grants(&mut self) -> Result<()> {
        let mut granted = BTreeSet::new();
        while granted.len() < self.local_keys.len() {
            let grant = self.grant_rx.recv().await.ok_or_else(|| {
                Error::ChannelClosed(format!(
                    "lock grant receiver closed for tx {}",
                    self.tx.tx_id
                ))
            })?;
            if !self.local_keys.contains(&grant.key) {
                return Err(Error::LockInvariant(format!(
                    "tx {} got unexpected lock grant for key {}",
                    self.tx.tx_id, grant.key
                )));
            }
            if !granted.insert(grant.key.clone()) {
                return Err(Error::LockInvariant(format!(
                    "tx {} got duplicate lock grant for key {}",
                    self.tx.tx_id, grant.key
                )));
            }
        }
        Ok(())
    }

    async fn send_local_reads(&self, local_reads: &BTreeMap<Key, ReadValue>) -> Result<usize> {
        let mut sent = 0usize;
        for target in &self.participants.active {
            if *target == self.core.shard_id {
                continue;
            }
            let endpoint = self
                .core
                .peer_endpoints
                .get(target)
                .ok_or(Error::MissingPeer(*target))?
                .clone();
            let request = pb::LocalReadResultRequest {
                batch_id: self.batch_id,
                tx_id: self.tx.tx_id,
                from_shard: self.core.shard_id,
                reads: read_entries_to_proto(local_reads),
                phase: read_phase_to_i32(ReadPhase::Calvin),
                status: local_read_status_to_i32(LocalReadStatus::Ok),
            };
            let mut client = pb::shard_client::ShardClient::connect(endpoint).await?;
            client.local_read_result(request).await?;
            sent += 1;
        }
        Ok(sent)
    }

    async fn collect_full_reads(
        &mut self,
        local_reads: BTreeMap<Key, ReadValue>,
    ) -> Result<(BTreeMap<Key, ReadValue>, usize)> {
        let mut full_reads = local_reads;
        let expected_remote: BTreeSet<ShardId> = self
            .participants
            .all
            .iter()
            .copied()
            .filter(|shard| *shard != self.core.shard_id)
            .collect();
        let mut received = BTreeSet::new();
        let remote_rx = self.remote_rx.as_mut().ok_or_else(|| {
            Error::InvalidBatch(format!(
                "active tx {} has no remote read mailbox",
                self.tx.tx_id
            ))
        })?;

        while received.len() < expected_remote.len() {
            let msg = remote_rx.recv().await.ok_or_else(|| {
                Error::ChannelClosed(format!(
                    "remote read mailbox closed for tx {}",
                    self.tx.tx_id
                ))
            })?;
            if msg.batch_id != self.batch_id || msg.tx_id != self.tx.tx_id {
                return Err(Error::InvalidBatch(format!(
                    "remote read result routed to wrong tx: got batch {} tx {}, expected batch {} tx {}",
                    msg.batch_id, msg.tx_id, self.batch_id, self.tx.tx_id
                )));
            }
            if msg.phase != ReadPhase::Calvin {
                return Err(Error::InvalidBatch(format!(
                    "remote read result routed to wrong phase for tx {}: got {:?}",
                    self.tx.tx_id, msg.phase
                )));
            }
            if msg.status != LocalReadStatus::Ok {
                return Err(Error::InvalidBatch(format!(
                    "Calvin read result for tx {} from shard {} has non-ok status {:?}",
                    self.tx.tx_id, msg.from_shard, msg.status
                )));
            }
            if !expected_remote.contains(&msg.from_shard) {
                return Err(Error::InvalidBatch(format!(
                    "unexpected read result for tx {} from shard {}",
                    self.tx.tx_id, msg.from_shard
                )));
            }
            if !received.insert(msg.from_shard) {
                return Err(Error::InvalidBatch(format!(
                    "duplicate read result for tx {} from shard {}",
                    self.tx.tx_id, msg.from_shard
                )));
            }

            let expected_keys = self.core.layout.local_read_keys(&self.tx, msg.from_shard);
            let actual_keys: BTreeSet<Key> = msg.reads.keys().cloned().collect();
            if actual_keys != expected_keys {
                return Err(Error::InvalidBatch(format!(
                    "tx {} read keys from shard {} mismatch: expected {:?}, got {:?}",
                    self.tx.tx_id, msg.from_shard, expected_keys, actual_keys
                )));
            }

            for (key, value) in msg.reads {
                if full_reads.insert(key.clone(), value).is_some() {
                    return Err(Error::InvalidBatch(format!(
                        "duplicate full read key {} for tx {}",
                        key, self.tx.tx_id
                    )));
                }
            }
        }

        let full_keys: BTreeSet<Key> = full_reads.keys().cloned().collect();
        if full_keys != self.tx.read_set {
            return Err(Error::InvalidBatch(format!(
                "tx {} full read set mismatch: expected {:?}, got {:?}",
                self.tx.tx_id, self.tx.read_set, full_keys
            )));
        }

        Ok((full_reads, received.len()))
    }

    fn validate_local_writes(&self, writes: &[crate::model::WriteOp]) -> Result<()> {
        let expected = self
            .core
            .layout
            .local_write_keys(&self.tx, self.core.shard_id);
        for write in writes {
            let key = write.key();
            if !expected.contains(key) {
                return Err(Error::InvalidBatch(format!(
                    "tx {} tried to write unexpected local key {}",
                    self.tx.tx_id, key
                )));
            }
            let owner = self.core.layout.shard_for_key(key);
            if owner != self.core.shard_id {
                return Err(Error::InvalidBatch(format!(
                    "tx {} on shard {} tried to write key {} owned by shard {}",
                    self.tx.tx_id, self.core.shard_id, key, owner
                )));
            }
        }
        Ok(())
    }
}

struct SccWorker {
    core: Arc<ShardCore>,
    batch_id: BatchId,
    tx_index: usize,
    tx: OrderedTx,
    tx_plan: SccTxPlan,
    participants: Participants,
    result_shard: ShardId,
    local_read_keys: BTreeSet<Key>,
    local_write_keys: BTreeSet<Key>,
    local_write_keys_by_tx: Arc<Vec<BTreeSet<Key>>>,
    base_read_cache: Arc<BTreeMap<Key, ReadValue>>,
    commit_seq: Arc<CommitSequence>,
    waiters: TxDagWaiters,
    effect_rx: mpsc::Receiver<LocalReadResult>,
    condition_rx: mpsc::Receiver<LocalReadResult>,
    profile: Option<SccWorkerProfile>,
    outcome_tx: mpsc::Sender<SccWorkerOutcome>,
}

struct SccWorkerOutcome {
    tx_index: usize,
    result: Result<SccWorkerCompletion>,
}

struct SccWorkerCompletion {
    record: Option<TxResultRecord>,
    profile: Option<SccWorkerProfile>,
}

fn spawn_scc_worker(worker: SccWorker) {
    tokio::spawn(async move {
        let tx_index = worker.tx_index;
        let outcome_tx = worker.outcome_tx.clone();
        let result = worker.run().await;
        let _ = outcome_tx.send(SccWorkerOutcome { tx_index, result }).await;
    });
}

impl SccWorker {
    async fn run(mut self) -> Result<SccWorkerCompletion> {
        let started = Instant::now();
        (&mut self.waiters.effect_ready).await.map_err(|_| {
            Error::ChannelClosed(format!(
                "SCC effect DAG ready channel closed for tx {}",
                self.tx.tx_id
            ))
        })?;
        if let Some(profile) = &mut self.profile {
            profile.effect_wait_ns = elapsed_ns(started);
        }

        let started = Instant::now();
        let effect_local_reads = match self.materialized_read(SccPhase::Effect).await {
            Ok(reads) => {
                if let Some(profile) = &mut self.profile {
                    profile.effect_materialize_ns = elapsed_ns(started);
                }
                reads
            }
            Err(Error::SpeculationFailed(reason)) => {
                if let Some(profile) = &mut self.profile {
                    profile.effect_materialize_ns = elapsed_ns(started);
                }
                let started = Instant::now();
                let sent = self
                    .send_scc_status(ReadPhase::SccEffect, LocalReadStatus::SpeculationFailed)
                    .await?;
                if let Some(profile) = &mut self.profile {
                    profile.effect_send_ns = elapsed_ns(started);
                    profile.remote_sent = profile.remote_sent.saturating_add(usize_to_u64(sent));
                }
                return self.fail(reason);
            }
            Err(err) => return Err(err),
        };
        let started = Instant::now();
        let sent = self
            .send_scc_reads(ReadPhase::SccEffect, &effect_local_reads)
            .await?;
        if let Some(profile) = &mut self.profile {
            profile.effect_send_ns = elapsed_ns(started);
            profile.remote_sent = profile.remote_sent.saturating_add(usize_to_u64(sent));
        }
        let started = Instant::now();
        let effect_full_reads = match self
            .collect_scc_full_reads(effect_local_reads, ReadPhase::SccEffect)
            .await
        {
            Ok((reads, received)) => {
                if let Some(profile) = &mut self.profile {
                    profile.effect_collect_ns = elapsed_ns(started);
                    profile.remote_received = profile
                        .remote_received
                        .saturating_add(usize_to_u64(received));
                }
                reads
            }
            Err(Error::SpeculationFailed(reason)) => return self.fail(reason),
            Err(err) => return Err(err),
        };

        let started = Instant::now();
        let output = execute_deterministic(&self.tx, &effect_full_reads);
        if let Some(profile) = &mut self.profile {
            profile.execute_ns = elapsed_ns(started);
        }
        if classify_actual_path(&self.tx, &output) != Some(self.tx_plan.predicted_path) {
            let reason = format!(
                "tx {} actual result {:?} does not match predicted success path {:?}",
                self.tx.tx_id, output.result, self.tx_plan.predicted_path
            );
            return self.fail(reason);
        }
        let started = Instant::now();
        let full_delta = output_to_delta(&self.tx, &effect_full_reads, output)?;
        let local_delta = filter_delta_to_keys(&full_delta, &self.local_write_keys);
        if let Some(profile) = &mut self.profile {
            profile.delta_build_ns = elapsed_ns(started);
            profile.delta_op_count = usize_to_u64(local_delta.ops.len());
        }

        let started = Instant::now();
        (&mut self.waiters.condition_ready).await.map_err(|_| {
            Error::ChannelClosed(format!(
                "SCC condition DAG ready channel closed for tx {}",
                self.tx.tx_id
            ))
        })?;
        if let Some(profile) = &mut self.profile {
            profile.condition_wait_ns = elapsed_ns(started);
        }

        if self.tx_plan.effect_prefix_covers_condition() {
            if let Some(profile) = &mut self.profile {
                profile.condition_skipped = true;
            }
            return self.commit_success(local_delta).await;
        }

        let started = Instant::now();
        let condition_local_reads = match self.materialized_read(SccPhase::Condition).await {
            Ok(reads) => {
                if let Some(profile) = &mut self.profile {
                    profile.condition_materialize_ns = elapsed_ns(started);
                }
                reads
            }
            Err(Error::SpeculationFailed(reason)) => {
                if let Some(profile) = &mut self.profile {
                    profile.condition_materialize_ns = elapsed_ns(started);
                }
                let started = Instant::now();
                let sent = self
                    .send_scc_status(ReadPhase::SccCondition, LocalReadStatus::SpeculationFailed)
                    .await?;
                if let Some(profile) = &mut self.profile {
                    profile.condition_send_ns = elapsed_ns(started);
                    profile.remote_sent = profile.remote_sent.saturating_add(usize_to_u64(sent));
                }
                return self.fail(reason);
            }
            Err(err) => return Err(err),
        };
        let started = Instant::now();
        let sent = self
            .send_scc_reads(ReadPhase::SccCondition, &condition_local_reads)
            .await?;
        if let Some(profile) = &mut self.profile {
            profile.condition_send_ns = elapsed_ns(started);
            profile.remote_sent = profile.remote_sent.saturating_add(usize_to_u64(sent));
        }
        let started = Instant::now();
        let condition_full_reads = match self
            .collect_scc_full_reads(condition_local_reads, ReadPhase::SccCondition)
            .await
        {
            Ok((reads, received)) => {
                if let Some(profile) = &mut self.profile {
                    profile.condition_collect_ns = elapsed_ns(started);
                    profile.remote_received = profile
                        .remote_received
                        .saturating_add(usize_to_u64(received));
                }
                reads
            }
            Err(Error::SpeculationFailed(reason)) => return self.fail(reason),
            Err(err) => return Err(err),
        };

        let started = Instant::now();
        if !check_success_path_condition(
            &self.tx,
            self.tx_plan.predicted_path,
            &condition_full_reads,
        )? {
            if let Some(profile) = &mut self.profile {
                profile.condition_check_ns = elapsed_ns(started);
            }
            let reason = format!("tx {} success path condition failed", self.tx.tx_id);
            return self.fail(reason);
        }
        if let Some(profile) = &mut self.profile {
            profile.condition_check_ns = elapsed_ns(started);
        }

        self.commit_success(local_delta).await
    }

    async fn commit_success(mut self, local_delta: TxDelta) -> Result<SccWorkerCompletion> {
        let started = Instant::now();
        if local_delta.ops.is_empty() {
            self.commit_seq
                .set_terminal_once(self.tx_index, CommitSlotState::NoOp)?;
        } else {
            self.commit_seq
                .set_terminal_once(self.tx_index, CommitSlotState::Delta(Arc::new(local_delta)))?;
        }

        if self.result_shard == self.core.shard_id {
            self.core
                .client_results
                .mark_ready(self.tx.tx_id, TxResult::Ok)
                .await;
        }
        if let Some(profile) = &mut self.profile {
            profile.commit_ns = elapsed_ns(started);
        }

        let record = self
            .participants
            .active
            .contains(&self.core.shard_id)
            .then_some(TxResultRecord {
                tx_id: self.tx.tx_id,
                shard_id: self.core.shard_id,
                result: TxResult::Ok,
            });
        Ok(SccWorkerCompletion {
            record,
            profile: self.profile,
        })
    }

    async fn materialized_read(&self, phase: SccPhase) -> Result<BTreeMap<Key, ReadValue>> {
        materialized_local_read(
            &self.tx_plan,
            phase,
            &self.local_read_keys,
            &self.local_write_keys_by_tx,
            &self.base_read_cache,
            &self.commit_seq,
        )
        .await
    }

    async fn send_scc_reads(
        &self,
        phase: ReadPhase,
        local_reads: &BTreeMap<Key, ReadValue>,
    ) -> Result<usize> {
        self.send_scc_read_result(phase, LocalReadStatus::Ok, local_reads)
            .await
    }

    async fn send_scc_status(&self, phase: ReadPhase, status: LocalReadStatus) -> Result<usize> {
        self.send_scc_read_result(phase, status, &BTreeMap::new())
            .await
    }

    async fn send_scc_read_result(
        &self,
        phase: ReadPhase,
        status: LocalReadStatus,
        local_reads: &BTreeMap<Key, ReadValue>,
    ) -> Result<usize> {
        let mut sent = 0usize;
        for target in &self.participants.all {
            if *target == self.core.shard_id {
                continue;
            }
            let endpoint = self
                .core
                .peer_endpoints
                .get(target)
                .ok_or(Error::MissingPeer(*target))?
                .clone();
            let request = pb::LocalReadResultRequest {
                batch_id: self.batch_id,
                tx_id: self.tx.tx_id,
                from_shard: self.core.shard_id,
                reads: if status == LocalReadStatus::Ok {
                    read_entries_to_proto(local_reads)
                } else {
                    Vec::new()
                },
                phase: read_phase_to_i32(phase),
                status: local_read_status_to_i32(status),
            };
            let mut client = pb::shard_client::ShardClient::connect(endpoint).await?;
            client.local_read_result(request).await?;
            sent += 1;
        }
        Ok(sent)
    }

    async fn collect_scc_full_reads(
        &mut self,
        local_reads: BTreeMap<Key, ReadValue>,
        phase: ReadPhase,
    ) -> Result<(BTreeMap<Key, ReadValue>, usize)> {
        let mut full_reads = local_reads;
        let expected_remote: BTreeSet<ShardId> = self
            .participants
            .all
            .iter()
            .copied()
            .filter(|shard| *shard != self.core.shard_id)
            .collect();
        let remote_rx = match phase {
            ReadPhase::SccEffect => &mut self.effect_rx,
            ReadPhase::SccCondition => &mut self.condition_rx,
            _ => {
                return Err(Error::InvalidBatch(format!(
                    "invalid SCC read phase {:?}",
                    phase
                )))
            }
        };
        let mut received = BTreeSet::new();
        while received.len() < expected_remote.len() {
            let msg = remote_rx.recv().await.ok_or_else(|| {
                Error::ChannelClosed(format!(
                    "SCC remote read mailbox closed for tx {} phase {:?}",
                    self.tx.tx_id, phase
                ))
            })?;
            if msg.batch_id != self.batch_id || msg.tx_id != self.tx.tx_id || msg.phase != phase {
                return Err(Error::InvalidBatch(format!(
                    "SCC read result routed to wrong target: got batch {} tx {} phase {:?}, expected batch {} tx {} phase {:?}",
                    msg.batch_id, msg.tx_id, msg.phase, self.batch_id, self.tx.tx_id, phase
                )));
            }
            if !expected_remote.contains(&msg.from_shard) {
                return Err(Error::InvalidBatch(format!(
                    "unexpected SCC read result for tx {} phase {:?} from shard {}",
                    self.tx.tx_id, phase, msg.from_shard
                )));
            }
            if !received.insert(msg.from_shard) {
                return Err(Error::InvalidBatch(format!(
                    "duplicate SCC read result for tx {} phase {:?} from shard {}",
                    self.tx.tx_id, phase, msg.from_shard
                )));
            }
            if msg.status == LocalReadStatus::SpeculationFailed {
                return Err(Error::SpeculationFailed(format!(
                    "tx {} phase {:?} received speculation failure from shard {}",
                    self.tx.tx_id, phase, msg.from_shard
                )));
            }
            if msg.status != LocalReadStatus::Ok {
                return Err(Error::InvalidBatch(format!(
                    "tx {} phase {:?} got invalid local read status {:?}",
                    self.tx.tx_id, phase, msg.status
                )));
            }

            let expected_keys = self.core.layout.local_read_keys(&self.tx, msg.from_shard);
            let actual_keys: BTreeSet<Key> = msg.reads.keys().cloned().collect();
            if actual_keys != expected_keys {
                return Err(Error::InvalidBatch(format!(
                    "tx {} phase {:?} read keys from shard {} mismatch: expected {:?}, got {:?}",
                    self.tx.tx_id, phase, msg.from_shard, expected_keys, actual_keys
                )));
            }
            for (key, value) in msg.reads {
                if full_reads.insert(key.clone(), value).is_some() {
                    return Err(Error::InvalidBatch(format!(
                        "duplicate SCC full read key {} for tx {} phase {:?}",
                        key, self.tx.tx_id, phase
                    )));
                }
            }
        }

        let full_keys: BTreeSet<Key> = full_reads.keys().cloned().collect();
        if full_keys != self.tx.read_set {
            return Err(Error::InvalidBatch(format!(
                "tx {} phase {:?} full read set mismatch: expected {:?}, got {:?}",
                self.tx.tx_id, phase, self.tx.read_set, full_keys
            )));
        }
        Ok((full_reads, received.len()))
    }

    fn fail(mut self, reason: String) -> Result<SccWorkerCompletion> {
        self.commit_seq
            .set_terminal_once(self.tx_index, CommitSlotState::Failed)?;
        tracing::debug!("SCC tx {} failed speculation: {}", self.tx.tx_id, reason);
        Ok(SccWorkerCompletion {
            record: None,
            profile: self.profile.take(),
        })
    }
}

async fn record_scc_reorder(
    core: Arc<ShardCore>,
    batch_id: BatchId,
    batch_len: usize,
    failed_indices: &BTreeSet<usize>,
) -> Result<()> {
    record_batch_reorder(core, batch_id, batch_len, failed_indices).await
}

async fn record_batch_reorder(
    core: Arc<ShardCore>,
    batch_id: BatchId,
    batch_len: usize,
    failed_indices: &BTreeSet<usize>,
) -> Result<()> {
    let all_indices: BTreeSet<usize> = (0..batch_len).collect();
    if !failed_indices.is_subset(&all_indices) {
        return Err(Error::InvalidBatch(format!(
            "batch failed set {:?} contains index outside batch length {}",
            failed_indices, batch_len
        )));
    }
    let speculative_success_indices = all_indices.difference(failed_indices).copied().collect();
    let fallback_indices = failed_indices.iter().copied().collect();
    let record = SccReorderRecord {
        batch_id,
        speculative_success_indices,
        fallback_indices,
    };
    let mut reorders = core.scc_reorders.lock().await;
    if reorders.insert(batch_id, record).is_some() {
        return Err(Error::InvalidBatch(format!(
            "duplicate batch reorder record for batch {}",
            batch_id
        )));
    }
    Ok(())
}

fn failed_indices_from_snapshot(snapshot: &[CommitSlotState]) -> BTreeSet<usize> {
    snapshot
        .iter()
        .enumerate()
        .filter_map(|(index, state)| matches!(state, CommitSlotState::Failed).then_some(index))
        .collect()
}

fn install_scc_successes(
    core: Arc<ShardCore>,
    snapshot: &[CommitSlotState],
    failed_indices: &BTreeSet<usize>,
) -> Result<()> {
    for (tx_index, state) in snapshot.iter().enumerate() {
        if failed_indices.contains(&tx_index) {
            continue;
        }
        match state {
            CommitSlotState::Pending => {
                return Err(Error::InvalidBatch(format!(
                    "SCC commit slot {} is still pending at install",
                    tx_index
                )))
            }
            CommitSlotState::Failed => {
                return Err(Error::InvalidBatch(format!(
                    "SCC commit slot {} failed but is absent from failed set",
                    tx_index
                )))
            }
            CommitSlotState::NoOp => {}
            CommitSlotState::Delta(delta) => {
                core.store.apply_delta_atomically(delta)?;
            }
        }
    }
    Ok(())
}

fn fallback_batch_from_failed_indices(batch: &Batch, failed_indices: &BTreeSet<usize>) -> Batch {
    let txs = failed_indices
        .iter()
        .enumerate()
        .map(|(fallback_index, original_index)| {
            let mut tx = batch.txs[*original_index].clone();
            tx.batch_index = fallback_index as u32;
            tx
        })
        .collect();
    Batch {
        batch_id: batch.batch_id,
        txs,
    }
}

#[derive(Clone, Debug)]
pub struct LocalReadResult {
    pub batch_id: BatchId,
    pub tx_id: TxId,
    pub phase: ReadPhase,
    pub from_shard: ShardId,
    pub status: LocalReadStatus,
    pub reads: BTreeMap<Key, ReadValue>,
}

#[derive(Clone)]
pub struct ReadResultMailboxRegistry {
    inner: Arc<Mutex<ReadResultMailboxMap>>,
}

type ReadResultMailboxKey = (BatchId, TxId, ReadPhase);
type ReadResultMailboxMap = BTreeMap<ReadResultMailboxKey, MailboxEntry>;

struct MailboxEntry {
    sender: mpsc::Sender<LocalReadResult>,
    receiver: Option<mpsc::Receiver<LocalReadResult>>,
}

impl ReadResultMailboxRegistry {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub async fn receiver(
        &self,
        batch_id: BatchId,
        tx_id: TxId,
        phase: ReadPhase,
        capacity: usize,
    ) -> Result<mpsc::Receiver<LocalReadResult>> {
        let mut inner = self.inner.lock().await;
        let entry = inner
            .entry((batch_id, tx_id, phase))
            .or_insert_with(|| new_mailbox(capacity.max(1)));
        entry.receiver.take().ok_or_else(|| {
            Error::InvalidBatch(format!(
                "remote read receiver already taken for batch {} tx {} phase {:?}",
                batch_id, tx_id, phase
            ))
        })
    }

    pub async fn route(&self, result: LocalReadResult) -> Result<()> {
        let sender = {
            let mut inner = self.inner.lock().await;
            inner
                .entry((result.batch_id, result.tx_id, result.phase))
                .or_insert_with(|| new_mailbox(DEFAULT_MAILBOX_CAPACITY))
                .sender
                .clone()
        };
        sender
            .send(result)
            .await
            .map_err(|_| Error::ChannelClosed("remote read mailbox receiver is closed".to_string()))
    }

    pub async fn cleanup_batch(&self, batch_id: BatchId) {
        let mut inner = self.inner.lock().await;
        inner.retain(|(entry_batch_id, _, _), _| *entry_batch_id != batch_id);
    }
}

impl Default for ReadResultMailboxRegistry {
    fn default() -> Self {
        Self::new()
    }
}

fn new_mailbox(capacity: usize) -> MailboxEntry {
    let (sender, receiver) = mpsc::channel(capacity);
    MailboxEntry {
        sender,
        receiver: Some(receiver),
    }
}

impl AriaBatchRegistry {
    fn new() -> Self {
        Self::default()
    }

    async fn entry(&self, batch_id: BatchId) -> Arc<AriaBatchEntry> {
        let mut inner = self.inner.lock().await;
        inner
            .entry(batch_id)
            .or_insert_with(|| {
                Arc::new(AriaBatchEntry {
                    state: Mutex::new(AriaBatchState::default()),
                    notify: Notify::new(),
                })
            })
            .clone()
    }

    async fn read_snapshot(
        &self,
        core: Arc<ShardCore>,
        batch_id: BatchId,
        tx_index: usize,
        _tx_id: TxId,
        _from_shard: ShardId,
        key: Key,
    ) -> Result<ReadValue> {
        let owner = core.layout.shard_for_key(&key);
        if owner != core.shard_id {
            return Err(Error::InvalidBatch(format!(
                "Aria read for key {} routed to shard {}, owner is {}",
                key, core.shard_id, owner
            )));
        }
        let mut keys = BTreeSet::new();
        keys.insert(key.clone());
        let value = core.store.read_many(&keys)?.remove(&key).ok_or_else(|| {
            Error::InvalidBatch(format!("missing Aria read value for key {}", key))
        })?;
        let entry = self.entry(batch_id).await;
        {
            let mut state = entry.state.lock().await;
            state
                .readers_by_key
                .entry(key)
                .or_default()
                .insert(tx_index);
        }
        entry.notify.notify_waiters();
        Ok(value)
    }

    async fn stage_outcome(&self, core: Arc<ShardCore>, outcome: AriaStageOutcome) -> Result<()> {
        let expected_from = aria_result_shard(outcome.tx_id, core.layout.shard_count);
        if outcome.from_shard != expected_from {
            return Err(Error::InvalidBatch(format!(
                "Aria staged outcome for tx {} came from shard {}, expected {}",
                outcome.tx_id, outcome.from_shard, expected_from
            )));
        }
        let is_result_shard = core.shard_id == expected_from;
        if outcome.is_result_shard != is_result_shard {
            return Err(Error::InvalidBatch(format!(
                "Aria staged outcome for tx {} has is_result_shard={}, expected {} on shard {}",
                outcome.tx_id, outcome.is_result_shard, is_result_shard, core.shard_id
            )));
        }
        for write in &outcome.writes {
            let owner = core.layout.shard_for_key(write.key());
            if owner != core.shard_id {
                return Err(Error::InvalidBatch(format!(
                    "Aria staged write for key {} routed to shard {}, owner is {}",
                    write.key(),
                    core.shard_id,
                    owner
                )));
            }
        }

        let entry = self.entry(outcome.batch_id).await;
        {
            let mut state = entry.state.lock().await;
            if state
                .staged_outcomes
                .insert(
                    outcome.tx_index,
                    AriaStagedOutcome {
                        tx_index: outcome.tx_index,
                        tx_id: outcome.tx_id,
                        result: outcome.is_result_shard.then_some(outcome.result),
                        local_writes: outcome.writes.clone(),
                        is_result_shard: outcome.is_result_shard,
                    },
                )
                .is_some()
            {
                return Err(Error::InvalidBatch(format!(
                    "duplicate Aria staged outcome for batch {} tx index {} on shard {}",
                    outcome.batch_id, outcome.tx_index, core.shard_id
                )));
            }
            for write in outcome.writes {
                state
                    .writers_by_key
                    .entry(write.key().clone())
                    .or_default()
                    .insert(outcome.tx_index);
            }
        }
        entry.notify.notify_waiters();
        Ok(())
    }

    async fn record_execution_done(
        &self,
        batch_id: BatchId,
        from_shard: ShardId,
        shard_count: ShardId,
    ) -> Result<()> {
        validate_aria_report_shard(batch_id, from_shard, shard_count)?;
        let entry = self.entry(batch_id).await;
        {
            let mut state = entry.state.lock().await;
            if !state.execution_done_reports.insert(from_shard) {
                return Err(Error::InvalidBatch(format!(
                    "duplicate Aria execution-done report for batch {} from shard {}",
                    batch_id, from_shard
                )));
            }
        }
        entry.notify.notify_waiters();
        Ok(())
    }

    async fn record_local_failures(
        &self,
        batch_id: BatchId,
        from_shard: ShardId,
        failed_indices: BTreeSet<usize>,
        shard_count: ShardId,
    ) -> Result<()> {
        validate_aria_report_shard(batch_id, from_shard, shard_count)?;
        let entry = self.entry(batch_id).await;
        {
            let mut state = entry.state.lock().await;
            if state
                .failure_reports
                .insert(from_shard, failed_indices)
                .is_some()
            {
                return Err(Error::InvalidBatch(format!(
                    "duplicate Aria local-failure report for batch {} from shard {}",
                    batch_id, from_shard
                )));
            }
        }
        entry.notify.notify_waiters();
        Ok(())
    }

    async fn wait_execution_done(&self, batch_id: BatchId, shard_count: usize) -> Result<()> {
        let entry = self.entry(batch_id).await;
        loop {
            {
                let state = entry.state.lock().await;
                if has_all_aria_reports(&state.execution_done_reports, shard_count) {
                    return Ok(());
                }
            }
            entry.notify.notified().await;
        }
    }

    async fn wait_failure_reports(
        &self,
        batch_id: BatchId,
        shard_count: usize,
    ) -> Result<BTreeSet<usize>> {
        let entry = self.entry(batch_id).await;
        loop {
            {
                let state = entry.state.lock().await;
                let reported_shards: BTreeSet<ShardId> =
                    state.failure_reports.keys().copied().collect();
                if has_all_aria_reports(&reported_shards, shard_count) {
                    return Ok(state
                        .failure_reports
                        .values()
                        .flat_map(|indices| indices.iter().copied())
                        .collect());
                }
            }
            entry.notify.notified().await;
        }
    }

    async fn local_failed_indices(&self, batch_id: BatchId) -> BTreeSet<usize> {
        let entry = self.entry(batch_id).await;
        let state = entry.state.lock().await;
        let mut failed = BTreeSet::new();
        for key in state
            .writers_by_key
            .keys()
            .chain(state.readers_by_key.keys())
        {
            let writers = state.writers_by_key.get(key);
            let Some(writers) = writers else {
                continue;
            };
            let Some(winner) = writers.iter().next().copied() else {
                continue;
            };
            for writer in writers {
                if *writer != winner {
                    failed.insert(*writer);
                }
            }
            if let Some(readers) = state.readers_by_key.get(key) {
                for reader in readers {
                    if winner < *reader {
                        failed.insert(*reader);
                    }
                }
            }
        }
        failed
    }

    async fn staged_outcomes(&self, batch_id: BatchId) -> Vec<AriaStagedOutcome> {
        let entry = self.entry(batch_id).await;
        let state = entry.state.lock().await;
        state.staged_outcomes.values().cloned().collect()
    }

    async fn cleanup_batch(&self, batch_id: BatchId) {
        let mut inner = self.inner.lock().await;
        inner.remove(&batch_id);
    }
}

fn validate_aria_report_shard(
    batch_id: BatchId,
    from_shard: ShardId,
    shard_count: ShardId,
) -> Result<()> {
    if from_shard >= shard_count {
        return Err(Error::InvalidBatch(format!(
            "Aria report for batch {} came from shard {}, but shard_count is {}",
            batch_id, from_shard, shard_count
        )));
    }
    Ok(())
}

fn has_all_aria_reports(reported_shards: &BTreeSet<ShardId>, shard_count: usize) -> bool {
    (0..shard_count).all(|shard| reported_shards.contains(&(shard as ShardId)))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClientTxResult {
    Ready(TxResult),
    NotResponsible,
}

#[derive(Clone)]
pub struct TxResultRegistry {
    inner: Arc<Mutex<BTreeMap<TxId, watch::Sender<ClientTxResultState>>>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ClientTxResultState {
    Pending,
    Ready(TxResult),
    NotResponsible,
}

impl TxResultRegistry {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub async fn ensure_pending(&self, tx_id: TxId) {
        let mut inner = self.inner.lock().await;
        inner
            .entry(tx_id)
            .or_insert_with(|| new_result_sender(ClientTxResultState::Pending));
    }

    pub async fn mark_ready(&self, tx_id: TxId, result: TxResult) {
        self.update(tx_id, ClientTxResultState::Ready(result)).await;
    }

    pub async fn mark_not_responsible(&self, tx_id: TxId) {
        self.update(tx_id, ClientTxResultState::NotResponsible)
            .await;
    }

    pub async fn wait(&self, tx_id: TxId) -> Result<ClientTxResult> {
        let mut rx = {
            let mut inner = self.inner.lock().await;
            inner
                .entry(tx_id)
                .or_insert_with(|| new_result_sender(ClientTxResultState::Pending))
                .subscribe()
        };

        loop {
            match *rx.borrow() {
                ClientTxResultState::Pending => {}
                ClientTxResultState::Ready(result) => return Ok(ClientTxResult::Ready(result)),
                ClientTxResultState::NotResponsible => return Ok(ClientTxResult::NotResponsible),
            }
            rx.changed().await.map_err(|_| {
                Error::ChannelClosed(format!("tx result registry closed for tx {}", tx_id))
            })?;
        }
    }

    async fn update(&self, tx_id: TxId, state: ClientTxResultState) {
        let sender = {
            let mut inner = self.inner.lock().await;
            inner
                .entry(tx_id)
                .or_insert_with(|| new_result_sender(ClientTxResultState::Pending))
                .clone()
        };
        sender.send_replace(state);
    }
}

impl Default for TxResultRegistry {
    fn default() -> Self {
        Self::new()
    }
}

fn new_result_sender(state: ClientTxResultState) -> watch::Sender<ClientTxResultState> {
    let (sender, _) = watch::channel(state);
    sender
}

#[derive(Clone, Debug)]
pub struct SequencerConfig {
    pub node_id: String,
    pub shard_count: u64,
    pub shard_endpoints: BTreeMap<ShardId, String>,
    pub max_batch_size: usize,
    pub batch_flush_interval: Duration,
    pub result_policy: SequencerResultPolicy,
}

impl SequencerConfig {
    pub fn default_batch_flush_interval() -> Duration {
        DEFAULT_BATCH_FLUSH_INTERVAL
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SequencerResultPolicy {
    #[default]
    StaticReadWriteSet,
    TxIdModulo,
}

pub struct SequencerRuntime {
    node_id: String,
    layout: ShardLayout,
    command_tx: mpsc::Sender<SequencerCommand>,
    batch_log: Arc<Mutex<Vec<Batch>>>,
}

pub struct SubmitBatchSummary {
    pub batch_id: BatchId,
    pub tx_ids: Vec<TxId>,
    pub tx_results: Vec<TxResultRecord>,
}

pub struct SubmitTxAck {
    pub tx_id: TxId,
    pub result_shard: ShardId,
}

impl SequencerRuntime {
    pub fn new(config: SequencerConfig) -> Self {
        let layout = ShardLayout::new(config.shard_count);
        let shard_endpoints = Arc::new(config.shard_endpoints);
        let batch_log = Arc::new(Mutex::new(Vec::new()));
        let (command_tx, command_rx) = mpsc::channel(SEQUENCER_COMMAND_CAPACITY);
        let (dispatch_tx, dispatch_rx) = mpsc::channel(BATCH_QUEUE_CAPACITY);

        tokio::spawn(run_batch_dispatcher(
            shard_endpoints,
            dispatch_rx,
            batch_log.clone(),
        ));
        tokio::spawn(run_sequencer_actor(SequencerActor {
            layout: layout.clone(),
            result_policy: config.result_policy,
            max_batch_size: config.max_batch_size,
            batch_flush_interval: config.batch_flush_interval,
            next_batch_id: 1,
            next_tx_id: 1,
            open_batch: None,
            command_tx: command_tx.clone(),
            command_rx,
            dispatch_tx,
        }));

        Self {
            node_id: config.node_id,
            layout,
            command_tx,
            batch_log,
        }
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    pub async fn submit_tx(&self, op: FsOp) -> Result<SubmitTxAck> {
        let (reply, response_rx) = oneshot::channel();
        self.command_tx
            .send(SequencerCommand::SubmitTx { op, reply })
            .await
            .map_err(|_| Error::ChannelClosed("sequencer actor is closed".to_string()))?;
        response_rx.await.map_err(|_| {
            Error::ChannelClosed("sequencer SubmitTx response channel is closed".to_string())
        })?
    }

    pub async fn submit_ops(&self, ops: Vec<FsOp>) -> Result<SubmitBatchSummary> {
        let (reply, response_rx) = oneshot::channel();
        self.command_tx
            .send(SequencerCommand::SubmitBatch { ops, reply })
            .await
            .map_err(|_| Error::ChannelClosed("sequencer actor is closed".to_string()))?;
        response_rx.await.map_err(|_| {
            Error::ChannelClosed("sequencer SubmitBatch response channel is closed".to_string())
        })?
    }

    pub async fn batch_log(&self) -> Vec<Batch> {
        self.batch_log.lock().await.clone()
    }

    pub fn layout(&self) -> &ShardLayout {
        &self.layout
    }
}

enum SequencerCommand {
    SubmitTx {
        op: FsOp,
        reply: oneshot::Sender<Result<SubmitTxAck>>,
    },
    SubmitBatch {
        ops: Vec<FsOp>,
        reply: oneshot::Sender<Result<SubmitBatchSummary>>,
    },
    FlushOpen {
        batch_id: BatchId,
    },
}

struct SequencerActor {
    layout: ShardLayout,
    result_policy: SequencerResultPolicy,
    max_batch_size: usize,
    batch_flush_interval: Duration,
    next_batch_id: BatchId,
    next_tx_id: TxId,
    open_batch: Option<OpenBatch>,
    command_tx: mpsc::Sender<SequencerCommand>,
    command_rx: mpsc::Receiver<SequencerCommand>,
    dispatch_tx: mpsc::Sender<DispatchBatchJob>,
}

struct OpenBatch {
    batch_id: BatchId,
    txs: Vec<OrderedTx>,
    tx_ids: Vec<TxId>,
}

struct DispatchBatchJob {
    batch: Batch,
    tx_ids: Vec<TxId>,
    reply: Option<oneshot::Sender<Result<SubmitBatchSummary>>>,
}

async fn run_sequencer_actor(mut actor: SequencerActor) {
    while let Some(command) = actor.command_rx.recv().await {
        match command {
            SequencerCommand::SubmitTx { op, reply } => {
                let response = actor.handle_submit_tx(op).await;
                let _ = reply.send(response);
            }
            SequencerCommand::SubmitBatch { ops, reply } => {
                actor.handle_submit_batch(ops, reply).await;
            }
            SequencerCommand::FlushOpen { batch_id } => {
                if actor
                    .open_batch
                    .as_ref()
                    .is_some_and(|open| open.batch_id == batch_id)
                {
                    let _ = actor.seal_open_batch().await;
                }
            }
        }
    }
}

impl SequencerActor {
    async fn handle_submit_tx(&mut self, op: FsOp) -> Result<SubmitTxAck> {
        let tx_id = self.next_tx_id;
        let (read_set, write_set) = derive_read_write_set(&op)?;
        let result_shard = self.result_shard_for_tx(tx_id, &read_set, &write_set)?;
        let max_batch_size = self.max_batch_size;
        let should_flush = {
            let batch = self.ensure_open_batch();
            let batch_index = batch.txs.len() as u32;
            batch.tx_ids.push(tx_id);
            batch.txs.push(OrderedTx {
                tx_id,
                batch_index,
                op,
                read_set,
                write_set,
            });
            batch.txs.len() >= max_batch_size
        };
        self.next_tx_id += 1;

        if should_flush {
            self.seal_open_batch().await?;
        }

        Ok(SubmitTxAck {
            tx_id,
            result_shard,
        })
    }

    async fn handle_submit_batch(
        &mut self,
        ops: Vec<FsOp>,
        reply: oneshot::Sender<Result<SubmitBatchSummary>>,
    ) {
        if ops.len() > self.max_batch_size {
            let _ = reply.send(Err(Error::BatchTooLarge {
                size: ops.len(),
                max: self.max_batch_size,
            }));
            return;
        }

        if let Err(err) = self.seal_open_batch().await {
            let _ = reply.send(Err(err));
            return;
        }

        let batch_id = self.take_next_batch_id();
        let mut txs = Vec::with_capacity(ops.len());
        let mut tx_ids = Vec::with_capacity(ops.len());
        for (batch_index, op) in ops.into_iter().enumerate() {
            let tx_id = self.next_tx_id;
            self.next_tx_id += 1;
            let (read_set, write_set) = match derive_read_write_set(&op) {
                Ok(sets) => sets,
                Err(err) => {
                    let _ = reply.send(Err(err));
                    return;
                }
            };
            tx_ids.push(tx_id);
            txs.push(OrderedTx {
                tx_id,
                batch_index: batch_index as u32,
                op,
                read_set,
                write_set,
            });
        }

        let batch = Batch { batch_id, txs };
        if let Err(err) = self.dispatch_batch(batch, tx_ids, Some(reply)).await {
            // The dispatch queue is closed, so there is no receiver left that can observe
            // this batch. The original reply has been dropped with the failed job.
            tracing::error!("failed to dispatch SubmitBatch: {err}");
        }
    }

    fn ensure_open_batch(&mut self) -> &mut OpenBatch {
        if self.open_batch.is_none() {
            let batch_id = self.take_next_batch_id();
            self.spawn_flush_timer(batch_id);
            self.open_batch = Some(OpenBatch {
                batch_id,
                txs: Vec::new(),
                tx_ids: Vec::new(),
            });
        }
        self.open_batch.as_mut().expect("open batch exists")
    }

    async fn seal_open_batch(&mut self) -> Result<()> {
        let Some(open) = self.open_batch.take() else {
            return Ok(());
        };
        if open.txs.is_empty() {
            return Ok(());
        }
        let batch = Batch {
            batch_id: open.batch_id,
            txs: open.txs,
        };
        self.dispatch_batch(batch, open.tx_ids, None).await
    }

    async fn dispatch_batch(
        &self,
        batch: Batch,
        tx_ids: Vec<TxId>,
        reply: Option<oneshot::Sender<Result<SubmitBatchSummary>>>,
    ) -> Result<()> {
        self.dispatch_tx
            .send(DispatchBatchJob {
                batch,
                tx_ids,
                reply,
            })
            .await
            .map_err(|_| Error::ChannelClosed("sequencer dispatch queue is closed".to_string()))
    }

    fn spawn_flush_timer(&self, batch_id: BatchId) {
        let command_tx = self.command_tx.clone();
        let interval = self.batch_flush_interval;
        tokio::spawn(async move {
            tokio::time::sleep(interval).await;
            let _ = command_tx
                .send(SequencerCommand::FlushOpen { batch_id })
                .await;
        });
    }

    fn take_next_batch_id(&mut self) -> BatchId {
        let batch_id = self.next_batch_id;
        self.next_batch_id += 1;
        batch_id
    }

    fn result_shard_for_tx(
        &self,
        tx_id: TxId,
        read_set: &BTreeSet<Key>,
        write_set: &BTreeSet<Key>,
    ) -> Result<ShardId> {
        match self.result_policy {
            SequencerResultPolicy::StaticReadWriteSet => self
                .layout
                .result_shard_for_sets(read_set, write_set)
                .ok_or_else(|| Error::InvalidBatch(format!("tx {} has no result shard", tx_id))),
            SequencerResultPolicy::TxIdModulo => {
                Ok(aria_result_shard(tx_id, self.layout.shard_count))
            }
        }
    }
}

async fn run_batch_dispatcher(
    shard_endpoints: Arc<BTreeMap<ShardId, String>>,
    mut dispatch_rx: mpsc::Receiver<DispatchBatchJob>,
    batch_log: Arc<Mutex<Vec<Batch>>>,
) {
    while let Some(job) = dispatch_rx.recv().await {
        let result = send_batch_to_all_shards(shard_endpoints.clone(), &job.batch).await;
        if result.is_ok() {
            batch_log.lock().await.push(job.batch.clone());
        }
        if let Some(reply) = job.reply {
            let response = result.map(|tx_results| SubmitBatchSummary {
                batch_id: job.batch.batch_id,
                tx_ids: job.tx_ids,
                tx_results,
            });
            let _ = reply.send(response);
        }
    }
}

async fn send_batch_to_all_shards(
    shard_endpoints: Arc<BTreeMap<ShardId, String>>,
    batch: &Batch,
) -> Result<Vec<TxResultRecord>> {
    let proto_batch = batch_to_proto(batch);
    let mut handles = Vec::new();
    for endpoint in shard_endpoints.values() {
        let endpoint = endpoint.clone();
        let request_batch = proto_batch.clone();
        handles.push(tokio::spawn(async move {
            let mut client = pb::shard_client::ShardClient::connect(endpoint).await?;
            let response = client
                .execute_batch(pb::ExecuteBatchRequest {
                    batch: Some(request_batch),
                })
                .await?
                .into_inner();
            let mut records = Vec::with_capacity(response.tx_results.len());
            for record in response.tx_results {
                records.push(crate::convert::tx_result_record_from_proto(record)?);
            }
            Ok::<_, Error>(records)
        }));
    }

    let mut all_results = Vec::new();
    for handle in handles {
        let records = handle
            .await
            .map_err(|err| Error::TaskJoin(err.to_string()))??;
        all_results.extend(records);
    }
    all_results.sort_by_key(|record| (record.tx_id, record.shard_id));
    Ok(all_results)
}
