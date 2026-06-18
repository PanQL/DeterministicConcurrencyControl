use crate::convert::{batch_to_proto, read_entries_to_proto};
use crate::error::{Error, Result};
use crate::executor::{
    derive_read_write_set, execute_deterministic, filter_local_writes, validate_sets,
};
use crate::lock::{LockGrant, LockTable};
use crate::model::{
    Batch, BatchId, FsOp, Inode, Key, OrderedTx, ReadValue, ShardId, TxId, TxResultRecord,
};
use crate::proto::pb;
use crate::router::{Participants, ShardLayout};
use crate::storage::RedbInMemoryInodeStore;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, Mutex};

const BATCH_QUEUE_CAPACITY: usize = 16;
const DEFAULT_MAILBOX_CAPACITY: usize = 1024;

#[derive(Clone, Debug)]
pub struct ShardConfig {
    pub node_id: String,
    pub shard_id: ShardId,
    pub shard_count: u64,
    pub peer_endpoints: BTreeMap<ShardId, String>,
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
}

pub struct BatchExecutionSummary {
    pub batch_id: BatchId,
    pub shard_id: ShardId,
    pub tx_results: Vec<TxResultRecord>,
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

    pub fn dump_state(&self) -> Result<BTreeMap<Key, Inode>> {
        self.core.store.dump()
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
    let result = execute_batch_on_shard_inner(core.clone(), batch.clone()).await;
    core.mailboxes.cleanup_batch(batch.batch_id).await;
    result
}

async fn execute_batch_on_shard_inner(
    core: Arc<ShardCore>,
    batch: Batch,
) -> Result<BatchExecutionSummary> {
    validate_batch_order(&batch)?;

    let mut lock_table = LockTable::new();
    let mut relevant_count = 0usize;
    let (outcome_tx, mut outcome_rx) = mpsc::channel(batch.txs.len().max(1));

    for tx in &batch.txs {
        validate_sets(tx)?;
        let local_keys = core.layout.local_lock_keys(tx, core.shard_id);
        if local_keys.is_empty() {
            continue;
        }

        relevant_count += 1;
        let (grant_tx, grant_rx) = mpsc::channel(local_keys.len().max(1));
        lock_table.enqueue(tx.tx_id, &local_keys, grant_tx);

        let participants = core.layout.participants(tx);
        let remote_rx = if participants.active.contains(&core.shard_id) {
            Some(
                core.mailboxes
                    .receiver(batch.batch_id, tx.tx_id, participants.all.len().max(1))
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
            local_keys,
            grant_rx,
            remote_rx,
            outcome_tx: outcome_tx.clone(),
        });
    }
    drop(outcome_tx);

    lock_table.grant_initial_heads().await?;

    let mut tx_results = Vec::new();
    for _ in 0..relevant_count {
        let outcome = outcome_rx.recv().await.ok_or_else(|| {
            Error::ChannelClosed("worker outcome channel closed before batch finished".to_string())
        })?;
        let tx_id = outcome.tx_id;
        lock_table
            .release_and_grant_next(tx_id, &outcome.local_keys)
            .await?;
        match outcome.result? {
            WorkerCompletion::Active(record) => tx_results.push(record),
            WorkerCompletion::Passive => {}
        }
    }

    tx_results.sort_by_key(|record| (record.tx_id, record.shard_id));
    Ok(BatchExecutionSummary {
        batch_id: batch.batch_id,
        shard_id: core.shard_id,
        tx_results,
    })
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

struct TxWorker {
    core: Arc<ShardCore>,
    batch_id: BatchId,
    tx: OrderedTx,
    participants: Participants,
    local_keys: BTreeSet<Key>,
    grant_rx: mpsc::Receiver<LockGrant>,
    remote_rx: Option<mpsc::Receiver<LocalReadResult>>,
    outcome_tx: mpsc::Sender<WorkerOutcome>,
}

struct WorkerOutcome {
    tx_id: TxId,
    local_keys: BTreeSet<Key>,
    result: Result<WorkerCompletion>,
}

enum WorkerCompletion {
    Active(TxResultRecord),
    Passive,
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
        self.wait_for_lock_grants().await?;

        let local_read_keys = self
            .core
            .layout
            .local_read_keys(&self.tx, self.core.shard_id);
        let local_reads = self.core.store.read_many(&local_read_keys)?;
        self.send_local_reads(&local_reads).await?;

        if !self.participants.active.contains(&self.core.shard_id) {
            return Ok(WorkerCompletion::Passive);
        }

        let full_reads = self.collect_full_reads(local_reads).await?;
        let output = execute_deterministic(&self.tx, &full_reads);
        let local_writes =
            filter_local_writes(output.writes, self.core.shard_id, &self.core.layout);
        self.validate_local_writes(&local_writes)?;
        self.core.store.apply_writes_atomically(&local_writes)?;

        Ok(WorkerCompletion::Active(TxResultRecord {
            tx_id: self.tx.tx_id,
            shard_id: self.core.shard_id,
            result: output.result,
        }))
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

    async fn send_local_reads(&self, local_reads: &BTreeMap<Key, ReadValue>) -> Result<()> {
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
            };
            let mut client = pb::shard_client::ShardClient::connect(endpoint).await?;
            client.local_read_result(request).await?;
        }
        Ok(())
    }

    async fn collect_full_reads(
        &mut self,
        local_reads: BTreeMap<Key, ReadValue>,
    ) -> Result<BTreeMap<Key, ReadValue>> {
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

        Ok(full_reads)
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

#[derive(Clone, Debug)]
pub struct LocalReadResult {
    pub batch_id: BatchId,
    pub tx_id: TxId,
    pub from_shard: ShardId,
    pub reads: BTreeMap<Key, ReadValue>,
}

#[derive(Clone)]
pub struct ReadResultMailboxRegistry {
    inner: Arc<Mutex<BTreeMap<(BatchId, TxId), MailboxEntry>>>,
}

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
        capacity: usize,
    ) -> Result<mpsc::Receiver<LocalReadResult>> {
        let mut inner = self.inner.lock().await;
        let entry = inner
            .entry((batch_id, tx_id))
            .or_insert_with(|| new_mailbox(capacity.max(1)));
        entry.receiver.take().ok_or_else(|| {
            Error::InvalidBatch(format!(
                "remote read receiver already taken for batch {} tx {}",
                batch_id, tx_id
            ))
        })
    }

    pub async fn route(&self, result: LocalReadResult) -> Result<()> {
        let sender = {
            let mut inner = self.inner.lock().await;
            inner
                .entry((result.batch_id, result.tx_id))
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
        inner.retain(|(entry_batch_id, _), _| *entry_batch_id != batch_id);
    }
}

fn new_mailbox(capacity: usize) -> MailboxEntry {
    let (sender, receiver) = mpsc::channel(capacity);
    MailboxEntry {
        sender,
        receiver: Some(receiver),
    }
}

#[derive(Clone, Debug)]
pub struct SequencerConfig {
    pub node_id: String,
    pub shard_count: u64,
    pub shard_endpoints: BTreeMap<ShardId, String>,
    pub max_batch_size: usize,
}

pub struct SequencerRuntime {
    node_id: String,
    layout: ShardLayout,
    shard_endpoints: Arc<BTreeMap<ShardId, String>>,
    max_batch_size: usize,
    next_batch_id: AtomicU64,
    next_tx_id: AtomicU64,
    submit_lock: Mutex<()>,
    batch_log: Mutex<Vec<Batch>>,
}

pub struct SubmitBatchSummary {
    pub batch_id: BatchId,
    pub tx_ids: Vec<TxId>,
    pub tx_results: Vec<TxResultRecord>,
}

impl SequencerRuntime {
    pub fn new(config: SequencerConfig) -> Self {
        Self {
            node_id: config.node_id,
            layout: ShardLayout::new(config.shard_count),
            shard_endpoints: Arc::new(config.shard_endpoints),
            max_batch_size: config.max_batch_size,
            next_batch_id: AtomicU64::new(1),
            next_tx_id: AtomicU64::new(1),
            submit_lock: Mutex::new(()),
            batch_log: Mutex::new(Vec::new()),
        }
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    pub async fn submit_ops(&self, ops: Vec<FsOp>) -> Result<SubmitBatchSummary> {
        let _guard = self.submit_lock.lock().await;
        if ops.len() > self.max_batch_size {
            return Err(Error::BatchTooLarge {
                size: ops.len(),
                max: self.max_batch_size,
            });
        }

        let batch_id = self.next_batch_id.fetch_add(1, Ordering::SeqCst);
        let mut txs = Vec::with_capacity(ops.len());
        let mut tx_ids = Vec::with_capacity(ops.len());
        for (batch_index, op) in ops.into_iter().enumerate() {
            let tx_id = self.next_tx_id.fetch_add(1, Ordering::SeqCst);
            let (read_set, write_set) = derive_read_write_set(&op)?;
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
        let tx_results = self.send_batch_to_all_shards(&batch).await?;
        self.batch_log.lock().await.push(batch);

        Ok(SubmitBatchSummary {
            batch_id,
            tx_ids,
            tx_results,
        })
    }

    pub async fn batch_log(&self) -> Vec<Batch> {
        self.batch_log.lock().await.clone()
    }

    pub fn layout(&self) -> &ShardLayout {
        &self.layout
    }

    async fn send_batch_to_all_shards(&self, batch: &Batch) -> Result<Vec<TxResultRecord>> {
        let proto_batch = batch_to_proto(batch);
        let mut handles = Vec::new();
        for endpoint in self.shard_endpoints.values() {
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
}
