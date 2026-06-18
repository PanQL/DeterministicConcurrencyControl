use crate::error::{Error, Result};
use crate::model::{Key, TxId};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use tokio::sync::mpsc;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LockGrant {
    pub key: Key,
}

#[derive(Clone)]
struct LockQueueEntry {
    tx_id: TxId,
    grant_tx: mpsc::Sender<LockGrant>,
}

pub struct LockTable {
    queues: BTreeMap<Key, VecDeque<LockQueueEntry>>,
}

impl LockTable {
    pub fn new() -> Self {
        Self {
            queues: BTreeMap::new(),
        }
    }

    pub fn enqueue(
        &mut self,
        tx_id: TxId,
        local_keys: &BTreeSet<Key>,
        grant_tx: mpsc::Sender<LockGrant>,
    ) {
        for key in local_keys {
            self.queues
                .entry(key.clone())
                .or_default()
                .push_back(LockQueueEntry {
                    tx_id,
                    grant_tx: grant_tx.clone(),
                });
        }
    }

    pub async fn grant_initial_heads(&self) -> Result<()> {
        for (key, queue) in &self.queues {
            if let Some(front) = queue.front() {
                send_grant(front, key).await?;
            }
        }
        Ok(())
    }

    pub async fn release_and_grant_next(
        &mut self,
        tx_id: TxId,
        local_keys: &BTreeSet<Key>,
    ) -> Result<()> {
        for key in local_keys {
            let queue = self.queues.get_mut(key).ok_or_else(|| {
                Error::LockInvariant(format!("missing lock queue for key {}", key))
            })?;
            let front = queue
                .front()
                .ok_or_else(|| Error::LockInvariant(format!("empty lock queue for key {}", key)))?;
            if front.tx_id != tx_id {
                return Err(Error::LockInvariant(format!(
                    "tx {} tried to release key {}, but queue head is tx {}",
                    tx_id, key, front.tx_id
                )));
            }
            queue.pop_front();
            if let Some(next) = queue.front() {
                send_grant(next, key).await?;
            }
        }
        Ok(())
    }
}

async fn send_grant(entry: &LockQueueEntry, key: &Key) -> Result<()> {
    entry
        .grant_tx
        .send(LockGrant { key: key.clone() })
        .await
        .map_err(|_| {
            Error::ChannelClosed(format!(
                "lock grant receiver for tx {} on key {} is closed",
                entry.tx_id, key
            ))
        })
}
