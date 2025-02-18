use {
    crate::{
        grpc_geyser::{GeyserSubscriber, SlotUpdateInfoWithCommitment},
        metrics::jet as metrics,
        util::{
            BlockHeight, CommitmentLevel, WaitShutdown, WaitShutdownJoinHandleResult,
            WaitShutdownSharedJoinHandle,
        },
    },
    solana_sdk::{clock::MAX_RECENT_BLOCKHASHES, hash::Hash},
    std::{collections::HashMap, ops::DerefMut, sync::Arc},
    tokio::sync::{broadcast, Notify, RwLock},
    tracing::debug,
};

type SharedSlots = Arc<RwLock<HashMap<Hash, SlotUpdateInfoWithCommitment>>>;

#[derive(Debug, Clone)]
pub struct BlockhashQueue {
    slots: SharedSlots,
    shutdown: Arc<Notify>,
    join_handle: WaitShutdownSharedJoinHandle,
}

impl WaitShutdown for BlockhashQueue {
    fn shutdown(&self) {
        self.shutdown.notify_one();
    }

    async fn wait_shutdown_future(self) -> WaitShutdownJoinHandleResult {
        let mut locked = self.join_handle.lock().await;
        locked.deref_mut().await
    }
}

impl BlockhashQueue {
    pub fn new(grpc: &GeyserSubscriber) -> Self {
        let shutdown = Arc::new(Notify::new());
        let slots = Arc::new(RwLock::new(HashMap::new()));
        Self {
            slots: Arc::clone(&slots),
            shutdown: Arc::clone(&shutdown),
            join_handle: Self::spawn(Self::subscribe(shutdown, slots, grpc.subscribe_slots())),
        }
    }

    async fn subscribe(
        shutdown: Arc<Notify>,
        slots: SharedSlots,
        mut slots_rx: broadcast::Receiver<SlotUpdateInfoWithCommitment>,
    ) -> anyhow::Result<()> {
        loop {
            let slot_update = tokio::select! {
                _ = shutdown.notified() => return Ok(()),
                message = slots_rx.recv() => message,
            }
            .map_err(|error| anyhow::anyhow!("BlockhashQueue: grpc stream finished: {error:?}"))?;

            let mut slots = slots.write().await;
            slots.insert(slot_update.block_hash, slot_update);

            if slot_update.commitment == CommitmentLevel::Finalized {
                slots.retain(|_hash, slot| {
                    if slot.commitment == CommitmentLevel::Finalized {
                        slot.block_height + MAX_RECENT_BLOCKHASHES as u64 > slot_update.block_height
                    } else {
                        slot.slot > slot_update.slot
                    }
                });
            }
            debug!(slot = slot_update.slot, "add slot to the queue");

            metrics::blockhash_queue_set_size(slots.len());
            metrics::blockhash_queue_set_slot(slot_update.commitment, slot_update.slot);
        }
    }

    pub async fn get_block_height(&self, block_hash: &Hash) -> Option<BlockHeight> {
        let slots = self.slots.read().await;
        slots.get(block_hash).map(|slot| slot.block_height)
    }

    pub async fn get_block_height_latest(
        &self,
        commitment: CommitmentLevel,
    ) -> Option<BlockHeight> {
        let slots = self.slots.read().await;
        slots
            .values()
            .filter(|info| info.commitment == commitment)
            .map(|info| info.block_height)
            .max()
    }
}

///
/// Interface to query block height information
///
#[async_trait::async_trait]
pub trait BlockHeighService {
    ///
    /// Get the block height for the given blockhash
    ///
    async fn get_block_height(&self, blockhash: &Hash) -> Option<BlockHeight>;
    ///
    /// Get the latest block height for the given commitment level
    ///
    async fn get_block_height_for_commitment(
        &self,
        commitment: CommitmentLevel,
    ) -> Option<BlockHeight>;
}

#[async_trait::async_trait]
impl BlockHeighService for BlockhashQueue {
    async fn get_block_height(&self, blockhash: &Hash) -> Option<BlockHeight> {
        self.get_block_height(blockhash).await
    }

    async fn get_block_height_for_commitment(
        &self,
        commitment: CommitmentLevel,
    ) -> Option<BlockHeight> {
        self.get_block_height_latest(commitment).await
    }
}

pub struct MockBlockhashQueue {
    slots: SharedSlots,
}

impl MockBlockhashQueue {
    pub fn new() -> Self {
        Self {
            slots: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn increase_block_height(&self, hash: Hash) {
        let mut slots = self.slots.write().await;
        let last_block = slots.values().last();

        let new_last_block = if let Some(last_block) = last_block {
            SlotUpdateInfoWithCommitment {
                block_hash: hash,
                block_height: last_block.block_height + 1,
                commitment: CommitmentLevel::Confirmed,
                slot: last_block.slot + 1,
            }
        } else {
            SlotUpdateInfoWithCommitment {
                block_hash: hash,
                block_height: 1,
                commitment: CommitmentLevel::Confirmed,
                slot: 1,
            }
        };

        slots.insert(new_last_block.block_hash, new_last_block);
    }

    pub async fn change_last_block_confirmation(&self) {
        let mut slots = self.slots.write().await;
        let last_block = slots.values_mut().last();
        if let Some(last_block) = last_block {
            last_block.commitment = CommitmentLevel::Finalized;
        }
    }
}

#[async_trait::async_trait]
impl BlockHeighService for MockBlockhashQueue {
    async fn get_block_height(&self, blockhash: &Hash) -> Option<BlockHeight> {
        let slots = self.slots.read().await;
        slots.get(blockhash).map(|slot| slot.block_height)
    }

    async fn get_block_height_for_commitment(
        &self,
        commitment: CommitmentLevel,
    ) -> Option<BlockHeight> {
        let slots = self.slots.read().await;
        slots
            .values()
            .filter(|info| info.commitment == commitment)
            .map(|info| info.block_height)
            .max()
    }
}
