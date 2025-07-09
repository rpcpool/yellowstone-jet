use {
    crate::{
        grpc_geyser::{GeyserStreams, SlotUpdateInfoWithCommitment},
        metrics::jet as metrics,
        util::{
            BlockHeight, CommitmentLevel, WaitShutdown, WaitShutdownJoinHandleResult,
            WaitShutdownSharedJoinHandle,
        },
    },
    solana_clock::MAX_RECENT_BLOCKHASHES,
    solana_hash::Hash,
    std::{
        collections::HashMap,
        ops::DerefMut,
        sync::{Arc, RwLock as StdRwLock},
    },
    tokio::sync::{Notify, broadcast},
    tracing::debug,
};

type SharedSlots = Arc<StdRwLock<HashMap<Hash, SlotUpdateInfoWithCommitment>>>;

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
    pub fn new<G>(grpc: &G) -> Self
    where
        G: GeyserStreams + Send + Sync + 'static,
    {
        let shutdown = Arc::new(Notify::new());
        let slots = Arc::new(StdRwLock::new(HashMap::new()));
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

            // IMPORTANT Don't hold the lock across await points
            let mut slots = slots
                .write()
                .expect("Failed to acquire write lock on slots");
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

    pub fn get_block_height(&self, block_hash: &Hash) -> Option<BlockHeight> {
        let slots = self
            .slots
            .read()
            .expect("Failed to acquire read lock on slots");
        slots.get(block_hash).map(|slot| slot.block_height)
    }

    pub fn get_block_height_latest(&self, commitment: CommitmentLevel) -> Option<BlockHeight> {
        let slots = self
            .slots
            .read()
            .expect("Failed to acquire read lock on slots");
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
pub trait BlockHeighService {
    ///
    /// Get the block height for the given blockhash
    ///
    fn get_block_height(&self, blockhash: &Hash) -> Option<BlockHeight>;

    ///
    /// Get the latest block height for the given commitment level
    ///
    fn get_block_height_for_commitment(&self, commitment: CommitmentLevel) -> Option<BlockHeight>;
}

impl BlockHeighService for BlockhashQueue {
    fn get_block_height(&self, blockhash: &Hash) -> Option<BlockHeight> {
        self.get_block_height(blockhash)
    }

    fn get_block_height_for_commitment(&self, commitment: CommitmentLevel) -> Option<BlockHeight> {
        self.get_block_height_latest(commitment)
    }
}

pub mod testkit {

    use {
        super::{BlockHeighService, SharedSlots},
        crate::{
            grpc_geyser::SlotUpdateInfoWithCommitment,
            util::{BlockHeight, CommitmentLevel},
        },
        solana_hash::Hash,
        std::{
            collections::HashMap,
            sync::{Arc, RwLock as StdRwLock},
        },
    };

    #[derive(Default, Clone)]
    pub struct MockBlockhashQueue {
        slots: SharedSlots,
    }

    impl MockBlockhashQueue {
        pub fn new() -> Self {
            Self {
                slots: Arc::new(StdRwLock::new(HashMap::new())),
            }
        }

        pub fn increase_block_height(&self, hash: Hash) {
            let mut slots = self
                .slots
                .write()
                .expect("Failed to acquire write lock on slots");
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

        pub fn change_last_block_confirmation(&self) {
            let mut slots = self
                .slots
                .write()
                .expect("Failed to acquire write lock on slots");
            let last_block = slots.values_mut().last();
            if let Some(last_block) = last_block {
                last_block.commitment = CommitmentLevel::Finalized;
            }
        }
    }

    impl BlockHeighService for MockBlockhashQueue {
        fn get_block_height(&self, blockhash: &Hash) -> Option<BlockHeight> {
            let slots = self
                .slots
                .read()
                .expect("Failed to acquire read lock on slots");
            slots.get(blockhash).map(|slot| slot.block_height)
        }

        fn get_block_height_for_commitment(
            &self,
            commitment: CommitmentLevel,
        ) -> Option<BlockHeight> {
            let slots = self
                .slots
                .read()
                .expect("Failed to acquire read lock on slots");
            slots
                .values()
                .filter(|info| info.commitment == commitment)
                .map(|info| info.block_height)
                .max()
        }
    }
}
