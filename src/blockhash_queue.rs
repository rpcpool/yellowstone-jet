use {
    crate::{
        grpc_geyser::{GeyserStreams, BlockMetaWithCommitment},
        metrics::jet as metrics,
        util::{
            BlockHeight, CommitmentLevel, WaitShutdown, WaitShutdownJoinHandleResult,
            WaitShutdownSharedJoinHandle,
        },
    },
    solana_clock::MAX_RECENT_BLOCKHASHES,
    solana_hash::Hash,
    std::{collections::HashMap, ops::DerefMut, sync::Arc},
    tokio::sync::{broadcast, Notify, RwLock},
    tracing::debug,
};

type SharedSlots = Arc<RwLock<HashMap<Hash, BlockMetaWithCommitment>>>;

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
        let slots = Arc::new(RwLock::new(HashMap::new()));
        Self {
            slots: Arc::clone(&slots),
            shutdown: Arc::clone(&shutdown),
            join_handle: Self::spawn(Self::subscribe(shutdown, slots, grpc.subscribe_block_meta())),
        }
    }

    async fn subscribe(
        shutdown: Arc<Notify>,
        slots: SharedSlots,
        mut block_meta_rx: broadcast::Receiver<BlockMetaWithCommitment>,
    ) -> anyhow::Result<()> {
        loop {
            let block_meta = tokio::select! {
                _ = shutdown.notified() => return Ok(()),
                message = block_meta_rx.recv() => message,
            }
            .map_err(|error| anyhow::anyhow!("BlockhashQueue: grpc stream finished: {error:?}"))?;

            let mut slots = slots.write().await;

            // Store block metadata for ALL commitment levels
            slots.insert(block_meta.block_hash, block_meta);

            // Only clean up when we get a finalized block
            if block_meta.commitment == CommitmentLevel::Finalized {
                slots.retain(|_hash, slot| {
                    if slot.commitment == CommitmentLevel::Finalized {
                        // Keep finalized blocks based on block height
                        slot.block_height + MAX_RECENT_BLOCKHASHES as u64 > block_meta.block_height
                    } else {
                        // Keep non-finalized blocks based on slot number
                        slot.slot > block_meta.slot
                    }
                });
            }

            debug!(
                slot = block_meta.slot,
                commitment = ?block_meta.commitment,
                "add block to the queue"
            );

            metrics::blockhash_queue_set_size(slots.len());
            metrics::blockhash_queue_set_slot(block_meta.commitment, block_meta.slot);
        }
    }

    pub async fn get_block_height(&self, block_hash: &Hash) -> Option<BlockHeight> {
        let slots = self.slots.read().await;
        slots.get(block_hash).map(|block_meta| block_meta.block_height)
    }

    pub async fn get_block_height_latest(
        &self,
        commitment: CommitmentLevel,
    ) -> Option<BlockHeight> {
        let slots = self.slots.read().await;
        slots
            .values()
            .filter(|block_meta| block_meta.commitment == commitment)
            .map(|block_meta| block_meta.block_height)
            .max()
    }
}

///
/// Interface to query block height information
///
#[async_trait::async_trait]
pub trait BlockHeightService {
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
impl BlockHeightService for BlockhashQueue {
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

pub mod testkit {
    use {
        super::{BlockHeightService, SharedSlots},
        crate::{
            grpc_geyser::BlockMetaWithCommitment,
            util::{BlockHeight, CommitmentLevel},
        },
        solana_hash::Hash,
        std::{collections::HashMap, sync::Arc},
        tokio::sync::RwLock,
    };

    #[derive(Default)]
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
                BlockMetaWithCommitment {
                    block_hash: hash,
                    block_height: last_block.block_height + 1,
                    commitment: CommitmentLevel::Confirmed,
                    slot: last_block.slot + 1,
                }
            } else {
                BlockMetaWithCommitment {
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
    impl BlockHeightService for MockBlockhashQueue {
        async fn get_block_height(&self, blockhash: &Hash) -> Option<BlockHeight> {
            let slots = self.slots.read().await;
            slots.get(blockhash).map(|block_meta| block_meta.block_height)
        }

        async fn get_block_height_for_commitment(
            &self,
            commitment: CommitmentLevel,
        ) -> Option<BlockHeight> {
            let slots = self.slots.read().await;
            slots
                .values()
                .filter(|block_meta| block_meta.commitment == commitment)
                .map(|block_meta| block_meta.block_height)
                .max()
        }
    }
}
