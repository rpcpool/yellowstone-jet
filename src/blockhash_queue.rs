use {
    crate::{
        grpc_geyser::BlockMetaWithCommitment,
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

type BlockMetaMap = Arc<StdRwLock<HashMap<Hash, BlockMetaWithCommitment>>>;

#[derive(Debug, Clone)]
pub struct BlockhashQueue {
    blockmeta_map: BlockMetaMap,
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
    // TODO: make use Stream generic
    pub fn new(grpc: broadcast::Receiver<BlockMetaWithCommitment>) -> Self {
        let shutdown = Arc::new(Notify::new());
        let blockmeta_map = Arc::new(StdRwLock::new(HashMap::new()));
        Self {
            blockmeta_map: Arc::clone(&blockmeta_map),
            shutdown: Arc::clone(&shutdown),
            join_handle: Self::spawn(Self::subscribe(shutdown, blockmeta_map, grpc)),
        }
    }

    async fn subscribe(
        shutdown: Arc<Notify>,
        blockmeta_map: BlockMetaMap,
        mut block_meta_rx: broadcast::Receiver<BlockMetaWithCommitment>,
    ) -> anyhow::Result<()> {
        loop {
            let block_meta = tokio::select! {
                _ = shutdown.notified() => return Ok(()),
                maybe = block_meta_rx.recv() => {
                    match maybe {
                        Ok(block_meta) => block_meta,
                        Err(broadcast::error::RecvError::Lagged(skipped)) => {
                            anyhow::bail!("BlockhashQueue: grpc stream lagged, skipped {skipped} messages");
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            return Ok(());
                        }
                    }
                }
            };

            // IMPORTANT Don't hold the lock across await points
            let mut blockmeta_map = blockmeta_map
                .write()
                .expect("Failed to acquire write lock on blockmeta_map");

            // Store block metadata for ALL commitment levels
            blockmeta_map.insert(block_meta.block_hash, block_meta);

            // Only clean up when we get a finalized block
            if block_meta.commitment == CommitmentLevel::Finalized {
                blockmeta_map.retain(|_hash, slot| {
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

            metrics::blockhash_queue_set_size(blockmeta_map.len());
            metrics::blockhash_queue_set_slot(block_meta.commitment, block_meta.slot);
        }
    }

    pub fn get_block_height(&self, block_hash: &Hash) -> Option<BlockHeight> {
        let blockmeta_map = self
            .blockmeta_map
            .read()
            .expect("Failed to acquire read lock on blockmeta_map");
        blockmeta_map
            .get(block_hash)
            .map(|block_meta| block_meta.block_height)
    }

    pub fn get_block_height_latest(&self, commitment: CommitmentLevel) -> Option<BlockHeight> {
        let blockmeta_map = self
            .blockmeta_map
            .read()
            .expect("Failed to acquire read lock on blockmeta_map");
        blockmeta_map
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
    fn get_block_height(&self, blockhash: &Hash) -> Option<BlockHeight>;

    ///
    /// Get the latest block height for the given commitment level
    ///
    fn get_block_height_for_commitment(&self, commitment: CommitmentLevel) -> Option<BlockHeight>;
}

impl BlockHeightService for BlockhashQueue {
    fn get_block_height(&self, blockhash: &Hash) -> Option<BlockHeight> {
        self.get_block_height(blockhash)
    }

    fn get_block_height_for_commitment(&self, commitment: CommitmentLevel) -> Option<BlockHeight> {
        self.get_block_height_latest(commitment)
    }
}

pub mod testkit {
    use {
        super::{BlockHeightService, BlockMetaMap},
        crate::{
            grpc_geyser::BlockMetaWithCommitment,
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
        blockmeta_map: BlockMetaMap,
    }

    impl MockBlockhashQueue {
        pub fn new() -> Self {
            Self {
                blockmeta_map: Arc::new(StdRwLock::new(HashMap::new())),
            }
        }

        pub fn increase_block_height(&self, hash: Hash) {
            let mut blockmeta_map = self
                .blockmeta_map
                .write()
                .expect("Failed to acquire write lock on blockmeta_map");
            let last_block = blockmeta_map.values().last();

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

            blockmeta_map.insert(new_last_block.block_hash, new_last_block);
        }

        pub fn change_last_block_confirmation(&self) {
            let mut blockmeta_map = self
                .blockmeta_map
                .write()
                .expect("Failed to acquire write lock on blockmeta_map");
            let last_block = blockmeta_map.values_mut().last();
            if let Some(last_block) = last_block {
                last_block.commitment = CommitmentLevel::Finalized;
            }
        }
    }

    impl BlockHeightService for MockBlockhashQueue {
        fn get_block_height(&self, blockhash: &Hash) -> Option<BlockHeight> {
            let blockmeta_map = self
                .blockmeta_map
                .read()
                .expect("Failed to acquire read lock on blockmeta_map");
            blockmeta_map
                .get(blockhash)
                .map(|block_meta| block_meta.block_height)
        }

        fn get_block_height_for_commitment(
            &self,
            commitment: CommitmentLevel,
        ) -> Option<BlockHeight> {
            let blockmeta_map = self
                .blockmeta_map
                .read()
                .expect("Failed to acquire read lock on blockmeta_map");
            blockmeta_map
                .values()
                .filter(|block_meta| block_meta.commitment == commitment)
                .map(|block_meta| block_meta.block_height)
                .max()
        }
    }
}
