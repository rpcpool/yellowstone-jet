use {
    crate::{
        config::TpuSenderConfig,
        core::{
            ConnectionEvictionStrategy, LeaderTpuInfoService, TpuSenderDriverSpawner,
            TpuSenderIdentityUpdater, TpuSenderSessionContext, TpuSenderTxn,
            UpcomingLeaderPredictor, ValidatorStakeInfoService,
        },
    },
    solana_keypair::Keypair,
    std::sync::Arc,
    tokio::sync::{Mutex, mpsc},
};

///
/// A TPU sender that can send transactions and update its identity.
///
/// Note: The TPU sender is thread-safe, a cheap `Clone` implementation is provided to allow multiple tasks to share the same TPU sender.
/// The API of the TPU uses `&mut self` to protect against bug or errors during identity updates.
///
#[derive(Clone)]
pub struct TpuSender {
    // The identity updater shared with the TPU sender task.
    // The [`TpuSenderIdentityUpdater`] cannot be cloned or called concurrently, so we wrap it in a Mutex.
    // We do this pre-cautionarily to avoid potential issues with miss-managed identity updates.
    identity_updated: Arc<Mutex<TpuSenderIdentityUpdater>>,
    txn_tx: mpsc::Sender<TpuSenderTxn>,
}

#[derive(Debug, thiserror::Error)]
#[error("disconnected")]
pub struct TpuSenderError(TpuSenderTxn);

impl TpuSender {
    ///
    /// Sends a transaction to the TPU sender task.
    ///
    pub async fn send_txn(&mut self, txn: TpuSenderTxn) -> Result<(), TpuSenderError> {
        // I put &mut self here to indicate that the caller should not be sending txns concurrently from multiple tasks.
        // This is the be consistent with the rest of the API which uses &mut self for updating identity.
        self.txn_tx.send(txn).await.map_err(|e| TpuSenderError(e.0))
    }

    ///
    /// Updates the identity used by the TPU sender.
    ///
    pub async fn update_identity(&mut self, new_identity: Keypair) {
        self.identity_updated
            .lock()
            .await
            .update_identity(new_identity)
            .await;
    }
}

pub async fn create_tpu_client(
    config: TpuSenderConfig,
    initial_identity: Keypair,
    tpu_info_service: Arc<dyn LeaderTpuInfoService + Send + Sync>,
    stake_map_service: Arc<dyn ValidatorStakeInfoService + Send + Sync>,
    eviction_strategy: Arc<dyn ConnectionEvictionStrategy + Send + Sync>,
    leader_schedule_predictor: Arc<dyn UpcomingLeaderPredictor + Send + Sync>,
    txn_capacity: usize,
) -> TpuSender {
    let spawner = TpuSenderDriverSpawner {
        stake_info_map: stake_map_service,
        leader_tpu_info_service: tpu_info_service,
        driver_tx_channel_capacity: txn_capacity,
    };

    let session = spawner.spawn(
        initial_identity,
        config,
        eviction_strategy,
        leader_schedule_predictor,
    );

    let TpuSenderSessionContext {
        identity_updater,
        driver_tx_sink,
        driver_response_source: _,
        driver_join_handle: _,
    } = session;

    let tpu_sender = TpuSender {
        identity_updated: Arc::new(Mutex::new(identity_updater)),
        txn_tx: driver_tx_sink,
    };

    tpu_sender
}
