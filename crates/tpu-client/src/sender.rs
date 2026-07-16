use {
    crate::{
        config::TpuSenderConfig,
        core::{
            ConnectionEvictionStrategy, LeaderTpuInfoService, TpuSenderDriverSpawner,
            TpuSenderIdentityUpdater, TpuSenderResponseCallback, TpuSenderSessionContext,
            TpuSenderTxn, UpcomingLeaderPredictor, ValidatorStakeInfoService,
        },
    },
    solana_keypair::Keypair,
    std::{
        pin::Pin,
        sync::Arc,
        task::{Context, Poll},
    },
    tokio::sync::Mutex,
    tokio_util::sync::PollSender,
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
    txn_tx: PollSender<TpuSenderTxn>,
}

#[derive(Debug, thiserror::Error)]
#[error("disconnected")]
pub struct TpuSenderError(TpuSenderTxn);

pub struct SendTxn<'a> {
    sink: &'a mut PollSender<TpuSenderTxn>,
    txn: Option<TpuSenderTxn>,
}

impl Future for SendTxn<'_> {
    type Output = Result<(), TpuSenderError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let txn = this.txn.as_mut().expect("SendTxn polled after completion");
        match this.sink.poll_reserve(cx) {
            Poll::Ready(Ok(())) => {
                let txn = this.txn.take().expect("checked above");
                match this.sink.send_item(txn) {
                    Ok(()) => Poll::Ready(Ok(())),
                    Err(err) => {
                        let txn = err.into_inner().expect("send_item keeps transaction");
                        Poll::Ready(Err(TpuSenderError(txn)))
                    }
                }
            }
            Poll::Ready(Err(_)) => {
                let txn = this.txn.take().expect("checked above");
                Poll::Ready(Err(TpuSenderError(txn)))
            }
            Poll::Pending => {
                let _ = txn;
                Poll::Pending
            }
        }
    }
}

impl TpuSender {
    pub(crate) fn poll_send_txn(
        &mut self,
        cx: &mut Context<'_>,
        txn: &mut Option<TpuSenderTxn>,
    ) -> Poll<Result<(), TpuSenderError>> {
        let _ = txn.as_ref().expect("poll_send_txn requires transaction");
        match self.txn_tx.poll_reserve(cx) {
            Poll::Ready(Ok(())) => {
                let txn_to_send = txn.take().expect("checked above");
                match self.txn_tx.send_item(txn_to_send) {
                    Ok(()) => Poll::Ready(Ok(())),
                    Err(err) => {
                        let txn = err.into_inner().expect("send_item keeps transaction");
                        Poll::Ready(Err(TpuSenderError(txn)))
                    }
                }
            }
            Poll::Ready(Err(_)) => {
                let txn = txn.take().expect("checked above");
                Poll::Ready(Err(TpuSenderError(txn)))
            }
            Poll::Pending => Poll::Pending,
        }
    }

    ///
    /// Sends a transaction to the TPU sender task.
    ///
    pub fn send_txn(&mut self, txn: TpuSenderTxn) -> SendTxn<'_> {
        // I put &mut self here to indicate that the caller should not be sending txns concurrently from multiple tasks.
        // This is the be consistent with the rest of the API which uses &mut self for updating identity.
        SendTxn {
            sink: &mut self.txn_tx,
            txn: Some(txn),
        }
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

///
/// Base factory function to create a TPU sender and its response receiver.
///
/// # Arguments
///
/// * `config` - Configuration for the TPU sender.
/// * `initial_identity` - The initial identity keypair for the TPU sender.
/// * `tpu_info_service` - Service to get TPU gossip info of leaders.
/// * `stake_map_service` - Service to get stake info of validators.
/// * `eviction_strategy` - Strategy to evict connections when needed.
/// * `leader_schedule_predictor` - Predictor for upcoming leaders.
/// * `txn_capacity` - Capacity of the transaction sender channel.
///
/// # Returns
///
/// A tuple containing the created `TpuSender` and a receiver for `TpuSenderResponse`.
/// You can drop the receiver if you don't need to handle responses.
///
/// Note: This function is `async` because it requires spawning async tasks for the TPU sender driver.
/// This function is a building block for higher-level TPU client factories.
///
#[allow(clippy::too_many_arguments)]
pub async fn create_base_tpu_client<CB>(
    config: TpuSenderConfig,
    initial_identity: Keypair,
    tpu_info_service: Arc<dyn LeaderTpuInfoService + Send + Sync>,
    stake_map_service: Arc<dyn ValidatorStakeInfoService + Send + Sync>,
    eviction_strategy: Arc<dyn ConnectionEvictionStrategy + Send + Sync>,
    leader_schedule_predictor: Arc<dyn UpcomingLeaderPredictor + Send + Sync>,
    callback: Option<CB>,
    txn_capacity: usize,
) -> TpuSender
where
    CB: TpuSenderResponseCallback,
{
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
        callback,
    );

    let TpuSenderSessionContext {
        identity_updater,
        driver_tx_sink,
        driver_join_handle: _,
    } = session;

    TpuSender {
        identity_updated: Arc::new(Mutex::new(identity_updater)),
        txn_tx: PollSender::new(driver_tx_sink),
    }
}
