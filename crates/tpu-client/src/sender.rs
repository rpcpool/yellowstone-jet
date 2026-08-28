use {
    crate::{
        config::TpuSenderConfig,
        core::{
            ConnectionEvictionStrategy, LeaderTpuInfoService, TpuSenderDriverSpawner,
            TpuSenderIdentityUpdater, TpuSenderResponse, TpuSenderResponseCallback,
            TpuSenderSessionContext, TpuSenderTxn, UpcomingLeaderPredictor, UpdateIdentity,
            ValidatorStakeInfoService,
        },
        identity::TpuIdentity,
    },
    futures::{Sink, SinkExt},
    std::{
        panic::{AssertUnwindSafe, catch_unwind},
        pin::Pin,
        sync::Arc,
        task::{Context, Poll, ready},
    },
    tokio::sync::{broadcast, mpsc::UnboundedSender},
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
    identity_updater: TpuSenderIdentityUpdater,
    txn_tx: PollSender<TpuSenderTxn>,
}

impl From<TpuSender> for PollTpuSender {
    fn from(sender: TpuSender) -> Self {
        Self::new(sender)
    }
}

#[derive(Debug, thiserror::Error)]
#[error("disconnected")]
pub struct TpuSenderError(Option<TpuSenderTxn>);

impl TpuSenderError {
    pub fn into_inner(self) -> Option<TpuSenderTxn> {
        self.0
    }
}

impl TpuSender {
    ///
    /// Sends a transaction to the TPU sender task.
    ///
    /// # Arguments
    ///
    /// - `txn`: The [`TpuSenderTxn`] to send
    ///
    /// # Returns
    ///
    /// `Ok(())` once the transaction has been handed off to the TPU sender task, or
    /// `Err(TpuSenderError)` if the TPU sender task is disconnected. On error, the transaction is
    /// recoverable via `TpuSenderError::into_inner()`.
    ///
    /// # Notes
    ///
    /// This function does not send anything until the returned future is polled (e.g. via
    /// `.await`).
    ///
    pub async fn send_txn(&mut self, txn: TpuSenderTxn) -> Result<(), TpuSenderError> {
        self.txn_tx
            .send(txn)
            .await
            .map_err(|e| TpuSenderError(e.into_inner()))
    }

    ///
    /// Updates the identity used by the TPU sender.
    ///
    pub fn update_identity(&mut self, new_identity: TpuIdentity) -> UpdateIdentity {
        self.identity_updater.update_identity(new_identity)
    }

    pub fn get_owned_identity_updater(&self) -> TpuSenderIdentityUpdater {
        self.identity_updater.clone()
    }

    ///
    /// Builds a [`TpuSender`] backed by a plain in-memory channel of the given capacity, for
    /// tests that need a working [`TpuSender`]/[`PollTpuSender`] without spawning the real TPU
    /// sender driver. Returns the paired receiver so tests can observe what gets sent.
    ///
    #[cfg(test)]
    pub(crate) fn new_test(
        channel_capacity: usize,
    ) -> (Self, tokio::sync::mpsc::Receiver<TpuSenderTxn>) {
        let (tx, rx) = tokio::sync::mpsc::channel(channel_capacity);
        let sender = Self {
            identity_updater: TpuSenderIdentityUpdater::new_test_disconnected(),
            txn_tx: PollSender::new(tx),
        };
        (sender, rx)
    }
}

///
/// Base factory function to create a TPU sender and its response receiver.
///
/// # Arguments
///
/// * `config` - Configuration for the TPU sender.
/// * `initial_identity` - The initial [`TpuIdentity`] for the TPU sender.
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
    initial_identity: TpuIdentity,
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
        identity_updater,
        txn_tx: PollSender::new(driver_tx_sink),
    }
}

impl TpuSenderResponseCallback for UnboundedSender<TpuSenderResponse> {
    fn call(&self, response: TpuSenderResponse) {
        let _ = self.send(response);
    }
}

impl TpuSenderResponseCallback for broadcast::Sender<TpuSenderResponse> {
    fn call(&self, response: TpuSenderResponse) {
        let _ = self.send(response);
    }
}

///
/// A poll-based, non-owning-future variant of [`TpuSender`] for sending transactions.
///
/// [`TpuSender::send_txn`] returns a future that borrows `&mut TpuSender` for its
/// whole lifetime, which is awkward to embed inside a caller's own hand-rolled `Future`/`Sink`
/// impl (e.g. one that first has to resolve a leader's TPU address, or fan a single transaction
/// out to several destinations before it can be considered "sent"). `PollTpuSender` exposes the
/// same underlying reservation as two free-standing methods, [`poll_reserve`] and [`send_item`],
/// so callers can drive the reserve/send steps manually from inside their own `poll_*` methods
/// and interleave arbitrary work (address resolution, fan-out, blocklist checks, ...) in between.
///
/// It also implements [`futures::Sink`], so it can be used anywhere a `Sink<TpuSenderTxn>` is
/// expected without the caller having to manage the reserve/send protocol directly.
///
/// # Protocol
///
/// [`poll_reserve`] and [`send_item`] follow the same contract as [`Sink::poll_ready`] /
/// [`Sink::start_send`]: a caller **must** call [`poll_reserve`] and get back
/// `Poll::Ready(Ok(()))` before calling [`send_item`]. Calling [`send_item`] without a prior
/// successful [`poll_reserve`] is a programmer error and **panics**. Each successful
/// [`poll_reserve`] reserves capacity for exactly one subsequent [`send_item`] call.
///
/// # Cloning
///
/// `PollTpuSender` is `Clone` (it wraps a `Clone`-able [`TpuSender`]), but a capacity reservation
/// obtained via [`poll_reserve`] on one clone is *not* visible to another clone: each clone polls
/// its own underlying channel sender, so interleaving reserve/send calls across clones of the
/// same `PollTpuSender` will panic on [`send_item`] just like polling from unrelated tasks would.
/// Keep the reserve/send pair on a single clone.
///
/// [`poll_reserve`]: PollTpuSender::poll_reserve
/// [`send_item`]: PollTpuSender::send_item
///
#[derive(Clone)]
pub struct PollTpuSender {
    inner: TpuSender,
}

impl PollTpuSender {
    ///
    /// Wraps an existing [`TpuSender`] to expose its poll-based reserve/send API.
    ///
    pub const fn new(inner: TpuSender) -> Self {
        Self { inner }
    }

    ///
    /// Reserves capacity in the underlying transaction channel for one [`send_item`] call.
    ///
    /// [`send_item`]: PollTpuSender::send_item
    ///
    /// # Returns
    ///
    /// - `Poll::Pending` if the channel is currently full. The task is registered to be woken
    ///   once capacity frees up.
    /// - `Poll::Ready(Ok(()))` once capacity has been reserved. The caller must follow up with
    ///   exactly one call to [`send_item`] before calling [`poll_reserve`] again.
    /// - `Poll::Ready(Err(_))` if the TPU sender task has disconnected. The returned
    ///   [`TpuSenderError`] carries no transaction (`into_inner()` returns `None`), since none
    ///   was consumed.
    ///
    /// [`poll_reserve`]: PollTpuSender::poll_reserve
    ///
    pub fn poll_reserve(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), TpuSenderError>> {
        let result = ready!(self.inner.txn_tx.poll_reserve(cx));
        match result {
            Ok(()) => Poll::Ready(Ok(())),
            Err(_err) => Poll::Ready(Err(TpuSenderError(None))),
        }
    }

    ///
    /// Sends `txn` into the previously reserved slot in the transaction channel.
    ///
    /// # Panics
    ///
    /// Panics with `"start_send called before poll_ready returned Ready"` if called without a
    /// prior [`poll_reserve`] call that returned `Poll::Ready(Ok(()))`. This mirrors the
    /// [`Sink::start_send`] contract, which forbids calling `start_send` before `poll_ready` has
    /// signaled readiness.
    ///
    /// # Returns
    ///
    /// - `Ok(())` if the transaction was handed off to the TPU sender task.
    /// - `Err(TpuSenderError)` if the TPU sender task disconnected before the send completed. The
    ///   error carries the transaction back via `into_inner()`, so it isn't silently dropped and
    ///   can be retried or logged by the caller.
    ///
    /// [`poll_reserve`]: PollTpuSender::poll_reserve
    ///
    pub fn send_item(&mut self, txn: TpuSenderTxn) -> Result<(), TpuSenderError> {
        let result = catch_unwind(AssertUnwindSafe(|| {
            self.inner
                .txn_tx
                .send_item(txn)
                .map_err(|e| TpuSenderError(e.into_inner()))
        }));
        match result {
            Ok(result) => result,
            Err(_) => {
                panic!("start_send called before poll_ready returned Ready");
            }
        }
    }

    ///
    /// Updates the identity used by the underlying [`TpuSender`]. See
    /// [`TpuSender::update_identity`].
    ///
    pub fn update_identity(&mut self, new_identity: TpuIdentity) -> UpdateIdentity {
        self.inner.update_identity(new_identity)
    }
}

///
/// `PollTpuSender` implements [`Sink`] by delegating straight to [`poll_reserve`] and
/// [`send_item`], so the same reserve-then-send contract applies here: [`start_send`] panics if
/// called without a preceding [`poll_ready`] that returned `Poll::Ready(Ok(()))`.
///
/// Flushing and closing are cheap: the underlying channel has no internal buffering to flush
/// (every reserved item is already handed to the channel by [`start_send`]), and closing simply
/// closes the sender half of the channel — it does not wait for the TPU sender task to drain or
/// shut down.
///
/// [`poll_reserve`]: PollTpuSender::poll_reserve
/// [`send_item`]: PollTpuSender::send_item
/// [`start_send`]: Sink::start_send
/// [`poll_ready`]: Sink::poll_ready
///
impl Sink<TpuSenderTxn> for PollTpuSender {
    type Error = TpuSenderError;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.poll_reserve(cx)
    }

    fn start_send(mut self: Pin<&mut Self>, item: TpuSenderTxn) -> Result<(), Self::Error> {
        self.send_item(item)
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        self.inner.txn_tx.close();
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use {super::*, bytes::Bytes, solana_pubkey::Pubkey};

    fn test_sender(capacity: usize) -> (PollTpuSender, tokio::sync::mpsc::Receiver<TpuSenderTxn>) {
        let (tpu_sender, rx) = TpuSender::new_test(capacity);
        (PollTpuSender::new(tpu_sender), rx)
    }

    fn sample_txn(remote_peer: Pubkey) -> TpuSenderTxn {
        TpuSenderTxn {
            wire: Bytes::from_static(b"wire-bytes"),
            remote_peer,
            info: None,
        }
    }

    #[tokio::test]
    async fn start_send_delivers_after_poll_ready() {
        let (mut sender, mut rx) = test_sender(4);
        let remote_peer = Pubkey::new_unique();

        futures::future::poll_fn(|cx| sender.poll_reserve(cx))
            .await
            .expect("poll_ready");
        sender
            .send_item(sample_txn(remote_peer))
            .expect("start_send");

        let received = rx.recv().await.expect("recv");
        assert_eq!(received.remote_peer, remote_peer);
    }

    #[tokio::test]
    #[should_panic(expected = "start_send called before poll_ready returned Ready")]
    async fn start_send_panics_without_prior_poll_ready() {
        let (mut sender, _rx) = test_sender(4);
        // Never polled `poll_ready`, so there's no reserved permit to send into.
        let _ = sender.send_item(sample_txn(Pubkey::new_unique()));
    }

    #[tokio::test]
    async fn poll_ready_errs_once_receiver_dropped() {
        let (mut sender, rx) = test_sender(1);
        drop(rx);

        let result = futures::future::poll_fn(|cx| sender.poll_reserve(cx)).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn poll_ready_is_pending_until_capacity_frees_up() {
        let (mut sender, mut rx) = test_sender(1);
        let remote_peer = Pubkey::new_unique();

        futures::future::poll_fn(|cx| sender.poll_reserve(cx))
            .await
            .expect("poll_ready");
        sender
            .send_item(sample_txn(remote_peer))
            .expect("start_send");

        // The channel's only slot is occupied by the unreceived item above, so a second
        // reservation must not resolve until the receiver drains it.
        let mut pending = futures::future::poll_fn(|cx| sender.poll_reserve(cx));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), &mut pending)
                .await
                .is_err(),
            "poll_ready should still be Pending while the channel is full"
        );

        rx.recv().await.expect("recv");
        pending
            .await
            .expect("poll_ready should resolve once capacity frees up");
    }
}
