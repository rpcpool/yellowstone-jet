//! Handle and commands for changing the driver's identity at runtime.

use {
    crate::identity::TpuIdentity,
    arc_swap::ArcSwap,
    core::fmt,
    futures::task::AtomicWaker,
    solana_pubkey::Pubkey,
    std::{
        future,
        sync::{Arc, atomic::AtomicU8},
        task::{Poll, ready},
    },
    tokio::sync::Barrier,
    tokio_util::sync::PollSender,
};

pub(crate) struct UpdateIdentityCallback {
    shared: Option<Arc<UpdateIdentityInner>>,
}

impl Drop for UpdateIdentityCallback {
    fn drop(&mut self) {
        if let Some(shared) = self.shared.take() {
            shared.state.store(
                UpdateIdentityInner::CANCELED,
                std::sync::atomic::Ordering::Release,
            );
            shared.waker.wake();
        }
    }
}

impl UpdateIdentityCallback {
    pub(crate) fn callback(mut self) {
        if let Some(shared) = self.shared.take() {
            shared.state.store(
                UpdateIdentityInner::TRUE,
                std::sync::atomic::Ordering::Release,
            );
            shared.waker.wake();
        }
    }
}

///
/// Inner part of the update identity command.
///
pub(crate) struct UpdateIdentityCommand {
    pub(crate) new_identity: TpuIdentity,
    pub(crate) callback: UpdateIdentityCallback,
}

pub(crate) struct MultiStepIdentitySynchronizationCommand {
    pub(crate) new_identity: TpuIdentity,
    pub(crate) barrier: Arc<Barrier>,
}

///
/// Command to control driver behavior.
///
pub(crate) enum DriverCommand {
    UpdateIdentity(UpdateIdentityCommand),
    MultiStepIdentitySynchronization(MultiStepIdentitySynchronizationCommand),
}

impl fmt::Debug for DriverCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DriverCommand::UpdateIdentity(cmd) => f
                .debug_struct("UpdateIdentity")
                .field("new_identity", &cmd.new_identity.pubkey())
                .finish(),
            DriverCommand::MultiStepIdentitySynchronization(cmd) => f
                .debug_struct("MultiStepIdentitySynchronization")
                .field("new_identity", &cmd.new_identity.pubkey())
                .finish(),
        }
    }
}

///
/// Handle to update the identity used by the TPU sender driver.
///
#[derive(Clone)]
pub struct TpuSenderIdentityUpdater {
    ///
    /// Command-and-control channel to send command to the QUIC driver
    ///
    pub(crate) cnc_tx: PollSender<DriverCommand>,

    ///
    /// Read-only handle onto the driver's current identity public key. The driver is the only
    /// writer; see [`TpuSenderIdentityUpdater::current_identity`].
    ///
    pub(crate) current_identity_pubkey: Arc<ArcSwap<Pubkey>>,
}

///
/// All the updater API is set a "mut" concurrent identity update.
///
impl TpuSenderIdentityUpdater {
    ///
    /// Returns the driver's current identity public key.
    ///
    /// This reads a shared, lock-free cell the driver updates whenever its identity changes --
    /// it does not round-trip through the command-and-control channel, so it reflects whatever
    /// the driver has *applied* so far, not necessarily an in-flight [`Self::update_identity`]
    /// that hasn't completed yet.
    ///
    pub fn current_identity(&self) -> Pubkey {
        *self.current_identity_pubkey.load_full()
    }

    ///
    /// Changes the configured identity in the QUIC driver
    ///
    pub fn update_identity(&mut self, identity: TpuIdentity) -> UpdateIdentity {
        let shared = UpdateIdentityInner {
            state: AtomicU8::new(UpdateIdentityInner::FALSE),
            waker: AtomicWaker::new(),
        };
        let shared = Arc::new(shared);
        let callback = UpdateIdentityCallback {
            shared: Some(Arc::clone(&shared)),
        };
        let cmd = UpdateIdentityCommand {
            new_identity: identity,
            callback,
        };
        let cnc_tx = self.cnc_tx.clone();

        UpdateIdentity {
            inner: shared,
            state: UpdateIdentityState::Init { cnc_tx, cmd },
        }
    }

    ///
    /// Changes the configured identity in the QUIC driver,
    ///
    /// waiting on the provided barrier before resuming driver operations.
    ///
    /// # Parameters
    ///
    /// - `identity`: The new identity to set in the driver.
    /// - `barrier`: An `Arc<Barrier>` that the driver will wait on before resuming operations.
    ///
    pub async fn update_identity_with_confirmation_barrier(
        &self,
        identity: TpuIdentity,
        barrier: Arc<Barrier>,
    ) {
        let cmd = MultiStepIdentitySynchronizationCommand {
            new_identity: identity,
            barrier,
        };
        let mut cnc_tx = self.cnc_tx.clone();
        future::poll_fn(|cx| cnc_tx.poll_reserve(cx))
            .await
            .expect("disconnected");
        cnc_tx
            .send_item(DriverCommand::MultiStepIdentitySynchronization(cmd))
            .expect("disconnected");
    }

    ///
    /// Builds a [`TpuSenderIdentityUpdater`] backed by an already-closed channel, for use in
    /// tests that only need a placeholder value (e.g. to construct a [`TpuSender`](crate::sender::TpuSender))
    /// and don't exercise identity updates.
    ///
    #[cfg(test)]
    pub(crate) fn new_test_disconnected() -> Self {
        let (cnc_tx, _cnc_rx) = tokio::sync::mpsc::channel(1);
        Self {
            cnc_tx: PollSender::new(cnc_tx),
            current_identity_pubkey: Arc::new(ArcSwap::new(Arc::new(Pubkey::default()))),
        }
    }
}

///
/// The shared state used to notify the completion of the identity update.
/// See [`UpdateIdentity`] for more details.
struct UpdateIdentityInner {
    state: AtomicU8,
    waker: AtomicWaker,
}

// impl Drop for UpdateIdentityInner {
//     fn drop(&mut self) {
//         // If the future is dropped before the identity update is completed, we need to notify the driver to cancel the update.
//         let last_state = self.state.load(std::sync::atomic::Ordering::Acquire);
//         if last_state == UpdateIdentityInner::TRUE || last_state == UpdateIdentityInner::CANCELED_FLAG {
//             // The update has already completed or been canceled, no need to do anything.
//             return;
//         }
//         self.state
//             .store(UpdateIdentityInner::CANCELED_FLAG, std::sync::atomic::Ordering::SeqCst);
//         self.waker.wake();
//     }
// }

impl UpdateIdentityInner {
    const FALSE: u8 = 0;
    const TRUE: u8 = 1;
    // the last bit is used to indicate that the update has been canceled, and the future should return an error.
    const CANCELED: u8 = 0x80;
}

#[allow(clippy::large_enum_variant)]
enum UpdateIdentityState {
    Init {
        cnc_tx: PollSender<DriverCommand>,
        cmd: UpdateIdentityCommand,
    },
    Closed,
    WaitingForCompletion,
}

///
/// Future that waits for the identity update to complete.
/// This future is used to ensure that the identity update is completed before proceeding.
///
pub struct UpdateIdentity {
    inner: Arc<UpdateIdentityInner>,
    state: UpdateIdentityState,
}

impl UpdateIdentity {
    const fn take_state(&mut self) -> UpdateIdentityState {
        std::mem::replace(&mut self.state, UpdateIdentityState::Closed)
    }
}

#[derive(Debug, thiserror::Error)]
#[error("tpu sender runtime closed")]
pub struct UpdateIdentityError;

impl Future for UpdateIdentity {
    type Output = Result<(), UpdateIdentityError>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.get_mut();

        let (result, next_state) = match this.take_state() {
            UpdateIdentityState::Init { mut cnc_tx, cmd } => {
                match ready!(cnc_tx.poll_reserve(cx)) {
                    Ok(()) => {
                        match cnc_tx.send_item(DriverCommand::UpdateIdentity(cmd)) {
                            Ok(()) => {
                                this.inner.waker.register(cx.waker());
                                // Between the time we send and the registration, the driver may have already completed the update and set the atomic bool.
                                // So we need to immediately wake the waker, so that the next poll will check the atomic bool and return ready if the update is already complete.
                                cx.waker().wake_by_ref();
                                (None, UpdateIdentityState::WaitingForCompletion)
                            }
                            Err(_) => (Some(Err(UpdateIdentityError)), UpdateIdentityState::Closed),
                        }
                    }
                    Err(_) => (Some(Err(UpdateIdentityError)), UpdateIdentityState::Closed),
                }
            }
            UpdateIdentityState::WaitingForCompletion => {
                match this.inner.state.load(std::sync::atomic::Ordering::Relaxed) {
                    UpdateIdentityInner::TRUE => (Some(Ok(())), UpdateIdentityState::Closed),
                    UpdateIdentityInner::FALSE => (None, UpdateIdentityState::WaitingForCompletion),
                    UpdateIdentityInner::CANCELED => {
                        (Some(Err(UpdateIdentityError)), UpdateIdentityState::Closed)
                    }
                    _ => panic!("Unexpected state"),
                }
            }
            UpdateIdentityState::Closed => {
                panic!("UpdateIdentity polled after completion");
            }
        };

        this.state = next_state;
        if let Some(result) = result {
            Poll::Ready(result)
        } else {
            Poll::Pending
        }
    }
}

#[cfg(test)]
mod test {
    use {
        super::{DriverCommand, TpuSenderIdentityUpdater, UpdateIdentityCommand},
        crate::identity::TpuIdentity,
        arc_swap::ArcSwap,
        solana_keypair::Keypair,
        solana_pubkey::Pubkey,
        std::{sync::Arc, time::Duration},
        tokio::sync::mpsc,
        tokio_util::sync::PollSender,
    };

    #[tokio::test]
    async fn update_identity_should_return_error_if_dropped_before_completion() {
        let (cnc_tx, cnc_rx) = mpsc::channel(10);
        let mut updater: TpuSenderIdentityUpdater = TpuSenderIdentityUpdater {
            cnc_tx: PollSender::new(cnc_tx),
            current_identity_pubkey: Arc::new(ArcSwap::new(Arc::new(Pubkey::default()))),
        };

        let identity = TpuIdentity::from_keypair(&Keypair::new());
        let mut update_fut = updater.update_identity(identity.insecure_clone());
        let xs = futures::poll!(&mut update_fut);
        assert!(xs.is_pending());
        drop(updater);
        drop(cnc_rx);
        let result = update_fut.await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_update_identity_fut() {
        let (cnc_tx, mut cnc_rx) = mpsc::channel(10);
        let mut updater = TpuSenderIdentityUpdater {
            cnc_tx: PollSender::new(cnc_tx),
            current_identity_pubkey: Arc::new(ArcSwap::new(Arc::new(Pubkey::default()))),
        };

        let jh = tokio::spawn(async move {
            let DriverCommand::UpdateIdentity(UpdateIdentityCommand {
                new_identity,
                callback,
            }) = cnc_rx.recv().await.unwrap()
            else {
                panic!("Expected UpdateIdenttiy command");
            };
            tokio::time::sleep(Duration::from_secs(2)).await;
            // This can be relaxed because `wake` hides `Released` memory barrier.
            callback.callback();
            new_identity
        });

        let identity = TpuIdentity::from_keypair(&Keypair::new());
        updater
            .update_identity(identity.insecure_clone())
            .await
            .unwrap();

        let actual = jh.await.unwrap();
        assert_eq!(actual.pubkey(), identity.pubkey())
    }
}
