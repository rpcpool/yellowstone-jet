use {
    crate::transactions::SendTransactionRequest,
    futures::{TryStream, TryStreamExt},
    std::{
        future::Future,
        pin::Pin,
        task::{Context, Poll, ready},
    },
    yellowstone_jet_tpu_client::yellowstone_grpc::sender::{
        PollYellowstoneTpuSender, SendErrorKind, ShieldBlockList, YellowstoneTpuSender,
    },
    yellowstone_shield_store::PolicyStore,
};

///
/// A [`TpuDrain`] is a [`Future`] that continuously drains a stream of [`SendTransactionRequest`]s and sends them to a TPU sender ([`PollYellowstoneTpuSender`]) until the stream is exhausted or an error occurs.
///
pub struct TpuDrain<St> {
    tpu_sender: PollYellowstoneTpuSender,
    source: St,
    shield_store: Option<PolicyStore>,
}

impl<St> TpuDrain<St> {
    pub const fn new(
        tpu_sender: YellowstoneTpuSender,
        source: St,
        shield_store: Option<PolicyStore>,
    ) -> Self {
        Self {
            tpu_sender: PollYellowstoneTpuSender::new(tpu_sender),
            source,
            shield_store,
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum TpuSinkError {
    #[error("slot tracker disconnected")]
    SlotTrackerDisconnected,
    #[error("managed leader schedule disconnected")]
    ManagedLeaderScheduleDisconnected,
}

impl<St> Future for TpuDrain<St>
where
    St: TryStream<Ok = SendTransactionRequest> + Unpin,
    St::Error: std::error::Error + Send + Sync + 'static,
{
    type Output = Result<(), TpuSinkError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        loop {
            // `PollYellowstoneTpuSender` requires `poll_send` to return `Ready` before every
            // `start_send_*` call -- this also drives any previously started send
            // (across all of its destinations) to completion.
            match ready!(this.tpu_sender.poll_send(cx)) {
                Ok(()) => {}
                Err(_e) => {
                    // THIS ERROR HAPPENS WHEN THE UNDERLYING TPU SENDER CLOSES.
                    // todo: maybe monitor the inner of the error once we have an alternative to lewis.
                    return Poll::Ready(Ok(()));
                }
            }

            let Some(result) = ready!(this.source.try_poll_next_unpin(cx)) else {
                return Poll::Ready(Ok(()));
            };

            let Ok(request) = result else {
                // A stream error occurred, we cannot continue processing transactions
                return Poll::Ready(Ok(()));
            };

            let start_result = match &this.shield_store {
                Some(shield_store) => {
                    let blocklist = ShieldBlockList {
                        policy_store: shield_store,
                        shield_policy_addresses: &request.policies,
                        default_return_value: true,
                    };

                    this.tpu_sender.start_send_txn_with_shield_policies(
                        request.signature,
                        request.wire_transaction,
                        blocklist,
                    )
                }
                None => this
                    .tpu_sender
                    .start_send_txn(request.signature, request.wire_transaction),
            };

            if let Err(e) = start_result {
                tracing::error!("failed to send transaction: {e}");

                match e.kind {
                    SendErrorKind::Closed => return Poll::Ready(Ok(())),
                    SendErrorKind::SlotTrackerDisconnected => {
                        return Poll::Ready(Err(TpuSinkError::SlotTrackerDisconnected));
                    }
                    SendErrorKind::ManagedLeaderScheduleDisconnected => {
                        return Poll::Ready(Err(TpuSinkError::ManagedLeaderScheduleDisconnected));
                    }
                    SendErrorKind::RemotePeerBlocked => continue,
                }
            }
        }
    }
}
