//! The transaction inlet of a spawned driver: the handle callers push transactions through.

#[cfg(feature = "prometheus")]
use crate::prom;
use {
    crate::core::{
        constants::PACKET_DATA_SIZE, leader_fast_path::LeaderFastPath, txn::TpuSenderTxn,
    },
    futures::{Sink, SinkExt},
    std::{
        fmt,
        pin::Pin,
        task::{Context, Poll, ready},
    },
    tokio::sync::mpsc::{self, error::TrySendError},
    tokio_util::sync::PollSender,
};

///
/// Errors returned by [`TpuSenderDriverInlet`].
///
#[derive(Debug, thiserror::Error)]
pub enum TpuSenderDriverInletError {
    ///
    /// The driver stopped, so its transaction channel is closed. Carries the transaction back
    /// when one was being sent, so the caller can retry or log it.
    ///
    #[error("TPU sender driver has stopped")]
    DriverStopped(Option<TpuSenderTxn>),
}

impl TpuSenderDriverInletError {
    ///
    /// Takes back the transaction that failed to send, if any.
    ///
    /// # Returns
    ///
    /// The [`TpuSenderTxn`] that could not be delivered, or [`None`] if no transaction was
    /// handed over yet.
    ///
    pub fn into_txn(self) -> Option<TpuSenderTxn> {
        match self {
            Self::DriverStopped(txn) => txn,
        }
    }
}

type Result<T> = std::result::Result<T, TpuSenderDriverInletError>;

///
/// Sends transactions to a spawned TPU sender driver.
///
/// A transaction for a current or upcoming leader that already has a worker goes straight to
/// that worker, skipping the driver. Every other transaction goes through the driver.
///
/// Cloning is cheap: every clone feeds the same driver. The driver stops once every clone has
/// been dropped or closed.
///
pub struct TpuSenderDriverInlet {
    txn_tx: PollSender<TpuSenderTxn>,
    fast_path: LeaderFastPath,
    ///
    /// A transaction accepted by [`Sink::start_send`] while the driver channel was full. It is
    /// delivered by the next [`Sink::poll_ready`], [`Sink::poll_flush`] or [`Sink::poll_close`].
    ///
    pending: Option<TpuSenderTxn>,
    ///
    /// Set when [`Sink::poll_ready`] returns `Poll::Ready(Ok(()))`, cleared by
    /// [`Sink::start_send`].
    ///
    ready: bool,
}

impl TpuSenderDriverInlet {
    ///
    /// Wraps the sending half of a driver's transaction channel.
    ///
    /// # Arguments
    ///
    /// * `txn_tx` - The [`mpsc::Sender`] whose receiver the driver reads transactions from.
    /// * `fast_path` - The [`LeaderFastPath`] table the driver publishes leader workers to.
    ///
    /// # Returns
    ///
    /// A new [`TpuSenderDriverInlet`] that sends through `fast_path` when it can, and through
    /// `txn_tx` otherwise.
    ///
    pub(crate) fn new(txn_tx: mpsc::Sender<TpuSenderTxn>, fast_path: LeaderFastPath) -> Self {
        Self {
            txn_tx: PollSender::new(txn_tx),
            fast_path,
            pending: None,
            ready: false,
        }
    }

    ///
    /// Wraps a channel with a fast path that never gets published, for tests that don't spawn
    /// a driver.
    ///
    /// # Arguments
    ///
    /// * `txn_tx` - The [`mpsc::Sender`] standing in for the driver's channel.
    ///
    /// # Returns
    ///
    /// A new [`TpuSenderDriverInlet`] that always sends through `txn_tx`.
    ///
    #[cfg(test)]
    pub(crate) fn new_without_fast_path(txn_tx: mpsc::Sender<TpuSenderTxn>) -> Self {
        Self::new(txn_tx, LeaderFastPath::new())
    }

    ///
    /// Sends one transaction to the driver, waiting while the driver channel is full.
    ///
    /// A transaction for a current or upcoming leader that already has a worker goes straight
    /// to that worker; any other transaction goes through the driver. This is the same path as
    /// the [`Sink`] implementation, driven to completion.
    ///
    /// # Cancel safety
    ///
    /// Dropping the returned future before it completes may drop `txn` without sending it. If
    /// `txn` was already accepted into this inlet's one-slot buffer, it is still delivered by
    /// the next call to [`TpuSenderDriverInlet::send`] or by flushing or closing the inlet.
    ///
    /// # Arguments
    ///
    /// * `txn` - The [`TpuSenderTxn`] to send.
    ///
    /// # Returns
    ///
    /// `Ok(())` once `txn` is in a worker's channel or the driver's channel.
    ///
    /// # Errors
    ///
    /// [`TpuSenderDriverInletError::DriverStopped`] if the driver has stopped. It carries `txn`
    /// back when the transaction itself could not be delivered.
    ///
    pub async fn send(&mut self, txn: TpuSenderTxn) -> Result<()> {
        SinkExt::send(self, txn).await
    }

    ///
    /// Delivers the buffered transaction, if any, through the driver channel.
    ///
    /// # Arguments
    ///
    /// * `cx` - The task [`Context`] to wake once the driver channel has room.
    ///
    /// # Returns
    ///
    /// `Poll::Ready(Ok(()))` once nothing is buffered, or `Poll::Pending` while the driver
    /// channel is full.
    ///
    /// # Errors
    ///
    /// [`TpuSenderDriverInletError::DriverStopped`] with the buffered transaction if the driver
    /// stopped.
    ///
    fn poll_send_pending(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        if self.pending.is_none() {
            return Poll::Ready(Ok(()));
        }
        if ready!(self.txn_tx.poll_reserve(cx)).is_err() {
            return Poll::Ready(Err(TpuSenderDriverInletError::DriverStopped(
                self.pending.take(),
            )));
        }
        let txn = self.pending.take().expect("pending transaction");
        Poll::Ready(
            self.txn_tx
                .send_item(txn)
                .map_err(|e| TpuSenderDriverInletError::DriverStopped(e.into_inner())),
        )
    }
}

impl Clone for TpuSenderDriverInlet {
    fn clone(&self) -> Self {
        Self {
            txn_tx: self.txn_tx.clone(),
            fast_path: self.fast_path.clone(),
            pending: None,
            ready: false,
        }
    }
}

impl fmt::Debug for TpuSenderDriverInlet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TpuSenderDriverInlet")
            .field("txn_tx", &self.txn_tx)
            .field("pending", &self.pending.is_some())
            .finish_non_exhaustive()
    }
}

///
/// Follows the usual [`Sink`] contract: call [`Sink::poll_ready`] until it returns
/// `Poll::Ready(Ok(()))`, then hand over exactly one transaction with [`Sink::start_send`].
/// Calling [`Sink::start_send`] without that readiness panics.
///
/// [`Sink::start_send`] tries the leader fast path first, then the driver channel. If the
/// driver channel is full, the transaction is buffered and [`Sink::poll_ready`] stays pending
/// until it is delivered, so at most one transaction per inlet clone waits in the buffer.
///
/// [`Sink::poll_flush`] delivers the buffered transaction. [`Sink::poll_close`] delivers it,
/// then closes this clone's side of the driver channel without waiting for the driver to drain
/// or stop.
///
impl Sink<TpuSenderTxn> for TpuSenderDriverInlet {
    type Error = TpuSenderDriverInletError;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        ready!(self.poll_send_pending(cx))?;
        let driver_stopped = self.txn_tx.get_ref().is_none_or(|tx| tx.is_closed());
        if driver_stopped {
            return Poll::Ready(Err(TpuSenderDriverInletError::DriverStopped(None)));
        }
        self.ready = true;
        Poll::Ready(Ok(()))
    }

    fn start_send(mut self: Pin<&mut Self>, txn: TpuSenderTxn) -> Result<()> {
        assert!(
            std::mem::take(&mut self.ready),
            "start_send called before poll_ready returned Ready"
        );

        // Oversized transactions go to the driver, which drops or accepts them per its config.
        let txn = if txn.wire.len() <= PACKET_DATA_SIZE {
            match self.fast_path.try_send(txn) {
                Ok(()) => {
                    #[cfg(feature = "prometheus")]
                    prom::incr_inlet_txn_fast_path();
                    return Ok(());
                }
                Err(txn) => txn,
            }
        } else {
            txn
        };

        let Some(driver_tx) = self.txn_tx.get_ref() else {
            return Err(TpuSenderDriverInletError::DriverStopped(Some(txn)));
        };
        let result = match driver_tx.try_send(txn) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(txn)) => {
                self.pending = Some(txn);
                Ok(())
            }
            Err(TrySendError::Closed(txn)) => {
                Err(TpuSenderDriverInletError::DriverStopped(Some(txn)))
            }
        };
        #[cfg(feature = "prometheus")]
        if result.is_ok() {
            prom::incr_inlet_txn_slow_path();
        }
        result
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.poll_send_pending(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        ready!(self.poll_send_pending(cx))?;
        self.txn_tx.close();
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{TpuSenderDriverInlet, TpuSenderDriverInletError},
        crate::core::{leader_fast_path::LeaderFastPath, txn::TpuSenderTxn},
        solana_pubkey::Pubkey,
        tokio::sync::mpsc::{self, error::TryRecvError},
    };

    fn txn(remote_peer: Pubkey) -> TpuSenderTxn {
        TpuSenderTxn::from_owned(remote_peer, b"txn".to_vec(), None)
    }

    #[tokio::test]
    async fn send_goes_through_driver_when_leader_has_no_worker() {
        let (driver_tx, mut driver_rx) = mpsc::channel(4);
        let mut inlet = TpuSenderDriverInlet::new_without_fast_path(driver_tx);
        let peer = Pubkey::new_unique();

        inlet.send(txn(peer)).await.expect("send");

        assert_eq!(driver_rx.recv().await.expect("recv").remote_peer, peer);
    }

    #[tokio::test]
    async fn send_goes_straight_to_published_leader_worker() {
        let (driver_tx, mut driver_rx) = mpsc::channel(4);
        let (worker_tx, mut worker_rx) = mpsc::channel(4);
        let peer = Pubkey::new_unique();
        let fast_path = LeaderFastPath::new();
        fast_path.publish([(peer, worker_tx)]);
        let mut inlet = TpuSenderDriverInlet::new(driver_tx, fast_path);

        inlet.send(txn(peer)).await.expect("send");

        assert_eq!(worker_rx.try_recv().expect("recv").remote_peer, peer);
        assert!(matches!(driver_rx.try_recv(), Err(TryRecvError::Empty)));
    }

    #[tokio::test]
    async fn send_errs_when_driver_stopped() {
        let (driver_tx, driver_rx) = mpsc::channel(4);
        let mut inlet = TpuSenderDriverInlet::new_without_fast_path(driver_tx);
        drop(driver_rx);

        let err = inlet
            .send(txn(Pubkey::new_unique()))
            .await
            .expect_err("driver stopped");
        assert!(matches!(err, TpuSenderDriverInletError::DriverStopped(_)));
    }
}
