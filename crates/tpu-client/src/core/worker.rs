//! Per-peer worker that sends transactions over one QUIC connection.

#[cfg(feature = "prometheus")]
use crate::prom;
use {
    crate::core::{
        response::{SendTxError, TpuSenderResponse, TpuSenderResponseCallback, TxFailed, TxSent},
        txn::TpuSenderTxn,
    },
    arc_swap::ArcSwap,
    quinn::{Connection, WriteError},
    solana_pubkey::Pubkey,
    std::{
        collections::VecDeque,
        net::SocketAddr,
        num::NonZeroUsize,
        sync::Arc,
        time::{Duration, Instant},
    },
    tokio::sync::{
        Notify,
        mpsc::{self},
    },
};

pub(crate) struct SentOk {
    pub e2e_time: Duration,
}

pub(crate) struct TxWorkerSenderHandle {
    pub(crate) remote_peer_addr: SocketAddr,
    pub(crate) connection_version: u64,
    pub(crate) sender: mpsc::Sender<TpuSenderTxn>,
    pub(crate) cancel_notify: Arc<Notify>,
}

pub(crate) struct TxWorkerMeta {
    pub(crate) remote_peer_identity: Pubkey,
}

pub(crate) struct WorkerTxnSender {
    pub(crate) tx: mpsc::Sender<TpuSenderTxn>,
    pub rtt: Arc<ArcSwap<Duration>>,
}

/// A transaction sender worker tied to a specific remote peer via a single connection.
///
/// To optimize performance for a [`quinn::Connection`], adhere to the following guidelines:
///
/// - Only one transaction sender worker/task should use the connection at a time.
/// - Transactions must be sent sequentially over the connection, not concurrently/parallel.
///
/// Rationale: Solana's use case deviates from the typical QUIC or `quinn` library design.
/// Excessive use of `quinn`'s concurrency features can lead to transaction fragmentation.
/// Counterintuitively, for Solana, minimizing fragmentation is prioritized over maximizing throughput,
/// concurrency, or parallelism, as `quinn`'s concurrency features do not enhance performance in this context.
///
/// Although it might seem appealing to use multiple streams across different Tokio tasks on the same connection,
/// benchmarks conducted by the Anza team indicate that this approach degrades performance rather than improving it.
pub(crate) struct QuicTxSenderWorker<CB> {
    pub(crate) remote_peer: Pubkey,
    pub(crate) remote_peer_addr: SocketAddr,
    pub(crate) connection: Arc<Connection>,
    /// The current client identity being used for the connection
    pub(crate) current_client_identity: Pubkey,
    pub(crate) incoming_rx: mpsc::Receiver<TpuSenderTxn>,
    pub(crate) output_tx: Option<CB>,
    pub(crate) tx_queue: VecDeque<(TpuSenderTxn, usize)>,
    pub(crate) cancel_notify: Arc<Notify>,
    pub(crate) max_tx_attempt: NonZeroUsize,
    pub(crate) txn_sent: usize,
    pub(crate) rtt: Arc<ArcSwap<Duration>>,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum TxSenderWorkerError {
    #[error(transparent)]
    ConnectionLost(#[from] quinn::ConnectionError),
    #[error("0-RTT rejected by remote peer")]
    ZeroRttRejected,
}

pub(crate) struct TxSenderWorkerCompleted {
    pub(crate) err: Option<TxSenderWorkerError>,
    pub(crate) rx: mpsc::Receiver<TpuSenderTxn>,
    pub(crate) pending_tx: VecDeque<(TpuSenderTxn, usize)>,
    pub(crate) canceled: bool,
}

impl<CB> QuicTxSenderWorker<CB>
where
    CB: TpuSenderResponseCallback,
{
    async fn send_tx(&mut self, tx: &[u8]) -> Result<SentOk, SendTxError> {
        let t = Instant::now();
        let mut uni = self.connection.open_uni().await?;
        uni.write_all(tx).await.map_err(|e| match e {
            WriteError::Stopped(var_int) => SendTxError::StreamStopped(var_int),
            WriteError::ConnectionLost(connection_error) => {
                SendTxError::ConnectionError(connection_error)
            }
            WriteError::ClosedStream => SendTxError::StreamClosed,
            WriteError::ZeroRttRejected => SendTxError::ZeroRttRejected,
        })?;
        self.txn_sent = self.txn_sent.saturating_add(1);
        let e2e_time = t.elapsed();
        let ok = SentOk { e2e_time };
        Ok(ok)
    }
    async fn process_tx(
        &mut self,
        tx: TpuSenderTxn,
        attempt: usize,
    ) -> Option<TxSenderWorkerError> {
        let result = self.send_tx(tx.wire.as_ref()).await;
        let remote_addr = self.remote_peer_addr;
        let tx_info = tx.info;
        match result {
            Ok(sent_ok) => {
                tracing::debug!(
                    "Tx sent to remote peer: {} in {:?}",
                    self.remote_peer,
                    sent_ok.e2e_time
                );
                let resp = TxSent {
                    remote_peer_identity: self.remote_peer,
                    remote_peer_addr: remote_addr,
                    info: tx_info,
                };
                if let Some(callback) = &self.output_tx {
                    callback.call(TpuSenderResponse::TxSent(resp));
                }
                #[cfg(feature = "prometheus")]
                {
                    prom::quic_send_attempts_inc(self.remote_peer, remote_addr, "success");
                    prom::incr_quic_gw_worker_tx_process_cnt(self.remote_peer, "success");
                    prom::observe_send_transaction_e2e_latency(self.remote_peer, sent_ok.e2e_time);
                }
                None
            }
            Err(e) => {
                #[cfg(feature = "prometheus")]
                {
                    prom::quic_send_attempts_inc(self.remote_peer, remote_addr, "error");
                }
                if attempt >= self.max_tx_attempt.get() {
                    #[cfg(feature = "prometheus")]
                    {
                        prom::incr_quic_gw_worker_tx_process_cnt(self.remote_peer, "error");
                    }

                    tracing::warn!(
                        "Giving up sending transaction to remote peer: {}, client identity: {}, after {} attempts, {} txn sent so far: {:?}",
                        self.remote_peer,
                        self.current_client_identity,
                        attempt,
                        self.txn_sent,
                        e
                    );
                    let resp = TxFailed {
                        remote_peer_identity: self.remote_peer,
                        remote_peer_addr: self.remote_peer_addr,
                        failure_reason: e.to_string(),
                        info: tx_info,
                    };
                    if let Some(callback) = &self.output_tx {
                        callback.call(TpuSenderResponse::TxFailed(resp));
                    }
                } else {
                    tracing::trace!(
                        "Retrying to send transaction to remote peer: {} after {} attempts: {:?}",
                        self.remote_peer,
                        attempt,
                        e
                    );
                    self.tx_queue.push_back((tx, attempt + 1));
                }

                match e {
                    SendTxError::ConnectionError(connection_error) => {
                        Some(TxSenderWorkerError::ConnectionLost(connection_error))
                    }
                    SendTxError::StreamStopped(_) | SendTxError::StreamClosed => {
                        tracing::trace!(
                            "Stream stopped or closed to remote peer: {}",
                            self.remote_peer
                        );
                        None
                    }
                    SendTxError::ZeroRttRejected => {
                        tracing::warn!("0-RTT rejected by remote peer: {}", self.remote_peer);
                        Some(TxSenderWorkerError::ZeroRttRejected)
                    }
                }
            }
        }
    }

    async fn try_process_tx_in_queue(&mut self) -> Option<TxSenderWorkerError> {
        while let Some((tx, attempt)) = self.tx_queue.pop_front() {
            if let Some(e) = self.process_tx(tx, attempt).await {
                return Some(e);
            }
        }
        None
    }

    fn fetch_conn_stats(&mut self) {
        // stats acquire a mutex lock under the hood, so maybe not wisest thing to call this too frequently.
        // this function should be called sparingly to avoid performance overhead.
        let stats = self.connection.stats();
        let rtt = Arc::new(stats.path.rtt);
        self.rtt.store(rtt);

        #[cfg(feature = "prometheus")]
        {
            let current_mtu = stats.path.current_mtu;
            let path_stats = stats.path;
            prom::set_leader_mtu(self.remote_peer, current_mtu);
            prom::observe_leader_rtt(self.remote_peer, path_stats.rtt);
        }
    }

    pub(crate) async fn run(mut self) -> TxSenderWorkerCompleted {
        let mut canceled = false;
        let mut last_activity = Instant::now();
        let mut burst_timer = Box::pin(tokio::time::sleep_until(
            (last_activity + Duration::from_secs(10)).into(),
        ));
        let mut txn_recv_count = 0;
        const MAX_IDLE_DURATION: Duration = Duration::from_secs(10);
        let maybe_err = loop {
            tracing::trace!(
                "worker {} tick loop -- queue size: {}",
                self.remote_peer,
                self.tx_queue.len()
            );

            if let Some(e) = self.try_process_tx_in_queue().await {
                break Some(e);
            }
            // Every 10 seconds we will look if there is new tx to processed.
            // If not we will close the worker to free up resources.
            tokio::select! {
                maybe = self.incoming_rx.recv() => {
                    match maybe {
                        Some(tx) => {
                            last_activity = Instant::now();
                            txn_recv_count += 1;

                            if txn_recv_count % 128 == 0 {
                                self.fetch_conn_stats();
                            }

                            self.tx_queue.push_back((tx, 1));
                        }
                        None => {
                            tracing::debug!("Transaction sender inlet closed for remote peer: {:?}", self.remote_peer);
                            break None;
                        }
                    }
                }
                _ = &mut burst_timer => {
                    self.fetch_conn_stats();
                    let idle_duration = Instant::now().duration_since(last_activity);
                    tracing::debug!(
                        "Transaction sender worker for remote peer: {:?} idle for {:?}, shutting down",
                        self.remote_peer,
                        idle_duration
                    );
                    if idle_duration >= MAX_IDLE_DURATION {
                        break None;
                    } else {
                        burst_timer.as_mut().reset(( last_activity + Duration::from_secs(10) ).into());
                    }
                }
                err = self.connection.closed() => {
                    // Agave client do connection eviction and can close the connection for least used or lower staked peers.
                    break Some(err.into())
                }
                _ = self.cancel_notify.notified() => {
                    tracing::debug!("Transaction sender worker for remote peer: {:?} is canceled", self.remote_peer);
                    canceled = true;
                    break None;
                }
            }
        };

        tracing::trace!(
            "Transaction sender worker for remote peer: {:?} completed with error: {:?}, canceled: {}",
            self.remote_peer,
            maybe_err,
            canceled
        );
        TxSenderWorkerCompleted {
            err: maybe_err,
            canceled,
            rx: self.incoming_rx,
            pending_tx: self.tx_queue,
        }
    }
}
