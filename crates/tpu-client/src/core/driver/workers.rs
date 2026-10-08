//! Routing incoming transactions to workers, and handling worker exits.

#[cfg(feature = "prometheus")]
use crate::prom;
use {
    super::{SpawnSource, TpuSenderDriver},
    crate::core::{
        constants::PACKET_DATA_SIZE,
        response::{TpuSenderResponse, TpuSenderResponseCallback, TxDrop, TxDropReason},
        txn::TpuSenderTxn,
        worker::{TxSenderWorkerCompleted, TxSenderWorkerError, TxWorkerMeta},
    },
    quinn::ConnectionError,
    std::{collections::VecDeque, time::Instant},
    tokio::{
        sync::mpsc::{self},
        task::{Id, JoinError},
    },
};

impl<CB> TpuSenderDriver<CB>
where
    CB: TpuSenderResponseCallback + Send + Sync + 'static,
{
    ///
    /// Accepts a transaction and determines how to handle it based on the remote peer's status.
    ///
    /// If a transaction sender worker exists for the remote peer, it is fowarded to it.
    /// If not, the transaction is queued for later processing and a connection attempt is scheduled.
    ///
    pub(super) fn accept_tx(&mut self, tx: TpuSenderTxn) {
        let remote_peer_identity = tx.remote_peer;
        self.last_peer_activity
            .insert(remote_peer_identity, Instant::now());

        // Check size
        if tx.wire.len() > PACKET_DATA_SIZE && !self.config.unsafe_allow_arbitrary_txn_size {
            let tx_drop = TxDrop {
                remote_peer_identity,
                drop_reason: TxDropReason::InvalidPacketSize,
                dropped_tx_vec: VecDeque::from([(tx, 1)]),
            };
            #[cfg(feature = "prometheus")]
            {
                prom::incr_quic_gw_drop_tx_cnt(remote_peer_identity, 1);
                prom::incr_invalid_txn_packet_size();
            }
            if let Some(callback) = self.response_outlet.as_ref() {
                callback.call(TpuSenderResponse::TxDrop(tx_drop));
            }
            return;
        }

        // Do I have a transaction sender worker for this remote peer?
        if let Some(handle) = self.tx_worker_handle_map.get(&remote_peer_identity) {
            // If we have an active transaction sender worker for the remote peer,
            #[cfg(feature = "prometheus")]
            {
                prom::incr_quic_gw_tx_connection_cache_hit_cnt();
            }
            match handle.sender.try_send(tx) {
                Ok(_) => {
                    #[cfg(feature = "prometheus")]
                    {
                        prom::incr_quic_gw_tx_relayed_to_worker(remote_peer_identity);
                    }
                }
                Err(e) => match e {
                    mpsc::error::TrySendError::Full(tx) => {
                        tracing::warn!(
                            "Remote peer: {:?} tx queue is full, dropping tx",
                            remote_peer_identity,
                        );
                        let txdrop = TxDrop {
                            remote_peer_identity,
                            drop_reason: TxDropReason::RateLimited,
                            dropped_tx_vec: VecDeque::from([(tx, 1)]),
                        };
                        #[cfg(feature = "prometheus")]
                        {
                            prom::incr_quic_gw_drop_tx_cnt(remote_peer_identity, 1);
                        }
                        if let Some(callback) = self.response_outlet.as_ref() {
                            callback.call(TpuSenderResponse::TxDrop(txdrop));
                        }
                    }
                    mpsc::error::TrySendError::Closed(tx) => {
                        self.tx_queues
                            .entry(remote_peer_identity)
                            .or_default()
                            .push_back((tx, 1));
                    }
                },
            }
        } else {
            #[cfg(feature = "prometheus")]
            {
                prom::incr_txn_worker_pre_installed_miss();
            }
            // We don't have any active transaction sender worker for the remote peer,
            // we need to queue the transaction and try to spawn a new connection.
            self.tx_queues
                .entry(remote_peer_identity)
                .or_default()
                .push_back((tx, 1));
            tracing::trace!("queuing tx for remote peer: {:?}", remote_peer_identity);

            // Check if we are not already connecting to this remote peer.
            // If the remote peer is already being connected, just queue the tx.
            self.spawn_connecting(remote_peer_identity, 1, SpawnSource::NewTransaction);
        }
    }

    ///
    /// One of the transaction sender worker has completed its work or failed.
    ///
    pub(super) fn handle_worker_result(
        &mut self,
        result: Result<(Id, TxSenderWorkerCompleted), JoinError>,
    ) {
        match result {
            Ok((id, mut worker_completed)) => {
                let TxWorkerMeta {
                    remote_peer_identity,
                } = self
                    .tx_worker_task_meta_map
                    .remove(&id)
                    .expect("tx worker meta");

                // We remove the stake from the remote peer address map
                self.active_staked_sorted_remote_peer
                    .remove(&remote_peer_identity);

                self.remote_peer_addr_watcher.forget(remote_peer_identity);

                let is_being_evicted = self.being_evicted_peers.remove(&remote_peer_identity);
                let worker_tx = self
                    .tx_worker_handle_map
                    .remove(&remote_peer_identity)
                    .expect("tx worker sender");

                if let Some(active_conn) = self.connection_map.get_mut(&worker_tx.remote_peer_addr)
                {
                    let active_conn_version = active_conn.connection_version;
                    let worker_conn_version = worker_tx.connection_version;
                    assert!(
                        active_conn_version == worker_conn_version,
                        "Connection version mismatch for remote peer: {remote_peer_identity}, active: {active_conn_version}, worker: {worker_conn_version}",
                    );

                    self.remove_worker_from_active_connection(
                        worker_tx.remote_peer_addr,
                        remote_peer_identity,
                        worker_conn_version,
                    );
                }
                self.last_peer_activity.remove(&remote_peer_identity);
                drop(worker_tx);

                tracing::trace!(
                    "Tx worker for remote peer: {:?} completed, err: {:?}, canceled: {}, evicted: {}",
                    remote_peer_identity,
                    worker_completed.err,
                    worker_completed.canceled,
                    is_being_evicted
                );

                // It's possible that the worker failed while having pending transactions.
                // We need to "rescue" those transactions if any and if the worker didn't fail due to fatal errors.

                let tx_to_rescue = self.tx_queues.entry(remote_peer_identity).or_default();
                while let Ok(tx) = worker_completed.rx.try_recv() {
                    tx_to_rescue.push_back((tx, 1));
                }
                while let Some((tx, attempt)) = worker_completed.pending_tx.pop_front() {
                    tx_to_rescue.push_back((tx, attempt));
                }

                let is_peer_unreachable = worker_completed
                    .err
                    .filter(|e| {
                        matches!(
                            e,
                            TxSenderWorkerError::ConnectionLost(ConnectionError::VersionMismatch)
                        )
                    })
                    .is_some();

                if worker_completed.canceled {
                    tracing::trace!("Remote peer: {remote_peer_identity} tx worker was canceled");
                }

                #[cfg(feature = "prometheus")]
                {
                    prom::incr_quic_gw_connection_close_cnt();
                }

                if is_peer_unreachable {
                    // If the peer is unreachable, we drop all queued transactions for it.
                    self.unreachable_peer(remote_peer_identity);
                } else if is_being_evicted {
                    // If the worker was schedule for eviction, we simply drop all queued transactions
                    // because evicted workers are not expected to reconnect soon since they have been
                    // chosen to be eviction strategy. We don't want to be stuck in a loop
                    // where we keep evicting the same peer, so we drop the queued transactions.
                    // and start from a clean slate.
                    tracing::trace!(
                        "Remote peer: {} tx worker was canceled, will not reconnect",
                        remote_peer_identity
                    );
                    self.drop_peer_queued_tx(
                        remote_peer_identity,
                        TxDropReason::RemotePeerBeingEvicted,
                    );
                } else if !tx_to_rescue.is_empty() {
                    // If the worker didn't have a fatal error and was not evicted and still has queued transactions,
                    // we can safely reattempt to connect to the remote peer.
                    // This can happen to transient network errors or remote peer being temporarily unavailable.
                    // THIS CAN ALSO HAPPEN IF THE REMOTE PEER CHANGED ITS ADDRESS.
                    // We can safely resume connection and try to send the queued transactions.
                    tracing::trace!(
                        "Remote peer: {} has queued tx, wil reconnect",
                        remote_peer_identity
                    );
                    self.last_peer_activity
                        .insert(remote_peer_identity, Instant::now());
                    self.spawn_connecting(remote_peer_identity, 1, SpawnSource::Rescue);
                } else {
                    // Worker returned without error, no work to do, all done.
                }
            }
            Err(join_err) => {
                let id = join_err.id();
                if let Some(meta) = self.tx_worker_task_meta_map.remove(&id) {
                    panic!(
                        "Join error during tx sender worker {} ended: {:?}",
                        meta.remote_peer_identity, join_err
                    );
                }
            }
        }
    }
}
