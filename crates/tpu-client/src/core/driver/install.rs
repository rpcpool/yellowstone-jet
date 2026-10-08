//! Installing per-peer sender workers, and shutting all of them down.

use {
    super::{DriverTaskMeta, TpuSenderDriver},
    crate::core::{
        response::{TpuSenderResponse, TpuSenderResponseCallback, TxDrop, TxDropReason},
        worker::{QuicTxSenderWorker, TxWorkerMeta, TxWorkerSenderHandle},
    },
    solana_pubkey::Pubkey,
    std::{collections::VecDeque, net::SocketAddr, sync::Arc},
    tokio::sync::{
        Notify,
        mpsc::{self},
    },
};

impl<CB> TpuSenderDriver<CB>
where
    CB: TpuSenderResponseCallback + Send + Sync + 'static,
{
    ///
    /// Installs a transaction sender worker for a remote peer with the given connection.
    ///
    /// # Arguments
    ///
    /// - `remote_peer_identity`: The public key of the remote peer.
    /// - `remote_peer_addr`: The socket address of the remote peer.
    ///
    /// # Panics
    ///
    /// Panics if there is no active connection for the given remote peer address.
    /// This function assumes that a connection has already been established for the remote peer address.
    ///
    /// See [`TpuSenderDriver::spawn_connecting`] for connection establishment
    /// and see [`TpuSenderDriver::handle_connecting_result`] for handling connection results.
    ///
    pub(super) fn install_worker(
        &mut self,
        remote_peer_identity: Pubkey,
        remote_peer_addr: SocketAddr,
    ) {
        let (tx, rx) = mpsc::channel(self.config.transaction_sender_worker_channel_capacity);

        let (connection, connection_version) = match self.connection_map.get(&remote_peer_addr) {
            Some(active_conn) => (
                Arc::clone(&active_conn.conn),
                active_conn.connection_version,
            ),
            None => {
                panic!("Active connection must exist for remote peer address: {remote_peer_addr}");
            }
        };

        let output_tx = self.response_outlet.clone();
        let cancel_notify = Arc::new(Notify::new());

        let worker = QuicTxSenderWorker {
            remote_peer: remote_peer_identity,
            remote_peer_addr,
            connection,
            current_client_identity: self.identity.pubkey(),
            incoming_rx: rx,
            output_tx,
            tx_queue: self
                .tx_queues
                .remove(&remote_peer_identity)
                .unwrap_or_default(),
            cancel_notify: Arc::clone(&cancel_notify),
            max_tx_attempt: self.config.max_send_attempt,
            txn_sent: 0,
        };

        let worker_fut = worker.run();
        let ah = self.tx_worker_set.spawn(worker_fut);
        let handle = TxWorkerSenderHandle {
            remote_peer_addr,
            sender: tx,
            cancel_notify,
            connection_version,
        };
        assert!(
            self.tx_worker_handle_map
                .insert(remote_peer_identity, handle)
                .is_none()
        );
        let task_id = ah.id();
        self.tx_worker_task_meta_map.insert(
            task_id,
            TxWorkerMeta {
                remote_peer_identity,
            },
        );
        let Some(active_conn) = self.connection_map.get_mut(&remote_peer_addr) else {
            unreachable!();
        };
        let remote_peer_stake = self
            .stake_info_map
            .get_stake_info(&remote_peer_identity)
            .unwrap_or(0);
        assert!(
            active_conn
                .multiplexed_remote_peer_identity_with_stake
                .insert(remote_peer_identity, remote_peer_stake)
                .is_none(),
            "duplicate remote peer identity in active connection"
        );
        let total_remote_addr_stake: u64 = active_conn
            .multiplexed_remote_peer_identity_with_stake
            .values()
            .sum();
        self.active_staked_sorted_remote_peer_addr
            .insert(remote_peer_addr, total_remote_addr_stake);
        self.active_staked_sorted_remote_peer
            .insert(remote_peer_identity, remote_peer_stake);
        self.remote_peer_addr_watcher
            .register_watch(remote_peer_identity, remote_peer_addr);

        // MAKE SURE THIS CONNECTION IS NOT IN THE UNUSED SET ANYMORE (IF IT WERE).
        self.orphan_connection_set
            .remove(&remote_peer_addr, connection_version);

        tracing::debug!("Installed tx worker for remote peer: {remote_peer_identity}");
    }

    ///
    /// Schedules a graceful drop of all transaction workers.
    ///
    /// The scheduled task waits for all transaction workers to complete and drop their senders.
    /// All transaction workers are detached from the driver runtime and not managed anymore.
    ///
    ///
    pub(super) fn schedule_graceful_drop_all_worker(&mut self) {
        tracing::trace!("Scheduling graceful drop of all transaction workers");
        let mut tx_worker_meta = std::mem::take(&mut self.tx_worker_task_meta_map);
        // Make sure to update the endpoint usage
        let tx_worker_sender_map = std::mem::take(&mut self.tx_worker_handle_map);
        let mut tx_worker_set = std::mem::take(&mut self.tx_worker_set);
        let mut tx_queues = std::mem::take(&mut self.tx_queues);
        self.active_staked_sorted_remote_peer.clear();
        let response_outlet = self.response_outlet.clone();
        let fut = async move {
            drop(tx_worker_sender_map);
            while let Some(result) = tx_worker_set.join_next_with_id().await {
                let id = match &result {
                    Ok((id, _)) => *id,
                    Err(e) => e.id(),
                };
                let TxWorkerMeta {
                    remote_peer_identity,
                } = tx_worker_meta.remove(&id).unwrap();

                let inflight_txn = match result {
                    Ok((_, mut worker_completed)) => {
                        let mut canceled_txn = VecDeque::new();
                        while let Ok(tx) = worker_completed.rx.try_recv() {
                            canceled_txn.push_back((tx, 1));
                        }
                        canceled_txn.extend(worker_completed.pending_tx);
                        canceled_txn
                    }
                    Err(_) => VecDeque::new(),
                };

                let mut canceled_txn_queue =
                    tx_queues.remove(&remote_peer_identity).unwrap_or_default();
                canceled_txn_queue.extend(inflight_txn);
                if let Some(callback) = response_outlet.as_ref() {
                    let tx_drop = TxDrop {
                        remote_peer_identity,
                        drop_reason: TxDropReason::DriverIdentityChanged,
                        dropped_tx_vec: canceled_txn_queue,
                    };
                    callback.call(TpuSenderResponse::TxDrop(tx_drop));
                }

                tracing::trace!(
                    "graceful drop worker for remote peer: {}",
                    remote_peer_identity
                );
            }
        };

        let ah = self.tasklet.spawn(fut);
        self.tasklet_meta
            .insert(ah.id(), DriverTaskMeta::DropAllWorkers);
    }
}
