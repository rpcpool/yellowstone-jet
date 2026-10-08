//! Connection attempts to remote peers, their results, and peer address changes.

#[cfg(feature = "prometheus")]
use crate::prom;
use {
    super::{ConnectingMeta, SpawnSource, TpuSenderDriver, WaitingEviction},
    crate::core::{
        eviction::strategy::{ActiveConnection, ConnectionEviction},
        quic::{ConnectingError, ConnectingTask},
        response::{TpuSenderResponseCallback, TxDropReason},
    },
    quinn::Connection,
    solana_pubkey::Pubkey,
    std::{collections::HashSet, sync::Arc, time::Instant},
    tokio::{
        sync::Notify,
        task::{self, JoinError},
    },
};

impl<CB> TpuSenderDriver<CB>
where
    CB: TpuSenderResponseCallback + Send + Sync + 'static,
{
    ///
    /// Spawns a "connecting" task to a remote peer.
    ///
    /// this is called when a transaction is received for a remote peer which does not have a worker installed yet.
    ///
    /// # Multiplexing Note
    ///
    /// Multiple remote peer identities may share the same socket address.
    ///
    /// This method will multiplex remote peer identities over the same connection if there is already a connecting task
    /// for the same remote peer address.
    ///
    /// If a [`quinn::Connection`] already exists for the  TPU address of the provided `remote_peer_identity`,
    /// this method will directly call [`TpuSenderDriver::install_worker`] for the remote peer identity using the existing connection
    /// and add it to the multiplexed remote peer identities of the connection.
    ///
    /// ## Fragmentation edge-case caused by multiplexing
    ///
    /// Since multiple [`QuicTxSenderWorker`] may share the same [`quinn::Connection`], we have to ensure that
    /// each worker sends transactions sequentially over the connection, not concurrently.
    ///
    /// Since there is only one leader at a time in Solana, it's expected that most of the time only one remote peer identity
    /// will be active on a connection, thus fragmentation should not exists.
    ///
    /// The only occurence of fragmentation would be during leader transitions over two validators sharing the same TPU address.
    /// In this case, both remote peer identities may have pending transactions to send over the same connection during the last half of a slot.
    /// NOTE: This very edge-case will be fixed in the future.
    ///
    /// Lastly, most validators will not multiplex multiple identities over the same address, thus fragmentation should be non-existent in practice.
    ///
    pub(super) fn spawn_connecting(
        &mut self,
        remote_peer_identity: Pubkey,
        attempt: usize,
        debug_source: SpawnSource,
    ) {
        if self
            .connecting_remote_peers
            .contains_key(&remote_peer_identity)
        {
            #[cfg(feature = "prometheus")]
            {
                if debug_source == SpawnSource::NewTransaction {
                    prom::incr_quic_gw_tx_connection_cache_miss_cnt();
                }
            }
            tracing::debug!(
                "Skipping connection attempt to remote peer: {} since it is already connecting",
                remote_peer_identity
            );
            return;
        }

        if self.being_evicted_peers.contains(&remote_peer_identity) {
            tracing::debug!(
                "Skipping connection attempt to remote peer: {} since it is being evicted",
                remote_peer_identity
            );
            return;
        }

        if self
            .tx_worker_handle_map
            .contains_key(&remote_peer_identity)
        {
            tracing::debug!(
                "Skipping connection attempt to remote peer: {} since it already has a worker",
                remote_peer_identity
            );
            return;
        }

        // Check if we already have a connection for the remote peer address
        // If not, check if there is already a connecting task for the same remote peer address
        // If not, spawn a new connecting task
        let Some(remote_peer_addr) = self
            .leader_tpu_info_service
            .get_quic_dest_addr(&remote_peer_identity, self.config.tpu_port)
        else {
            self.unreachable_peer(remote_peer_identity);
            return;
        };

        if self
            .pending_connection_eviction_set
            .contains_socket_addr(&remote_peer_addr)
        {
            return;
        }

        if self.connection_map.contains_key(&remote_peer_addr) {
            // We already have an active connection for the remote peer address
            tracing::debug!(
                "Re-using existing active connection to remote peer address: {} for remote peer identity: {}",
                remote_peer_addr,
                remote_peer_identity
            );
            #[cfg(feature = "prometheus")]
            {
                if debug_source == SpawnSource::NewTransaction {
                    prom::incr_quic_gw_tx_connection_cache_hit_cnt();
                }
                prom::incr_fast_txn_worker_install_path();
            }
            self.install_worker(remote_peer_identity, remote_peer_addr);
            return;
        }

        #[cfg(feature = "prometheus")]
        {
            if debug_source == SpawnSource::NewTransaction {
                prom::incr_quic_gw_tx_connection_cache_miss_cnt();
            }
        }

        let maybe_existing_connecting_task =
            self.connecting_remote_peers_addr.get(&remote_peer_addr);

        // We need signal to wait for eviction to complete before we can proceed with the connection.
        // Otherwise, the connecting attempt may fail to bind a local port.
        let maybe_wait_for_eviction =
            if !self.has_connection_capacity() && maybe_existing_connecting_task.is_none() {
                // We need to evict a connection before we can proceed.
                let notify = Arc::new(Notify::new());
                let waiting_eviction = WaitingEviction {
                    remote_peer_addr,
                    notify: Arc::clone(&notify),
                };
                tracing::trace!(
                    "Remote peer: {} connection attempt blocked, waiting for eviction",
                    remote_peer_identity
                );
                self.connecting_blocked_by_eviction_list
                    .push_back(waiting_eviction);
                Some(notify)
            } else {
                // There is room for new connections, no need to wait for eviction.
                None
            };

        match maybe_existing_connecting_task {
            Some(existing_task_id) => {
                // There is already a connecting task for the same remote peer address.
                // Due to multiplexing, we can re-use the existing connecting task.
                tracing::info!(
                    "Re-using existing connecting task to remote peer address: {} for remote peer identity: {}",
                    remote_peer_addr,
                    remote_peer_identity
                );
                let meta = self
                    .connecting_meta
                    .get_mut(existing_task_id)
                    .expect("missing connecting meta for existing task");
                meta.multiplexed_remote_peer_identity_vec
                    .push(remote_peer_identity);

                // Just add yourself to the connecting remote peers map.
                let old = self
                    .connecting_remote_peers
                    .insert(remote_peer_identity, *existing_task_id);
                assert!(
                    old.is_none(),
                    "Remote peer should not be already connecting"
                );
            }
            None => {
                // No existing connecting task for the remote peer address, spawn a new one.
                let endpoint_idx = self.next_endpoint_idx();
                let identity = self.identity.insecure_clone();
                let max_idle_timeout = self.config.max_idle_timeout;
                let fut = ConnectingTask {
                    remote_peer_identity,
                    identity,
                    max_idle_timeout,
                    connection_timeout: self.config.connecting_timeout,
                    wait_for_eviction: maybe_wait_for_eviction,
                    endpoint: self.endpoints[endpoint_idx].clone(),
                }
                .run(remote_peer_addr);
                let meta = ConnectingMeta {
                    current_client_identity: self.identity.pubkey(),
                    multiplexed_remote_peer_identity_vec: vec![remote_peer_identity],
                    remote_peer_address: remote_peer_addr,
                    connection_attempt: attempt,
                    created_at: Instant::now(),
                };

                let abort_handle = self.connecting_tasks.spawn(fut);

                tracing::info!(
                    "Spawning connection for remote peer: {remote_peer_identity}, attempt: {attempt}, source: {debug_source:?}",
                );
                let old = self
                    .connecting_remote_peers
                    .insert(remote_peer_identity, abort_handle.id());
                assert!(
                    old.is_none(),
                    "Remote peer should not be already connecting"
                );
                self.connecting_meta.insert(abort_handle.id(), meta);
                self.connecting_remote_peers_addr
                    .insert(remote_peer_addr, abort_handle.id());
            }
        }
    }

    ///
    /// Handles the result of a connection attempt to a remote peer.
    ///
    /// Reattempts the connection if it fails, up to the maximum number of attempts, unless the peer is unreachable.
    ///
    /// If the connection is successful, this function call [`TpuSenderDriver::install_worker`] to set up a transaction sender worker for each
    /// multiplexed remote peer identity over the connection.
    ///
    pub(super) fn handle_connecting_result(
        &mut self,
        result: Result<(task::Id, Result<Connection, ConnectingError>), JoinError>,
    ) {
        match result {
            Ok((task_id, result)) => {
                #[allow(unused_variables)]
                let ConnectingMeta {
                    current_client_identity,
                    multiplexed_remote_peer_identity_vec,
                    connection_attempt,
                    remote_peer_address,
                    created_at,
                } = self
                    .connecting_meta
                    .remove(&task_id)
                    .expect("connecting_meta");
                #[cfg(feature = "prometheus")]
                {
                    prom::observe_quic_gw_connection_time(created_at.elapsed());
                }
                for remote_peer_identity in &multiplexed_remote_peer_identity_vec {
                    self.connecting_remote_peers.remove(remote_peer_identity);
                }
                self.connecting_remote_peers_addr
                    .remove(&remote_peer_address);

                if self.identity.pubkey() != current_client_identity {
                    // THIS SHOULD NOT HAPPEN SINCE ON IDENTITY CHANGE WE ABORT ALL CONNECTING TASKS
                    // BUT JUST IN CASE, WE CHECK AGAIN.
                    tracing::warn!(
                        "Abandoning connection attempt to remote peer: {multiplexed_remote_peer_identity_vec:?} since the client identity has changed"
                    );
                    for remote_peer_identity in multiplexed_remote_peer_identity_vec {
                        self.spawn_connecting(
                            remote_peer_identity,
                            connection_attempt,
                            SpawnSource::UpdateIdentity,
                        );
                    }
                    return;
                }

                match result {
                    Ok(conn) => {
                        #[cfg(feature = "prometheus")]
                        {
                            prom::incr_quic_gw_connection_success_cnt();
                        }
                        let conn = Arc::new(conn);
                        let active_connection = ActiveConnection {
                            remote_peer_addr: remote_peer_address,
                            conn: Arc::clone(&conn),
                            connection_version: self.next_connection_version(),
                            multiplexed_remote_peer_identity_with_stake: Default::default(),
                        };
                        self.connection_map
                            .insert(remote_peer_address, active_connection);
                        for remote_peer_identity in multiplexed_remote_peer_identity_vec {
                            tracing::debug!("Connected to remote peer: {:?}", remote_peer_identity);
                            self.install_worker(remote_peer_identity, remote_peer_address);
                        }
                    }
                    Err(connect_err) => {
                        #[cfg(feature = "prometheus")]
                        {
                            prom::incr_quic_gw_connection_failure_cnt();
                        }

                        match connect_err {
                            ConnectingError::ConnectError(
                                quinn::ConnectError::EndpointStopping,
                            ) => {
                                // This should never happen, but if it does, we panic.
                                // The endpoint is stopping, so we cannot connect to the remote peer.
                                panic!(
                                    "Endpoint is stopping, cannot connect to remote peer: {multiplexed_remote_peer_identity_vec:?}, with identity: {current_client_identity}"
                                );
                            }
                            ConnectingError::ConnectionError(_)
                                if connection_attempt < self.config.max_connection_attempts =>
                            {
                                // HERE'S THE CODE THE HANDLE RETRY LOGIC.
                                // NOTE: THE RETRY COUNT IS NOT INSIDE A SPECIFIC REGISTER OR MAP,
                                // IT'S STATELESS MEANING THE RETRY COUNT IS DETERMINED BY THE NUMBER OF ATTEMPTS STORED ON EACH CONNECTING TASK.
                                // EACH REATTEMPT SPAWNS A NEW CONNECTING TASK WITH ATTEMPT COUNT EQUALS TO PREVIOUS ATTEMPT COUNT + 1.
                                // AFTER REACHING MAX ATTEMPTS, THE DEFAULT MATCH BRANCH WILL HANDLE THE FAILURE CALLED "whatever".

                                tracing::info!(
                                    "Connection attempt {} to remote peer: {multiplexed_remote_peer_identity_vec:?} failed, retrying...",
                                    connection_attempt
                                );

                                //
                                for remote_peer_identity in multiplexed_remote_peer_identity_vec {
                                    let latest_remote_peer_address =
                                        self.leader_tpu_info_service.get_quic_dest_addr(
                                            &remote_peer_identity,
                                            self.config.tpu_port,
                                        );
                                    let Some(latest_remote_peer_address) =
                                        latest_remote_peer_address
                                    else {
                                        self.unreachable_peer(remote_peer_identity);
                                        continue;
                                    };
                                    let new_connection_attempt =
                                        if latest_remote_peer_address != remote_peer_address {
                                            1
                                        } else {
                                            connection_attempt.saturating_add(1)
                                        };
                                    self.spawn_connecting(
                                        remote_peer_identity,
                                        new_connection_attempt,
                                        SpawnSource::Reattempt,
                                    );
                                }
                            }
                            whatever => {
                                tracing::warn!(
                                    "Failed to connect to remote peer: {multiplexed_remote_peer_identity_vec:?}, with identity: {current_client_identity}, error: {whatever:?}"
                                );
                                for remote_peer_identity in multiplexed_remote_peer_identity_vec {
                                    self.unreachable_peer(remote_peer_identity);
                                }
                            }
                        }
                    }
                }
            }
            Err(join_err) => {
                #[cfg(feature = "prometheus")]
                {
                    prom::incr_quic_gw_connection_failure_cnt();
                }
                let ConnectingMeta {
                    current_client_identity: _,
                    multiplexed_remote_peer_identity_vec,
                    connection_attempt: _,
                    remote_peer_address,
                    created_at: _,
                } = self
                    .connecting_meta
                    .remove(&join_err.id())
                    .expect("connecting_meta");

                self.connecting_remote_peers_addr
                    .remove(&remote_peer_address);
                for remote_peer_identity in multiplexed_remote_peer_identity_vec {
                    let _ = self.connecting_remote_peers.remove(&remote_peer_identity);
                    self.drop_peer_queued_tx(
                        remote_peer_identity,
                        TxDropReason::RemotePeerUnreachable,
                    );
                    tracing::error!(
                        "Join error during connecting to {remote_peer_identity:?}: {:?}",
                        join_err
                    );
                }
            }
        }
    }

    ///
    /// Generates the next connection version number.
    ///
    /// We use connection versioning to track connection re-establishments.
    ///
    /// This is more a sanity check than anything else and not part of any core business logic.
    ///
    /// Why we are doing connection version tracking? Because this tpu sender driver is like a complex state-machine with alot of moving parts.
    /// Connections can be re-established, workers can be spawned and evicted, all happening concurrently.
    /// Making sure that a worker is using the correct connection is important to avoid subtle bugs.
    /// So we use connection versioning to `assert!` that we didn't create any orphan worker or orphan connection in the code.
    ///
    ///
    const fn next_connection_version(&mut self) -> u64 {
        let ret = self.connection_version;
        self.connection_version += 1;
        ret
    }

    ///
    /// Round-robin endpoint selection
    ///
    const fn next_endpoint_idx(&mut self) -> usize {
        let ret = self.endpoint_sequence;
        self.endpoint_sequence = (self.endpoint_sequence + 1) % self.endpoints.len();
        ret
    }

    pub(super) fn handle_remote_peer_addr_change(&mut self, remote_peers_changed: HashSet<Pubkey>) {
        for remote_peer in remote_peers_changed {
            if let Some(handle) = self.tx_worker_handle_map.get(&remote_peer) {
                // If we have a worker for the remote peer, we need to update its address.
                let maybe_new_addr = self
                    .leader_tpu_info_service
                    .get_quic_dest_addr(&remote_peer, self.config.tpu_port);
                match maybe_new_addr {
                    Some(new_addr) => {
                        if new_addr != handle.remote_peer_addr {
                            tracing::debug!(
                                "Remote peer address changed: {} from {:?} to {:?}, will cancel worker...",
                                remote_peer,
                                handle.remote_peer_addr,
                                new_addr
                            );
                            // Update the worker's remote address, and its peer too.
                            handle.cancel_notify.notify_one();
                            // WE DON'T WANT TO EVICT THE CONNECTION OR OTHER MULTIPLEXED PEERS SHARING THE SAME CONNECTION.
                            // WE CAN'T ASSUME IF A REMOTE PEER ADDRESS CHANGED, THE OTHER MULTIPLEXED PEERS ADDRESSES ALSO CHANGED.
                            // WE SIMPLY CANCEL THE WORKER, AND WHEN THE WORKER RECONNECTS, IT WILL USE THE NEW ADDRESS.

                            #[cfg(feature = "prometheus")]
                            {
                                prom::incr_quic_gw_remote_peer_addr_changes_detected();
                            }
                        }
                    }
                    None => {
                        // If we don't have a new address, we need to drop the worker.
                        handle.cancel_notify.notify_one();
                        let connection_version = handle.connection_version;
                        let connection_eviction = ConnectionEviction {
                            remote_peer_addr: handle.remote_peer_addr,
                            connection_version,
                        };
                        self.pending_connection_eviction_set
                            .insert(connection_eviction);
                        #[cfg(feature = "prometheus")]
                        {
                            prom::incr_quic_gw_remote_peer_addr_changes_detected();
                        }
                    }
                }
            }
        }
    }
}
