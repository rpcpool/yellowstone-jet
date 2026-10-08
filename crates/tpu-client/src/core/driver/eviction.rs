//! Connection capacity checks and eviction of active and orphan connections.

#[cfg(feature = "prometheus")]
use crate::prom;
use {
    super::{TpuSenderDriver, WaitingEviction, connection_set::OrphanConnectionInfo},
    crate::core::{
        eviction::strategy::{ConnectionEviction, ConnectionMap, RemotePeerAddrMap},
        response::TpuSenderResponseCallback,
    },
    quinn::VarInt,
    solana_pubkey::Pubkey,
    std::{net::SocketAddr, sync::Arc, time::Instant},
};

impl<CB> TpuSenderDriver<CB>
where
    CB: TpuSenderResponseCallback + Send + Sync + 'static,
{
    pub(super) fn has_connection_capacity(&self) -> bool {
        self.connection_map.len() < self.max_concurrent_connection()
    }

    const fn max_concurrent_connection(&self) -> usize {
        self.config.max_concurrent_connection
    }

    ///
    /// Evicts a remote peer connection based on the stake and last activity.
    ///
    /// Since highly staked remote peers are more likely to be re-used in the future,
    /// we evict the lowest staked remote peer connection first, unless it has been used recently.
    ///
    pub(super) fn do_eviction_if_required(&mut self) {
        let eviction_count_required = self
            .connecting_blocked_by_eviction_list
            .len()
            .saturating_sub(self.pending_connection_eviction_set.len());

        if eviction_count_required == 0 {
            tracing::trace!("No eviction required at this time");
            return;
        }
        tracing::info!(
            "Eviction required for {} connections",
            eviction_count_required
        );
        let connection_map = ConnectionMap::Quinn(&self.connection_map);
        let addr_map = RemotePeerAddrMap {
            connection_map,
            staked_sorted_address_set: &self.active_staked_sorted_remote_peer_addr,
        };

        let mut eviction_plan = self.eviction_strategy.plan_eviction_with_addr_map(
            Instant::now(),
            &self.active_staked_sorted_remote_peer,
            &self.last_peer_activity,
            &self.being_evicted_peers,
            eviction_count_required,
            &addr_map,
        );
        tracing::trace!("Eviction plan len {}", eviction_plan.len());
        if eviction_plan.is_empty() && self.pending_connection_eviction_set.is_empty() {
            // If the evictin plan is empty, pick the connection with the least amount of a active stake
            let min_staked_active_conn = self.connection_map.values().min_by_key(|active_conn| {
                active_conn
                    .multiplexed_remote_peer_identity_with_stake
                    .values()
                    .sum::<u64>()
            });
            if let Some(active_conn) = min_staked_active_conn {
                for peer in active_conn
                    .multiplexed_remote_peer_identity_with_stake
                    .keys()
                {
                    eviction_plan.push(*peer);
                }
            }
        }

        // Because of multiplexing, in order to do the connection eviction we must
        // evict all tx workers that are using the same connection.
        // For each remote peer to evict, we need to extends the eviction set to include all neighboring
        // remote peers that are multiplexed on the same connection.
        let mut cancel_handles_vec = Vec::with_capacity(eviction_plan.len());
        for peer in eviction_plan {
            if let Some(handle) = self.tx_worker_handle_map.get(&peer) {
                let remote_peer_addr = handle.remote_peer_addr;
                self.being_evicted_peers.insert(peer);
                cancel_handles_vec.push(Arc::clone(&handle.cancel_notify));
                if let Some(active_conn) = self.connection_map.get(&remote_peer_addr) {
                    if active_conn.connection_version != handle.connection_version {
                        // This means the connection has been re-established since the worker was created.
                        // So we don't need to evict other multiplexed peers on this connection.
                        continue;
                    }
                    let connection_eviction = ConnectionEviction {
                        remote_peer_addr: active_conn.remote_peer_addr,
                        connection_version: active_conn.connection_version,
                    };
                    self.pending_connection_eviction_set
                        .insert(connection_eviction);
                    for multiplexed_peer in active_conn
                        .multiplexed_remote_peer_identity_with_stake
                        .keys()
                    {
                        self.being_evicted_peers.insert(*multiplexed_peer);
                        if let Some(handle) = self.tx_worker_handle_map.get(multiplexed_peer) {
                            let cancel_notify = Arc::clone(&handle.cancel_notify);
                            cancel_handles_vec.push(cancel_notify);
                        }
                    }
                }
            }
        }

        tracing::trace!(
            "Evicting {} remote peer connections",
            cancel_handles_vec.len()
        );
        for cancel_handle in cancel_handles_vec {
            cancel_handle.notify_one();
        }
    }

    ///
    /// Unblocks a connection attempt that was waiting for eviction to complete.
    /// We do this blocking to avoid connection capacity exhaustion.
    /// Since we have a maximum number of concurrent connections that can be configured by the user.
    fn unblock_eviction_waiting_connection(&mut self) {
        let Some(WaitingEviction {
            remote_peer_addr,
            notify,
        }) = self.connecting_blocked_by_eviction_list.pop_front()
        else {
            return;
        };
        tracing::trace!(
            "Unblocking connection attempt to remote peer address: {} after eviction",
            remote_peer_addr
        );
        notify.notify_one();
    }

    ///
    /// Removes a worker from an active connection's multiplexed peer list.
    ///
    /// If the connection has no more multiplexed peers and is marked for eviction,
    /// the connection is removed from the active connection map and eviction waiters are unblocked.
    ///
    /// # Note
    ///
    /// if the connection version does not match the expected version, the removal operation is ignored, not mutation is performed.
    ///
    pub(super) fn remove_worker_from_active_connection(
        &mut self,
        remote_peer_addr: SocketAddr,
        remote_peer_identity: Pubkey,
        expected_connection_version: u64,
    ) {
        if let Some(active_conn) = self.connection_map.get_mut(&remote_peer_addr) {
            if active_conn.connection_version != expected_connection_version {
                // This means the connection has been re-established since the worker was created.
                // So we don't need to remove the multiplexed peer from this connection.
                tracing::warn!(
                    "Skipping removal of remote peer: {} from active connection at address: {} due to connection version mismatch, expected: {}, actual: {}",
                    remote_peer_identity,
                    remote_peer_addr,
                    expected_connection_version,
                    active_conn.connection_version,
                );
                return;
            }
            active_conn
                .multiplexed_remote_peer_identity_with_stake
                .remove(&remote_peer_identity);

            let connection_eviction = ConnectionEviction {
                remote_peer_addr,
                connection_version: expected_connection_version,
            };
            let is_connection_mark_for_eviction = self
                .pending_connection_eviction_set
                .contains(&connection_eviction);
            let has_no_worker = active_conn
                .multiplexed_remote_peer_identity_with_stake
                .is_empty();

            let new_stake_for_addr: u64 = active_conn
                .multiplexed_remote_peer_identity_with_stake
                .values()
                .sum();

            self.active_staked_sorted_remote_peer_addr
                .insert(remote_peer_addr, new_stake_for_addr);

            if has_no_worker {
                if is_connection_mark_for_eviction {
                    self.connection_map.remove(&remote_peer_addr);
                    self.pending_connection_eviction_set
                        .remove(&connection_eviction);
                    self.unblock_eviction_waiting_connection();
                    self.active_staked_sorted_remote_peer_addr
                        .remove(&remote_peer_addr);
                } else {
                    let orphan_conn_info = OrphanConnectionInfo {
                        remote_peer_addr,
                        connection_version: expected_connection_version,
                    };
                    self.orphan_connection_set
                        .insert(orphan_conn_info, Instant::now());
                }
            }
        }
    }

    pub(super) fn next_orphan_connection_expiration(&self) -> Option<Instant> {
        let oldest = self.orphan_connection_set.oldest()?;
        Some(oldest + self.config.orphan_connection_ttl)
    }

    pub(super) fn try_evict_orphan_connections(&mut self) {
        let now = Instant::now();
        while let Some(oldest) = self.orphan_connection_set.oldest() {
            if oldest + self.config.orphan_connection_ttl > now {
                break;
            }
            let Some(unused_conn_info_vec) = self.orphan_connection_set.pop() else {
                break;
            };
            for unused_conn_info in unused_conn_info_vec {
                let OrphanConnectionInfo {
                    remote_peer_addr,
                    connection_version,
                } = unused_conn_info;
                let Some(active_conn) = self.connection_map.get(&remote_peer_addr) else {
                    continue;
                };
                if active_conn.connection_version != connection_version {
                    // Connection has been re-established since it was marked as unused.
                    continue;
                }
                if !active_conn
                    .multiplexed_remote_peer_identity_with_stake
                    .is_empty()
                {
                    // If for some reason, the connection is still active and has multiplexed peers,
                    // this orphan entry is stale.
                    continue;
                }
                let Some(active_conn) = self.connection_map.remove(&remote_peer_addr) else {
                    continue;
                };

                assert!(
                    active_conn
                        .multiplexed_remote_peer_identity_with_stake
                        .is_empty(),
                    "Evicting connection to remote peer address: {remote_peer_addr} that still has multiplexed peers",
                );
                active_conn.conn.close(VarInt::from_u32(0), &[0u8]);
                drop(active_conn);
                self.unblock_eviction_waiting_connection();
                #[cfg(feature = "prometheus")]
                {
                    prom::incr_evicted_orphan_connections();
                }
            }
        }
    }
}
