//! Applying identity updates received over the command channel.

#[cfg(feature = "prometheus")]
use crate::prom;
use {
    super::{SpawnSource, TpuSenderDriver},
    crate::{
        core::{
            identity_update::{
                DriverCommand, MultiStepIdentitySynchronizationCommand, UpdateIdentityCommand,
            },
            response::TpuSenderResponseCallback,
        },
        identity::TpuIdentity,
    },
    std::sync::Arc,
};

impl<CB> TpuSenderDriver<CB>
where
    CB: TpuSenderResponseCallback + Send + Sync + 'static,
{
    ///
    /// Updates the driver identity and reconnects to all remote peers with the new identity.
    ///
    /// # DANGER
    ///
    /// This function is super important to get right. Changing the identity of the driver
    /// means that all existing connections are invalidated and must be re-established.
    ///
    /// It also means we need to properly clean up all existing state associated with the old identity and prior connections.
    ///
    /// # Steps performed
    ///
    /// 1. Schedule a graceful drop of all transaction workers.
    /// 2. Clear the connection map.
    /// 3. Store inflight connecting task metadata, in order to replay them later.
    /// 4. Abort and detach all inflight connecting tasks.
    /// 5. Clear all connecting remote peers and their addresses.
    /// 6. Clear the connecting blocked by eviction list.
    /// 7. Update the identity and client certificate.
    /// 8. Replay all connecting tasks with the new identity and connecting meta stored at step #3.
    ///  
    ///
    async fn update_identity(&mut self, new_identity: TpuIdentity, barrier_like: impl Future) {
        self.schedule_graceful_drop_all_worker();
        self.connection_map.clear();
        self.orphan_connection_set.clear();
        self.being_evicted_peers.clear();
        self.pending_connection_eviction_set.clear();
        self.connecting_tasks.abort_all();
        self.connecting_tasks.detach_all();
        self.connecting_remote_peers.clear();
        self.connecting_remote_peers_addr.clear();
        self.connecting_blocked_by_eviction_list.clear();
        // active staked sroted remote peer map is clear in `schedule_graceful_drop_all_worker`
        self.active_staked_sorted_remote_peer_addr.clear();

        let connecting_meta = std::mem::take(&mut self.connecting_meta);

        self.identity = new_identity;
        self.current_identity_pubkey
            .store(Arc::new(self.identity.pubkey()));
        #[cfg(feature = "prometheus")]
        {
            prom::quic_set_identity(self.identity.pubkey());
        }

        barrier_like.await;

        connecting_meta.values().for_each(|meta| {
            meta.multiplexed_remote_peer_identity_vec
                .iter()
                .for_each(|remote_peer_identity| {
                    self.spawn_connecting(
                        *remote_peer_identity,
                        meta.connection_attempt,
                        SpawnSource::UpdateIdentity,
                    );
                });
        });

        if !connecting_meta.is_empty() {
            tracing::trace!(
                "Will auto-reconnect to {} remote peers after identity update",
                connecting_meta.len()
            );
        }

        tracing::trace!(
            "Updated tpu sender driver identity to: {}",
            self.identity.pubkey()
        );
    }

    pub(super) async fn handle_cnc(&mut self, command: DriverCommand) {
        match command {
            DriverCommand::UpdateIdentity(cmd) => {
                let UpdateIdentityCommand {
                    new_identity,
                    callback,
                } = cmd;
                let fake_barrier = async {
                    callback.callback();
                };
                self.update_identity(new_identity, fake_barrier).await;
            }
            DriverCommand::MultiStepIdentitySynchronization(
                multi_step_identity_synchronization_command,
            ) => {
                let MultiStepIdentitySynchronizationCommand {
                    new_identity,
                    barrier,
                } = multi_step_identity_synchronization_command;
                let barrier_fut = async {
                    barrier.wait().await;
                };
                self.update_identity(new_identity, barrier_fut).await;
            }
        }
    }
}
