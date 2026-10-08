//! Publishing the leader fast-path table, and reading back the activity it records.

use {
    super::TpuSenderDriver, crate::core::response::TpuSenderResponseCallback, solana_pubkey::Pubkey,
};

impl<CB> TpuSenderDriver<CB>
where
    CB: TpuSenderResponseCallback + Send + Sync + 'static,
{
    ///
    /// Republishes the fast-path table if `remote_peer` is one of the tracked leaders, e.g.
    /// after its worker was installed or removed.
    ///
    /// # Arguments
    ///
    /// * `remote_peer` - The peer whose worker changed.
    ///
    pub(super) fn refresh_fast_path_if_leader(&self, remote_peer: &Pubkey) {
        if self.fast_path_leaders.contains(remote_peer) {
            self.refresh_fast_path();
        }
    }

    ///
    /// Copies the last fast-path send time of each tracked leader into `last_peer_activity`,
    /// so the eviction strategy sees traffic that skipped [`TpuSenderDriver::accept_tx`].
    ///
    pub(super) fn merge_fast_path_activity(&mut self) {
        let last_peer_activity = &mut self.last_peer_activity;
        self.fast_path.for_each_last_used(|remote_peer, last_used| {
            last_peer_activity
                .entry(remote_peer)
                .and_modify(|seen| *seen = (*seen).max(last_used))
                .or_insert(last_used);
        });
    }

    ///
    /// Publishes the tracked leaders in `fast_path_leaders` that currently have a worker.
    ///
    pub(super) fn refresh_fast_path(&self) {
        let workers = self.fast_path_leaders.iter().filter_map(|remote_peer| {
            self.tx_worker_handle_map
                .get(remote_peer)
                .map(|handle| (*remote_peer, handle.sender.clone()))
        });
        self.fast_path.publish(workers);
    }
}
