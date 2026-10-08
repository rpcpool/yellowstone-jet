//! Dropping the transactions queued for a remote peer.

#[cfg(feature = "prometheus")]
use crate::prom;
use {
    super::TpuSenderDriver,
    crate::core::response::{TpuSenderResponse, TpuSenderResponseCallback, TxDrop, TxDropReason},
    solana_pubkey::Pubkey,
};

impl<CB> TpuSenderDriver<CB>
where
    CB: TpuSenderResponseCallback + Send + Sync + 'static,
{
    ///
    /// Drops all queued transactions for a remote peer and notify the response outlet.
    ///
    pub(super) fn drop_peer_queued_tx(
        &mut self,
        remote_peer_identity: Pubkey,
        reason: TxDropReason,
    ) {
        tracing::trace!(
            "Dropping queued tx for remote peer: {} due to reason: {:?}",
            remote_peer_identity,
            reason
        );
        if let Some(tx_queues) = self.tx_queues.remove(&remote_peer_identity) {
            #[cfg(feature = "prometheus")]
            {
                let total = tx_queues.len();
                prom::incr_quic_gw_drop_tx_cnt(remote_peer_identity, total as u64);
            }
            let tx_drop = TxDrop {
                remote_peer_identity,
                drop_reason: reason.clone(),
                dropped_tx_vec: tx_queues,
            };
            if let Some(callback) = self.response_outlet.as_ref() {
                callback.call(TpuSenderResponse::TxDrop(tx_drop));
            }
        }
    }

    pub(super) fn unreachable_peer(&mut self, remote_peer_identity: Pubkey) {
        #[cfg(feature = "prometheus")]
        {
            prom::inc_quic_gw_unreachable_peer_count(remote_peer_identity);
        }
        self.drop_peer_queued_tx(remote_peer_identity, TxDropReason::RemotePeerUnreachable);
    }
}
