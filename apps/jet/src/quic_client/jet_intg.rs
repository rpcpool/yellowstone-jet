use std::{net::SocketAddr, sync::Arc};

use crate::{cluster_tpu_info::ClusterTpuInfo, identity::JetIdentitySyncMember, quic_client::core::{GatewayIdentityUpdater, UpcomingLeaderPredictor, ValidatorStakeInfoService}, stake::StakeInfoMap};
use solana_keypair::Keypair;
use solana_pubkey::Pubkey;
use crate::quic_client::core::LeaderTpuInfoService;


impl LeaderTpuInfoService for ClusterTpuInfo {
    fn get_quic_tpu_socket_addr(&self, leader_pubkey: Pubkey) -> Option<SocketAddr> {
        self.get_cluster_nodes()
            .get(&leader_pubkey)
            .and_then(|node| node.tpu_quic)
    }
    fn get_quic_tpu_fwd_socket_addr(&self, leader_pubkey: Pubkey) -> Option<SocketAddr> {
        self.get_cluster_nodes()
            .get(&leader_pubkey)
            .and_then(|node| node.tpu_forwards_quic)
    }
}

impl UpcomingLeaderPredictor for ClusterTpuInfo {
    fn try_predict_next_n_leaders(&self, n: usize) -> Vec<Pubkey> {
        self.get_leader_tpus(n)
            .iter()
            .map(|info| info.leader)
            .collect()
    }
}


impl ValidatorStakeInfoService for StakeInfoMap {
    fn get_stake_info(&self, peer_pubkey: &Pubkey) -> Option<u64> {
        self.get_stake_info(*peer_pubkey)
    }
}

#[async_trait::async_trait]
impl JetIdentitySyncMember for GatewayIdentityUpdater {
    async fn pause_for_identity_update(
        &self,
        new_identity: Keypair,
        barrier: Arc<tokio::sync::Barrier>,
    ) {
        self.update_identity_with_confirmation_barrier(new_identity, barrier).await;
    }
}