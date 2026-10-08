use {
    crate::{cluster_tpu_info::ClusterTpuInfo, stake::StakeInfoMap},
    solana_pubkey::Pubkey,
    std::{mem::MaybeUninit, net::SocketAddr},
    yellowstone_jet_tpu_client::core::{
        LeaderTpuInfoService, UpcomingLeaderPredictor, ValidatorStakeInfoService,
    },
};

impl LeaderTpuInfoService for ClusterTpuInfo {
    fn get_quic_tpu_socket_addr(&self, leader_pubkey: &Pubkey) -> Option<SocketAddr> {
        self.get_rpc_contact_info(leader_pubkey)
            .and_then(|node| node.tpu_quic)
    }
    fn get_quic_tpu_fwd_socket_addr(&self, leader_pubkey: &Pubkey) -> Option<SocketAddr> {
        self.get_rpc_contact_info(leader_pubkey)
            .and_then(|node| node.tpu_forwards_quic)
    }
}

impl UpcomingLeaderPredictor for ClusterTpuInfo {
    fn try_predict_next_n_leader_inclusive(&self, out: &mut [MaybeUninit<Pubkey>]) -> usize {
        // `get_leader_tpus(k)` yields the current leader plus `k` upcoming ones.
        let leaders = self
            .get_leader_tpus(out.len().saturating_sub(1))
            .into_iter()
            .map(|info| info.leader);
        let mut written = 0;
        for (dst, leader) in out.iter_mut().zip(leaders) {
            dst.write(leader);
            written += 1;
        }
        written
    }
}

impl ValidatorStakeInfoService for StakeInfoMap {
    fn get_stake_info(&self, peer_pubkey: &Pubkey) -> Option<u64> {
        self.get_stake_info(*peer_pubkey)
    }
}
