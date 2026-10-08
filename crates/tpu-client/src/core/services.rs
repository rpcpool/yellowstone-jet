//! Services the driver queries: validator stake, leader TPU addresses and upcoming leaders.

use {
    crate::config::{TpuOverrideInfo, TpuPortKind},
    humantime_serde::re::humantime::Duration,
    solana_pubkey::Pubkey,
    std::net::SocketAddr,
};

///
/// Base trait for predicting upcoming leaders in the Solana cluster.
///
pub trait UpcomingLeaderPredictor {
    ///
    /// Tries to predict the next `n` leaders based on the current leader.
    ///
    fn try_predict_next_n_leaders(&self, n: usize) -> Vec<Pubkey>;
}

///
/// A dummy upcoming leader predictor that does not predict any leaders.
///
#[derive(Debug, Default)]
pub struct IgnorantLeaderPredictor;

impl UpcomingLeaderPredictor for IgnorantLeaderPredictor {
    fn try_predict_next_n_leaders(&self, _n: usize) -> Vec<Pubkey> {
        Vec::new()
    }
}

pub trait ValidatorStakeInfoService {
    ///
    /// Gets the stake info for a given validator pubkey.
    ///
    fn get_stake_info(&self, validator_pubkey: &Pubkey) -> Option<u64>;
}

pub trait LeaderTpuInfoService {
    fn get_quic_tpu_socket_addr(&self, leader_pubkey: &Pubkey) -> Option<SocketAddr>;
    fn get_quic_tpu_fwd_socket_addr(&self, leader_pubkey: &Pubkey) -> Option<SocketAddr>;
    fn get_quic_dest_addr(
        &self,
        leader_pubkey: &Pubkey,
        tpu_port_kind: TpuPortKind,
    ) -> Option<SocketAddr> {
        match tpu_port_kind {
            TpuPortKind::Normal => self.get_quic_tpu_socket_addr(leader_pubkey),
            TpuPortKind::Forwards => self.get_quic_tpu_fwd_socket_addr(leader_pubkey),
        }
    }
}

///
/// A service that overrides TPU information for specific peers.
///
pub struct OverrideTpuInfoService<I> {
    pub override_vec: Vec<TpuOverrideInfo>,
    pub other: I,
}

impl<I> LeaderTpuInfoService for OverrideTpuInfoService<I>
where
    I: LeaderTpuInfoService,
{
    fn get_quic_tpu_socket_addr(&self, leader_pubkey: &Pubkey) -> Option<SocketAddr> {
        self.override_vec
            .iter()
            .find(|info| &info.remote_peer == leader_pubkey)
            .map(|info| info.quic_tpu)
            .or_else(|| self.other.get_quic_tpu_socket_addr(leader_pubkey))
    }
    fn get_quic_tpu_fwd_socket_addr(&self, leader_pubkey: &Pubkey) -> Option<SocketAddr> {
        self.override_vec
            .iter()
            .find(|info| &info.remote_peer == leader_pubkey)
            .map(|info| info.quic_tpu_forward)
            .or_else(|| self.other.get_quic_tpu_fwd_socket_addr(leader_pubkey))
    }
}

#[cfg(test)]
mod leader_tpu_info_service_test {
    use {
        crate::{
            config::{TpuOverrideInfo, TpuPortKind},
            core::{LeaderTpuInfoService, OverrideTpuInfoService},
        },
        solana_pubkey::Pubkey,
        std::{
            collections::HashMap,
            net::SocketAddr,
            sync::{Arc, RwLock},
        },
    };

    #[derive(Debug, Clone)]
    struct TpuInfo {
        normal: SocketAddr,
        fwd: SocketAddr,
    }

    #[derive(Clone, Debug, Default)]
    struct FakeTpuInfoService {
        // Fake implementation details
        inner: Arc<RwLock<HashMap<Pubkey, TpuInfo>>>,
    }

    impl LeaderTpuInfoService for FakeTpuInfoService {
        fn get_quic_tpu_socket_addr(&self, leader_pubkey: &Pubkey) -> Option<SocketAddr> {
            self.inner
                .read()
                .unwrap()
                .get(leader_pubkey)
                .map(|info| info.normal)
        }

        fn get_quic_tpu_fwd_socket_addr(&self, leader_pubkey: &Pubkey) -> Option<SocketAddr> {
            self.inner
                .read()
                .unwrap()
                .get(leader_pubkey)
                .map(|info| info.fwd)
        }
    }

    impl FakeTpuInfoService {
        fn from_iter<IT>(it: IT) -> Self
        where
            IT: IntoIterator<Item = (Pubkey, TpuInfo)>,
        {
            let mut inner = HashMap::default();
            for (pubkey, info) in it {
                inner.insert(pubkey, info);
            }
            Self {
                inner: Arc::new(RwLock::new(inner)),
            }
        }
    }

    #[test]
    fn test_override_tpu_info() {
        // Test the override functionality of the TPU info service
        let pk1 = Pubkey::new_unique();
        let pk1_tpu_info = TpuInfo {
            normal: "127.0.0.1:8000".parse().unwrap(),
            fwd: "127.0.0.1:8001".parse().unwrap(),
        };

        let pk2 = Pubkey::new_unique();
        let pk2_tpu_info = TpuInfo {
            normal: "127.0.0.1:8002".parse().unwrap(),
            fwd: "127.0.0.1:8003".parse().unwrap(),
        };

        let service = FakeTpuInfoService::from_iter(vec![(pk1, pk1_tpu_info), (pk2, pk2_tpu_info)]);

        let override_svc = OverrideTpuInfoService {
            other: service.clone(),
            override_vec: vec![TpuOverrideInfo {
                remote_peer: pk1,
                quic_tpu: "127.0.0.1:9000".parse().unwrap(),
                quic_tpu_forward: "127.0.0.1:9001".parse().unwrap(),
            }],
        };

        let actual_fwd = override_svc.get_quic_dest_addr(&pk1, TpuPortKind::Forwards);
        let actual_normal = override_svc.get_quic_dest_addr(&pk1, TpuPortKind::Normal);
        assert_eq!(actual_normal, Some("127.0.0.1:9000".parse().unwrap()));
        assert_eq!(actual_fwd, Some("127.0.0.1:9001".parse().unwrap()));

        // It should not override anything if there is no override spec
        let actual_fwd = override_svc.get_quic_dest_addr(&pk2, TpuPortKind::Forwards);
        let actual_normal = override_svc.get_quic_dest_addr(&pk2, TpuPortKind::Normal);
        assert_eq!(actual_normal, Some("127.0.0.1:8002".parse().unwrap()));
        assert_eq!(actual_fwd, Some("127.0.0.1:8003".parse().unwrap()));

        // It should work with empty override spec too
        let override_svc = OverrideTpuInfoService {
            other: service.clone(),
            override_vec: vec![],
        };

        let actual_fwd = override_svc.get_quic_dest_addr(&pk1, TpuPortKind::Forwards);
        let actual_normal = override_svc.get_quic_dest_addr(&pk1, TpuPortKind::Normal);
        assert_eq!(actual_normal, Some("127.0.0.1:8000".parse().unwrap()));
        assert_eq!(actual_fwd, Some("127.0.0.1:8001".parse().unwrap()));
    }
}
