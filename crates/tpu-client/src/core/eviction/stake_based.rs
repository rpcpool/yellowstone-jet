//! Default stake-based connection eviction strategy.

use {
    crate::core::{
        constants::DEFAULT_EVICTION_GRACE_DURATION,
        eviction::{
            staked_set::StakeSortedPeerSet,
            strategy::{ConnectionEvictionStrategy, RemotePeerAddrMap},
        },
    },
    solana_pubkey::Pubkey,
    std::{
        collections::{HashMap, HashSet},
        time::{Duration, Instant},
    },
};

///
/// An eviction strategy that evicts the lowest staked remote peer first,
/// unless it has been used recently.
///
/// # Multiplexing Note
///
/// Because multiple remote peer identities may share the same socket address,
/// evicting one remote peer identity will also terminate the connection for all other
/// remote peer identities sharing the same socket address.
///
/// This strategy takes that into account by evicting groups of remote peer identities
/// multiplexed over the same socket address.
///
/// Because of this its recommended to use [`ConnectionEvictionStrategy::plan_eviction_with_addr_map`]
///
/// # Grace Period
///
/// This strategy applies a grace period to avoid evicting remote peers that have been used recently.
///
#[derive(Debug)]
pub struct StakeBasedEvictionStrategy {
    ///
    /// The duration of inactivity after which a remote peer is considered elligible for eviction.
    ///
    pub peer_idle_eviction_grace_period: Duration,
}

impl Default for StakeBasedEvictionStrategy {
    fn default() -> Self {
        Self {
            peer_idle_eviction_grace_period: DEFAULT_EVICTION_GRACE_DURATION,
        }
    }
}

impl ConnectionEvictionStrategy for StakeBasedEvictionStrategy {
    fn plan_eviction(
        &self,
        now: Instant,
        ss_identies: &StakeSortedPeerSet,
        usage_table: &HashMap<Pubkey, Instant>,
        already_evicting: &HashSet<Pubkey>,
        plan_ahead_size: usize,
    ) -> Vec<Pubkey> {
        if ss_identies.is_empty() {
            tracing::warn!("No active connections to evict");
            return Vec::new();
        }

        // We always evict to lowest staked remote peer first unless it has been used recently.
        // However, if the there only one connection available to evict, we evict it regardless of its stake and last usage.
        let plan = ss_identies
            .iter()
            .filter(|(_, peer)| !already_evicting.contains(peer))
            .filter(|(_, peer)| {
                let Some(last_usage) = usage_table.get(peer) else {
                    return true;
                };
                let elapsed = now.saturating_duration_since(*last_usage);
                elapsed >= self.peer_idle_eviction_grace_period
            })
            .take(plan_ahead_size)
            .map(|(_, peer)| peer)
            .collect::<Vec<_>>();

        if plan.is_empty() {
            // Or else we don't care, just evict the lowest-staked peer.
            ss_identies
                .iter()
                .filter(|(_, peer)| !already_evicting.contains(peer))
                .take(plan_ahead_size)
                .map(|(_, peer)| peer)
                .collect()
        } else {
            plan
        }
    }

    ///
    /// Plan up to `plan_ahead_size` [`quinn::Connection`] to evicts, with additional context from remote peer address map.
    ///
    fn plan_eviction_with_addr_map(
        &self,
        now: Instant,
        _ss_identies: &StakeSortedPeerSet,
        usage_table: &HashMap<Pubkey, Instant>,
        already_evicting: &HashSet<Pubkey>,
        plan_ahead_size: usize,
        remote_peer_address_map: &RemotePeerAddrMap<'_>,
    ) -> Vec<Pubkey> {
        let mut proposed_eviction = Vec::with_capacity(plan_ahead_size);

        remote_peer_address_map
            .staked_sorted_remote_peer_groups()
            .filter_map(|group| {
                for (peer, _peer_stake) in group.iter() {
                    if already_evicting.contains(peer) {
                        return None;
                    }
                    let Some(last_usage) = usage_table.get(peer) else {
                        continue;
                    };
                    let elapsed = now.saturating_duration_since(*last_usage);
                    if elapsed < self.peer_idle_eviction_grace_period {
                        return None;
                    }
                }
                Some(group)
            })
            .take(plan_ahead_size)
            .for_each(|group| {
                for (peer, _peer_stake) in group.iter() {
                    proposed_eviction.push(*peer);
                }
            });
        tracing::trace!("Planned eviction peers len: {:?}", proposed_eviction.len());
        if proposed_eviction.is_empty() {
            // Or else we don't care, just evict the lowest-staked peer group.
            remote_peer_address_map
                .staked_sorted_remote_peer_groups()
                .filter_map(|group| {
                    for (peer, _peer_stake) in group.iter() {
                        if already_evicting.contains(peer) {
                            return None;
                        }
                    }
                    Some(group)
                })
                .take(plan_ahead_size)
                .for_each(|group| {
                    for (peer, _peer_stake) in group.iter() {
                        proposed_eviction.push(*peer);
                    }
                });
        }
        proposed_eviction
    }
}

#[cfg(test)]
mod stake_based_eviction_strategy_test {
    use {
        super::{ConnectionEvictionStrategy, StakeSortedPeerSet},
        crate::core::eviction::{
            staked_set::StakedSortedSet,
            strategy::{ConnectionMap, RemotePeerAddrMap},
        },
        solana_pubkey::Pubkey,
        std::{
            collections::HashMap,
            net::SocketAddr,
            time::{Duration, Instant},
        },
    };

    #[test]
    fn it_should_evict_lowest_stake_peer() {
        let strategy = super::StakeBasedEvictionStrategy {
            // We put no grace period for this test.
            peer_idle_eviction_grace_period: Duration::ZERO,
        };

        let mut active_staked_sorted_remote_peer = StakeSortedPeerSet::default();
        let mut last_peer_activity = std::collections::HashMap::new();

        let peer1 = Pubkey::new_unique();
        let peer2 = Pubkey::new_unique();
        let peer3 = Pubkey::new_unique();

        active_staked_sorted_remote_peer.insert(peer1, 100);
        active_staked_sorted_remote_peer.insert(peer2, 50);
        active_staked_sorted_remote_peer.insert(peer3, 10);

        last_peer_activity.insert(peer1, std::time::Instant::now());
        last_peer_activity.insert(peer2, std::time::Instant::now());
        last_peer_activity.insert(peer3, std::time::Instant::now());

        let eviction_plan = strategy.plan_eviction(
            Instant::now(),
            &active_staked_sorted_remote_peer,
            &last_peer_activity,
            &Default::default(), // max connections to evict
            1,
        );
        // It should propose to evict the lowest staked peer
        assert_eq!(eviction_plan.len(), 1);
        assert!(eviction_plan.contains(&peer3));
    }

    #[test]
    fn it_should_take_into_account_usage_grace_period() {
        let now = Instant::now();
        let grace_period = Duration::from_secs(1);
        let strategy = super::StakeBasedEvictionStrategy {
            // We put no grace period for this test.
            peer_idle_eviction_grace_period: grace_period,
        };

        let mut active_staked_sorted_remote_peer = StakeSortedPeerSet::default();
        let mut last_peer_activity = std::collections::HashMap::new();

        let peer1 = Pubkey::new_unique();
        let peer2 = Pubkey::new_unique();
        let peer3 = Pubkey::new_unique();

        active_staked_sorted_remote_peer.insert(peer1, 100);
        active_staked_sorted_remote_peer.insert(peer2, 50);
        active_staked_sorted_remote_peer.insert(peer3, 10);

        last_peer_activity.insert(peer1, now - grace_period);
        last_peer_activity.insert(peer2, now - grace_period);
        // Lets make the lowest staked peer seems recently active.
        // this should prevent it from being evicted.
        last_peer_activity.insert(peer3, now);

        let eviction_plan = strategy.plan_eviction(
            now,
            &active_staked_sorted_remote_peer,
            &last_peer_activity,
            &Default::default(), // max connections to evict
            1,
        );
        // It should propose to evict the 2nd lowest staked peer
        // Since the `peer3` has been used recently.
        tracing::trace!("Eviction plan: {:?}", eviction_plan);
        assert_eq!(eviction_plan.len(), 1);
        assert!(eviction_plan.contains(&peer2));
    }

    #[test]
    fn it_should_pick_lowest_stake_peer_if_all_peers_have_been_recently_used() {
        let now = Instant::now();
        let grace_period = Duration::from_secs(100);
        let strategy = super::StakeBasedEvictionStrategy {
            // We put no grace period for this test.
            peer_idle_eviction_grace_period: grace_period,
        };

        let mut active_staked_sorted_remote_peer = StakeSortedPeerSet::default();
        let mut last_peer_activity = std::collections::HashMap::new();

        let peer1 = Pubkey::new_unique();
        let peer2 = Pubkey::new_unique();
        let peer3 = Pubkey::new_unique();

        active_staked_sorted_remote_peer.insert(peer1, 100);
        active_staked_sorted_remote_peer.insert(peer2, 50);
        active_staked_sorted_remote_peer.insert(peer3, 10);

        // Sets all peers as recently used
        last_peer_activity.insert(peer1, now);
        last_peer_activity.insert(peer2, now);
        last_peer_activity.insert(peer3, now);

        let eviction_plan = strategy.plan_eviction(
            Instant::now(),
            &active_staked_sorted_remote_peer,
            &last_peer_activity,
            &Default::default(), // max connections to evict
            1,
        );
        // It should propose to evict the lowest staked peer
        assert_eq!(eviction_plan.len(), 1);
        assert!(eviction_plan.contains(&peer3));
    }

    #[test]
    fn it_should_pick_lowest_multiplexed_staked() {
        let addr1: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let addr2: SocketAddr = "127.0.0.1:8081".parse().unwrap();

        let peer1 = Pubkey::new_unique();
        let peer2 = Pubkey::new_unique();
        let peer3 = Pubkey::new_unique();

        let multiplexed_gr1 = HashMap::from_iter([(peer1, 1), (peer2, 1000)]);
        let multiplexed_gr2 = HashMap::from_iter([(peer3, 500)]);

        let connection_map =
            HashMap::from_iter([(addr1, multiplexed_gr1), (addr2, multiplexed_gr2)]);

        let mut staked_sorted_address_set = StakedSortedSet::default();
        staked_sorted_address_set.insert(addr1, 1001); // 1 + 1000
        staked_sorted_address_set.insert(addr2, 500);
        let connection_map = ConnectionMap::Test(&connection_map);
        let remote_addr_map = RemotePeerAddrMap {
            connection_map,
            staked_sorted_address_set: &staked_sorted_address_set,
        };

        let strategy = super::StakeBasedEvictionStrategy {
            // We put no grace period for this test.
            peer_idle_eviction_grace_period: Duration::ZERO,
        };

        let active_staked_sorted_remote_peer = StakeSortedPeerSet::default();
        let last_peer_activity = std::collections::HashMap::new();

        let now = Instant::now();
        let actual = strategy.plan_eviction_with_addr_map(
            now,
            &active_staked_sorted_remote_peer,
            &last_peer_activity,
            &Default::default(), // max connections to evict
            1,
            &remote_addr_map,
        );

        // It should propose to evict the lowest multiplexed staked address (addr2)
        assert_eq!(actual.len(), 1);
        assert!(actual.contains(&peer3));
    }
}
