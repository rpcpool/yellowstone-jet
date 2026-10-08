//! The connection eviction strategy trait and the connection view it plans against.

use {
    crate::core::eviction::staked_set::{StakeSortedPeerSet, StakedSortedSet},
    quinn::Connection,
    solana_pubkey::Pubkey,
    std::{
        collections::{HashMap, HashSet},
        net::SocketAddr,
        sync::Arc,
        time::Instant,
    },
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ConnectionEviction {
    ///
    /// The remote peer identity being evicted.
    ///
    pub remote_peer_addr: SocketAddr,
    ///
    /// The connection version being evicted.
    ///
    pub connection_version: u64,
}

pub struct ActiveConnection {
    pub(crate) remote_peer_addr: SocketAddr,
    pub(crate) conn: Arc<Connection>,
    pub(crate) connection_version: u64,
    pub(crate) multiplexed_remote_peer_identity_with_stake: HashMap<Pubkey, u64>,
}

///
/// Base trait for connection eviction strategy.
///
/// Connection eviction is called when the QUIC driver does not have a local port available
/// to use for new QUIC connections.
///
///
pub trait ConnectionEvictionStrategy {
    ///
    /// Plan up to `plan_ahead_size` [`quinn::Connection`] to evicts.
    ///
    /// # Arguments
    ///
    /// `now`: the current time, can be used by the strategy to apply grace period if it supports it.
    /// `ss_identites`: a sorted set of remote pubkeys currently connected to.
    /// `usage_table`: A lookup table from remote peer identity to last time a transaction was routed to.
    /// `evicting_masq` : a set of pubkey already schedule for evicting, may overlap with `ss_identities`.
    /// The resulted plan should not include any of `already_evicting`.
    /// `plan_ahead_size` : how far ahead should the strategy plan ahead future evictions.
    ///
    /// Returns:
    ///
    /// A list of [`quinn::Connection`] to evict in order of evicting priority.
    ///
    /// Post Conditions:
    ///
    /// 0 <= eviction plan length <= `plan_ahead_size`.
    ///
    fn plan_eviction(
        &self,
        now: Instant,
        ss_identies: &StakeSortedPeerSet,
        usage_table: &HashMap<Pubkey, Instant>,
        evicting_masq: &HashSet<Pubkey>,
        plan_ahead_size: usize,
    ) -> Vec<Pubkey>;

    ///
    /// Plan up to `plan_ahead_size` [`quinn::Connection`] to evicts, with additional context from remote peer address map.
    ///
    /// This default implementation simply calls [`Self::plan_eviction`].
    ///
    /// Strategies that need additional context from remote peer address map should override this method.
    ///
    /// # Arguments
    ///
    /// `now`: the current time, can be used by the strategy to apply grace period if it supports it.
    /// `ss_identies`: a sorted set of remote pubkeys currently connected to.
    /// `usage_table`: A lookup table from remote peer identity to last time a transaction was routed to.
    /// `evicting_masq` : a set of pubkey already schedule for evicting, may overlap with `ss_identities`.
    /// The resulted plan should not include any of `already_evicting`.
    /// `plan_ahead_size` : how far ahead should the strategy plan ahead future evictions.
    /// `remote_peer_address_map`: A [`RemotePeerAddrMap`] that provides information about remote peer addresses and their connections.
    ///
    /// # Returns
    ///
    /// A list of remote peer identity to evict in order of evicting priority.
    ///
    /// # Multiplexing Note
    ///
    /// Multiple remote peer identities may share the same socket address.
    /// Evicting one remote peer identity will also terminate the connection for all other the remote peer identities sharing the same socket address.
    ///
    /// See [`StakeBasedEvictionStrategy`](crate::core::StakeBasedEvictionStrategy) for an example of eviction strategy that uses this method.
    ///
    #[allow(unused_variables)]
    fn plan_eviction_with_addr_map(
        &self,
        now: Instant,
        ss_identies: &StakeSortedPeerSet,
        usage_table: &HashMap<Pubkey, Instant>,
        already_evicting: &HashSet<Pubkey>,
        plan_ahead_size: usize,
        remote_peer_address_map: &RemotePeerAddrMap<'_>,
    ) -> Vec<Pubkey> {
        self.plan_eviction(
            now,
            ss_identies,
            usage_table,
            already_evicting,
            plan_ahead_size,
        )
    }
}

///
/// A map that provides information about remote peer addresses and their connections.
///
/// # Note
///
/// This struct is used to provide additional context to eviction strategies that need to know.
/// You can get the address of a peer, the connected stake at an address, and iterate over staked sorted remote peer groups.
///
pub struct RemotePeerAddrMap<'a> {
    pub(crate) connection_map: ConnectionMap<'a>,
    pub(crate) staked_sorted_address_set: &'a StakedSortedSet<SocketAddr>,
}

pub(crate) enum ConnectionMap<'a> {
    Quinn(&'a HashMap<SocketAddr, ActiveConnection>),
    // Only use for test
    #[allow(dead_code)]
    Test(&'a HashMap<SocketAddr, HashMap<Pubkey, u64>>),
}

impl ConnectionMap<'_> {
    fn get_peer_stake_mapping(&self, addr: &SocketAddr) -> Option<&HashMap<Pubkey, u64>> {
        match self {
            ConnectionMap::Quinn(map) => map
                .get(addr)
                .map(|active_conn| &active_conn.multiplexed_remote_peer_identity_with_stake),
            ConnectionMap::Test(map) => map.get(addr),
        }
    }
}

///
/// A group of remote peer identities multiplexed over the same socket address.
///
/// # Note
///
/// Multiple remote peer identities may share the same socket address.
/// This struct represents such a group, along with their associated stake values.
///
pub struct MultiplexedPeerGroup<'a> {
    pub socket_addr: SocketAddr,
    group: &'a HashMap<Pubkey, u64>,
}

impl MultiplexedPeerGroup<'_> {
    ///
    /// Gets the total stake of all remote peer identities in the group.
    ///
    pub fn total_stake(&self) -> u64 {
        self.group.values().sum()
    }

    ///
    /// Yields iterator over the remote peer identities and their stake in the group.
    ///
    pub fn iter(&self) -> impl Iterator<Item = (&Pubkey, &u64)> {
        self.group.iter()
    }
}

impl RemotePeerAddrMap<'_> {
    ///
    /// Gets the total connected stake at a given socket address.
    ///
    /// # Multiplexing Note
    ///
    /// Multiple remote peer identities may share the same socket address.
    /// The total connected stake is the sum of the stake of all remote peer identities
    ///
    pub fn get_connected_stake_at_addr(&self, addr: &SocketAddr) -> Option<u64> {
        self.connection_map
            .get_peer_stake_mapping(addr)
            .map(|peer_stake_map| peer_stake_map.values().sum())
    }

    ///
    /// Yields instance of [`MultiplexedPeerGroup`] in order of ascending stake.
    ///
    /// A [`MultiplexedPeerGroup`] represents a group of remote peer identities multiplexed over the same socket address.
    ///
    /// The stake of each group is the sum of the stake of all remote peer identities in the group.
    ///
    /// # Eviction Strategy Usage
    ///
    /// This function can be used by eviction strategies to make informed decisions based on the stake distribution across different remote peer groups.
    /// Because multiple remote peer may share the same socket address, evicting one remote peer identity will also terminate the connection for all other
    /// remote peer identities sharing the same socket address.
    ///
    /// If a remote connection host peers with various stake levels, evicting the lowest staked peer may not be optimal.
    ///
    pub fn staked_sorted_remote_peer_groups(
        &self,
    ) -> impl Iterator<Item = MultiplexedPeerGroup<'_>> + '_ {
        let iter = Box::new(self.staked_sorted_address_set.iter());
        struct MyIterator<'a> {
            iter: Box<dyn Iterator<Item = (u64, SocketAddr)> + 'a>,
            connection_map: &'a ConnectionMap<'a>,
        }

        impl<'a> Iterator for MyIterator<'a> {
            type Item = MultiplexedPeerGroup<'a>;

            #[allow(clippy::while_let_on_iterator)]
            fn next(&mut self) -> Option<Self::Item> {
                while let Some((_, addr)) = self.iter.next() {
                    tracing::trace!("Yielding multiplexed peer group at address: {}", addr);
                    if let Some(peer_stake_map) = self.connection_map.get_peer_stake_mapping(&addr)
                    {
                        return Some(MultiplexedPeerGroup {
                            socket_addr: addr,
                            group: peer_stake_map,
                        });
                    }
                }
                None
            }
        }
        MyIterator {
            iter,
            connection_map: &self.connection_map,
        }
    }
}

#[cfg(test)]
mod test_remote_peer_addr_map {

    use {
        super::RemotePeerAddrMap,
        crate::core::eviction::{staked_set::StakedSortedSet, strategy::ConnectionMap},
        solana_pubkey::Pubkey,
        std::{collections::HashMap, net::SocketAddr},
    };

    #[test]
    fn test_empty_map() {
        let connection_map = HashMap::<SocketAddr, HashMap<Pubkey, u64>>::new();
        let connection_map = ConnectionMap::Test(&connection_map);
        let staked_sorted_address_set = StakedSortedSet::default();
        let remote_addr_map = RemotePeerAddrMap {
            connection_map,
            staked_sorted_address_set: &staked_sorted_address_set,
        };
        let actual = remote_addr_map.staked_sorted_remote_peer_groups().count();
        assert_eq!(actual, 0);
    }

    #[test]
    fn test_remote_peer_addr_map_sort_order() {
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

        let actual_multiplexed_stake = remote_addr_map.get_connected_stake_at_addr(&addr1).unwrap();
        let actual_multiplexed_stake2 =
            remote_addr_map.get_connected_stake_at_addr(&addr2).unwrap();

        assert_eq!(actual_multiplexed_stake, 1001);
        assert_eq!(actual_multiplexed_stake2, 500);

        // See if the sort is correct
        let actual_sorted_address = remote_addr_map
            .staked_sorted_remote_peer_groups()
            .map(|group| group.socket_addr)
            .collect::<Vec<_>>();

        let expected_sorted_address = vec![addr2, addr1]; // addr2 has less stake than addr1
        assert_eq!(actual_sorted_address, expected_sorted_address);
    }
}
