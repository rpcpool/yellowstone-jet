//! Stake-ordered sets used to rank remote peers and their addresses.

use {
    solana_pubkey::Pubkey,
    std::collections::{BTreeMap, HashMap, HashSet},
};

pub type StakeSortedPeerSet = StakedSortedSet<Pubkey>;

///
/// A set of remote peer identities sorted by their stake value.
///
#[derive(Debug)]
pub struct StakedSortedSet<V> {
    peer_stake_map: HashMap<V, u64 /* stake */>,
    sorted_map: BTreeMap<u64 /* stake */, HashSet<V>>,
}

impl<V> Default for StakedSortedSet<V> {
    fn default() -> Self {
        Self {
            peer_stake_map: Default::default(),
            sorted_map: Default::default(),
        }
    }
}

impl<V> StakedSortedSet<V>
where
    V: Clone + Eq + std::hash::Hash,
{
    ///
    /// Removes a peer from the set.
    ///
    /// # Returns
    ///
    /// `true` if the peer was present and removed, `false` otherwise.
    ///
    pub fn remove(&mut self, peer: &V) -> bool {
        if let Some(old_stake) = self.peer_stake_map.remove(peer) {
            let mut is_entry_empty = false;
            if let Some(peers) = self.sorted_map.get_mut(&old_stake) {
                peers.remove(peer);
                is_entry_empty = peers.is_empty();
            }

            if is_entry_empty {
                self.sorted_map.remove(&old_stake);
            }

            true
        } else {
            false
        }
    }

    pub fn clear(&mut self) {
        self.peer_stake_map.clear();
        self.sorted_map.clear();
    }

    ///
    /// Inserts or updates a peer with the given stake.
    ///
    /// # Returns
    ///
    /// `true` if the peer was already present and updated, `false` if it was newly inserted.
    ///
    pub fn insert(&mut self, peer: V, stake: u64) -> bool {
        let already_present = self.remove(&peer);
        self.peer_stake_map.insert(peer.clone(), stake);
        self.sorted_map
            .entry(stake)
            .or_default()
            .insert(peer.clone());
        already_present
    }

    pub fn get(&self, peer: &V) -> Option<u64> {
        self.peer_stake_map.get(peer).copied()
    }

    ///
    /// Iterates over the set in ascending order of stake.
    ///
    pub fn iter(&self) -> impl Iterator<Item = (u64, V)> {
        self.sorted_map
            .iter()
            .flat_map(|(stake, peers)| peers.iter().map(|peer| (*stake, peer.clone())))
    }

    ///
    /// Checks if the set is empty.
    ///
    pub fn is_empty(&self) -> bool {
        self.peer_stake_map.is_empty()
    }
}

#[cfg(test)]
mod test {
    use {super::StakeSortedPeerSet, solana_pubkey::Pubkey};

    #[test]
    fn test_stake_sorted_peer() {
        let mut set = StakeSortedPeerSet::default();

        let pk1 = Pubkey::new_unique();
        let pk2 = Pubkey::new_unique();
        let pk3 = Pubkey::new_unique();
        assert!(!set.insert(pk3, 100));
        assert!(!set.insert(pk2, 10));
        assert!(!set.insert(pk1, 1));

        let actual = set.iter().map(|(_, pk)| pk).collect::<Vec<_>>();

        assert_eq!(actual, vec![pk1, pk2, pk3]);

        assert!(set.remove(&pk1));
        assert!(set.remove(&pk2));
        assert!(set.remove(&pk3));

        assert!(!set.remove(&pk3));

        let actual = set.iter().count();
        assert_eq!(actual, 0);
    }
}
