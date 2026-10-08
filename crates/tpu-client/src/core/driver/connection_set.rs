//! Bookkeeping sets for orphan connections and pending connection evictions.

use {
    crate::core::eviction::strategy::ConnectionEviction,
    std::{
        collections::{BTreeMap, HashMap, HashSet},
        net::SocketAddr,
        time::Instant,
    },
};

#[derive(Default)]
pub(super) struct OrphanConnectionSet {
    prio_queue: BTreeMap<Instant, Vec<OrphanConnectionInfo>>,
    ///
    /// Reverse index from socket addr to its position in the priority queue.
    ///
    prio_queue_rev_index: HashMap<SocketAddr, Vec<(Instant, u64)>>,

    curr_len: usize,
}

impl OrphanConnectionSet {
    #[allow(dead_code)]
    pub(super) const fn len(&self) -> usize {
        self.curr_len
    }

    pub(super) fn clear(&mut self) {
        self.prio_queue.clear();
        self.prio_queue_rev_index.clear();
        self.curr_len = 0;
    }

    pub(super) fn insert(&mut self, info: OrphanConnectionInfo, now: Instant) {
        // Check if not already present
        if let Some(version_set) = self.prio_queue_rev_index.get_mut(&info.remote_peer_addr) {
            if version_set
                .iter()
                .any(|(_, v)| *v == info.connection_version)
            {
                return;
            }
            version_set.push((now, info.connection_version));
        } else {
            self.prio_queue_rev_index
                .insert(info.remote_peer_addr, vec![(now, info.connection_version)]);
        }
        self.prio_queue.entry(now).or_default().push(info);
        self.curr_len += 1;
    }

    pub(super) fn remove(
        &mut self,
        socket_addr: &SocketAddr,
        connection_version: u64,
    ) -> Option<OrphanConnectionInfo> {
        if let Some(version_set) = self.prio_queue_rev_index.get_mut(socket_addr) {
            if let Some(pos) = version_set
                .iter()
                .position(|(_, v)| *v == connection_version)
            {
                let (inserted_at, _) = version_set.remove(pos);
                if version_set.is_empty() {
                    self.prio_queue_rev_index.remove(socket_addr);
                }
                if let Some(unused_conn_vec) = self.prio_queue.get_mut(&inserted_at) {
                    if let Some(info_pos) = unused_conn_vec.iter().position(|info| {
                        info.remote_peer_addr == *socket_addr
                            && info.connection_version == connection_version
                    }) {
                        let info = unused_conn_vec.remove(info_pos);
                        if unused_conn_vec.is_empty() {
                            self.prio_queue.remove(&inserted_at);
                        }
                        self.curr_len -= 1;
                        return Some(info);
                    }
                }
            }
        }
        None
    }

    pub(super) fn oldest(&self) -> Option<Instant> {
        self.prio_queue.keys().next().cloned()
    }

    pub(super) fn pop(&mut self) -> Option<Vec<OrphanConnectionInfo>> {
        let (inserted_at, unused_conn_vec) = self.prio_queue.pop_first()?;
        for info in &unused_conn_vec {
            if let Some(version_set) = self.prio_queue_rev_index.get_mut(&info.remote_peer_addr) {
                version_set.retain(|(ts, v)| *v != info.connection_version || *ts != inserted_at);
                if version_set.is_empty() {
                    self.prio_queue_rev_index.remove(&info.remote_peer_addr);
                }
            }
        }
        self.curr_len -= unused_conn_vec.len();
        Some(unused_conn_vec)
    }
}

pub(super) struct OrphanConnectionInfo {
    pub(super) remote_peer_addr: SocketAddr,
    pub(super) connection_version: u64,
}

#[derive(Default)]
pub(super) struct ConnectionEvictionSet {
    set: HashSet<ConnectionEviction>,
    socket_addr_set: HashSet<SocketAddr>,
    socket_addr_refcount: HashMap<SocketAddr, usize>,
}

impl ConnectionEvictionSet {
    pub(super) fn len(&self) -> usize {
        self.set.len()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.set.is_empty()
    }

    pub(super) fn clear(&mut self) {
        self.set.clear();
        self.socket_addr_set.clear();
        self.socket_addr_refcount.clear();
    }

    pub(super) fn insert(&mut self, eviction: ConnectionEviction) -> bool {
        let inserted = self.set.insert(eviction.clone());
        if inserted {
            self.socket_addr_set.insert(eviction.remote_peer_addr);
            self.socket_addr_refcount
                .entry(eviction.remote_peer_addr)
                .and_modify(|count| *count += 1)
                .or_insert(1);
        }
        inserted
    }

    pub(super) fn remove(&mut self, eviction: &ConnectionEviction) -> bool {
        let removed = self.set.remove(eviction);
        if removed {
            let entry = self
                .socket_addr_refcount
                .get_mut(&eviction.remote_peer_addr)
                .expect("missing socket addr refcount for existing eviction");
            *entry = entry.saturating_sub(1);
            if *entry == 0 {
                self.socket_addr_refcount.remove(&eviction.remote_peer_addr);
                self.socket_addr_set.remove(&eviction.remote_peer_addr);
            }
        }
        removed
    }

    pub(super) fn contains(&self, eviction: &ConnectionEviction) -> bool {
        self.set.contains(eviction)
    }

    pub(super) fn contains_socket_addr(&self, addr: &SocketAddr) -> bool {
        self.socket_addr_set.contains(addr)
    }
}

#[cfg(test)]
mod orphan_connect_set_test {
    use {
        super::{OrphanConnectionInfo, OrphanConnectionSet},
        std::{net::SocketAddr, time::Instant},
    };

    #[test]
    fn test_empty_unused_connection_set() {
        let mut set = OrphanConnectionSet::default();

        assert!(set.oldest().is_none());
        assert!(set.pop().is_none());
    }

    #[test]
    fn test_push_and_pop_unused_connection() {
        let mut set = OrphanConnectionSet::default();

        let now = Instant::now();
        let addr1: SocketAddr = "127.0.0.1:12345".parse().unwrap();
        let info = OrphanConnectionInfo {
            remote_peer_addr: addr1,
            connection_version: 1,
        };
        set.insert(info, now);
        assert_eq!(set.len(), 1);

        assert_eq!(set.oldest(), Some(now));
        let popped = set.pop().unwrap();
        assert_eq!(popped.len(), 1);
        assert_eq!(popped[0].remote_peer_addr, addr1);
        assert_eq!(popped[0].connection_version, 1);
        assert!(set.len() == 0);
    }

    #[test]
    fn test_idempotency() {
        let mut set = OrphanConnectionSet::default();

        let now = Instant::now();
        let addr1: SocketAddr = "127.0.0.1:12345".parse().unwrap();
        let info = OrphanConnectionInfo {
            remote_peer_addr: addr1,
            connection_version: 1,
        };
        let info_clone = OrphanConnectionInfo {
            remote_peer_addr: addr1,
            connection_version: 1,
        };

        // INSERT TWICE
        set.insert(info, now);
        set.insert(info_clone, now);

        assert_eq!(set.len(), 1);
        assert_eq!(set.oldest(), Some(now));
        let popped = set.pop().unwrap();
        assert_eq!(popped.len(), 1);
        assert_eq!(popped[0].remote_peer_addr, addr1);
        assert_eq!(popped[0].connection_version, 1);
    }

    #[test]
    fn test_remove() {
        let mut set = OrphanConnectionSet::default();

        let now = Instant::now();
        let addr1: SocketAddr = "127.0.0.1:12345".parse().unwrap();
        let info = OrphanConnectionInfo {
            remote_peer_addr: addr1,
            connection_version: 1,
        };
        set.insert(info, now);

        assert_eq!(set.oldest(), Some(now));
        set.remove(&addr1, 1);
        assert!(set.oldest().is_none());
        assert!(set.pop().is_none());

        // insert the same connection with two versions
        let info_v1 = OrphanConnectionInfo {
            remote_peer_addr: addr1,
            connection_version: 1,
        };
        set.insert(info_v1, now);
        let info_v2 = OrphanConnectionInfo {
            remote_peer_addr: addr1,
            connection_version: 2,
        };
        set.insert(info_v2, now);

        assert_eq!(set.len(), 2);
        assert_eq!(set.oldest(), Some(now));
        let popped = set.pop().unwrap();
        assert_eq!(popped.len(), 2);
        assert!(set.pop().is_none());
        assert_eq!(set.len(), 0);

        // remove non-existent connection
        let info3 = OrphanConnectionInfo {
            remote_peer_addr: addr1,
            connection_version: 3,
        };
        set.remove(&info3.remote_peer_addr, info3.connection_version);
        assert!(set.pop().is_none());
        assert_eq!(set.len(), 0);
    }
}

#[cfg(test)]
mod connection_eviction_set_test {
    use {super::ConnectionEvictionSet, crate::core::ConnectionEviction};

    #[test]
    fn remove_keeps_socket_addr_index_if_another_version_exists() {
        let mut set = ConnectionEvictionSet::default();
        let addr = "127.0.0.1:9999".parse().unwrap();
        let ev1 = ConnectionEviction {
            remote_peer_addr: addr,
            connection_version: 1,
        };
        let ev2 = ConnectionEviction {
            remote_peer_addr: addr,
            connection_version: 2,
        };

        assert!(set.insert(ev1.clone()));
        assert!(set.insert(ev2.clone()));
        assert!(set.contains_socket_addr(&addr));

        assert!(set.remove(&ev1));
        assert!(set.contains_socket_addr(&addr));
        assert!(set.contains(&ev2));
    }
}
