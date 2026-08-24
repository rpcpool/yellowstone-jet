//! Caps how many concurrent connections a single [`ClientIdentity`] (account +
//! subscription, per the client certificate's Subject Alternative Name) may hold open
//! at once -- see [`ConnectionLimiter`] and
//! [`super::ServerBuilder::max_connections_per_client`].

use {
    super::client_identity::ClientIdentity,
    std::{
        collections::HashMap,
        num::NonZeroUsize,
        sync::{Arc, Mutex},
    },
};

/// Cheap to [`Clone`] (an `Arc`-backed count map inside, rather than the whole type
/// needing to be wrapped in one) -- shared across every `Server`/`Endpoint` shard
/// produced from one `ServerBuilder` (worker sharding via `SO_REUSEPORT` or a bound port
/// range): each shard runs its own accept loop, so the cap must live outside any single
/// one of them -- otherwise a client could multiply its effective limit by however many
/// shards its connections happen to land on. Connection accept is nowhere near a
/// per-message hot path, so a plain mutex-guarded map is the right tool here.
#[derive(Clone)]
pub struct ConnectionLimiter {
    max: NonZeroUsize,
    counts: Arc<Mutex<HashMap<ClientIdentity, usize>>>,
}

impl ConnectionLimiter {
    pub fn new(max: NonZeroUsize) -> Self {
        Self {
            max,
            counts: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Reserves one connection slot for `identity`, or returns `None` (reserving
    /// nothing) if that identity already holds `max` concurrent connections. Hold the
    /// returned permit for the lifetime of the connection -- it releases the slot when
    /// dropped.
    pub fn try_acquire(&self, identity: ClientIdentity) -> Option<ConnectionPermit> {
        let mut counts = self
            .counts
            .lock()
            .expect("connection limiter mutex poisoned");
        let count = counts.entry(identity.clone()).or_insert(0);
        if *count >= self.max.get() {
            return None;
        }
        *count += 1;
        Some(ConnectionPermit {
            counts: Arc::clone(&self.counts),
            identity,
        })
    }
}

/// RAII handle for one reserved connection slot; releases it back to the limiter on
/// drop.
pub struct ConnectionPermit {
    counts: Arc<Mutex<HashMap<ClientIdentity, usize>>>,
    identity: ClientIdentity,
}

impl Drop for ConnectionPermit {
    fn drop(&mut self) {
        let mut counts = self
            .counts
            .lock()
            .expect("connection limiter mutex poisoned");
        let Some(count) = counts.get_mut(&self.identity) else {
            return;
        };
        *count -= 1;
        if *count == 0 {
            counts.remove(&self.identity);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity(account_id: &str) -> ClientIdentity {
        ClientIdentity {
            account_id: account_id.to_owned(),
            subscription_id: uuid::Uuid::new_v4(),
        }
    }

    #[test]
    fn rejects_beyond_the_limit_and_releases_on_drop() {
        let limiter = ConnectionLimiter::new(NonZeroUsize::new(2).unwrap());
        let id = identity("acct-1");

        let first = limiter
            .try_acquire(id.clone())
            .expect("first connection allowed");
        let second = limiter
            .try_acquire(id.clone())
            .expect("second connection allowed");
        assert!(
            limiter.try_acquire(id.clone()).is_none(),
            "third connection should be rejected"
        );

        drop(first);
        let third = limiter
            .try_acquire(id.clone())
            .expect("slot freed after a permit is dropped");

        drop(second);
        drop(third);
        assert!(
            limiter.try_acquire(id).is_some(),
            "all slots should be free again once every permit is dropped"
        );
    }

    #[test]
    fn tracks_each_identity_independently() {
        let limiter = ConnectionLimiter::new(NonZeroUsize::new(1).unwrap());
        let a = identity("acct-a");
        let b = identity("acct-b");

        let _a = limiter
            .try_acquire(a.clone())
            .expect("first identity allowed");
        let _b = limiter.try_acquire(b).expect("second identity allowed");
        assert!(
            limiter.try_acquire(a).is_none(),
            "identity `a` is already at its own limit"
        );
    }
}
