//! QUIC transport and driver tuning constants.

use std::time::Duration;

/// Solana's max transaction wire size, as raised to `4096` bytes by SIMD-0296. See
/// `accept_tx`'s size check, which rejects anything larger than this (unless
/// `unsafe_allow_arbitrary_txn_size` is set).
pub const PACKET_DATA_SIZE: usize = 4096;

pub const QUIC_SEND_FAIRNESS: bool = false;

/// since agave 4.2 we should expect lower leader duration as they go from 400 -> 350 -> 300 -> 250 -> 200ms.
/// though this constant is only used during eviction, and as a grace period.
/// essentially, when a new connection is establish because of a new leader round, the connection should
/// be un-evictable for 2 seconds. Even if the leader duration is 200ms,
/// it should be acceptable to give a connection 2 seconds of grace period before it can be evicted.
///
/// Once mainnet has migrated fully to 200ms, we could decide if it's worth changing.
/// Giving more should not impact the performance or the runtime of the driver.
pub const DEFAULT_EVICTION_GRACE_DURATION: Duration = Duration::from_secs(2); // 400ms * 4 rounded to seconds

/// Keep-alive interval for QUIC connections.
/// The rate at which we send PING frames to keep the connection alive.
/// So apparently this is consistent across the network and solana client.
/// putting 1s makes it the safest option.
pub const QUIC_KEEP_ALIVE: Duration = Duration::from_secs(1); // seconds

// TODO see if its worth making this configurable
#[cfg(feature = "prometheus")]
pub(crate) const METRIC_UPDATE_INTERVAL: Duration = Duration::from_secs(5);

pub(crate) const NUM_CONSECUTIVE_LEADER_SLOTS: u64 = 4;

pub(crate) const FOREVER: Duration = Duration::from_secs(31_536_000); // One year is considered "forever" in this context.
