//! The TPU sender driver: a single tokio task that owns every connection and worker.
//!
//! Here's the simplified flow of a transaction through the QUIC driver:
//!
//!  ┌────────────┐      ┌────────────┐       ┌───────────────┐
//!  │Transaction │      │  QUIC      │       │ TxSenderWorker│        (Remote Validator)
//!  │ Source     ┼──1──►│ Driver     ┼──2────►               ┼──3────►
//!  └────────────┘      └────▲───────┘       └─────┬─────────┘
//!                           │                     │
//!                           │                     │
//!                           └───────4*─Failure────┘
//!
//!
//! Lazy connection establishment:
//!
//!  ┌───────────────┐
//!  │New Transaction│
//!  │  for Peer "X" │
//!  └───────┬───────┘
//!          forward
//!          │
//!   ┌──────▼─────────┐           ┌─────────────────────┐
//!   │ Do I have a    │           │    Send it to       │
//!   │a TxSenderWorker┼───Yes─────►TxSenderWork(#peer X)│
//!   │ for Peer "X"?  │           └─────────────────────┘
//!   └──────┬─────────┘
//!          No
//!          │
//!   ┌──────▼────────────┐
//!   │  Queue the        │
//!   │ transaction       │
//!   │  and schedule     │
//!   │ connection attempt│
//!   │  to peer "X"      │
//!   └───────────────────┘
//!
//! # Module layout
//!
//! This module only defines `TpuSenderDriver` and the private types its children share. Each
//! child adds one `impl TpuSenderDriver` block and only calls methods from a lower layer, so the
//! module graph has no cycles. Keep it that way when adding a method: put it in the lowest layer
//! whose methods it calls.
//!
//! 1. `event_loop`: the `select!` loop that dispatches every event.
//! 2. `workers`, `identity`, `prediction`: event handlers.
//! 3. `connect`: connection attempts and their results.
//! 4. `install`, `eviction`, `tx_queue`: building blocks.
//! 5. `fast_path`, `connection_set`: bookkeeping used by the layers above; they call no other
//!    child's methods.
//!
//! `spawner` builds the driver and starts `event_loop`; nothing calls into it.

mod connect;
mod connection_set;
mod event_loop;
mod eviction;
mod fast_path;
mod identity;
mod install;
mod prediction;
pub(super) mod spawner;
mod tx_queue;
mod workers;

use {
    self::connection_set::{ConnectionEvictionSet, OrphanConnectionSet},
    crate::{
        config::TpuSenderConfig,
        core::{
            eviction::{
                staked_set::{StakeSortedPeerSet, StakedSortedSet},
                strategy::{ActiveConnection, ConnectionEvictionStrategy},
            },
            identity_update::DriverCommand,
            leader_fast_path::LeaderFastPath,
            peer_addr_watcher::RemotePeerAddrWatcher,
            quic::ConnectingError,
            services::{LeaderTpuInfoService, UpcomingLeaderPredictor, ValidatorStakeInfoService},
            txn::TpuSenderTxn,
            worker::{TxSenderWorkerCompleted, TxWorkerMeta, TxWorkerSenderHandle},
        },
        identity::TpuIdentity,
    },
    arc_swap::ArcSwap,
    quinn::{Connection, Endpoint},
    solana_pubkey::Pubkey,
    std::{
        collections::{HashMap, HashSet, VecDeque},
        mem::MaybeUninit,
        net::SocketAddr,
        sync::Arc,
        time::Instant,
    },
    tokio::{
        sync::{
            Notify,
            mpsc::{self},
        },
        task::{Id, JoinSet},
    },
};

///
/// Metadata about an inflight connection attempt to a remote peer.
///
struct ConnectingMeta {
    /// The identity used in the certificate to connect with.
    current_client_identity: Pubkey,
    /// List of all remote peer identities being connected to in this task.
    /// (multiplexing multiple remote peer connection in one task since multiple remote peer can share same endpoint).
    multiplexed_remote_peer_identity_vec: Vec<Pubkey>,
    remote_peer_address: SocketAddr,
    connection_attempt: usize,
    created_at: Instant,
}

enum DriverTaskMeta {
    DropAllWorkers,
}

struct WaitingEviction {
    remote_peer_addr: SocketAddr,
    notify: Arc<Notify>,
}

///
/// Tokio-based driver for tpu sender.
///
pub(crate) struct TpuSenderDriver<CB> {
    ///
    /// The stake info map used to compute max stream limit
    ///
    stake_info_map: Arc<dyn ValidatorStakeInfoService + Send + Sync + 'static>,

    ///
    /// Holds on-going remote peer transaction sender workers.
    ///
    tx_worker_handle_map: HashMap<Pubkey, TxWorkerSenderHandle>,

    ///
    /// Maps active remote peer connection to their stake.
    ///
    active_staked_sorted_remote_peer: StakeSortedPeerSet,

    ///
    /// Map from tokio task id to the remote peer it refers too.
    ///
    tx_worker_task_meta_map: HashMap<Id, TxWorkerMeta>,

    ///
    /// JoinSet of all transaction sender workers.
    ///
    tx_worker_set: JoinSet<TxSenderWorkerCompleted>,

    ///
    /// Transaction queues per remote identity waiting for connection to be come available.
    ///
    tx_queues: HashMap<Pubkey, VecDeque<(TpuSenderTxn, usize)>>,

    endpoints: Vec<Endpoint>,

    ///
    /// JoinSet of inflight connection attempt
    ///
    connecting_tasks: JoinSet<Result<Connection, ConnectingError>>,

    ///
    /// Metadata about inflight connection attempt.
    ///
    connecting_meta: HashMap<tokio::task::Id, ConnectingMeta>,

    ///
    /// Reversed of [`TokioQuicDriver::connecting_meta`]
    ///
    connecting_remote_peers: HashMap<Pubkey, tokio::task::Id>,

    ///
    /// Map from remote peer socket address to the connecting task ids.
    /// Task Id is the key for [`TokioQuicDriver::connecting_tasks`].
    connecting_remote_peers_addr: HashMap<SocketAddr, tokio::task::Id>,

    ///
    /// Service to locate tpu port address from remote peer identity.
    ///
    leader_tpu_info_service: Arc<dyn LeaderTpuInfoService + Send + Sync + 'static>,

    config: TpuSenderConfig,

    ///
    /// Current driver identity: public key plus its derived QUIC client TLS credentials.
    ///
    identity: TpuIdentity,

    ///
    /// The driver is the only writer of this cell; it's shared with [`TpuSenderIdentityUpdater`]
    /// so callers can cheaply read the current identity's public key without round-tripping
    /// through the command-and-control channel.
    ///
    current_identity_pubkey: Arc<ArcSwap<Pubkey>>,

    ///
    /// Transaction inlet channel : where transaction comes from.
    ///
    tx_inlet: mpsc::Receiver<TpuSenderTxn>,

    ///
    /// Outlet to send transaction "sent" status.
    ///
    response_outlet: Option<CB>,

    ///
    /// Command-and-control channel : low-bandwidth channel to receive driver configuration mutation.
    ///
    cnc_rx: mpsc::Receiver<DriverCommand>,

    tasklet: JoinSet<()>,
    tasklet_meta: HashMap<Id, DriverTaskMeta>,

    last_peer_activity: HashMap<Pubkey, Instant>,

    ///
    /// Sets of ongoing eviction of peers.
    ///
    being_evicted_peers: HashSet<Pubkey>,

    ///
    /// Eviction strategy to uses.
    ///
    eviction_strategy: Arc<dyn ConnectionEvictionStrategy + Send + Sync + 'static>,

    connecting_blocked_by_eviction_list: VecDeque<WaitingEviction>,

    remote_peer_addr_watcher: RemotePeerAddrWatcher,

    ///
    /// Upcoming leader predictor to use.
    ///
    leader_predictor: Arc<dyn UpcomingLeaderPredictor + Send + Sync + 'static>,

    ///
    /// Output buffer reused by every call to
    /// [`UpcomingLeaderPredictor::try_predict_next_n_leader_inclusive`]. It holds the current
    /// leader plus [`TpuSenderConfig::leader_prediction_lookahead`] upcoming ones, and is empty
    /// when prediction is disabled.
    ///
    /// Every element is initialized when the driver is built, so a predictor that overreports
    /// how many leaders it wrote yields stale keys rather than uninitialized memory.
    ///
    upcoming_leaders_buf: Box<[MaybeUninit<Pubkey>]>,

    ///
    /// Table of leader workers shared with every inlet, so their transactions skip the driver.
    ///
    fast_path: LeaderFastPath,

    ///
    /// Leaders from the latest prediction that the fast path tracks, current leader first.
    ///
    fast_path_leaders: Vec<Pubkey>,

    ///
    /// Next leader prediction deadline.
    ///
    next_leader_prediction_deadline: Instant,

    ///
    /// A map of active connections.
    ///
    connection_map: HashMap<SocketAddr, ActiveConnection>,

    ///
    /// Current connection version.
    /// Each new connection gets assigned a new connection version.
    /// This is used during connection eviction.
    connection_version: u64,

    ///
    /// Used to do round-robin selection of endpoint for new connections.
    ///
    endpoint_sequence: usize,

    ///
    /// Sets of pending connection eviction.
    ///
    pending_connection_eviction_set: ConnectionEvictionSet,

    active_staked_sorted_remote_peer_addr: StakedSortedSet<SocketAddr>,

    ///
    /// Set of unused connections (no tx worker associated).
    ///
    orphan_connection_set: OrphanConnectionSet,
}

///
/// Source of spawning a connection task.
///
/// Spawning a connection task can be triggered by different events.
/// This enum helps for debugging of logging purposes.
///
/// See [`TpuSenderDriver::spawn_connecting`] for more information.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SpawnSource {
    /// A new transaction arrived for a remote peer without a worker.
    NewTransaction,
    /// Connection spawned because the underlying driver's identity used to sign certicated was updated.
    UpdateIdentity,
    ///
    /// Connection spawned because a prediction that a remote peer will be needed soon.
    ///
    Prediction,
    /// Connection spawned as part of re-attempting failed connections.
    Reattempt,
    /// Connection spawned as part of rescuing queued transactions for a remote peer's worker.
    Rescue,
}
