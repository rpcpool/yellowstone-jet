//!
//! Core types and traits for TPU client implementations.
//!
//! # Overview
//!
//! This module contains the core event loop driver and related types for implementing
//! a TPU sender using Tokio and QUIC via the `quinn` library.
//!
//! It defines the main driver struct `TpuSenderDriver`, which manages connections to remote peers,
//! transaction sending workers, and leader prediction.
//!
//! # Connection Eviction
//!
//! The driver supports connection eviction strategies via the [`crate::core::ConnectionEvictionStrategy`] trait.
//! See [`crate::core::StakeBasedEvictionStrategy`] for a basic implementation.
//!
//! # Upcoming Leader Prediction
//!
//! The driver can predict upcoming leaders using the [`crate::core::UpcomingLeaderPredictor`] trait.
//!
//! # Callback Mechanism
//!
//! The driver supports a callback mechanism for notifying the caller about transaction send results.
//! See [`crate::core::TpuSenderResponseCallback`] for details.
//!
//! ## Example
//!
//! ```ignore
//! #[derive(Clone)]
//! struct LoggingCallback;
//!
//! impl TpuSenderResponseCallback for LoggingCallback {
//!     fn call(&self, response: TpuSenderResponse) {
//!         tracing::info!("Received TPU sender response: {:?}", response);
//!     }
//! }
//! ```
//!
//!

#[cfg(feature = "prometheus")]
use crate::prom;
use {
    crate::config::{TpuOverrideInfo, TpuPortKind, TpuSenderConfig},
    bytes::Bytes,
    derive_more::Display,
    futures::task::AtomicWaker,
    quinn::{
        ClientConfig, Connection, ConnectionError, Endpoint, IdleTimeout, TransportConfig, VarInt,
        WriteError, crypto::rustls::QuicClientConfig,
    },
    rustls::{NamedGroup, crypto::CryptoProvider},
    solana_clock::{DEFAULT_MS_PER_SLOT, NUM_CONSECUTIVE_LEADER_SLOTS},
    solana_keypair::Keypair,
    solana_pubkey::Pubkey,
    solana_signature::Signature,
    solana_signer::Signer,
    solana_streamer::nonblocking::quic::ALPN_TPU_PROTOCOL_ID,
    solana_tls_utils::{QuicClientCertificate, SkipServerVerification, new_dummy_x509_certificate},
    std::{
        collections::{BTreeMap, HashMap, HashSet, VecDeque},
        net::{IpAddr, Ipv4Addr, SocketAddr},
        num::NonZeroUsize,
        pin::Pin,
        sync::{Arc, Mutex as StdMutex, atomic::AtomicBool},
        task::Poll,
        time::{Duration, Instant},
    },
    tokio::{
        runtime::Handle,
        sync::{
            Barrier, Notify,
            mpsc::{self},
        },
        task::{self, Id, JoinError, JoinHandle, JoinSet},
        time::{Sleep, interval},
    },
};

pub const PACKET_DATA_SIZE: usize = 1232;

pub const QUIC_SEND_FAIRNESS: bool = false;

///
/// MAX TIMEOUT is not consistent across solana clients, firedancer has much lower timeout than agave.a
/// 10 seconds the least common timeout.
///
pub const QUIC_MAX_TIMEOUT: Duration = Duration::from_secs(10);

/// Default duration after which an unused connection is evicted.
pub const DEFAULT_UNUSED_CONNECTION_TTL: Duration = Duration::from_secs(10);

pub const DEFAULT_LEADER_DURATION: Duration = Duration::from_secs(2); // 400ms * 4 rounded to seconds

/// Keep-alive interval for QUIC connections.
/// The rate at which we send PING frames to keep the connection alive.
/// So apparently this is consistent across the network and solana client.
/// putting 1s makes it the safest option.
pub const QUIC_KEEP_ALIVE: Duration = Duration::from_secs(1); // seconds

// TODO see if its worth making this configurable
#[cfg(feature = "prometheus")]
pub(crate) const METRIC_UPDATE_INTERVAL: Duration = Duration::from_secs(5);

#[derive(thiserror::Error, Debug)]
pub(crate) enum ConnectingError {
    #[error(transparent)]
    ConnectError(#[from] quinn::ConnectError),
    #[error(transparent)]
    ConnectionError(#[from] quinn::ConnectionError),
}

pub(crate) struct SentOk {
    pub e2e_time: Duration,
}

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

///
/// Inner part of the update identity command.
///
struct UpdateIdentityCommand {
    new_identity: Keypair,
    callback: Arc<UpdateIdentityInner>,
}

struct MultiStepIdentitySynchronizationCommand {
    new_identity: Keypair,
    barrier: Arc<Barrier>,
}

///
/// Command to control driver behavior.
///
enum DriverCommand {
    UpdateIdenttiy(UpdateIdentityCommand),
    MultiStepIdentitySynchronization(MultiStepIdentitySynchronizationCommand),
}

enum DriverTaskMeta {
    DropAllWorkers,
}

struct TxWorkerSenderHandle {
    remote_peer_addr: SocketAddr,
    connection_version: u64,
    sender: mpsc::Sender<TpuSenderTxn>,
    cancel_notify: Arc<Notify>,
}

struct WaitingEviction {
    remote_peer_addr: SocketAddr,
    notify: Arc<Notify>,
}

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

const FOREVER: Duration = Duration::from_secs(31_536_000); // One year is considered "forever" in this context.

struct TxWorkerMeta {
    remote_peer_identity: Pubkey,
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
    /// Current certificate set
    ///
    client_certificate: Arc<QuicClientCertificate>,

    ///
    /// Current set driver identity
    ///
    identity: Keypair,

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

#[derive(Default)]
struct OrphanConnectionSet {
    prio_queue: BTreeMap<Instant, Vec<OrphanConnectionInfo>>,
    ///
    /// Reverse index from socket addr to its position in the priority queue.
    ///
    prio_queue_rev_index: HashMap<SocketAddr, Vec<(Instant, u64)>>,

    curr_len: usize,
}

impl OrphanConnectionSet {
    fn len(&self) -> usize {
        self.curr_len
    }

    fn insert(&mut self, info: OrphanConnectionInfo, now: Instant) {
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

    fn remove(
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

    fn oldest(&self) -> Option<Instant> {
        self.prio_queue.keys().next().cloned()
    }

    fn pop(&mut self) -> Option<Vec<OrphanConnectionInfo>> {
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

struct OrphanConnectionInfo {
    remote_peer_addr: SocketAddr,
    connection_version: u64,
}

#[derive(Default)]
struct ConnectionEvictionSet {
    set: HashSet<ConnectionEviction>,
    socket_addr_set: HashSet<SocketAddr>,
}

impl ConnectionEvictionSet {
    fn len(&self) -> usize {
        self.set.len()
    }

    fn is_empty(&self) -> bool {
        self.set.is_empty()
    }

    fn clear(&mut self) {
        self.set.clear();
        self.socket_addr_set.clear();
    }

    fn insert(&mut self, eviction: ConnectionEviction) -> bool {
        let inserted = self.set.insert(eviction.clone());
        if inserted {
            self.socket_addr_set.insert(eviction.remote_peer_addr);
        }
        inserted
    }

    fn remove(&mut self, eviction: &ConnectionEviction) -> bool {
        let removed = self.set.remove(eviction);
        if removed {
            self.socket_addr_set.remove(&eviction.remote_peer_addr);
        }
        removed
    }

    fn contains(&self, eviction: &ConnectionEviction) -> bool {
        self.set.contains(eviction)
    }

    fn contains_socket_addr(&self, addr: &SocketAddr) -> bool {
        self.socket_addr_set.contains(addr)
    }
}

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
    conn: Arc<Connection>,
    connection_version: u64,
    multiplexed_remote_peer_identity_with_stake: HashMap<Pubkey, u64>,
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
///
/// A transaction with destination details to be sent to a remote peer.
///
#[derive(Debug)]
pub struct TpuSenderTxn {
    /// Id set by the sender to identify the transaction. Only meaningful to the sender.
    pub tx_sig: Signature,
    /// The wire format of the transaction.
    pub(crate) wire: Bytes,
    /// The pubkey of the remote peer to send the transaction to.
    pub remote_peer: Pubkey,
}

impl TpuSenderTxn {
    pub fn from_bytes(tx_sig: Signature, remote_peer: Pubkey, wire: Bytes) -> Self {
        Self {
            tx_sig,
            wire,
            remote_peer,
        }
    }

    pub fn from_owned<T>(tx_sig: Signature, remote_peer: Pubkey, wire: T) -> Self
    where
        T: AsRef<[u8]> + Send + 'static,
    {
        Self {
            tx_sig,
            wire: Bytes::from_owner(wire),
            remote_peer,
        }
    }
}

///
/// Errors that can occur when sending a transaction.
///
#[derive(thiserror::Error, Debug)]
pub enum SendTxError {
    ///
    /// [`ConnectionError`] from quinn when attempting to write to the stream.
    #[error(transparent)]
    ConnectionError(#[from] ConnectionError),
    ///
    /// [`WriteError`] from quinn when attempting to write to the stream.
    ///
    #[error("Failed to send transaction to remote peer {0:?}")]
    StreamStopped(VarInt),
    ///
    /// Stream is closed or reset by the remote peer (dropped or disallow).
    ///
    /// # Note
    ///
    /// Stream may be closed primarly for two reason:
    ///
    /// 1. "dropped" : The remote peer has evicted this connection to make room for other peers.
    /// 2. "disallow" : The remote peer is throttling our IP Address (too many connectino opened from same IP in the last throttling window).
    ///
    #[error("stream is closed or reset by remote peer")]
    StreamClosed,
    ///
    /// 0-RTT rejected by remote peer.
    ///
    /// # Note
    ///
    /// As of Agave v3.0.x, this error should not be raised since agave does not support 0-RTT yet.
    #[error("0-RTT rejected by remote peer")]
    ZeroRttRejected,
}

///
/// Information about a successful transaction send.
///
/// Note: The transaction may still fail to be processed by the remote peer.
/// This struct only indicates that the transaction was successfully sent over quinn's internal stream buffers.
///
#[derive(Debug)]
pub struct TxSent {
    ///
    /// The remote peer identity to which the transaction was sent.
    ///
    pub remote_peer_identity: Pubkey,
    ///
    /// The remote peer socket address to which the transaction was sent.
    ///
    pub remote_peer_addr: SocketAddr,
    ///
    /// The transaction signature.
    ///
    pub tx_sig: Signature,
}

///
/// Information about a failed transaction send attempt.
///
#[derive(Debug)]
pub struct TxFailed {
    ///
    /// The remote peer identity to which the transaction send failed.
    pub remote_peer_identity: Pubkey,
    ///
    /// The remote peer socket address to which the transaction send failed.
    ///
    pub remote_peer_addr: SocketAddr,
    ///
    /// The transaction signature.
    ///
    pub tx_sig: Signature,
    ///
    /// Low-level reason for the failure.
    ///
    pub failure_reason: String,
}

///
/// Reason why a transaction was dropped.
///
#[derive(Clone, Debug, Display)]
pub enum TxDropReason {
    ///
    /// The transaction queue for the remote peer reached its maximum capacity.
    ///
    #[display("reached downstream transaction worker transaction queue capacity")]
    RateLimited,
    ///
    /// The remote peer is unreachable via its gossup TPU QUIC contact info.
    ///
    #[display("remote peer is unreachable")]
    RemotePeerUnreachable,
    ///
    /// The internal event loop schedule dropped the transaction due to overload or out-dated information.
    ///
    #[display("tx got drop by driver")]
    DropByDriver,
    ///
    /// The remote peer is being evicted to make room for higher staked connections.
    ///
    #[display("remote peer is being evicted")]
    RemotePeerBeingEvicted,
    ///
    /// The transaction is invalid.
    ///
    #[display("transaction packet size is exceed PACKET_DATA_SIZE (1232 bytes)")]
    InvalidPacketSize,
    ///
    /// The remote peer identity changed.
    ///
    #[display("driver QUIC identity changed")]
    DriverIdentityChanged,
}

///
/// Information about dropped transactions.
///
#[derive(Debug)]
pub struct TxDrop {
    ///
    /// The remote peer identity for which the transaction(s) were dropped.
    ///
    pub remote_peer_identity: Pubkey,
    ///
    /// Reason why the transaction(s) were dropped.
    ///
    pub drop_reason: TxDropReason,
    ///
    /// The list of dropped transactions with their attempt count.
    pub dropped_tx_vec: VecDeque<(TpuSenderTxn, usize)>,
}

///
/// Response from the internal TPU sender.
///
#[derive(Debug)]
pub enum TpuSenderResponse {
    /// Transaction sucessfully written to a QUIC STREAM Frame.
    TxSent(TxSent),
    /// Transaction failed to be sent after retries.
    TxFailed(TxFailed),
    /// Transaction(s) dropped before being sent.
    TxDrop(TxDrop),
}

///
/// A task to connect to a remote peer.
///
struct ConnectingTask {
    remote_peer_identity: Pubkey,
    cert: Arc<QuicClientCertificate>,
    max_idle_timeout: Duration,
    connection_timeout: Duration,
    wait_for_eviction: Option<Arc<Notify>>,
    endpoint: Endpoint,
}

/// Translate a SocketAddr into a valid SNI for the purposes of QUIC connection
///
/// We do not actually check if the server holds a cert for this server_name
/// since Solana does not rely on DNS names, but we need to provide a unique
/// one to ensure that we present correct QUIC tokens to the correct server.
///
/// Code taken from <https://github.com/anza-xyz/agave/pull/7260>
pub fn socket_addr_to_quic_server_name(peer: SocketAddr) -> String {
    format!("{}.{}.sol", peer.ip(), peer.port())
}

pub fn crypto_provider() -> CryptoProvider {
    let mut provider = rustls::crypto::ring::default_provider();
    // Disable all key exchange algorithms except X25519
    provider
        .kx_groups
        .retain(|kx| kx.name() == NamedGroup::X25519);
    provider
}

impl ConnectingTask {
    async fn run(self, remote_peer_addr: SocketAddr) -> Result<Connection, ConnectingError> {
        if let Some(signal) = &self.wait_for_eviction {
            tracing::trace!(
                "Waiting for eviction to complete before connecting to remote peer: {}",
                self.remote_peer_identity
            );
            signal.notified().await;
            tracing::trace!(
                "Eviction completed, proceeding to connect to remote peer: {}",
                self.remote_peer_identity
            );
        }

        let mut crypto = rustls::ClientConfig::builder_with_provider(Arc::new(crypto_provider()))
            .with_safe_default_protocol_versions()
            .expect("Failed to set QUIC client protocol versions")
            .dangerous()
            .with_custom_certificate_verifier(SkipServerVerification::new())
            .with_client_auth_cert(
                vec![self.cert.certificate.clone()],
                self.cert.key.clone_key(),
            )
            .expect("Failed to set QUIC client certificates");
        crypto.enable_early_data = true;
        crypto.alpn_protocols = vec![ALPN_TPU_PROTOCOL_ID.to_vec()];

        let transport_config = {
            let mut res = TransportConfig::default();

            let max_idle_timeout = IdleTimeout::try_from(self.max_idle_timeout)
                .expect("Failed to set QUIC max idle timeout");
            res.max_idle_timeout(Some(max_idle_timeout));
            res.keep_alive_interval(Some(QUIC_KEEP_ALIVE));
            // We don't want fairness : https://github.com/quinn-rs/quinn/pull/2002
            // Fairness use round-robin scheduling to write stream data into the next frame.
            // Disabling fairness makes that once a stream starts to write it won't be interrupted by round-robin.
            // This reduce the time the receive the (fin) "end" of a transaction, thus reducing latency.
            res.send_fairness(QUIC_SEND_FAIRNESS);
            res
        };

        let mut config = ClientConfig::new(Arc::new(QuicClientConfig::try_from(crypto).unwrap()));
        config.transport_config(Arc::new(transport_config));

        let server_name = socket_addr_to_quic_server_name(remote_peer_addr);
        let connecting = self
            .endpoint
            .connect_with(config, remote_peer_addr, server_name.as_str())
            .map_err(ConnectingError::ConnectError)?;

        tracing::trace!(
            "Connecting to remote peer: {} at address: {}",
            self.remote_peer_identity,
            remote_peer_addr,
        );
        let conn = tokio::time::timeout(self.connection_timeout, connecting)
            .await
            .map_err(|_| ConnectingError::ConnectionError(ConnectionError::TimedOut))??;

        Ok(conn)
    }
}

/// A transaction sender worker tied to a specific remote peer via a single connection.
///
/// To optimize performance for a [`quinn::Connection`], adhere to the following guidelines:
///
/// - Only one transaction sender worker/task should use the connection at a time.
/// - Transactions must be sent sequentially over the connection, not concurrently/parallel.
///
/// Rationale: Solana's use case deviates from the typical QUIC or `quinn` library design.
/// Excessive use of `quinn`'s concurrency features can lead to transaction fragmentation.
/// Counterintuitively, for Solana, minimizing fragmentation is prioritized over maximizing throughput,
/// concurrency, or parallelism, as `quinn`'s concurrency features do not enhance performance in this context.
///
/// Although it might seem appealing to use multiple streams across different Tokio tasks on the same connection,
/// benchmarks conducted by the Anza team indicate that this approach degrades performance rather than improving it.
struct QuicTxSenderWorker<CB> {
    remote_peer: Pubkey,
    connection: Arc<Connection>,
    /// The current client identity being used for the connection
    current_client_identity: Pubkey,
    incoming_rx: mpsc::Receiver<TpuSenderTxn>,
    output_tx: Option<CB>,
    tx_queue: VecDeque<(TpuSenderTxn, usize)>,
    cancel_notify: Arc<Notify>,
    max_tx_attempt: NonZeroUsize,
    txn_sent: usize,
}

#[derive(Debug, thiserror::Error)]
enum TxSenderWorkerError {
    #[error(transparent)]
    ConnectionLost(#[from] quinn::ConnectionError),
    #[error("0-RTT rejected by remote peer")]
    ZeroRttRejected,
}

struct TxSenderWorkerCompleted {
    err: Option<TxSenderWorkerError>,
    rx: mpsc::Receiver<TpuSenderTxn>,
    pending_tx: VecDeque<(TpuSenderTxn, usize)>,
    canceled: bool,
}

impl<CB> QuicTxSenderWorker<CB>
where
    CB: TpuSenderResponseCallback,
{
    async fn send_tx(&mut self, tx: &[u8]) -> Result<SentOk, SendTxError> {
        let t = Instant::now();
        let mut uni = self.connection.open_uni().await?;
        uni.write_all(tx).await.map_err(|e| match e {
            WriteError::Stopped(var_int) => SendTxError::StreamStopped(var_int),
            WriteError::ConnectionLost(connection_error) => {
                SendTxError::ConnectionError(connection_error)
            }
            WriteError::ClosedStream => SendTxError::StreamClosed,
            WriteError::ZeroRttRejected => SendTxError::ZeroRttRejected,
        })?;
        self.txn_sent = self.txn_sent.saturating_add(1);
        let e2e_time = t.elapsed();
        let ok = SentOk { e2e_time };
        Ok(ok)
    }
    async fn process_tx(
        &mut self,
        tx: TpuSenderTxn,
        attempt: usize,
    ) -> Option<TxSenderWorkerError> {
        let result = self.send_tx(tx.wire.as_ref()).await;
        let remote_addr = self.connection.remote_address();
        let tx_sig = tx.tx_sig;
        match result {
            Ok(sent_ok) => {
                tracing::debug!(
                    "Tx sent to remote peer: {} in {:?}",
                    self.remote_peer,
                    sent_ok.e2e_time
                );
                let resp = TxSent {
                    remote_peer_identity: self.remote_peer,
                    remote_peer_addr: remote_addr,
                    tx_sig,
                };
                if let Some(callback) = &self.output_tx {
                    callback.call(TpuSenderResponse::TxSent(resp));
                }
                #[cfg(feature = "prometheus")]
                {
                    let path_stats = self.connection.stats().path;
                    let current_mut = path_stats.current_mtu;
                    prom::quic_send_attempts_inc(self.remote_peer, remote_addr, "success");
                    prom::incr_quic_gw_worker_tx_process_cnt(self.remote_peer, "success");
                    prom::observe_send_transaction_e2e_latency(self.remote_peer, sent_ok.e2e_time);
                    prom::set_leader_mtu(self.remote_peer, current_mut);
                    prom::observe_leader_rtt(self.remote_peer, path_stats.rtt);
                }
                None
            }
            Err(e) => {
                #[cfg(feature = "prometheus")]
                {
                    prom::quic_send_attempts_inc(self.remote_peer, remote_addr, "error");
                }
                if attempt >= self.max_tx_attempt.get() {
                    #[cfg(feature = "prometheus")]
                    {
                        prom::incr_quic_gw_worker_tx_process_cnt(self.remote_peer, "error");
                    }

                    tracing::warn!(
                        "Giving up sending transaction to remote peer: {}, client identity: {}, after {} attempts, {} txn sent so far: {:?}",
                        self.remote_peer,
                        self.current_client_identity,
                        attempt,
                        self.txn_sent,
                        e
                    );
                    let resp = TxFailed {
                        remote_peer_identity: self.remote_peer,
                        remote_peer_addr: self.connection.remote_address(),
                        failure_reason: e.to_string(),
                        tx_sig,
                    };
                    if let Some(callback) = &self.output_tx {
                        callback.call(TpuSenderResponse::TxFailed(resp));
                    }
                } else {
                    tracing::trace!(
                        "Retrying to send transaction: {} to remote peer: {} after {} attempts: {:?}",
                        tx_sig,
                        self.remote_peer,
                        attempt,
                        e
                    );
                    self.tx_queue.push_back((tx, attempt + 1));
                }

                match e {
                    SendTxError::ConnectionError(connection_error) => {
                        Some(TxSenderWorkerError::ConnectionLost(connection_error))
                    }
                    SendTxError::StreamStopped(_) | SendTxError::StreamClosed => {
                        tracing::trace!(
                            "Stream stopped or closed for tx: {} to remote peer: {}",
                            tx_sig,
                            self.remote_peer
                        );
                        None
                    }
                    SendTxError::ZeroRttRejected => {
                        tracing::warn!(
                            "0-RTT rejected by remote peer: {} for tx: {}",
                            self.remote_peer,
                            tx_sig
                        );
                        Some(TxSenderWorkerError::ZeroRttRejected)
                    }
                }
            }
        }
    }

    async fn try_process_tx_in_queue(&mut self) -> Option<TxSenderWorkerError> {
        while let Some((tx, attempt)) = self.tx_queue.pop_front() {
            tracing::trace!(
                "Processing tx: {} for remote peer: {} with attempt: {}",
                tx.tx_sig,
                self.remote_peer,
                attempt
            );
            if let Some(e) = self.process_tx(tx, attempt).await {
                return Some(e);
            }
        }
        None
    }

    async fn run(mut self) -> TxSenderWorkerCompleted {
        let mut canceled = false;
        let mut last_activity = Instant::now();
        let mut burst_timer = Box::pin(tokio::time::sleep_until(
            (last_activity + Duration::from_secs(10)).into(),
        ));
        const MAX_IDLE_DURATION: Duration = Duration::from_secs(10);
        let maybe_err = loop {
            tracing::trace!(
                "worker {} tick loop -- queue size: {}",
                self.remote_peer,
                self.tx_queue.len()
            );

            if let Some(e) = self.try_process_tx_in_queue().await {
                break Some(e);
            }
            // Every 10 seconds we will look if there is new tx to processed.
            // If not we will close the worker to free up resources.
            tokio::select! {
                maybe = self.incoming_rx.recv() => {
                    match maybe {
                        Some(tx) => {
                            last_activity = Instant::now();
                            tracing::trace!("Received tx: {} for remote peer: {}", tx.tx_sig, self.remote_peer);
                            self.tx_queue.push_back((tx, 1));
                        }
                        None => {
                            tracing::debug!("Transaction sender inlet closed for remote peer: {:?}", self.remote_peer);
                            break None;
                        }
                    }
                }
                _ = &mut burst_timer => {
                    let idle_duration = Instant::now().duration_since(last_activity);
                    tracing::debug!(
                        "Transaction sender worker for remote peer: {:?} idle for {:?}, shutting down",
                        self.remote_peer,
                        idle_duration
                    );
                    if idle_duration >= MAX_IDLE_DURATION {
                        break None;
                    } else {
                        burst_timer.as_mut().reset(( last_activity + Duration::from_secs(10) ).into());
                    }
                }
                err = self.connection.closed() => {
                    // Agave client do connection eviction and can close the connection for least used or lower staked peers.
                    break Some(err.into())
                }
                _ = self.cancel_notify.notified() => {
                    tracing::debug!("Transaction sender worker for remote peer: {:?} is canceled", self.remote_peer);
                    canceled = true;
                    break None;
                }
            }
        };

        tracing::trace!(
            "Transaction sender worker for remote peer: {:?} completed with error: {:?}, canceled: {}",
            self.remote_peer,
            maybe_err,
            canceled
        );
        TxSenderWorkerCompleted {
            err: maybe_err,
            canceled,
            rx: self.incoming_rx,
            pending_tx: self.tx_queue,
        }
    }
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
    /// See [`StakeBasedEvictionStrategy`] for an example of eviction strategy that uses this method.
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
    connection_map: ConnectionMap<'a>,
    staked_sorted_address_set: &'a StakedSortedSet<SocketAddr>,
}

enum ConnectionMap<'a> {
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
            peer_idle_eviction_grace_period: DEFAULT_LEADER_DURATION,
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

/// Here's the simplified flow of a transaction through the QUIC driver:
///
///  ┌────────────┐      ┌────────────┐       ┌───────────────┐
///  │Transaction │      │  QUIC      │       │ TxSenderWorker│        (Remote Validator)
///  │ Source     ┼──1──►│ Driver     ┼──2────►               ┼──3────►
///  └────────────┘      └────▲───────┘       └─────┬─────────┘
///                           │                     │
///                           │                     │
///                           └───────4*─Failure────┘
///
///
/// Lazy connection establishment:
///
///  ┌───────────────┐
///  │New Transaction│
///  │  for Peer "X" │
///  └───────┬───────┘
///          forward
///          │
///   ┌──────▼─────────┐           ┌─────────────────────┐
///   │ Do I have a    │           │    Send it to       │
///   │a TxSenderWorker┼───Yes─────►TxSenderWork(#peer X)│
///   │ for Peer "X"?  │           └─────────────────────┘
///   └──────┬─────────┘
///          No
///          │
///   ┌──────▼────────────┐
///   │  Queue the        │
///   │ transaction       │
///   │  and schedule     │
///   │ connection attempt│
///   │  to peer "X"      │
///   └───────────────────┘
///
impl<CB> TpuSenderDriver<CB>
where
    CB: TpuSenderResponseCallback + Send + Sync + 'static,
{
    ///
    /// Spawns a "connecting" task to a remote peer.
    ///
    /// this is called when a transaction is received for a remote peer which does not have a worker installed yet.
    ///
    /// # Multiplexing Note
    ///
    /// Multiple remote peer identities may share the same socket address.
    ///
    /// This method will multiplex remote peer identities over the same connection if there is already a connecting task
    /// for the same remote peer address.
    ///
    /// If a [`quinn::Connection`] already exists for the  TPU address of the provided `remote_peer_identity`,
    /// this method will directly call [`TpuSenderDriver::install_worker`] for the remote peer identity using the existing connection
    /// and add it to the multiplexed remote peer identities of the connection.
    ///
    /// ## Fragmentation edge-case caused by multiplexing
    ///
    /// Since multiple [`QuicTxSenderWorker`] may share the same [`quinn::Connection`], we have to ensure that
    /// each worker sends transactions sequentially over the connection, not concurrently.
    ///
    /// Since there is only one leader at a time in Solana, it's expected that most of the time only one remote peer identity
    /// will be active on a connection, thus fragmentation should not exists.
    ///
    /// The only occurence of fragmentation would be during leader transitions over two validators sharing the same TPU address.
    /// In this case, both remote peer identities may have pending transactions to send over the same connection during the last half of a slot.
    /// NOTE: This very edge-case will be fixed in the future.
    ///
    /// Lastly, most validators will not multiplex multiple identities over the same address, thus fragmentation should be non-existent in practice.
    ///
    fn spawn_connecting(
        &mut self,
        remote_peer_identity: Pubkey,
        attempt: usize,
        debug_source: SpawnSource,
    ) {
        if self
            .connecting_remote_peers
            .contains_key(&remote_peer_identity)
        {
            #[cfg(feature = "prometheus")]
            {
                if debug_source == SpawnSource::NewTransaction {
                    prom::incr_quic_gw_tx_connection_cache_miss_cnt();
                }
            }
            tracing::warn!(
                "Skipping connection attempt to remote peer: {} since it is already connecting",
                remote_peer_identity
            );
            return;
        }

        if self.being_evicted_peers.contains(&remote_peer_identity) {
            tracing::warn!(
                "Skipping connection attempt to remote peer: {} since it is being evicted",
                remote_peer_identity
            );
            return;
        }

        if self
            .tx_worker_handle_map
            .contains_key(&remote_peer_identity)
        {
            tracing::warn!(
                "Skipping connection attempt to remote peer: {} since it already has a worker",
                remote_peer_identity
            );
            return;
        }

        // Check if we already have a connection for the remote peer address
        // If not, check if there is already a connecting task for the same remote peer address
        // If not, spawn a new connecting task
        let Some(remote_peer_addr) = self
            .leader_tpu_info_service
            .get_quic_dest_addr(&remote_peer_identity, self.config.tpu_port)
        else {
            self.unreachable_peer(remote_peer_identity);
            return;
        };

        if self
            .pending_connection_eviction_set
            .contains_socket_addr(&remote_peer_addr)
        {
            return;
        }

        if self.connection_map.contains_key(&remote_peer_addr) {
            // We already have an active connection for the remote peer address
            tracing::debug!(
                "Re-using existing active connection to remote peer address: {} for remote peer identity: {}",
                remote_peer_addr,
                remote_peer_identity
            );
            #[cfg(feature = "prometheus")]
            {
                if debug_source == SpawnSource::NewTransaction {
                    prom::incr_quic_gw_tx_connection_cache_hit_cnt();
                }
                prom::incr_fast_txn_worker_install_path();
            }
            self.install_worker(remote_peer_identity, remote_peer_addr);
            return;
        }

        #[cfg(feature = "prometheus")]
        {
            if debug_source == SpawnSource::NewTransaction {
                prom::incr_quic_gw_tx_connection_cache_miss_cnt();
            }
        }

        let maybe_existing_connecting_task =
            self.connecting_remote_peers_addr.get(&remote_peer_addr);

        // We need signal to wait for eviction to complete before we can proceed with the connection.
        // Otherwise, the connecting attempt may fail to bind a local port.
        let maybe_wait_for_eviction =
            if !self.has_connection_capacity() && maybe_existing_connecting_task.is_none() {
                // We need to evict a connection before we can proceed.
                let notify = Arc::new(Notify::new());
                let waiting_eviction = WaitingEviction {
                    remote_peer_addr,
                    notify: Arc::clone(&notify),
                };
                tracing::trace!(
                    "Remote peer: {} connection attempt blocked, waiting for eviction",
                    remote_peer_identity
                );
                self.connecting_blocked_by_eviction_list
                    .push_back(waiting_eviction);
                Some(notify)
            } else {
                // There is room for new connections, no need to wait for eviction.
                None
            };

        match maybe_existing_connecting_task {
            Some(existing_task_id) => {
                // There is already a connecting task for the same remote peer address.
                // Due to multiplexing, we can re-use the existing connecting task.
                tracing::info!(
                    "Re-using existing connecting task to remote peer address: {} for remote peer identity: {}",
                    remote_peer_addr,
                    remote_peer_identity
                );
                let meta = self
                    .connecting_meta
                    .get_mut(existing_task_id)
                    .expect("missing connecting meta for existing task");
                meta.multiplexed_remote_peer_identity_vec
                    .push(remote_peer_identity);

                // Just add yourself to the connecting remote peers map.
                let old = self
                    .connecting_remote_peers
                    .insert(remote_peer_identity, *existing_task_id);
                assert!(
                    old.is_none(),
                    "Remote peer should not be already connecting"
                );
            }
            None => {
                // No existing connecting task for the remote peer address, spawn a new one.
                let endpoint_idx = self.next_endpoint_idx();
                let cert = Arc::clone(&self.client_certificate);
                let max_idle_timeout = self.config.max_idle_timeout;
                let fut = ConnectingTask {
                    remote_peer_identity,
                    cert,
                    max_idle_timeout,
                    connection_timeout: self.config.connecting_timeout,
                    wait_for_eviction: maybe_wait_for_eviction,
                    endpoint: self.endpoints[endpoint_idx].clone(),
                }
                .run(remote_peer_addr);
                let meta = ConnectingMeta {
                    current_client_identity: self.identity.pubkey(),
                    multiplexed_remote_peer_identity_vec: vec![remote_peer_identity],
                    remote_peer_address: remote_peer_addr,
                    connection_attempt: attempt,
                    created_at: Instant::now(),
                };

                let abort_handle = self.connecting_tasks.spawn(fut);

                tracing::info!(
                    "Spawning connection for remote peer: {remote_peer_identity}, attempt: {attempt}, source: {debug_source:?}",
                );
                let old = self
                    .connecting_remote_peers
                    .insert(remote_peer_identity, abort_handle.id());
                assert!(
                    old.is_none(),
                    "Remote peer should not be already connecting"
                );
                self.connecting_meta.insert(abort_handle.id(), meta);
                self.connecting_remote_peers_addr
                    .insert(remote_peer_addr, abort_handle.id());
            }
        }
    }

    ///
    /// Drops all queued transactions for a remote peer and notify the response outlet.
    ///
    fn drop_peer_queued_tx(&mut self, remote_peer_identity: Pubkey, reason: TxDropReason) {
        tracing::trace!(
            "Dropping queued tx for remote peer: {} due to reason: {:?}",
            remote_peer_identity,
            reason
        );
        if let Some(tx_queues) = self.tx_queues.remove(&remote_peer_identity) {
            #[cfg(feature = "prometheus")]
            {
                let total = tx_queues.len();
                prom::incr_quic_gw_drop_tx_cnt(remote_peer_identity, total as u64);
            }
            let tx_drop = TxDrop {
                remote_peer_identity,
                drop_reason: reason.clone(),
                dropped_tx_vec: tx_queues,
            };
            if let Some(callback) = self.response_outlet.as_ref() {
                callback.call(TpuSenderResponse::TxDrop(tx_drop));
            }
        }
    }

    fn unreachable_peer(&mut self, remote_peer_identity: Pubkey) {
        #[cfg(feature = "prometheus")]
        {
            prom::inc_quic_gw_unreachable_peer_count(remote_peer_identity);
        }
        self.drop_peer_queued_tx(remote_peer_identity, TxDropReason::RemotePeerUnreachable);
    }

    fn has_connection_capacity(&self) -> bool {
        self.connection_map.len() < self.max_concurrent_connection()
    }

    const fn max_concurrent_connection(&self) -> usize {
        self.config.max_concurrent_connection
    }

    ///
    /// Evicts a remote peer connection based on the stake and last activity.
    ///
    /// Since highly staked remote peers are more likely to be re-used in the future,
    /// we evict the lowest staked remote peer connection first, unless it has been used recently.
    ///
    fn do_eviction_if_required(&mut self) {
        let eviction_count_required = self
            .connecting_blocked_by_eviction_list
            .len()
            .saturating_sub(self.pending_connection_eviction_set.len());

        if eviction_count_required == 0 {
            tracing::trace!("No eviction required at this time");
            return;
        }
        tracing::info!(
            "Eviction required for {} connections",
            eviction_count_required
        );
        let connection_map = ConnectionMap::Quinn(&self.connection_map);
        let addr_map = RemotePeerAddrMap {
            connection_map,
            staked_sorted_address_set: &self.active_staked_sorted_remote_peer_addr,
        };

        let mut eviction_plan = self.eviction_strategy.plan_eviction_with_addr_map(
            Instant::now(),
            &self.active_staked_sorted_remote_peer,
            &self.last_peer_activity,
            &self.being_evicted_peers,
            eviction_count_required,
            &addr_map,
        );
        tracing::trace!("Eviction plan len {}", eviction_plan.len());
        if eviction_plan.is_empty() && self.pending_connection_eviction_set.is_empty() {
            // If the evictin plan is empty, pick the connection with the least amount of a active stake
            let min_staked_active_conn = self.connection_map.values().min_by_key(|active_conn| {
                active_conn
                    .multiplexed_remote_peer_identity_with_stake
                    .values()
                    .sum::<u64>()
            });
            if let Some(active_conn) = min_staked_active_conn {
                for peer in active_conn
                    .multiplexed_remote_peer_identity_with_stake
                    .keys()
                {
                    eviction_plan.push(*peer);
                }
            }
        }

        // Because of multiplexing, in order to do the connection eviction we must
        // evict all tx workers that are using the same connection.
        // For each remote peer to evict, we need to extends the eviction set to include all neighboring
        // remote peers that are multiplexed on the same connection.
        let mut cancel_handles_vec = Vec::with_capacity(eviction_plan.len());
        for peer in eviction_plan {
            if let Some(handle) = self.tx_worker_handle_map.get(&peer) {
                let remote_peer_addr = handle.remote_peer_addr;
                self.being_evicted_peers.insert(peer);
                cancel_handles_vec.push(Arc::clone(&handle.cancel_notify));
                if let Some(active_conn) = self.connection_map.get(&remote_peer_addr) {
                    if active_conn.connection_version != handle.connection_version {
                        // This means the connection has been re-established since the worker was created.
                        // So we don't need to evict other multiplexed peers on this connection.
                        continue;
                    }
                    let connection_eviction = ConnectionEviction {
                        remote_peer_addr: active_conn.conn.remote_address(),
                        connection_version: active_conn.connection_version,
                    };
                    self.pending_connection_eviction_set
                        .insert(connection_eviction);
                    for multiplexed_peer in active_conn
                        .multiplexed_remote_peer_identity_with_stake
                        .keys()
                    {
                        self.being_evicted_peers.insert(*multiplexed_peer);
                        if let Some(handle) = self.tx_worker_handle_map.get(multiplexed_peer) {
                            let cancel_notify = Arc::clone(&handle.cancel_notify);
                            cancel_handles_vec.push(cancel_notify);
                        }
                    }
                }
            }
        }

        tracing::trace!(
            "Evicting {} remote peer connections",
            cancel_handles_vec.len()
        );
        for cancel_handle in cancel_handles_vec {
            cancel_handle.notify_one();
        }
    }

    ///
    /// Installs a transaction sender worker for a remote peer with the given connection.
    ///
    /// # Arguments
    ///
    /// - `remote_peer_identity`: The public key of the remote peer.
    /// - `remote_peer_addr`: The socket address of the remote peer.
    ///
    /// # Panics
    ///
    /// Panics if there is no active connection for the given remote peer address.
    /// This function assumes that a connection has already been established for the remote peer address.
    ///
    /// See [`TpuSenderDriver::spawn_connecting`] for connection establishment
    /// and see [`TpuSenderDriver::handle_connecting_result`] for handling connection results.
    ///
    fn install_worker(&mut self, remote_peer_identity: Pubkey, remote_peer_addr: SocketAddr) {
        let (tx, rx) = mpsc::channel(self.config.transaction_sender_worker_channel_capacity);

        let (connection, connection_version) = match self.connection_map.get(&remote_peer_addr) {
            Some(active_conn) => (
                Arc::clone(&active_conn.conn),
                active_conn.connection_version,
            ),
            None => {
                panic!("Active connection must exist for remote peer address: {remote_peer_addr}");
            }
        };

        let output_tx = self.response_outlet.clone();
        let cancel_notify = Arc::new(Notify::new());

        let worker = QuicTxSenderWorker {
            remote_peer: remote_peer_identity,
            connection,
            current_client_identity: self.identity.pubkey(),
            incoming_rx: rx,
            output_tx,
            tx_queue: self
                .tx_queues
                .remove(&remote_peer_identity)
                .unwrap_or_default(),
            cancel_notify: Arc::clone(&cancel_notify),
            max_tx_attempt: self.config.max_send_attempt,
            txn_sent: 0,
        };

        let worker_fut = worker.run();
        let ah = self.tx_worker_set.spawn(worker_fut);
        let handle = TxWorkerSenderHandle {
            remote_peer_addr,
            sender: tx,
            cancel_notify,
            connection_version,
        };
        assert!(
            self.tx_worker_handle_map
                .insert(remote_peer_identity, handle)
                .is_none()
        );
        let task_id = ah.id();
        self.tx_worker_task_meta_map.insert(
            task_id,
            TxWorkerMeta {
                remote_peer_identity,
            },
        );
        let Some(active_conn) = self.connection_map.get_mut(&remote_peer_addr) else {
            unreachable!();
        };
        let remote_peer_stake = self
            .stake_info_map
            .get_stake_info(&remote_peer_identity)
            .unwrap_or(0);
        assert!(
            active_conn
                .multiplexed_remote_peer_identity_with_stake
                .insert(remote_peer_identity, remote_peer_stake)
                .is_none(),
            "duplicate remote peer identity in active connection"
        );
        let total_remote_addr_stake: u64 = active_conn
            .multiplexed_remote_peer_identity_with_stake
            .values()
            .sum();
        self.active_staked_sorted_remote_peer_addr
            .insert(remote_peer_addr, total_remote_addr_stake);
        self.active_staked_sorted_remote_peer
            .insert(remote_peer_identity, remote_peer_stake);
        self.remote_peer_addr_watcher
            .register_watch(remote_peer_identity, remote_peer_addr);

        // MAKE SURE THIS CONNECTION IS NOT IN THE UNUSED SET ANYMORE (IF IT WERE).
        self.orphan_connection_set
            .remove(&remote_peer_addr, connection_version);

        tracing::debug!("Installed tx worker for remote peer: {remote_peer_identity}");
    }

    ///
    /// Handles the result of a connection attempt to a remote peer.
    ///
    /// Reattempts the connection if it fails, up to the maximum number of attempts, unless the peer is unreachable.
    ///
    /// If the connection is successful, this function call [`TpuSenderDriver::install_worker`] to set up a transaction sender worker for each
    /// multiplexed remote peer identity over the connection.
    ///
    fn handle_connecting_result(
        &mut self,
        result: Result<(task::Id, Result<Connection, ConnectingError>), JoinError>,
    ) {
        match result {
            Ok((task_id, result)) => {
                #[allow(unused_variables)]
                let ConnectingMeta {
                    current_client_identity,
                    multiplexed_remote_peer_identity_vec,
                    connection_attempt,
                    remote_peer_address,
                    created_at,
                } = self
                    .connecting_meta
                    .remove(&task_id)
                    .expect("connecting_meta");
                #[cfg(feature = "prometheus")]
                {
                    prom::observe_quic_gw_connection_time(created_at.elapsed());
                }
                for remote_peer_identity in &multiplexed_remote_peer_identity_vec {
                    self.connecting_remote_peers.remove(remote_peer_identity);
                }
                self.connecting_remote_peers_addr
                    .remove(&remote_peer_address);

                if self.identity.pubkey() != current_client_identity {
                    // THIS SHOULD NOT HAPPEN SINCE ON IDENTITY CHANGE WE ABORT ALL CONNECTING TASKS
                    // BUT JUST IN CASE, WE CHECK AGAIN.
                    tracing::warn!(
                        "Abandoning connection attempt to remote peer: {multiplexed_remote_peer_identity_vec:?} since the client identity has changed"
                    );
                    for remote_peer_identity in multiplexed_remote_peer_identity_vec {
                        self.spawn_connecting(
                            remote_peer_identity,
                            connection_attempt,
                            SpawnSource::UpdateIdentity,
                        );
                    }
                    return;
                }

                match result {
                    Ok(conn) => {
                        #[cfg(feature = "prometheus")]
                        {
                            prom::incr_quic_gw_connection_success_cnt();
                        }
                        let conn = Arc::new(conn);
                        let active_connection = ActiveConnection {
                            conn: Arc::clone(&conn),
                            connection_version: self.next_connection_version(),
                            multiplexed_remote_peer_identity_with_stake: Default::default(),
                        };
                        self.connection_map
                            .insert(remote_peer_address, active_connection);
                        for remote_peer_identity in multiplexed_remote_peer_identity_vec {
                            tracing::debug!("Connected to remote peer: {:?}", remote_peer_identity);
                            self.install_worker(remote_peer_identity, remote_peer_address);
                        }
                    }
                    Err(connect_err) => {
                        #[cfg(feature = "prometheus")]
                        {
                            prom::incr_quic_gw_connection_failure_cnt();
                        }

                        match connect_err {
                            ConnectingError::ConnectError(
                                quinn::ConnectError::EndpointStopping,
                            ) => {
                                // This should never happen, but if it does, we panic.
                                // The endpoint is stopping, so we cannot connect to the remote peer.
                                panic!(
                                    "Endpoint is stopping, cannot connect to remote peer: {multiplexed_remote_peer_identity_vec:?}, with identity: {current_client_identity}"
                                );
                            }
                            ConnectingError::ConnectionError(_)
                                if connection_attempt < self.config.max_connection_attempts =>
                            {
                                // HERE'S THE CODE THE HANDLE RETRY LOGIC.
                                // NOTE: THE RETRY COUNT IS NOT INSIDE A SPECIFIC REGISTER OR MAP,
                                // IT'S STATELESS MEANING THE RETRY COUNT IS DETERMINED BY THE NUMBER OF ATTEMPTS STORED ON EACH CONNECTING TASK.
                                // EACH REATTEMPT SPAWNS A NEW CONNECTING TASK WITH ATTEMPT COUNT EQUALS TO PREVIOUS ATTEMPT COUNT + 1.
                                // AFTER REACHING MAX ATTEMPTS, THE DEFAULT MATCH BRANCH WILL HANDLE THE FAILURE CALLED "whatever".

                                tracing::warn!(
                                    "Connection attempt {} to remote peer: {multiplexed_remote_peer_identity_vec:?} failed, retrying...",
                                    connection_attempt
                                );

                                //
                                for remote_peer_identity in multiplexed_remote_peer_identity_vec {
                                    let latest_remote_peer_address =
                                        self.leader_tpu_info_service.get_quic_dest_addr(
                                            &remote_peer_identity,
                                            self.config.tpu_port,
                                        );
                                    let Some(latest_remote_peer_address) =
                                        latest_remote_peer_address
                                    else {
                                        self.unreachable_peer(remote_peer_identity);
                                        continue;
                                    };
                                    let new_connection_attempt =
                                        if latest_remote_peer_address != remote_peer_address {
                                            1
                                        } else {
                                            connection_attempt.saturating_add(1)
                                        };
                                    self.spawn_connecting(
                                        remote_peer_identity,
                                        new_connection_attempt,
                                        SpawnSource::Reattempt,
                                    );
                                }
                            }
                            whatever => {
                                tracing::warn!(
                                    "Failed to connect to remote peer: {multiplexed_remote_peer_identity_vec:?}, with identity: {current_client_identity}, error: {whatever:?}"
                                );
                                for remote_peer_identity in multiplexed_remote_peer_identity_vec {
                                    self.unreachable_peer(remote_peer_identity);
                                }
                            }
                        }
                    }
                }
            }
            Err(join_err) => {
                #[cfg(feature = "prometheus")]
                {
                    prom::incr_quic_gw_connection_failure_cnt();
                }
                let ConnectingMeta {
                    current_client_identity: _,
                    multiplexed_remote_peer_identity_vec,
                    connection_attempt: _,
                    remote_peer_address,
                    created_at: _,
                } = self
                    .connecting_meta
                    .remove(&join_err.id())
                    .expect("connecting_meta");

                self.connecting_remote_peers_addr
                    .remove(&remote_peer_address);
                for remote_peer_identity in multiplexed_remote_peer_identity_vec {
                    let _ = self.connecting_remote_peers.remove(&remote_peer_identity);
                    self.drop_peer_queued_tx(
                        remote_peer_identity,
                        TxDropReason::RemotePeerUnreachable,
                    );
                    tracing::error!(
                        "Join error during connecting to {remote_peer_identity:?}: {:?}",
                        join_err
                    );
                }
            }
        }
    }

    ///
    /// Generates the next connection version number.
    ///
    /// We use connection versioning to track connection re-establishments.
    ///
    /// This is more a sanity check than anything else and not part of any core business logic.
    ///
    /// Why we are doing connection version tracking? Because this tpu sender driver is like a complex state-machine with alot of moving parts.
    /// Connections can be re-established, workers can be spawned and evicted, all happening concurrently.
    /// Making sure that a worker is using the correct connection is important to avoid subtle bugs.
    /// So we use connection versioning to `assert!` that we didn't create any orphan worker or orphan connection in the code.
    ///
    ///
    fn next_connection_version(&mut self) -> u64 {
        let ret = self.connection_version;
        self.connection_version += 1;
        ret
    }

    ///
    /// Round-robin endpoint selection
    ///
    fn next_endpoint_idx(&mut self) -> usize {
        let ret = self.endpoint_sequence;
        self.endpoint_sequence = (self.endpoint_sequence + 1) % self.endpoints.len();
        ret
    }

    ///
    /// Accepts a transaction and determines how to handle it based on the remote peer's status.
    ///
    /// If a transaction sender worker exists for the remote peer, it is fowarded to it.
    /// If not, the transaction is queued for later processing and a connection attempt is scheduled.
    ///
    fn accept_tx(&mut self, tx: TpuSenderTxn) {
        let remote_peer_identity = tx.remote_peer;
        self.last_peer_activity
            .insert(remote_peer_identity, Instant::now());
        let tx_id = tx.tx_sig;

        // Check size
        if tx.wire.len() > PACKET_DATA_SIZE && !self.config.unsafe_allow_arbitrary_txn_size {
            let tx_drop = TxDrop {
                remote_peer_identity,
                drop_reason: TxDropReason::InvalidPacketSize,
                dropped_tx_vec: VecDeque::from([(tx, 1)]),
            };
            #[cfg(feature = "prometheus")]
            {
                prom::incr_quic_gw_drop_tx_cnt(remote_peer_identity, 1);
                prom::incr_invalid_txn_packet_size();
            }
            if let Some(callback) = self.response_outlet.as_ref() {
                callback.call(TpuSenderResponse::TxDrop(tx_drop));
            }
            return;
        }

        // Do I have a transaction sender worker for this remote peer?
        if let Some(handle) = self.tx_worker_handle_map.get(&remote_peer_identity) {
            // If we have an active transaction sender worker for the remote peer,
            #[cfg(feature = "prometheus")]
            {
                prom::incr_quic_gw_tx_connection_cache_hit_cnt();
            }
            match handle.sender.try_send(tx) {
                Ok(_) => {
                    tracing::trace!("{tx_id} sent to worker");
                    #[cfg(feature = "prometheus")]
                    {
                        prom::incr_quic_gw_tx_relayed_to_worker(remote_peer_identity);
                    }
                }
                Err(e) => match e {
                    mpsc::error::TrySendError::Full(tx) => {
                        tracing::warn!(
                            "Remote peer: {:?} tx queue is full, dropping tx: {:?}",
                            remote_peer_identity,
                            tx_id
                        );
                        let txdrop = TxDrop {
                            remote_peer_identity,
                            drop_reason: TxDropReason::RateLimited,
                            dropped_tx_vec: VecDeque::from([(tx, 1)]),
                        };
                        #[cfg(feature = "prometheus")]
                        {
                            prom::incr_quic_gw_drop_tx_cnt(remote_peer_identity, 1);
                        }
                        if let Some(callback) = self.response_outlet.as_ref() {
                            callback.call(TpuSenderResponse::TxDrop(txdrop));
                        }
                    }
                    mpsc::error::TrySendError::Closed(tx) => {
                        tracing::debug!("Enqueuing tx: {tx_id:.10}",);
                        self.tx_queues
                            .entry(remote_peer_identity)
                            .or_default()
                            .push_back((tx, 1));
                    }
                },
            }
        } else {
            #[cfg(feature = "prometheus")]
            {
                prom::incr_txn_worker_pre_installed_miss();
            }
            // We don't have any active transaction sender worker for the remote peer,
            // we need to queue the transaction and try to spawn a new connection.
            self.tx_queues
                .entry(remote_peer_identity)
                .or_default()
                .push_back((tx, 1));
            tracing::trace!("queuing tx: {:?}", tx_id);

            // Check if we are not already connecting to this remote peer.
            // If the remote peer is already being connected, just queue the tx.
            self.spawn_connecting(remote_peer_identity, 1, SpawnSource::NewTransaction);
        }
    }

    ///
    /// Unblocks a connection attempt that was waiting for eviction to complete.
    /// We do this blocking to avoid connection capacity exhaustion.
    /// Since we have a maximum number of concurrent connections that can be configured by the user.
    fn unblock_eviction_waiting_connection(&mut self) {
        let Some(WaitingEviction {
            remote_peer_addr,
            notify,
        }) = self.connecting_blocked_by_eviction_list.pop_front()
        else {
            return;
        };
        tracing::trace!(
            "Unblocking connection attempt to remote peer address: {} after eviction",
            remote_peer_addr
        );
        notify.notify_one();
    }

    ///
    /// Removes a worker from an active connection's multiplexed peer list.
    ///
    /// If the connection has no more multiplexed peers and is marked for eviction,
    /// the connection is removed from the active connection map and eviction waiters are unblocked.
    ///
    /// # Note
    ///
    /// if the connection version does not match the expected version, the removal operation is ignored, not mutation is performed.
    ///
    fn remove_worker_from_active_connection(
        &mut self,
        remote_peer_addr: SocketAddr,
        remote_peer_identity: Pubkey,
        expected_connection_version: u64,
    ) {
        if let Some(active_conn) = self.connection_map.get_mut(&remote_peer_addr) {
            if active_conn.connection_version != expected_connection_version {
                // This means the connection has been re-established since the worker was created.
                // So we don't need to remove the multiplexed peer from this connection.
                tracing::warn!(
                    "Skipping removal of remote peer: {} from active connection at address: {} due to connection version mismatch, expected: {}, actual: {}",
                    remote_peer_identity,
                    remote_peer_addr,
                    expected_connection_version,
                    active_conn.connection_version,
                );
                return;
            }
            active_conn
                .multiplexed_remote_peer_identity_with_stake
                .remove(&remote_peer_identity);

            let connection_eviction = ConnectionEviction {
                remote_peer_addr,
                connection_version: expected_connection_version,
            };
            let is_connection_mark_for_eviction = self
                .pending_connection_eviction_set
                .contains(&connection_eviction);
            let has_no_worker = active_conn
                .multiplexed_remote_peer_identity_with_stake
                .is_empty();

            let new_stake_for_addr: u64 = active_conn
                .multiplexed_remote_peer_identity_with_stake
                .values()
                .sum();

            self.active_staked_sorted_remote_peer_addr
                .insert(remote_peer_addr, new_stake_for_addr);

            if has_no_worker {
                if is_connection_mark_for_eviction {
                    self.connection_map.remove(&remote_peer_addr);
                    self.pending_connection_eviction_set
                        .remove(&connection_eviction);
                    self.unblock_eviction_waiting_connection();
                    self.active_staked_sorted_remote_peer_addr
                        .remove(&remote_peer_addr);
                } else {
                    let orphan_conn_info = OrphanConnectionInfo {
                        remote_peer_addr,
                        connection_version: expected_connection_version,
                    };
                    self.orphan_connection_set
                        .insert(orphan_conn_info, Instant::now());
                }
            }
        }
    }

    ///
    /// One of the transaction sender worker has completed its work or failed.
    ///
    fn handle_worker_result(&mut self, result: Result<(Id, TxSenderWorkerCompleted), JoinError>) {
        match result {
            Ok((id, mut worker_completed)) => {
                let TxWorkerMeta {
                    remote_peer_identity,
                } = self
                    .tx_worker_task_meta_map
                    .remove(&id)
                    .expect("tx worker meta");

                // We remove the stake from the remote peer address map
                self.active_staked_sorted_remote_peer
                    .remove(&remote_peer_identity);

                self.remote_peer_addr_watcher.forget(remote_peer_identity);

                let is_being_evicted = self.being_evicted_peers.remove(&remote_peer_identity);
                let worker_tx = self
                    .tx_worker_handle_map
                    .remove(&remote_peer_identity)
                    .expect("tx worker sender");

                if let Some(active_conn) = self.connection_map.get_mut(&worker_tx.remote_peer_addr)
                {
                    let active_conn_version = active_conn.connection_version;
                    let worker_conn_version = worker_tx.connection_version;
                    assert!(
                        active_conn_version == worker_conn_version,
                        "Connection version mismatch for remote peer: {remote_peer_identity}, active: {active_conn_version}, worker: {worker_conn_version}",
                    );

                    self.remove_worker_from_active_connection(
                        worker_tx.remote_peer_addr,
                        remote_peer_identity,
                        worker_conn_version,
                    );
                }
                self.last_peer_activity.remove(&remote_peer_identity);
                drop(worker_tx);

                tracing::trace!(
                    "Tx worker for remote peer: {:?} completed, err: {:?}, canceled: {}, evicted: {}",
                    remote_peer_identity,
                    worker_completed.err,
                    worker_completed.canceled,
                    is_being_evicted
                );

                // It's possible that the worker failed while having pending transactions.
                // We need to "rescue" those transactions if any and if the worker didn't fail due to fatal errors.

                let tx_to_rescue = self.tx_queues.entry(remote_peer_identity).or_default();
                while let Ok(tx) = worker_completed.rx.try_recv() {
                    tx_to_rescue.push_back((tx, 1));
                }
                while let Some((tx, attempt)) = worker_completed.pending_tx.pop_front() {
                    tx_to_rescue.push_back((tx, attempt));
                }

                let is_peer_unreachable = worker_completed
                    .err
                    .filter(|e| {
                        matches!(
                            e,
                            TxSenderWorkerError::ConnectionLost(ConnectionError::VersionMismatch)
                        )
                    })
                    .is_some();

                if worker_completed.canceled {
                    tracing::trace!("Remote peer: {remote_peer_identity} tx worker was canceled");
                }

                #[cfg(feature = "prometheus")]
                {
                    prom::incr_quic_gw_connection_close_cnt();
                }

                if is_peer_unreachable {
                    // If the peer is unreachable, we drop all queued transactions for it.
                    self.unreachable_peer(remote_peer_identity);
                } else if is_being_evicted {
                    // If the worker was schedule for eviction, we simply drop all queued transactions
                    // because evicted workers are not expected to reconnect soon since they have been
                    // chosen to be eviction strategy. We don't want to be stuck in a loop
                    // where we keep evicting the same peer, so we drop the queued transactions.
                    // and start from a clean slate.
                    tracing::trace!(
                        "Remote peer: {} tx worker was canceled, will not reconnect",
                        remote_peer_identity
                    );
                    self.drop_peer_queued_tx(
                        remote_peer_identity,
                        TxDropReason::RemotePeerBeingEvicted,
                    );
                } else if !tx_to_rescue.is_empty() {
                    // If the worker didn't have a fatal error and was not evicted and still has queued transactions,
                    // we can safely reattempt to connect to the remote peer.
                    // This can happen to transient network errors or remote peer being temporarily unavailable.
                    // THIS CAN ALSO HAPPEN IF THE REMOTE PEER CHANGED ITS ADDRESS.
                    // We can safely resume connection and try to send the queued transactions.
                    tracing::trace!(
                        "Remote peer: {} has queued tx, wil reconnect",
                        remote_peer_identity
                    );
                    self.last_peer_activity
                        .insert(remote_peer_identity, Instant::now());
                    self.spawn_connecting(remote_peer_identity, 1, SpawnSource::Rescue);
                } else {
                    // Worker returned without error, no work to do, all done.
                }
            }
            Err(join_err) => {
                let id = join_err.id();
                if let Some(meta) = self.tx_worker_task_meta_map.remove(&id) {
                    panic!(
                        "Join error during tx sender worker {} ended: {:?}",
                        meta.remote_peer_identity, join_err
                    );
                }
            }
        }
    }

    ///
    /// Schedules a graceful drop of all transaction workers.
    ///
    /// The scheduled task waits for all transaction workers to complete and drop their senders.
    /// All transaction workers are detached from the driver runtime and not managed anymore.
    ///
    ///
    fn schedule_graceful_drop_all_worker(&mut self) {
        tracing::trace!("Scheduling graceful drop of all transaction workers");
        let mut tx_worker_meta = std::mem::take(&mut self.tx_worker_task_meta_map);
        // Make sure to update the endpoint usage
        let tx_worker_sender_map = std::mem::take(&mut self.tx_worker_handle_map);
        let mut tx_worker_set = std::mem::take(&mut self.tx_worker_set);
        let mut tx_queues = std::mem::take(&mut self.tx_queues);
        self.active_staked_sorted_remote_peer.clear();
        let response_outlet = self.response_outlet.clone();
        let fut = async move {
            drop(tx_worker_sender_map);
            while let Some(result) = tx_worker_set.join_next_with_id().await {
                let id = match &result {
                    Ok((id, _)) => *id,
                    Err(e) => e.id(),
                };
                let TxWorkerMeta {
                    remote_peer_identity,
                } = tx_worker_meta.remove(&id).unwrap();

                let inflight_txn = match result {
                    Ok((_, mut worker_completed)) => {
                        let mut canceled_txn = VecDeque::new();
                        while let Ok(tx) = worker_completed.rx.try_recv() {
                            canceled_txn.push_back((tx, 1));
                        }
                        canceled_txn.extend(worker_completed.pending_tx.into_iter());
                        canceled_txn
                    }
                    Err(_) => VecDeque::new(),
                };

                let mut canceled_txn_queue =
                    tx_queues.remove(&remote_peer_identity).unwrap_or_default();
                canceled_txn_queue.extend(inflight_txn);
                if let Some(callback) = response_outlet.as_ref() {
                    let tx_drop = TxDrop {
                        remote_peer_identity,
                        drop_reason: TxDropReason::DriverIdentityChanged,
                        dropped_tx_vec: canceled_txn_queue,
                    };
                    callback.call(TpuSenderResponse::TxDrop(tx_drop));
                }

                tracing::trace!(
                    "graceful drop worker for remote peer: {}",
                    remote_peer_identity
                );
            }
        };

        let ah = self.tasklet.spawn(fut);
        self.tasklet_meta
            .insert(ah.id(), DriverTaskMeta::DropAllWorkers);
    }

    ///
    /// Updates the driver identity and reconnects to all remote peers with the new identity.
    ///
    /// # DANGER
    ///
    /// This function is super important to get right. Changing the identity of the driver
    /// means that all existing connections are invalidated and must be re-established.
    ///
    /// It also means we need to properly clean up all existing state associated with the old identity and prior connections.
    ///
    /// # Steps performed
    ///
    /// 1. Schedule a graceful drop of all transaction workers.
    /// 2. Clear the connection map.
    /// 3. Store inflight connecting task metadata, in order to replay them later.
    /// 4. Abort and detach all inflight connecting tasks.
    /// 5. Clear all connecting remote peers and their addresses.
    /// 6. Clear the connecting blocked by eviction list.
    /// 7. Update the identity and client certificate.
    /// 8. Replay all connecting tasks with the new identity and connecting meta stored at step #3.
    ///  
    ///
    async fn update_identity(&mut self, new_identity: Keypair, barrier_like: impl Future) {
        self.schedule_graceful_drop_all_worker();
        self.connection_map.clear();
        self.being_evicted_peers.clear();
        self.pending_connection_eviction_set.clear();
        self.connecting_tasks.abort_all();
        self.connecting_tasks.detach_all();
        self.connecting_remote_peers.clear();
        self.connecting_remote_peers_addr.clear();
        self.connecting_blocked_by_eviction_list.clear();
        // active staked sroted remote peer map is clear in `schedule_graceful_drop_all_worker`
        self.active_staked_sorted_remote_peer_addr.clear();

        let connecting_meta = std::mem::take(&mut self.connecting_meta);

        // Update identity
        let (certificate, privkey) = new_dummy_x509_certificate(&new_identity);
        let cert = Arc::new(QuicClientCertificate {
            certificate,
            key: privkey,
        });

        self.client_certificate = cert;
        self.identity = new_identity;
        #[cfg(feature = "prometheus")]
        {
            prom::quic_set_identity(self.identity.pubkey());
        }

        barrier_like.await;

        connecting_meta.values().for_each(|meta| {
            meta.multiplexed_remote_peer_identity_vec
                .iter()
                .for_each(|remote_peer_identity| {
                    self.spawn_connecting(
                        *remote_peer_identity,
                        meta.connection_attempt,
                        SpawnSource::UpdateIdentity,
                    );
                });
        });

        if !connecting_meta.is_empty() {
            tracing::trace!(
                "Will auto-reconnect to {} remote peers after identity update",
                connecting_meta.len()
            );
        }

        tracing::trace!(
            "Updated tpu sender driver identity to: {}",
            self.identity.pubkey()
        );
    }

    async fn handle_cnc(&mut self, command: DriverCommand) {
        match command {
            DriverCommand::UpdateIdenttiy(cmd) => {
                let UpdateIdentityCommand {
                    new_identity,
                    callback,
                } = cmd;
                let fake_barrier = async {
                    callback
                        .set
                        .store(true, std::sync::atomic::Ordering::SeqCst);
                    callback.waker.wake();
                };
                self.update_identity(new_identity, fake_barrier).await;
            }
            DriverCommand::MultiStepIdentitySynchronization(
                multi_step_identity_synchronization_command,
            ) => {
                let MultiStepIdentitySynchronizationCommand {
                    new_identity,
                    barrier,
                } = multi_step_identity_synchronization_command;
                let barrier_fut = async {
                    barrier.wait().await;
                };
                self.update_identity(new_identity, barrier_fut).await;
            }
        }
    }

    fn handle_tasklet_result(&mut self, result: Result<(Id, ()), JoinError>) {
        let id = match &result {
            Ok((id, _)) => *id,
            Err(join_err) => join_err.id(),
        };
        let meta = self.tasklet_meta.remove(&id).expect("tasklet meta");

        match meta {
            DriverTaskMeta::DropAllWorkers => {
                tracing::info!(
                    "finished graceful drop of all transaction workers with : {result:?}"
                );
            }
        }
    }

    #[cfg(feature = "prometheus")]
    fn update_prom_metrics(&self) {
        let num_active_workers = self.tx_worker_handle_map.len();
        let num_connecting_tasks = self.connecting_tasks.len();
        let num_queued_tx = self.tx_queues.values().map(|q| q.len()).sum::<usize>();
        prom::set_orphan_connections(self.orphan_connection_set.len());
        prom::set_active_quic_tx_senders(num_active_workers);
        prom::set_active_quic_connections(self.connection_map.len());
        prom::set_quic_gw_connecting_cnt(num_connecting_tasks);
        prom::set_num_conn_to_evict(self.pending_connection_eviction_set.len());
        prom::set_txn_blocked_by_connection(num_queued_tx);
    }

    fn handle_remote_peer_addr_change(&mut self, remote_peers_changed: HashSet<Pubkey>) {
        for remote_peer in remote_peers_changed {
            if let Some(handle) = self.tx_worker_handle_map.get(&remote_peer) {
                // If we have a worker for the remote peer, we need to update its address.
                let maybe_new_addr = self
                    .leader_tpu_info_service
                    .get_quic_dest_addr(&remote_peer, self.config.tpu_port);
                match maybe_new_addr {
                    Some(new_addr) => {
                        if new_addr != handle.remote_peer_addr {
                            tracing::debug!(
                                "Remote peer address changed: {} from {:?} to {:?}, will cancel worker...",
                                remote_peer,
                                handle.remote_peer_addr,
                                new_addr
                            );
                            // Update the worker's remote address, and its peer too.
                            handle.cancel_notify.notify_one();
                            // WE DON'T WANT TO EVICT THE CONNECTION OR OTHER MULTIPLEXED PEERS SHARING THE SAME CONNECTION.
                            // WE CAN'T ASSUME IF A REMOTE PEER ADDRESS CHANGED, THE OTHER MULTIPLEXED PEERS ADDRESSES ALSO CHANGED.
                            // WE SIMPLY CANCEL THE WORKER, AND WHEN THE WORKER RECONNECTS, IT WILL USE THE NEW ADDRESS.

                            #[cfg(feature = "prometheus")]
                            {
                                prom::incr_quic_gw_remote_peer_addr_changes_detected();
                            }
                        }
                    }
                    None => {
                        // If we don't have a new address, we need to drop the worker.
                        handle.cancel_notify.notify_one();
                        let connection_version = handle.connection_version;
                        let connection_eviction = ConnectionEviction {
                            remote_peer_addr: handle.remote_peer_addr,
                            connection_version,
                        };
                        self.pending_connection_eviction_set
                            .insert(connection_eviction);
                        #[cfg(feature = "prometheus")]
                        {
                            prom::incr_quic_gw_remote_peer_addr_changes_detected();
                        }
                    }
                }
            }
        }
    }

    fn try_predict_upcoming_leaders_if_necessary(&mut self) {
        // THIS BRANCH IS HIGHLY LIKELY TO BE TRUE
        if self.next_leader_prediction_deadline.elapsed() == Duration::ZERO {
            return;
        }

        if let Some(lh) = self
            .config
            .leader_prediction_lookahead
            .map(|nz| nz.get() as u64)
        {
            // Predict every 3 slot.
            let wait_dur_ms = DEFAULT_MS_PER_SLOT * (NUM_CONSECUTIVE_LEADER_SLOTS - 1);
            let wait_dur =
                Duration::from_millis(wait_dur_ms).max(Duration::from_millis(DEFAULT_MS_PER_SLOT));
            self.next_leader_prediction_deadline = Instant::now() + wait_dur;

            let upcoming_leaders = self
                .leader_predictor
                .try_predict_next_n_leaders(lh as usize);
            let mut visited = HashSet::<Pubkey>::with_capacity(upcoming_leaders.len());
            for upcoming_leader in upcoming_leaders {
                let is_already_connectish =
                    self.tx_worker_handle_map.contains_key(&upcoming_leader)
                        || self.connecting_remote_peers.contains_key(&upcoming_leader);

                if !visited.insert(upcoming_leader) {
                    // We have already processed this upcoming leader in this iteration.
                    continue;
                }

                if !is_already_connectish {
                    #[cfg(feature = "prometheus")]
                    {
                        prom::incr_quic_gw_leader_prediction_hit();
                    }
                    tracing::trace!(
                        "Spawning connection for predicted upcoming leader: {}",
                        upcoming_leader
                    );
                    self.spawn_connecting(upcoming_leader, 1, SpawnSource::Prediction);
                } else {
                    #[cfg(feature = "prometheus")]
                    {
                        prom::incr_quic_gw_leader_prediction_miss();
                    }
                }
            }
        } else {
            // If we don't have leader prediction lookahead configured, we don't predict upcoming leaders.
            // Set the next prediction deadline to a long time in the future.
            // So this function exit early next time it is called.
            self.next_leader_prediction_deadline = Instant::now() + FOREVER;
        }
    }

    fn next_orphan_connection_expiration(&self) -> Option<Instant> {
        let oldest = self.orphan_connection_set.oldest()?;
        Some(oldest + self.config.orphan_connection_ttl)
    }

    fn try_evict_orphan_connections(&mut self) {
        let now = Instant::now();
        loop {
            let Some(oldest) = self.orphan_connection_set.oldest() else {
                break;
            };
            if oldest + self.config.orphan_connection_ttl > now {
                break;
            }
            let Some(unused_conn_info_vec) = self.orphan_connection_set.pop() else {
                break;
            };
            for unused_conn_info in unused_conn_info_vec {
                let OrphanConnectionInfo {
                    remote_peer_addr,
                    connection_version,
                } = unused_conn_info;
                // If for some reason, the connection is still active and has multiplexed peers, we skip eviction.
                let is_false_positive =
                    self.connection_map
                        .get(&remote_peer_addr)
                        .is_some_and(|active_conn| {
                            active_conn.connection_version == connection_version
                                && !active_conn
                                    .multiplexed_remote_peer_identity_with_stake
                                    .is_empty()
                        });

                if is_false_positive {
                    continue;
                }

                let Some(active_conn) = self.connection_map.remove(&remote_peer_addr) else {
                    continue;
                };

                if active_conn.connection_version != connection_version {
                    // Connection has been re-established since it was marked as unused.
                    continue;
                }

                assert!(
                    active_conn
                        .multiplexed_remote_peer_identity_with_stake
                        .is_empty(),
                    "Evicting connection to remote peer address: {remote_peer_addr} that still has multiplexed peers",
                );
                active_conn.conn.close(VarInt::from_u32(0), &[0u8]);
                drop(active_conn);
                self.unblock_eviction_waiting_connection();
                #[cfg(feature = "prometheus")]
                {
                    prom::incr_evicted_orphan_connections();
                }
            }
        }
    }

    pub async fn run(mut self) {
        #[cfg(feature = "prometheus")]
        {
            prom::quic_set_identity(self.identity.pubkey());
        }
        #[allow(unused_mut, dead_code)]
        let mut last_metric_update = Instant::now();
        let mut sleep_timer: Option<Pin<Box<Sleep>>> = None;
        loop {
            self.do_eviction_if_required();
            #[cfg(feature = "prometheus")]
            {
                if last_metric_update.elapsed() >= METRIC_UPDATE_INTERVAL {
                    self.update_prom_metrics();
                    last_metric_update = Instant::now();
                }
            }
            self.try_predict_upcoming_leaders_if_necessary();

            let next_connection_expiration = self.next_orphan_connection_expiration();
            match next_connection_expiration {
                Some(expiration_instant) => {
                    let now = Instant::now();
                    if sleep_timer.is_none() {
                        let sleep_dur = expiration_instant.saturating_duration_since(now);
                        // If I understand tokio correclty, the first time you poll a timer it must acquire a mutex lock.
                        // So we box it and pin it to avoid re-creating the timer on every loop iteration since next orphan connection expiration
                        // is unlikely to change until we evict some connections.
                        // Also, the next orphan connection deadline can only increase overtime.
                        sleep_timer = Some(Box::pin(tokio::time::sleep(sleep_dur)));
                    }
                }
                None => {
                    sleep_timer = None;
                }
            };
            tokio::select! {
                maybe = self.tx_inlet.recv() => {
                    match maybe {
                        Some(tx) => {
                            self.accept_tx(tx);
                        }
                        None => {
                            tracing::debug!("Transaction driver inlet closed");
                            break;
                        }
                    }
                }
                _ = async { sleep_timer.as_mut().unwrap().await }, if sleep_timer.is_some() => {
                    self.try_evict_orphan_connections();
                }
                // If cnc_rx returns None, we don't care as clients can safely drop cnc sender and the runtime should keep function.
                Some(command) = self.cnc_rx.recv() => {
                    self.handle_cnc(command).await;
                }

                Some(result) = self.tx_worker_set.join_next_with_id() => {
                    self.handle_worker_result(result);
                }

                Some(result) = self.connecting_tasks.join_next_with_id() => {
                    self.handle_connecting_result(result);
                }

                Some(result) = self.tasklet.join_next_with_id() => {
                    self.handle_tasklet_result(result);
                }
                changes = self.remote_peer_addr_watcher.notified() => {
                    self.handle_remote_peer_addr_change(changes);
                }
            }
        }

        self.schedule_graceful_drop_all_worker();
        while let Some(result) = self.tasklet.join_next_with_id().await {
            self.handle_tasklet_result(result);
        }
    }
}

struct RemotePeerAddrWatcher {
    changes: Arc<StdMutex<HashSet<Pubkey>>>,
    cnc_tx: mpsc::UnboundedSender<RemotePeerAddrWatcherCommand>,
    notify: Arc<Notify>,
}

impl RemotePeerAddrWatcher {
    fn new(
        refresh_interval: Duration,
        tpu_port_kind: TpuPortKind,
        leader_tpu_info_service: Arc<dyn LeaderTpuInfoService + Send + Sync + 'static>,
    ) -> Self {
        tracing::trace!(
            "Spawning remote peer address watcher with refresh interval: {refresh_interval:?}"
        );
        let shared = Default::default();
        let (cnc_tx, cnc_rx) = mpsc::unbounded_channel();
        let notify = Arc::new(Notify::new());

        // The event loop closes when the watcher is dropped.
        let ev_loop = RemotePeerAddrWatcherEvLoop {
            changes: Arc::clone(&shared),
            leader_tpu_info_service,
            cnc_rx,
            peers_to_watch_map: HashMap::new(),
            tpu_port_kind,
            interval: refresh_interval,
            notify: Arc::clone(&notify),
        };

        tokio::spawn(async move {
            ev_loop.run().await;
        });

        RemotePeerAddrWatcher {
            changes: shared,
            cnc_tx,
            notify,
        }
    }

    ///
    /// Registers a watch for a remote peer's address.
    ///
    /// If the remote peer's address changes, it will be notified through the `notified` method.
    /// Note that remote peer address watches are only good for one address change. That means if the remote peer address changes again,
    /// you need to register a new watch in order to be notified about the next change.
    ///
    fn register_watch(&self, remote_peer: Pubkey, addr: SocketAddr) {
        self.cnc_tx
            .send(RemotePeerAddrWatcherCommand::RegisterWatch(
                remote_peer,
                addr,
            ))
            .expect("Remote peer address watcher command channel is closed");
    }

    ///
    /// Deregisters a watch for a remote peer's address.
    ///
    fn forget(&self, remote_peer: Pubkey) {
        self.cnc_tx
            .send(RemotePeerAddrWatcherCommand::DeregisterWatch(remote_peer))
            .expect("Remote peer address watcher command channel is closed");
    }

    ///
    /// Waits for a remote peer address change notification.
    /// Returns a set of remote peers whose addresses have changed since the last notification.
    ///
    /// Cancel-Sefety:
    ///
    /// This method is cancel-safe and won't lose notifications.
    async fn notified(&mut self) -> HashSet<Pubkey> {
        self.notify.notified().await;
        let mut guard = self.changes.lock().expect("Mutex is poisoned");
        std::mem::take(&mut *guard)
    }
}

enum RemotePeerAddrWatcherCommand {
    RegisterWatch(Pubkey, SocketAddr),
    DeregisterWatch(Pubkey),
}

struct RemotePeerAddrWatcherEvLoop {
    changes: Arc<StdMutex<HashSet<Pubkey>>>,
    leader_tpu_info_service: Arc<dyn LeaderTpuInfoService + Send + Sync + 'static>,
    cnc_rx: mpsc::UnboundedReceiver<RemotePeerAddrWatcherCommand>,
    peers_to_watch_map: HashMap<Pubkey, SocketAddr>,
    tpu_port_kind: TpuPortKind,
    interval: Duration,
    notify: Arc<Notify>,
}

impl RemotePeerAddrWatcherEvLoop {
    fn handle_cnc(&mut self, command: RemotePeerAddrWatcherCommand) {
        match command {
            RemotePeerAddrWatcherCommand::RegisterWatch(pubkey, initial_addr) => {
                self.peers_to_watch_map
                    .entry(pubkey)
                    .or_insert(initial_addr);
            }
            RemotePeerAddrWatcherCommand::DeregisterWatch(pubkey) => {
                self.peers_to_watch_map.remove(&pubkey);
            }
        }
    }

    fn update_peers_to_watch(&mut self) {
        let diff = self
            .peers_to_watch_map
            .iter()
            .filter_map(|(pubkey, last_known_addr)| {
                let new_addr = self
                    .leader_tpu_info_service
                    .get_quic_dest_addr(pubkey, self.tpu_port_kind);
                match new_addr {
                    Some(new_addr) => {
                        if new_addr != *last_known_addr {
                            Some(*pubkey)
                        } else {
                            None
                        }
                    }
                    None => Some(*pubkey),
                }
            })
            .collect::<Vec<_>>();
        if diff.is_empty() {
            return;
        }
        {
            let mut guard = self.changes.lock().expect("Mutex is poisoned");
            for pubkey in diff {
                let old = self.peers_to_watch_map.remove(&pubkey).unwrap();
                tracing::trace!(
                    "Remote peer address changed: {}, used to be advertised on {:?}",
                    pubkey,
                    old,
                );
                guard.insert(pubkey);
            }
            self.notify.notify_one();
        }
    }

    async fn run(mut self) {
        let mut interval = interval(self.interval);
        loop {
            tokio::select! {
                _  = interval.tick() => {
                    self.update_peers_to_watch();
                },
                maybe = self.cnc_rx.recv() => {
                    match maybe {
                        Some(command) => {
                            self.handle_cnc(command);
                        }
                        None => {
                            tracing::debug!("Remote peer address watcher inlet closed");
                            break;
                        }
                    }
                }
            }
        }
    }
}

///
/// Context struct holding handles to interact with a spawned TPU sender driver.
///
pub struct TpuSenderSessionContext {
    ///
    /// The [`TpuSenderIdentityUpdater`] use to change the driver configured [`Keypair`].
    ///
    pub identity_updater: TpuSenderIdentityUpdater,

    ///
    /// Sink to send transaction to.
    /// If all reference to the sink are dropped, the underlying driver runtime will stop too.
    ///
    pub driver_tx_sink: mpsc::Sender<TpuSenderTxn>,

    ///
    /// Handle to tokio-based QUIC driver runtime.
    /// Dropping this handle does not interrupt the driver runtime.
    ///
    pub driver_join_handle: JoinHandle<()>,
}

///
/// Callback trait to handle TPU [`TpuSenderResponse`]s.
///
/// # Clone + Safety
///
/// The implementee must be cloneable since each remote peer connection will hold its own instance
/// and call it independently.
///
/// Lastly, the implementation is expected to be thread-safe since the callback can be called from multiple
/// threads.
///
/// # Note
/// A no-op implementation is provided via the [`Nothing`] struct.
///
pub trait TpuSenderResponseCallback: Clone + Send + Sync + 'static {
    fn call(&self, response: TpuSenderResponse);
}

///
/// A no-op implementation of [`TpuSenderResponseCallback`].
///
#[derive(Debug, Clone)]
pub struct Nothing;

impl TpuSenderResponseCallback for Nothing {
    fn call(&self, _response: TpuSenderResponse) {
        // Do nothing
    }
}

///
/// Factory struct to spawn tokio-based QUIC driver
///
pub struct TpuSenderDriverSpawner {
    /// Service to get validator stake info.
    pub stake_info_map: Arc<dyn ValidatorStakeInfoService + Send + Sync + 'static>,
    /// Service to get peers TPU gossip info
    pub leader_tpu_info_service: Arc<dyn LeaderTpuInfoService + Send + Sync + 'static>,
    /// Capacity of the channel used to send transaction to the driver.
    pub driver_tx_channel_capacity: usize,
}

impl TpuSenderDriverSpawner {
    pub fn spawn_default_with_callback<CB>(
        &self,
        identity: Keypair,
        callback_sink: CB,
    ) -> TpuSenderSessionContext
    where
        CB: TpuSenderResponseCallback,
    {
        self.spawn::<CB>(
            identity,
            Default::default(),
            Arc::new(StakeBasedEvictionStrategy::default()),
            Arc::new(IgnorantLeaderPredictor),
            Some(callback_sink),
        )
    }

    pub fn spawn_with_default(&self, identity: Keypair) -> TpuSenderSessionContext {
        self.spawn::<Nothing>(
            identity,
            Default::default(),
            Arc::new(StakeBasedEvictionStrategy::default()),
            Arc::new(IgnorantLeaderPredictor),
            None,
        )
    }

    pub fn spawn<CB>(
        &self,
        identity: Keypair,
        config: TpuSenderConfig,
        eviction_strategy: Arc<dyn ConnectionEvictionStrategy + Send + Sync + 'static>,
        leader_schedule: Arc<dyn UpcomingLeaderPredictor + Send + Sync + 'static>,
        callback_sink: Option<CB>,
    ) -> TpuSenderSessionContext
    where
        CB: TpuSenderResponseCallback,
    {
        self.spawn_on(
            identity,
            config,
            eviction_strategy,
            leader_schedule,
            callback_sink,
            tokio::runtime::Handle::current(),
        )
    }

    pub fn spawn_on<CB>(
        &self,
        identity: Keypair,
        config: TpuSenderConfig,
        eviction_strategy: Arc<dyn ConnectionEvictionStrategy + Send + Sync + 'static>,
        leader_predictor: Arc<dyn UpcomingLeaderPredictor + Send + Sync + 'static>,
        response_callback: Option<CB>,
        driver_rt: Handle,
    ) -> TpuSenderSessionContext
    where
        CB: TpuSenderResponseCallback,
    {
        if config.unsafe_allow_arbitrary_txn_size {
            #[cfg(feature = "intg-testing")]
            {
                tracing::info!(
                    "TpuSenderConfig::allow_arbitrary_txn_size is set to true. This is allowed in integration testing builds."
                );
            }
            #[cfg(not(feature = "intg-testing"))]
            {
                panic!(
                    "TpuSenderConfig::allow_arbitrary_txn_size can only be set to true in integration testing builds."
                );
            }
        }

        let (tx_inlet, tx_outlet) = mpsc::channel(self.driver_tx_channel_capacity);
        let (driver_cnc_tx, driver_cnc_rx) = mpsc::channel(10);

        let (certificate, private_key) = new_dummy_x509_certificate(&identity);
        let cert = Arc::new(QuicClientCertificate {
            certificate,
            key: private_key,
        });

        let mut endpoints = vec![];
        for _ in 0..config.num_endpoints.get() {
            let endpoint = (0..config.max_local_port_binding_attempts)
                .find_map(|_| {
                    let (_, client_socket) = solana_net_utils::bind_in_range(
                        IpAddr::V4(Ipv4Addr::UNSPECIFIED),
                        config.endpoint_port_range,
                    )
                    .ok()?;
                    Endpoint::new(
                        quinn::EndpointConfig::default(),
                        None,
                        client_socket,
                        Arc::new(quinn::TokioRuntime),
                    )
                    .ok()
                })
                .expect("Failed to create QUIC endpoint");

            endpoints.push(endpoint);
        }

        let remote_peer_addr_watcher = RemotePeerAddrWatcher::new(
            config.remote_peer_addr_watch_interval,
            config.tpu_port,
            Arc::clone(&self.leader_tpu_info_service),
        );
        let driver = TpuSenderDriver {
            stake_info_map: Arc::clone(&self.stake_info_map),
            tx_worker_handle_map: Default::default(),
            tx_worker_task_meta_map: Default::default(),
            tx_worker_set: Default::default(),
            active_staked_sorted_remote_peer: Default::default(),
            tx_queues: Default::default(),
            identity,
            connecting_tasks: JoinSet::new(),
            connecting_meta: Default::default(),
            connecting_remote_peers: Default::default(),
            leader_tpu_info_service: Arc::clone(&self.leader_tpu_info_service),
            config,
            client_certificate: cert,
            tx_inlet: tx_outlet,
            response_outlet: response_callback.clone(),
            cnc_rx: driver_cnc_rx,
            tasklet: Default::default(),
            tasklet_meta: Default::default(),
            last_peer_activity: Default::default(),
            being_evicted_peers: Default::default(),
            eviction_strategy,
            connecting_blocked_by_eviction_list: Default::default(),
            endpoints,
            remote_peer_addr_watcher,
            leader_predictor,
            next_leader_prediction_deadline: Instant::now(),
            connecting_remote_peers_addr: Default::default(),
            connection_map: Default::default(),
            connection_version: 0,
            endpoint_sequence: 0,
            pending_connection_eviction_set: Default::default(),
            active_staked_sorted_remote_peer_addr: Default::default(),
            orphan_connection_set: Default::default(),
        };

        let jh = driver_rt.spawn(driver.run());

        TpuSenderSessionContext {
            driver_tx_sink: tx_inlet,
            identity_updater: TpuSenderIdentityUpdater {
                cnc_tx: driver_cnc_tx,
            },
            driver_join_handle: jh,
        }
    }
}

///
/// Handle to update the identity used by the TPU sender driver.
///
pub struct TpuSenderIdentityUpdater {
    ///
    /// Command-and-control channel to send command to the QUIC driver
    ///  
    cnc_tx: mpsc::Sender<DriverCommand>,
}

///
/// All the updater API is set a "mut" concurrent identity update.
///
impl TpuSenderIdentityUpdater {
    ///
    /// Changes the configured identity in the QUIC driver
    ///
    pub async fn update_identity(&mut self, identity: Keypair) {
        let shared = UpdateIdentityInner {
            set: AtomicBool::new(false),
            waker: AtomicWaker::new(),
        };
        let shared = Arc::new(shared);
        let cmd = UpdateIdentityCommand {
            new_identity: identity,
            callback: Arc::clone(&shared),
        };
        self.cnc_tx
            .send(DriverCommand::UpdateIdenttiy(cmd))
            .await
            .expect("disconnected");
        let update_identity = UpdateIdentity {
            inner: shared,
            _this: self,
        };
        update_identity.await
    }

    ///
    /// Changes the configured identity in the QUIC driver,
    ///
    /// waiting on the provided barrier before resuming driver operations.
    ///
    /// # Parameters
    ///
    /// - `identity`: The new identity to set in the driver.
    /// - `barrier`: An `Arc<Barrier>` that the driver will wait on before resuming operations.
    ///
    pub async fn update_identity_with_confirmation_barrier(
        &self,
        identity: Keypair,
        barrier: Arc<Barrier>,
    ) {
        let cmd = MultiStepIdentitySynchronizationCommand {
            new_identity: identity,
            barrier,
        };
        self.cnc_tx
            .send(DriverCommand::MultiStepIdentitySynchronization(cmd))
            .await
            .expect("disconnected");
    }
}

///
/// The shared state used to notify the completion of the identity update.
/// See [`UpdateIdentity`] for more details.
struct UpdateIdentityInner {
    set: AtomicBool,
    waker: AtomicWaker,
}

///
/// Future that waits for the identity update to complete.
/// This future is used to ensure that the identity update is completed before proceeding.
///
pub struct UpdateIdentity<'a> {
    inner: Arc<UpdateIdentityInner>,
    _this: &'a TpuSenderIdentityUpdater, /* phantom data to prevent two threads from updating the identity at the same time */
}

impl Future for UpdateIdentity<'_> {
    type Output = ();

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        // quick check to avoid registration if already done.
        if self.inner.set.load(std::sync::atomic::Ordering::Relaxed) {
            return Poll::Ready(());
        }

        self.inner.waker.register(cx.waker());

        // Need to check condition **after** `register` to avoid a race
        // condition that would result in lost notifications.
        if self.inner.set.load(std::sync::atomic::Ordering::Relaxed) {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

pub const fn module_path_for_test() -> &'static str {
    module_path!()
}

#[cfg(test)]
mod test {
    use {
        super::{
            DriverCommand, StakeSortedPeerSet, TpuSenderIdentityUpdater, UpdateIdentityCommand,
        },
        solana_keypair::Keypair,
        solana_pubkey::Pubkey,
        std::time::Duration,
        tokio::sync::mpsc,
    };

    #[tokio::test]
    async fn test_update_identity_fut() {
        let (cnc_tx, mut cnc_rx) = mpsc::channel(10);
        let mut updater = TpuSenderIdentityUpdater { cnc_tx };

        let jh = tokio::spawn(async move {
            let DriverCommand::UpdateIdenttiy(UpdateIdentityCommand {
                new_identity,
                callback,
            }) = cnc_rx.recv().await.unwrap()
            else {
                panic!("Expected UpdateIdenttiy command");
            };
            tokio::time::sleep(Duration::from_secs(2)).await;
            // This can be relaxed because `wake` hides `Released` memory barrier.
            callback
                .set
                .store(true, std::sync::atomic::Ordering::Relaxed);
            callback.waker.wake();
            new_identity
        });

        let identity = Keypair::new();
        updater.update_identity(identity.insecure_clone()).await;

        let actual = jh.await.unwrap();
        assert_eq!(actual, identity)
    }

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
mod stake_based_eviction_strategy_test {
    use {
        super::{ConnectionEvictionStrategy, StakeSortedPeerSet},
        crate::core::{ConnectionMap, RemotePeerAddrMap, StakedSortedSet},
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

#[cfg(test)]
mod test_remote_peer_addr_map {

    use {
        super::RemotePeerAddrMap,
        crate::core::{ConnectionMap, StakedSortedSet},
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
