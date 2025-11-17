//!
//! This module replaces the legacy `ConnectionCache` from Agave, which was known to over-create connections.
//!
//! Following discussions with Anza, it was concluded that the previous implementation led to excessive fragmentation and increased ping overhead on the network.
//!
//! For a summary of the issues in the old implementation, see: https://gist.github.com/lvboudre/86e965389338758391f72834def72d9b
//!
//! Rather than maintaining a "pool of connection pools" to remote peers, this module optimizes the use of QUIC connections
//! by opening multiple streams over a single connection and multiplexing transactions across them.
//!
//! The number of streams is limited based on the gateway's stake.
//!
//! This design also decouples transaction sending from connection establishment. Since connections can drop during active streams,
//! embedding reconnection logic directly into the sending path would introduce unnecessary complexity and responsibility creep.
//!
//! Connection lifecycle management—including establishment and failure recovery—is handled by `TokioQuicGatewayRuntime`.
//! Meanwhile, transaction sending is delegated to `QuicTxSenderWorker`.
//!
//! `TokioQuicGatewayRuntime` spawns a `QuicTxSenderWorker` for each remote peer. If a connection to a peer is lost,
//! the corresponding worker will terminate and can be restarted by the runtime.
//!
//! Whether or not to re-establish a connection depends on the nature of the error encountered.
//!
//! Compared to the old `ConnectionCache`, this module includes significantly more robust error handling.
//! QUIC connections can fail for a wide variety of reasons, so it’s important to distinguish between recoverable and unrecoverable errors.
//! Added error handlings includes:
//!  1. Fatal errors like incompatible protocol versions or unsupported ALPN protocols will not trigger a reconnection.
//!  2. Non-fatal errors like stream limit exceeded or connection closed will trigger a reconnection.
//!
//! Unlike the deprecated `ConnectionCache`, the QUIC gateway manage connections eviction which is an important
//! part of the QUIC gateway design as it allows to evict lesser staked connections in favor of higher staked connections in
//! case of port exhaustion or maximum number of concurrent connections reached.
//!
//! The [`ConnectionEvictionStrategy`] trait is used to define the eviction strategy.
//! The default eviction strategy is [`StakedBaseEvictionStrategy`], which evicts the lowest staked connections first AND least recently used peer.
//!
//!
//! Finally, QUIC-Gateway reuses the same [`quinn::Endpoint`] for multiple remote peers. Unlike `ConnectionCache`, which created a new endpoint for each remote peer,
//! this should considerably reduce overhead as each [`quinn::Endpoint`] has its own event loop.
//!
//!
//! Note that this module does not implement retry logic beyond attempting to reconnect when appropriate and safe to do so.
//!
use {
    crate::quic_client::{
        config::{ConfigQuicTpuPort, TpuOverrideInfo},
        prom,
    },
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
    solana_net_utils::{PortRange, VALIDATOR_PORT_RANGE},
    solana_pubkey::Pubkey,
    solana_quic_definitions::{QUIC_KEEP_ALIVE, QUIC_MAX_TIMEOUT, QUIC_SEND_FAIRNESS},
    solana_signature::Signature,
    solana_signer::Signer,
    solana_streamer::nonblocking::quic::ALPN_TPU_PROTOCOL_ID,
    solana_tls_utils::{QuicClientCertificate, SkipServerVerification, new_dummy_x509_certificate},
    std::{
        collections::{BTreeMap, HashMap, HashSet, VecDeque},
        net::{IpAddr, Ipv4Addr, SocketAddr},
        num::NonZeroUsize,
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
        time::interval,
    },
};

///
/// Each [`quinn::Endpoint`] has its own event-loop.
/// Each endpoint can manage thousands of connections concurrently.
/// HOWEVER, each [`quinn::Endpoint`] has a state mutex lock.
/// Quickly looking at quinn's source code, it seems that each lock acquisition is quite short live.
/// If we have too many connections over a single endpoint, we might end up with a lot of contention on the endpoint mutex.
/// At the same time, if we have too many endpoints, we might end up with too many event loops running concurrently.
/// Talking with Anza, we should not open more than 5 endpoints to host QUIC connections.
pub const DEFAULT_QUIC_GATEWAY_ENDPOINT_COUNT: NonZeroUsize =
    NonZeroUsize::new(5).expect("default endpoint count must be non-zero");
pub const DEFAULT_CONNECTION_TIMEOUT: Duration = Duration::from_secs(2);
pub const DEFAULT_QUIC_MAX_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
pub const DEFAULT_MAX_CONSECUTIVE_CONNECTION_ATTEMPT: usize = 3;
pub const DEFAULT_PER_PEER_TRANSACTION_QUEUE_SIZE: usize = 10_000;
pub const DEFAULT_MAX_CONCURRENT_CONNECTIONS: usize = 1024;
pub const DEFAULT_MAX_LOCAL_BINDING_PORT_ATTEMPTS: usize = 3;
pub const DEFAULT_LEADER_DURATION: Duration = Duration::from_secs(2); // 400ms * 4 rounded to seconds
pub const DEFAULT_MAX_SEND_ATTEMPT: NonZeroUsize = NonZeroUsize::new(3).unwrap();
pub const DEFAULT_REMOTE_PEER_ADDR_WATCH_INTERVAL: Duration = Duration::from_secs(5);
pub const DEFAULT_TX_SEND_TIMEOUT: Duration = Duration::from_secs(2);

pub const DEFAULT_LEADER_PREDICTION_LOOKAHEAD: NonZeroUsize = NonZeroUsize::new(4).unwrap();

#[derive(thiserror::Error, Debug)]
pub(crate) enum ConnectingError {
    #[error(transparent)]
    ConnectError(#[from] quinn::ConnectError),
    #[error(transparent)]
    ConnectionError(#[from] quinn::ConnectionError),
    #[error("Connection to remote peer not in leader schedule")]
    PeerNotInLeaderSchedule,
}

pub struct QuicGatewayConfig {
    pub port_range: PortRange,

    pub max_idle_timeout: Duration,

    // TODO check if we really need keep alive interval.
    // we could use `max_idle_timeout` to detect dead connections and naturally stopped tx sender workers.
    // pub keep_alive_interval: Option<Duration>,
    ///
    /// Maximum number of consecutive connection attempts
    ///
    pub max_connection_attempts: usize,

    ///
    /// Capacity of the transaction sender worker channel per remote peer.
    ///
    pub transaction_sender_worker_channel_capacity: usize,

    ///
    /// Timeout for establishing a connection to a remote peer.
    ///
    pub connecting_timeout: Duration,

    pub tpu_port_kind: ConfigQuicTpuPort,

    pub max_concurrent_connections: usize,

    ///
    /// Maximum number of attempts to bind a local port to a remote peer.
    ///
    pub max_local_port_binding_attempts: usize,

    ///
    /// How many endpoints to create for the QUIC gateway.
    /// Each endpoint will be bound to a different port in the port range.
    /// Each endpoint has its own "event loop" and can handle multiple connections concurrently.
    ///
    /// The number of endpoints should not be greater than the numbe of CPU cores dedicated to jet.
    ///
    /// The number of endpoints depends on the stake of the gateway as lower stake gateway should require less endpoints.
    ///
    /// Recommanded try 1 endpoint per 8 CPU cores dedicated to jet.
    ///
    pub num_endpoints: NonZeroUsize,

    ///
    /// Maximum number of consecutive transaction sending attempts to a remote peer.
    /// Attempt may fail due to connection losts, stream limit exceeded, etc.
    /// It might be useful to retry sending a transaction at least 2-3 times before giving up.
    ///
    pub max_send_attempt: NonZeroUsize,

    ///
    /// Interval to watch remote peer address changes.
    ///
    pub remote_peer_addr_watch_interval: Duration,

    ///
    /// Timeout for sending a transaction to a remote peer.
    ///
    pub send_timeout: Duration,

    ///
    /// Maximum number of leaders to predict
    ///
    pub leader_prediction_lookahead: Option<NonZeroUsize>,
}

impl Default for QuicGatewayConfig {
    fn default() -> Self {
        Self {
            port_range: VALIDATOR_PORT_RANGE,
            max_idle_timeout: QUIC_MAX_TIMEOUT,
            max_connection_attempts: DEFAULT_MAX_CONSECUTIVE_CONNECTION_ATTEMPT,
            transaction_sender_worker_channel_capacity: DEFAULT_PER_PEER_TRANSACTION_QUEUE_SIZE,
            connecting_timeout: DEFAULT_CONNECTION_TIMEOUT,
            max_concurrent_connections: DEFAULT_MAX_CONCURRENT_CONNECTIONS,
            max_local_port_binding_attempts: DEFAULT_MAX_LOCAL_BINDING_PORT_ATTEMPTS,
            tpu_port_kind: ConfigQuicTpuPort::default(),
            num_endpoints: DEFAULT_QUIC_GATEWAY_ENDPOINT_COUNT,
            max_send_attempt: DEFAULT_MAX_SEND_ATTEMPT,
            remote_peer_addr_watch_interval: DEFAULT_REMOTE_PEER_ADDR_WATCH_INTERVAL,
            send_timeout: DEFAULT_TX_SEND_TIMEOUT,
            leader_prediction_lookahead: Some(DEFAULT_LEADER_PREDICTION_LOOKAHEAD),
        }
    }
}

pub struct SentOk {
    pub e2e_time: Duration,
}

///
/// Metadata about an inflight connection attempt to a remote peer.
///
struct ConnectingMeta {
    current_client_identity: Pubkey,
    remote_peer_identity: Pubkey,
    connection_attempt: usize,
    endpoint_idx: usize,
    created_at: Instant,
}

///
/// Inner part of the update identity command.
///
struct UpdateGatewayIdentityCommand {
    new_identity: Keypair,
    callback: Arc<UpdateIdentityInner>,
}

struct MultiStepIdentitySynchronizationCommand {
    new_identity: Keypair,
    barrier: Arc<Barrier>,
}

///
/// Command to control gateway behavior.
///
enum GatewayCommand {
    UpdateIdenttiy(UpdateGatewayIdentityCommand),
    MultiStepIdentitySynchronization(MultiStepIdentitySynchronizationCommand),
}

enum TokioGatewayTaskMeta {
    DropAllWorkers,
}

struct TxWorkerSenderHandle {
    remote_peer_identity: Pubkey,
    remote_peer_addr: SocketAddr,
    sender: mpsc::Sender<GatewayTransaction>,
    cancel_notify: Arc<Notify>,
}

struct WaitingEviction {
    remote_peer_identity: Pubkey,
    notify: Arc<Notify>,
}

#[derive(Debug, Default)]
struct EndpointUsage {
    connected_remote_peers: HashSet<Pubkey>,
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
    fn get_stake_info(&self, validator_pubkey: &Pubkey) -> Option<u64>;
}

const FOREVER: Duration = Duration::from_secs(31_536_000); // One year is considered "forever" in this context.

///
/// Tokio-based runtime to driver a QUIC gateway.
pub(crate) struct TokioQuicGatewayRuntime {
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
    tx_worker_task_meta_map: HashMap<Id, Pubkey>,

    ///
    /// JoinSet of all transaction sender workers.
    ///
    tx_worker_set: JoinSet<TxSenderWorkerCompleted>,

    ///
    /// Transaction queues per remote identity waiting for connection to be come available.
    ///
    tx_queues: HashMap<Pubkey, VecDeque<(GatewayTransaction, usize)>>,

    ///
    /// The runtime to spawn transation sender worker on.
    tx_worker_rt: tokio::runtime::Handle,

    endpoints: Vec<Endpoint>,
    endpoints_usage: Vec<EndpointUsage>,

    ///
    /// JoinSet of inflight connection attempt
    ///
    connecting_tasks: JoinSet<Result<Connection, ConnectingError>>,

    ///
    /// Metadata about inflight connection attempt.
    ///
    connecting_meta: HashMap<tokio::task::Id, ConnectingMeta>,

    ///
    /// Reversed of [`TokioQuicGatewayRuntime::connecting_meta`]
    ///
    connecting_remote_peers: HashMap<Pubkey, tokio::task::Id>,

    ///
    /// Service to locate tpu port address from remote peer identity.
    ///
    leader_tpu_info_service: Arc<dyn LeaderTpuInfoService + Send + Sync + 'static>,

    config: QuicGatewayConfig,

    ///
    /// Current certificate set
    ///
    client_certificate: Arc<QuicClientCertificate>,

    ///
    /// Current set gateway identity
    ///
    identity: Keypair,

    ///
    /// Transaction inlet channel : where transaction comes from.
    ///
    tx_inlet: mpsc::Receiver<GatewayTransaction>,

    ///
    /// Outlet to send transaction "sent" status.
    ///
    response_outlet: mpsc::UnboundedSender<GatewayResponse>,

    ///
    /// Command-and-control channel : low-bandwidth channel to receive gateway configuration mutation.
    ///
    cnc_rx: mpsc::Receiver<GatewayCommand>,

    tasklet: JoinSet<()>,
    tasklet_meta: HashMap<Id, TokioGatewayTaskMeta>,

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
}

pub trait LeaderTpuInfoService {
    fn get_quic_tpu_socket_addr(&self, leader_pubkey: Pubkey) -> Option<SocketAddr>;
    fn get_quic_tpu_fwd_socket_addr(&self, leader_pubkey: Pubkey) -> Option<SocketAddr>;
    fn get_quic_dest_addr(
        &self,
        leader_pubkey: Pubkey,
        tpu_port_kind: ConfigQuicTpuPort,
    ) -> Option<SocketAddr> {
        match tpu_port_kind {
            ConfigQuicTpuPort::Normal => self.get_quic_tpu_socket_addr(leader_pubkey),
            ConfigQuicTpuPort::Forwards => self.get_quic_tpu_fwd_socket_addr(leader_pubkey),
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
    fn get_quic_tpu_socket_addr(&self, leader_pubkey: Pubkey) -> Option<SocketAddr> {
        self.override_vec
            .iter()
            .find(|info| info.remote_peer == leader_pubkey)
            .map(|info| info.quic_tpu)
            .or_else(|| self.other.get_quic_tpu_socket_addr(leader_pubkey))
    }
    fn get_quic_tpu_fwd_socket_addr(&self, leader_pubkey: Pubkey) -> Option<SocketAddr> {
        self.override_vec
            .iter()
            .find(|info| info.remote_peer == leader_pubkey)
            .map(|info| info.quic_tpu_forward)
            .or_else(|| self.other.get_quic_tpu_fwd_socket_addr(leader_pubkey))
    }
}

///
/// A transaction with destination details to be sent to a remote peer.
///
#[derive(Debug, Clone)]
pub struct GatewayTransaction {
    /// Id set by the sender to identify the transaction. Only meaningful to the sender.
    pub tx_sig: Signature,
    /// The wire format of the transaction.
    pub wire: Bytes,
    /// The pubkey of the remote peer to send the transaction to.
    pub remote_peer: Pubkey,
}

#[derive(thiserror::Error, Debug)]
pub enum SendTxError {
    #[error(transparent)]
    ConnectionError(#[from] ConnectionError),
    #[error("Failed to send transaction to remote peer {0:?}")]
    StreamStopped(VarInt),
    #[error("stream is closed or reset by remote peer")]
    StreamClosed,
    #[error("0-RTT rejected by remote peer")]
    ZeroRttRejected,
}

#[derive(Debug)]
pub struct GatewayTxSent {
    pub remote_peer_identity: Pubkey,
    pub remote_peer_addr: SocketAddr,
    pub tx_sig: Signature,
}

#[derive(Debug)]
pub struct GatewayTxFailed {
    pub remote_peer_identity: Pubkey,
    pub remote_peer_addr: SocketAddr,
    pub tx_sig: Signature,
    pub failure_reason: String,
}

#[derive(Clone, Debug, Display)]
pub enum TxDropReason {
    #[display("reached downstream transaction worker transaction queue capacity")]
    RateLimited,
    #[display("remote peer is unreachable")]
    RemotePeerUnreachable,
    #[display("tx got drop by gateway")]
    DropByGateway,
    #[display("remote peer is being evicted")]
    RemotePeerBeingEvicted,
}

#[derive(Debug)]
pub struct TxDrop {
    pub remote_peer_identity: Pubkey,
    // pub tx_sig: Signatu
    pub drop_reason: TxDropReason,
    pub dropped_gateway_tx_vec: VecDeque<(GatewayTransaction, usize)>,
}

#[derive(Debug)]
pub enum GatewayResponse {
    TxSent(GatewayTxSent),
    TxFailed(GatewayTxFailed),
    TxDrop(TxDrop),
}

///
/// A task to connect to a remote peer.
///
struct ConnectingTask {
    service: Arc<dyn LeaderTpuInfoService + Send + Sync + 'static>,
    remote_peer_identity: Pubkey,
    cert: Arc<QuicClientCertificate>,
    max_idle_timeout: Duration,
    connection_timeout: Duration,
    tpu_port_kind: ConfigQuicTpuPort,
    wait_for_eviction: Option<Arc<Notify>>,
    endpoint: Endpoint,
}

/// Translate a SocketAddr into a valid SNI for the purposes of QUIC connection
///
/// We do not actually check if the server holds a cert for this server_name
/// since Solana does not rely on DNS names, but we need to provide a unique
/// one to ensure that we present correct QUIC tokens to the correct server.
///
/// Code taken from https://github.com/anza-xyz/agave/pull/7260
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
    async fn run(self) -> Result<Connection, ConnectingError> {
        if let Some(signal) = &self.wait_for_eviction {
            tracing::trace!(
                "Waiting for eviction to complete before connecting to remote peer: {}",
                self.remote_peer_identity
            );
            signal.notified().await;
        }

        let remote_peer_addr = self
            .service
            .get_quic_dest_addr(self.remote_peer_identity, self.tpu_port_kind);
        let remote_peer_addr = remote_peer_addr.ok_or(ConnectingError::PeerNotInLeaderSchedule)?;
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
struct QuicTxSenderWorker {
    remote_peer: Pubkey,
    connection: Connection,
    /// The current client identity being used for the connection
    current_client_identity: Pubkey,
    incoming_rx: mpsc::Receiver<GatewayTransaction>,
    output_tx: mpsc::UnboundedSender<GatewayResponse>,
    tx_queue: VecDeque<(GatewayTransaction, usize)>,
    cancel_notify: Arc<Notify>,
    max_tx_attempt: NonZeroUsize,
    // TODO: Check if this is necessary, since there is already flow control in QUIC
    // Moreover, most QUIC API are instantaneous and do not block
    // and if a connection is idle for 2s it will be closed anyway, moreover remote validator could technically decide to kick us off
    #[allow(dead_code)]
    tx_send_timeout: Duration,
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
    rx: mpsc::Receiver<GatewayTransaction>,
    pending_tx: VecDeque<(GatewayTransaction, usize)>,
    canceled: bool,
}

impl QuicTxSenderWorker {
    async fn send_tx(&mut self, tx: Bytes) -> Result<SentOk, SendTxError> {
        let t = Instant::now();
        let mut uni = self.connection.open_uni().await?;
        uni.write_all(&tx).await.map_err(|e| match e {
            WriteError::Stopped(var_int) => SendTxError::StreamStopped(var_int),
            WriteError::ConnectionLost(connection_error) => {
                SendTxError::ConnectionError(connection_error)
            }
            WriteError::ClosedStream => SendTxError::StreamClosed,
            WriteError::ZeroRttRejected => SendTxError::ZeroRttRejected,
        })?;
        let e2e_time = t.elapsed();
        let ok = SentOk { e2e_time };
        Ok(ok)
    }
    async fn process_tx(
        &mut self,
        tx: GatewayTransaction,
        attempt: usize,
    ) -> Option<TxSenderWorkerError> {
        let result = self.send_tx(tx.wire.clone()).await;
        let remote_addr = self.connection.remote_address();
        let tx_sig = tx.tx_sig;
        match result {
            Ok(sent_ok) => {
                prom::quic_send_attempts_inc(self.remote_peer, remote_addr, "success");
                prom::incr_quic_gw_worker_tx_process_cnt(self.remote_peer, "success");
                tracing::debug!(
                    "Tx sent to remote peer: {} in {:?}",
                    self.remote_peer,
                    sent_ok.e2e_time
                );
                let resp = GatewayTxSent {
                    remote_peer_identity: self.remote_peer,
                    remote_peer_addr: self.connection.remote_address(),
                    tx_sig,
                };
                let _ = self.output_tx.send(GatewayResponse::TxSent(resp));
                prom::observe_send_transaction_e2e_latency(self.remote_peer, sent_ok.e2e_time);
                let path_stats = self.connection.stats().path;
                let current_mut = path_stats.current_mtu;
                prom::set_leader_mtu(self.remote_peer, current_mut);
                prom::observe_leader_rtt(self.remote_peer, path_stats.rtt);
                None
            }
            Err(e) => {
                prom::quic_send_attempts_inc(self.remote_peer, remote_addr, "error");
                if attempt >= self.max_tx_attempt.get() {
                    prom::incr_quic_gw_worker_tx_process_cnt(self.remote_peer, "error");
                    let resp = GatewayTxFailed {
                        remote_peer_identity: self.remote_peer,
                        remote_peer_addr: self.connection.remote_address(),
                        failure_reason: e.to_string(),
                        tx_sig,
                    };
                    tracing::warn!(
                        "Giving up sending transaction: {} to remote peer: {}, client identity: {}, after {} attempts: {:?}",
                        tx_sig,
                        self.remote_peer,
                        self.current_client_identity,
                        attempt,
                        e
                    );
                    let _ = self.output_tx.send(GatewayResponse::TxFailed(resp));
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
        let maybe_err = loop {
            tracing::trace!(
                "worker {} tick loop -- queue size: {}",
                self.remote_peer,
                self.tx_queue.len()
            );

            if let Some(e) = self.try_process_tx_in_queue().await {
                break Some(e);
            }

            tokio::select! {
                maybe = self.incoming_rx.recv() => {
                    match maybe {
                        Some(tx) => {
                            tracing::trace!("Received tx: {} for remote peer: {}", tx.tx_sig, self.remote_peer);
                            self.tx_queue.push_back((tx, 1));
                        }
                        None => {
                            tracing::debug!("Transaction sender inlet closed for remote peer: {:?}", self.remote_peer);
                            break None;
                        }
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
/// Connection eviction is called when the QUIC gateway does not have a local port available
/// to use for new QUIC connections.
///
pub trait ConnectionEvictionStrategy {
    ///
    /// Plan up to `plan_ahead_size` [`quinn::Connection`] to evicts.
    ///
    /// Arguments:
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
}

#[derive(Debug, Default)]
pub struct StakeSortedPeerSet {
    peer_stake_map: HashMap<Pubkey, u64 /* stake */>,
    sorted_map: BTreeMap<u64 /* stake */, HashSet<Pubkey>>,
}

impl StakeSortedPeerSet {
    pub fn remove(&mut self, peer: &Pubkey) -> bool {
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

    pub fn insert(&mut self, peer: Pubkey, stake: u64) -> bool {
        let already_present = self.remove(&peer);
        self.peer_stake_map.insert(peer, stake);
        self.sorted_map.entry(stake).or_default().insert(peer);
        already_present
    }

    pub fn iter(&self) -> impl Iterator<Item = (u64, Pubkey)> {
        self.sorted_map
            .iter()
            .flat_map(|(stake, peers)| peers.iter().map(|peer| (*stake, *peer)))
    }

    pub fn is_empty(&self) -> bool {
        self.peer_stake_map.is_empty()
    }
}

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
                let last_usage = usage_table.get(peer).expect("missing last activity");
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
}

/// Here's the simplified flow of a transaction through the QUIC gateway:
///
///  ┌────────────┐      ┌────────────┐       ┌───────────────┐
///  │Transaction │      │  QUIC      │       │ TxSenderWorker│        (Remote Validator)
///  │ Source     ┼──1──►│ Gateway    ┼──2────►               ┼──3────►
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
impl TokioQuicGatewayRuntime {
    ///
    /// Spawns a "connecting" task to a remote peer.
    /// this is called when a transaction is received for a remote peer which does not have a worker installed yet.
    ///
    fn spawn_connecting(&mut self, remote_peer_identity: Pubkey, attempt: usize) {
        if self
            .connecting_remote_peers
            .contains_key(&remote_peer_identity)
        {
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
            self.drop_peer_queued_tx(remote_peer_identity, TxDropReason::RemotePeerBeingEvicted);
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

        // We need signal to wait for eviction to complete before we can proceed with the connection.
        // Otherwise, the connecting attempt may fail to bind a local port.
        let maybe_wait_for_eviction = if !self.has_connection_capacity() {
            let notify = Arc::new(Notify::new());
            let waiting_eviction = WaitingEviction {
                remote_peer_identity,
                notify: Arc::clone(&notify),
            };
            self.connecting_blocked_by_eviction_list
                .push_back(waiting_eviction);
            Some(notify)
        } else {
            None
        };

        let service = Arc::clone(&self.leader_tpu_info_service);
        let endpoint_idx = self.get_least_used_endpoint();
        let cert = Arc::clone(&self.client_certificate);
        let max_idle_timeout = self.config.max_idle_timeout;
        let fut = ConnectingTask {
            service,
            remote_peer_identity,
            cert,
            max_idle_timeout,
            connection_timeout: self.config.connecting_timeout,
            tpu_port_kind: self.config.tpu_port_kind,
            wait_for_eviction: maybe_wait_for_eviction,
            endpoint: self.endpoints[endpoint_idx].clone(),
        }
        .run();
        let meta = ConnectingMeta {
            current_client_identity: self.identity.pubkey(),
            remote_peer_identity,
            connection_attempt: attempt,
            endpoint_idx,
            created_at: Instant::now(),
        };
        self.endpoints_usage[endpoint_idx]
            .connected_remote_peers
            .insert(remote_peer_identity);
        let abort_handle = self.connecting_tasks.spawn(fut);
        tracing::trace!(
            "Spawning connection for remote peer: {remote_peer_identity}, attempt: {attempt}"
        );
        self.connecting_remote_peers
            .insert(remote_peer_identity, abort_handle.id());
        self.connecting_meta.insert(abort_handle.id(), meta);
    }

    fn get_least_used_endpoint(&mut self) -> usize {
        self.endpoints_usage
            .iter()
            .enumerate()
            .min_by_key(|(_, usage)| usage.connected_remote_peers.len())
            .map(|(idx, _)| idx)
            .expect("At least one endpoint should be available")
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
            let total = tx_queues.len();
            prom::incr_quic_gw_drop_tx_cnt(remote_peer_identity, total as u64);
            let tx_drop = TxDrop {
                remote_peer_identity,
                drop_reason: reason.clone(),
                dropped_gateway_tx_vec: tx_queues,
            };
            let _ = self.response_outlet.send(GatewayResponse::TxDrop(tx_drop));
        }
    }

    fn unreachable_peer(&mut self, remote_peer_identity: Pubkey) {
        prom::inc_quic_gw_unreachable_peer_count(remote_peer_identity);
        self.drop_peer_queued_tx(remote_peer_identity, TxDropReason::RemotePeerUnreachable);
    }

    fn has_connection_capacity(&self) -> bool {
        self.tx_worker_handle_map.len() < self.max_concurrent_connection()
    }

    const fn max_concurrent_connection(&self) -> usize {
        self.config.max_concurrent_connections
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
            .saturating_sub(self.being_evicted_peers.len());

        if eviction_count_required == 0 {
            return;
        }
        let eviction_plan = self.eviction_strategy.plan_eviction(
            Instant::now(),
            &self.active_staked_sorted_remote_peer,
            &self.last_peer_activity,
            &self.being_evicted_peers,
            eviction_count_required,
        );
        tracing::trace!("Planned {} evictions", eviction_plan.len());
        eviction_plan
            .into_iter()
            .flat_map(|peer| self.tx_worker_handle_map.get(&peer))
            .for_each(|handle| {
                handle.cancel_notify.notify_one();
                prom::incr_quic_gw_total_connection_evictions_cnt(1);
                self.being_evicted_peers.insert(handle.remote_peer_identity);
            });
    }

    ///
    /// Installs a transaction sender worker for a remote peer with the given connection.
    ///
    fn install_worker(&mut self, remote_peer_identity: Pubkey, connection: Connection) {
        let (tx, rx) = mpsc::channel(self.config.transaction_sender_worker_channel_capacity);

        let remote_peer_addr = connection.remote_address();
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
            tx_send_timeout: self.config.send_timeout,
        };

        let worker_fut = worker.run();
        let ah = self.tx_worker_set.spawn_on(worker_fut, &self.tx_worker_rt);
        let handle = TxWorkerSenderHandle {
            remote_peer_identity,
            remote_peer_addr,
            sender: tx,
            cancel_notify,
        };
        assert!(
            self.tx_worker_handle_map
                .insert(remote_peer_identity, handle)
                .is_none()
        );
        self.tx_worker_task_meta_map
            .insert(ah.id(), remote_peer_identity);
        tracing::debug!("Installed tx worker for remote peer: {remote_peer_identity}");
    }

    ///
    /// Handles the result of a connection attempt to a remote peer.
    ///
    /// Reattempts the connection if it fails, up to the maximum number of attempts, unless the peer is unreachable.
    ///
    fn handle_connecting_result(
        &mut self,
        result: Result<(task::Id, Result<Connection, ConnectingError>), JoinError>,
    ) {
        match result {
            Ok((task_id, result)) => {
                let ConnectingMeta {
                    current_client_identity,
                    remote_peer_identity,
                    connection_attempt,
                    endpoint_idx,
                    created_at,
                } = self
                    .connecting_meta
                    .remove(&task_id)
                    .expect("connecting_meta");
                prom::observe_quic_gw_connection_time(created_at.elapsed());
                self.endpoints_usage[endpoint_idx]
                    .connected_remote_peers
                    .remove(&remote_peer_identity);
                let _ = self.connecting_remote_peers.remove(&remote_peer_identity);

                if self.identity.pubkey() != current_client_identity {
                    // THIS SHOULD NOT HAPPEN SINCE ON IDENTITY CHANGE WE ABORT ALL CONNECTING TASKS
                    // BUT JUST IN CASE, WE CHECK AGAIN.
                    tracing::warn!(
                        "Abandoning connection attempt to remote peer: {remote_peer_identity} since the client identity has changed"
                    );
                    self.spawn_connecting(remote_peer_identity, connection_attempt);
                    return;
                }

                match result {
                    Ok(conn) => {
                        tracing::debug!("Connected to remote peer: {:?}", remote_peer_identity);
                        let remote_peer_stake = self
                            .stake_info_map
                            .get_stake_info(&remote_peer_identity)
                            .unwrap_or(0);
                        self.active_staked_sorted_remote_peer
                            .insert(remote_peer_identity, remote_peer_stake);

                        self.remote_peer_addr_watcher
                            .register_watch(remote_peer_identity, conn.remote_address());

                        self.install_worker(remote_peer_identity, conn);
                        prom::incr_quic_gw_connection_success_cnt();
                    }
                    Err(connect_err) => {
                        prom::incr_quic_gw_connection_failure_cnt();

                        match connect_err {
                            ConnectingError::ConnectError(
                                quinn::ConnectError::EndpointStopping,
                            ) => {
                                // This should never happen, but if it does, we panic.
                                // The endpoint is stopping, so we cannot connect to the remote peer.
                                panic!(
                                    "Endpoint is stopping, cannot connect to remote peer: {remote_peer_identity}, with identity: {current_client_identity}"
                                );
                            }
                            ConnectingError::ConnectionError(_)
                                if connection_attempt < self.config.max_connection_attempts =>
                            {
                                self.spawn_connecting(
                                    remote_peer_identity,
                                    connection_attempt.saturating_add(1),
                                );
                            }
                            whatever => {
                                tracing::warn!(
                                    "Failed to connect to remote peer: {remote_peer_identity}, with identity: {current_client_identity}, error: {whatever:?}"
                                );
                                self.unreachable_peer(remote_peer_identity);
                            }
                        }
                    }
                }
            }
            Err(join_err) => {
                prom::incr_quic_gw_connection_failure_cnt();
                let ConnectingMeta {
                    current_client_identity: _,
                    remote_peer_identity,
                    connection_attempt: _,
                    endpoint_idx,
                    created_at: _,
                } = self
                    .connecting_meta
                    .remove(&join_err.id())
                    .expect("connecting_meta");

                self.endpoints_usage[endpoint_idx]
                    .connected_remote_peers
                    .remove(&remote_peer_identity);
                let _ = self.connecting_remote_peers.remove(&remote_peer_identity);
                tracing::error!(
                    "Join error during connecting to {remote_peer_identity:?}: {:?}",
                    join_err
                );
            }
        }
    }

    ///
    /// Accepts a transaction and determines how to handle it based on the remote peer's status.
    ///
    /// If a transaction sender worker exists for the remote peer, it is fowarded to it.
    /// If not, the transaction is queued for later processing and a connection attempt is scheduled.
    ///
    fn accept_tx(&mut self, tx: GatewayTransaction) {
        let remote_peer_identity = tx.remote_peer;
        self.last_peer_activity
            .insert(remote_peer_identity, Instant::now());
        let tx_id = tx.tx_sig;
        // Do I have a transaction sender worker for this remote peer?
        if let Some(handle) = self.tx_worker_handle_map.get(&remote_peer_identity) {
            // If we have an active transaction sender worker for the remote peer,
            prom::incr_quic_gw_tx_connection_cache_hit_cnt();
            match handle.sender.try_send(tx) {
                Ok(_) => {
                    tracing::trace!("{tx_id} sent to worker");
                    prom::incr_quic_gw_tx_relayed_to_worker(remote_peer_identity);
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
                            dropped_gateway_tx_vec: VecDeque::from([(tx, 1)]),
                        };
                        prom::incr_quic_gw_drop_tx_cnt(remote_peer_identity, 1);
                        let _ = self.response_outlet.send(GatewayResponse::TxDrop(txdrop));
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
            prom::incr_quic_gw_tx_connection_cache_miss_cnt();
            // We don't have any active transaction sender worker for the remote peer,
            // we need to queue the transaction and try to spawn a new connection.
            self.tx_queues
                .entry(remote_peer_identity)
                .or_default()
                .push_back((tx, 1));
            tracing::trace!("queuing tx: {:?}", tx_id);

            // Check if we are not already connecting to this remote peer.
            if !self
                .connecting_remote_peers
                .contains_key(&remote_peer_identity)
            {
                // If the remote peer is already being connected, just queue the tx.
                self.spawn_connecting(remote_peer_identity, 1);
            }
        }
    }

    fn unblock_eviction_waiting_connection(&mut self) {
        while let Some(WaitingEviction {
            remote_peer_identity,
            notify,
        }) = self.connecting_blocked_by_eviction_list.pop_front()
        {
            notify.notify_one();

            // If we are still trying to connect to this peer, stop unblocking more.
            if self
                .connecting_remote_peers
                .contains_key(&remote_peer_identity)
            {
                tracing::trace!(
                    "Unblocked waiting connection for remote peer: {}",
                    remote_peer_identity
                );
                break;
            }
            // Else continue the loop to unblock the next waiting connection.
        }
    }

    ///
    /// One of the transaction sender worker has completed its work or failed.
    ///
    fn handle_worker_result(&mut self, result: Result<(Id, TxSenderWorkerCompleted), JoinError>) {
        match result {
            Ok((id, mut worker_completed)) => {
                let remote_peer_identity = self
                    .tx_worker_task_meta_map
                    .remove(&id)
                    .expect("tx worker meta");
                self.active_staked_sorted_remote_peer
                    .remove(&remote_peer_identity);
                self.remote_peer_addr_watcher.forget(remote_peer_identity);

                self.unblock_eviction_waiting_connection();
                let is_evicted_conn = self.being_evicted_peers.remove(&remote_peer_identity);
                let worker_tx = self
                    .tx_worker_handle_map
                    .remove(&remote_peer_identity)
                    .expect("tx worker sender");
                self.last_peer_activity.remove(&remote_peer_identity);
                drop(worker_tx);

                tracing::trace!(
                    "Tx worker for remote peer: {:?} completed, err: {:?}, canceled: {}, evicted: {}",
                    remote_peer_identity,
                    worker_completed.err,
                    worker_completed.canceled,
                    is_evicted_conn
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

                prom::incr_quic_gw_connection_close_cnt();
                if is_peer_unreachable {
                    // If the peer is unreachable, we drop all queued transactions for it.
                    self.unreachable_peer(remote_peer_identity);
                } else if is_evicted_conn {
                    // If the worker was schedule for eviction, we simply drop all queued transactions
                    // because evicted workers are not expected to reconnect soon since they have been
                    // chosen to be eviction strategy. We don't want to be stuck in a loop
                    // where we keep evicting the same peer, so we drop the queued transactions.
                    // and start from a clean slate.
                    tracing::trace!(
                        "Remote peer: {} tx worker was canceled, will not reconnect",
                        remote_peer_identity
                    );
                    self.drop_peer_queued_tx(remote_peer_identity, TxDropReason::DropByGateway);
                } else if !tx_to_rescue.is_empty() {
                    // If the worker didn't have a fatal error and was not evicted and still has queued transactions,
                    // we can safely reattempt to connect to the remote peer.
                    // This can happen to transient network errors or remote peer being temporarily unavailable.
                    // We can safely resume connection and try to send the queued transactions.
                    tracing::trace!(
                        "Remote peer: {} has queued tx, wil reconnect",
                        remote_peer_identity
                    );
                    self.last_peer_activity
                        .insert(remote_peer_identity, Instant::now());
                    self.spawn_connecting(remote_peer_identity, 1);
                }
            }
            Err(join_err) => {
                panic!("Join error during tx sender worker ended: {:?}", join_err);
            }
        }
    }

    ///
    /// Schedules a graceful drop of all transaction workers.
    ///
    /// The scheduled task waits for all transaction workers to complete and drop their senders.
    /// All transaction workers are detached from the gateway runtime and not managed anymore.
    ///
    ///
    fn schedule_graceful_drop_all_worker(&mut self) {
        tracing::trace!("Scheduling graceful drop of all transaction workers");
        let mut tx_worker_meta = std::mem::take(&mut self.tx_worker_task_meta_map);
        let tx_worker_sender_map = std::mem::take(&mut self.tx_worker_handle_map);
        let mut tx_worker_set = std::mem::take(&mut self.tx_worker_set);

        let fut = async move {
            drop(tx_worker_sender_map);
            while let Some(result) = tx_worker_set.join_next_with_id().await {
                let id = match &result {
                    Ok((id, _)) => *id,
                    Err(e) => e.id(),
                };
                let remote_peer = tx_worker_meta.remove(&id).unwrap();
                tracing::trace!("graceful drop worker for remote peer: {}", remote_peer);
                if let Err(e) = result {
                    tracing::debug!("remote peer {remote_peer} join failed with {e:?}");
                }
            }
        };

        let ah = self.tasklet.spawn(fut);
        self.tasklet_meta
            .insert(ah.id(), TokioGatewayTaskMeta::DropAllWorkers);
    }

    ///
    /// Updates the gateway identity and reconnects to all remote peers with the new identity.
    ///
    fn update_identity(&mut self, update_identity_cmd: UpdateGatewayIdentityCommand) {
        let UpdateGatewayIdentityCommand {
            new_identity,
            callback,
        } = update_identity_cmd;
        self.schedule_graceful_drop_all_worker();
        self.connecting_tasks.abort_all();
        self.connecting_tasks.detach_all();
        self.connecting_remote_peers.clear();
        let connecting_meta = std::mem::take(&mut self.connecting_meta);

        // Update identity
        let (certificate, privkey) = new_dummy_x509_certificate(&new_identity);
        let cert = Arc::new(QuicClientCertificate {
            certificate,
            key: privkey,
        });

        self.client_certificate = cert;
        self.identity = new_identity;
        prom::quic_set_identity(self.identity.pubkey());
        connecting_meta.values().for_each(|meta| {
            self.spawn_connecting(meta.remote_peer_identity, meta.connection_attempt);
        });

        if !connecting_meta.is_empty() {
            tracing::trace!(
                "Will auto-reconnect to {} remote peers after identity update",
                connecting_meta.len()
            );
        }

        callback
            .set
            .store(true, std::sync::atomic::Ordering::Relaxed);
        callback.waker.wake();
        tracing::trace!("Updated gateway identity to: {}", self.identity.pubkey());
    }

    ///
    /// Similar to [`TokioQuicGatewayRuntime::update_identity`], but await a synchronization barrier
    /// before resuming gateway operations.
    ///
    async fn update_identity_sync(&mut self, command: MultiStepIdentitySynchronizationCommand) {
        let MultiStepIdentitySynchronizationCommand {
            new_identity,
            barrier,
        } = command;

        self.schedule_graceful_drop_all_worker();
        self.connecting_tasks.abort_all();
        self.connecting_tasks.detach_all();
        self.connecting_remote_peers.clear();
        let connecting_meta = std::mem::take(&mut self.connecting_meta);

        // Update identity
        let (certificate, privkey) = new_dummy_x509_certificate(&new_identity);
        let cert = Arc::new(QuicClientCertificate {
            certificate,
            key: privkey,
        });

        self.client_certificate = cert;
        self.identity = new_identity;
        prom::quic_set_identity(self.identity.pubkey());
        // Wait for the barrier to be released
        barrier.wait().await;
        connecting_meta.values().for_each(|meta| {
            self.spawn_connecting(meta.remote_peer_identity, meta.connection_attempt);
        });

        if !connecting_meta.is_empty() {
            tracing::trace!(
                "Will auto-reconnect to {} remote peers after identity update",
                connecting_meta.len()
            );
        }

        tracing::trace!("Updated gateway identity to: {}", self.identity.pubkey());
    }

    async fn handle_cnc(&mut self, command: GatewayCommand) {
        match command {
            GatewayCommand::UpdateIdenttiy(update_gateway_identity_command) => {
                self.update_identity(update_gateway_identity_command);
            }
            GatewayCommand::MultiStepIdentitySynchronization(
                multi_step_identity_synchronization_command,
            ) => {
                self.update_identity_sync(multi_step_identity_synchronization_command)
                    .await;
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
            TokioGatewayTaskMeta::DropAllWorkers => {
                tracing::info!(
                    "finished graceful drop of all transaction workers with : {result:?}"
                );
            }
        }
    }

    fn update_prom_metrics(&self) {
        let num_active_workers = self.tx_worker_handle_map.len();
        let num_connecting_tasks = self.connecting_tasks.len();
        let num_queued_tx = self.tx_queues.values().map(|q| q.len()).sum::<usize>();
        prom::set_quic_gw_active_connection_cnt(num_active_workers);
        prom::set_quic_gw_connecting_cnt(num_connecting_tasks);
        prom::set_quic_gw_ongoing_evictions_cnt(self.being_evicted_peers.len());
        prom::set_quic_gw_tx_blocked_by_connecting_cnt(num_queued_tx);
    }

    fn handle_remote_peer_addr_change(&mut self, remote_peers_changed: HashSet<Pubkey>) {
        for remote_peer in remote_peers_changed {
            if let Some(handle) = self.tx_worker_handle_map.get(&remote_peer) {
                // If we have a worker for the remote peer, we need to update its address.
                let maybe_new_addr = self
                    .leader_tpu_info_service
                    .get_quic_dest_addr(remote_peer, self.config.tpu_port_kind);
                match maybe_new_addr {
                    Some(new_addr) => {
                        if new_addr != handle.remote_peer_addr {
                            tracing::debug!(
                                "Remote peer address changed: {} from {:?} to {:?}, will cancel worker...",
                                remote_peer,
                                handle.remote_peer_addr,
                                new_addr
                            );
                            // Update the worker's remote address.
                            handle.cancel_notify.notify_one();
                            prom::incr_quic_gw_remote_peer_addr_changes_detected();
                        }
                    }
                    None => {
                        // If we don't have a new address, we need to drop the worker.
                        handle.cancel_notify.notify_one();
                        prom::incr_quic_gw_remote_peer_addr_changes_detected();
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
            for upcoming_leader in upcoming_leaders {
                let is_already_connectish =
                    self.tx_worker_handle_map.contains_key(&upcoming_leader)
                        || self.connecting_remote_peers.contains_key(&upcoming_leader);

                if !is_already_connectish {
                    prom::incr_quic_gw_leader_prediction_hit();
                    tracing::trace!(
                        "Spawning connection for predicted upcoming leader: {}",
                        upcoming_leader
                    );
                    self.spawn_connecting(upcoming_leader, self.config.max_connection_attempts);
                } else {
                    prom::incr_quic_gw_leader_prediction_miss();
                }
            }
        } else {
            // If we don't have leader prediction lookahead configured, we don't predict upcoming leaders.
            // Set the next prediction deadline to a long time in the future.
            // So this function exit early next time it is called.
            self.next_leader_prediction_deadline = Instant::now() + FOREVER;
        }
    }

    pub async fn run(mut self) {
        prom::quic_set_identity(self.identity.pubkey());

        loop {
            self.do_eviction_if_required();
            self.update_prom_metrics();
            self.try_predict_upcoming_leaders_if_necessary();

            tokio::select! {
                maybe = self.tx_inlet.recv() => {
                    match maybe {
                        Some(tx) => {
                            self.accept_tx(tx);
                        }
                        None => {
                            tracing::debug!("Transaction gateway inlet closed");
                            break;
                        }
                    }
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
        tpu_port_kind: ConfigQuicTpuPort,
        leader_tpu_info_service: Arc<dyn LeaderTpuInfoService + Send + Sync + 'static>,
    ) -> Self {
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
    tpu_port_kind: ConfigQuicTpuPort,
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
                    .get_quic_dest_addr(*pubkey, self.tpu_port_kind);
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

pub struct TokioQuicGatewaySession {
    ///
    /// The [`GatewayIdentityUpdater`] use to change the gateway configured [`Keypair`].
    ///
    pub gateway_identity_updater: GatewayIdentityUpdater,

    ///
    /// Sink to send transaction to.
    /// If all reference to the sink are dropped, the underlying gateway runtime will stop too.
    ///
    pub gateway_tx_sink: mpsc::Sender<GatewayTransaction>,

    ///
    /// Source emitting gateway response.
    ///
    pub gateway_response_source: mpsc::UnboundedReceiver<GatewayResponse>,

    ///
    /// Handle to tokio-based QUIC gateway runtime.
    /// Dropping this handle does not interrupt the gateway runtime.
    ///
    pub gateway_join_handle: JoinHandle<()>,
}

///
/// Factory struct to spawn tokio-based QUIC gateway
///
pub struct TokioQuicGatewaySpawner {
    pub stake_info_map: Arc<dyn ValidatorStakeInfoService + Send + Sync + 'static>,
    pub leader_tpu_info_service: Arc<dyn LeaderTpuInfoService + Send + Sync + 'static>,
    pub gateway_tx_channel_capacity: usize,
}

impl TokioQuicGatewaySpawner {
    pub fn spawn_with_default(&self, identity: Keypair) -> TokioQuicGatewaySession {
        self.spawn(
            identity,
            Default::default(),
            Arc::new(StakeBasedEvictionStrategy::default()),
            Arc::new(IgnorantLeaderPredictor),
        )
    }

    pub fn spawn(
        &self,
        identity: Keypair,
        config: QuicGatewayConfig,
        eviction_strategy: Arc<dyn ConnectionEvictionStrategy + Send + Sync + 'static>,
        leader_schedule: Arc<dyn UpcomingLeaderPredictor + Send + Sync + 'static>,
    ) -> TokioQuicGatewaySession {
        self.spawn_on(
            identity,
            config,
            eviction_strategy,
            leader_schedule,
            tokio::runtime::Handle::current(),
        )
    }

    pub fn spawn_on(
        &self,
        identity: Keypair,
        config: QuicGatewayConfig,
        eviction_strategy: Arc<dyn ConnectionEvictionStrategy + Send + Sync + 'static>,
        leader_predictor: Arc<dyn UpcomingLeaderPredictor + Send + Sync + 'static>,
        gateway_rt: Handle,
    ) -> TokioQuicGatewaySession {
        let (tx_inlet, tx_outlet) = mpsc::channel(self.gateway_tx_channel_capacity);
        let (gateway_resp_tx, gateway_resp_rx) = mpsc::unbounded_channel();
        let (gateway_cnc_tx, gateway_cnc_rx) = mpsc::channel(10);

        let (certificate, private_key) = new_dummy_x509_certificate(&identity);
        let cert = Arc::new(QuicClientCertificate {
            certificate,
            key: private_key,
        });

        let mut endpoints = vec![];
        let mut endpoints_usage = vec![];
        for _ in 0..config.num_endpoints.get() {
            let endpoint = (0..config.max_local_port_binding_attempts)
                .find_map(|_| {
                    let (_, client_socket) = solana_net_utils::bind_in_range(
                        IpAddr::V4(Ipv4Addr::UNSPECIFIED),
                        config.port_range,
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
            endpoints_usage.push(EndpointUsage::default());
        }

        let remote_peer_addr_watcher = RemotePeerAddrWatcher::new(
            config.remote_peer_addr_watch_interval,
            config.tpu_port_kind,
            Arc::clone(&self.leader_tpu_info_service),
        );

        let gateway_runtime = TokioQuicGatewayRuntime {
            stake_info_map: Arc::clone(&self.stake_info_map),
            tx_worker_handle_map: Default::default(),
            tx_worker_task_meta_map: Default::default(),
            tx_worker_set: Default::default(),
            active_staked_sorted_remote_peer: Default::default(),
            tx_queues: Default::default(),
            tx_worker_rt: gateway_rt.clone(),
            identity,
            connecting_tasks: JoinSet::new(),
            connecting_meta: Default::default(),
            connecting_remote_peers: Default::default(),
            leader_tpu_info_service: Arc::clone(&self.leader_tpu_info_service),
            config,
            client_certificate: cert,
            tx_inlet: tx_outlet,
            response_outlet: gateway_resp_tx,
            cnc_rx: gateway_cnc_rx,
            tasklet: Default::default(),
            tasklet_meta: Default::default(),
            last_peer_activity: Default::default(),
            being_evicted_peers: Default::default(),
            eviction_strategy,
            connecting_blocked_by_eviction_list: Default::default(),
            endpoints,
            endpoints_usage,
            remote_peer_addr_watcher,
            leader_predictor,
            next_leader_prediction_deadline: Instant::now(),
        };

        let jh = gateway_rt.spawn(gateway_runtime.run());

        TokioQuicGatewaySession {
            gateway_tx_sink: tx_inlet,
            gateway_identity_updater: GatewayIdentityUpdater {
                cnc_tx: gateway_cnc_tx,
            },
            gateway_response_source: gateway_resp_rx,
            gateway_join_handle: jh,
        }
    }
}

pub struct GatewayIdentityUpdater {
    ///
    /// Command-and-control channel to send command to the QUIC gateway
    cnc_tx: mpsc::Sender<GatewayCommand>,
}

///
/// All the updater API is set a "mut" concurrent identity update.
///
impl GatewayIdentityUpdater {
    ///
    /// Changes the configured identity in the QUIC gateway
    ///
    pub async fn update_identity(&mut self, identity: Keypair) {
        let shared = UpdateIdentityInner {
            set: AtomicBool::new(false),
            waker: AtomicWaker::new(),
        };
        let shared = Arc::new(shared);
        let cmd = UpdateGatewayIdentityCommand {
            new_identity: identity,
            callback: Arc::clone(&shared),
        };
        self.cnc_tx
            .send(GatewayCommand::UpdateIdenttiy(cmd))
            .await
            .expect("disconnected");
        let update_identity = UpdateIdentity {
            inner: shared,
            _this: self,
        };
        update_identity.await
    }

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
            .send(GatewayCommand::MultiStepIdentitySynchronization(cmd))
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
    _this: &'a GatewayIdentityUpdater, /* phantom data to prevent two threads from updating the identity at the same time */
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
            GatewayCommand, GatewayIdentityUpdater, StakeSortedPeerSet,
            UpdateGatewayIdentityCommand,
        },
        solana_keypair::Keypair,
        solana_pubkey::Pubkey,
        std::time::Duration,
        tokio::sync::mpsc,
    };

    #[tokio::test]
    async fn test_update_identity_fut() {
        let (cnc_tx, mut cnc_rx) = mpsc::channel(10);
        let mut updater = GatewayIdentityUpdater { cnc_tx };

        let jh = tokio::spawn(async move {
            let GatewayCommand::UpdateIdenttiy(UpdateGatewayIdentityCommand {
                new_identity,
                callback,
            }) = cnc_rx.recv().await.unwrap()
            else {
                panic!("Expected UpdateIdenttiy command");
            };
            tokio::time::sleep(Duration::from_secs(2)).await;
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
mod stake_based_eviction_strategy_test {
    use {
        super::{ConnectionEvictionStrategy, StakeSortedPeerSet},
        solana_pubkey::Pubkey,
        std::time::{Duration, Instant},
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
}

#[cfg(test)]
mod leader_tpu_info_service_test {
    use {
        crate::quic_client::{
            config::{ConfigQuicTpuPort, TpuOverrideInfo},
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
        fn get_quic_tpu_socket_addr(&self, leader_pubkey: Pubkey) -> Option<SocketAddr> {
            self.inner
                .read()
                .unwrap()
                .get(&leader_pubkey)
                .map(|info| info.normal)
        }

        fn get_quic_tpu_fwd_socket_addr(&self, leader_pubkey: Pubkey) -> Option<SocketAddr> {
            self.inner
                .read()
                .unwrap()
                .get(&leader_pubkey)
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

        let actual_fwd = override_svc.get_quic_dest_addr(pk1, ConfigQuicTpuPort::Forwards);
        let actual_normal = override_svc.get_quic_dest_addr(pk1, ConfigQuicTpuPort::Normal);
        assert_eq!(actual_normal, Some("127.0.0.1:9000".parse().unwrap()));
        assert_eq!(actual_fwd, Some("127.0.0.1:9001".parse().unwrap()));

        // It should not override anything if there is no override spec
        let actual_fwd = override_svc.get_quic_dest_addr(pk2, ConfigQuicTpuPort::Forwards);
        let actual_normal = override_svc.get_quic_dest_addr(pk2, ConfigQuicTpuPort::Normal);
        assert_eq!(actual_normal, Some("127.0.0.1:8002".parse().unwrap()));
        assert_eq!(actual_fwd, Some("127.0.0.1:8003".parse().unwrap()));

        // It should work with empty override spec too
        let override_svc = OverrideTpuInfoService {
            other: service.clone(),
            override_vec: vec![],
        };

        let actual_fwd = override_svc.get_quic_dest_addr(pk1, ConfigQuicTpuPort::Forwards);
        let actual_normal = override_svc.get_quic_dest_addr(pk1, ConfigQuicTpuPort::Normal);
        assert_eq!(actual_normal, Some("127.0.0.1:8000".parse().unwrap()));
        assert_eq!(actual_fwd, Some("127.0.0.1:8001".parse().unwrap()));
    }
}
