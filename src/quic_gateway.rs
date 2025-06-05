use {
    crate::{
        cluster_tpu_info::ClusterTpuInfo,
        crypto_provider::crypto_provider,
        identity::JetIdentitySyncMember,
        metrics::{
            self,
            jet::{observe_leader_rtt, observe_send_transaction_e2e_latency, set_leader_mtu},
        },
        stake::StakeInfoMap,
    },
    bytes::Bytes,
    derive_more::Display,
    futures::task::AtomicWaker,
    quinn::{
        ClientConfig, Connection, ConnectionError, Endpoint, IdleTimeout, StoppedError,
        TransportConfig, VarInt, WriteError, crypto::rustls::QuicClientConfig,
    },
    quinn_proto::TransportError,
    solana_net_utils::{PortRange, VALIDATOR_PORT_RANGE},
    solana_quic_client::nonblocking::quic_client::{QuicClientCertificate, SkipServerVerification},
    solana_sdk::{
        pubkey::Pubkey,
        quic::{QUIC_MAX_UNSTAKED_CONCURRENT_STREAMS, QUIC_SEND_FAIRNESS},
        signature::{Keypair, Signature},
        signer::Signer,
    },
    solana_streamer::{
        nonblocking::quic::ALPN_TPU_PROTOCOL_ID, tls_certificates::new_dummy_x509_certificate,
    },
    std::{
        collections::{HashMap, VecDeque},
        net::{IpAddr, Ipv4Addr, SocketAddr},
        sync::{Arc, atomic::AtomicBool},
        task::Poll,
        time::{Duration, Instant},
    },
    tokio::{
        runtime::Handle,
        sync::{
            Barrier,
            mpsc::{self},
        },
        task::{self, Id, JoinError, JoinHandle, JoinSet},
    },
};

pub const DEFAULT_QUIC_MAX_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
pub const DEFAULT_MAX_CONSECUTIVE_CONNECTION_ATTEMPT: usize = 3;
pub const DEFAULT_PER_PEER_TRANSACTION_QUEUE_SIZE: usize = 10_000;

pub(crate) struct InflightMeta {
    tx_sig: Signature,
    prior_inflight_load: usize,
}

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
}

impl Default for QuicGatewayConfig {
    fn default() -> Self {
        Self {
            port_range: VALIDATOR_PORT_RANGE,
            max_idle_timeout: DEFAULT_QUIC_MAX_IDLE_TIMEOUT,
            // keep_alive_interval: None,
            max_connection_attempts: DEFAULT_MAX_CONSECUTIVE_CONNECTION_ATTEMPT,
            transaction_sender_worker_channel_capacity: DEFAULT_PER_PEER_TRANSACTION_QUEUE_SIZE,
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
    remote_peer_identity: Pubkey,
    connection_attempt: usize,
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

enum TokioGateawyTaskMeta {
    DropAllWorkers,
}

///
/// Tokio-based runtime to driver a QUIC gateway.
pub(crate) struct TokioQuicGatewayRuntime {
    ///
    /// The stake info map used to compute max stream limit
    ///
    stake_info_map: StakeInfoMap,

    ///
    /// Holds on-going remote peer transaction sender workers.
    ///
    tx_worker_sender_map: HashMap<Pubkey, mpsc::Sender<GatewayTransaction>>,

    ///
    /// Map from tokio task id to the remote peer it refers too.
    ///
    tx_worker_meta: HashMap<Id, Pubkey>,

    ///
    /// JoinSet of all transaction sender workers.
    ///
    tx_worker_set: JoinSet<TxSenderWorkerCompleted>,

    ///
    /// Transaction queues per remote identity waiting for connection to be come available.
    ///
    tx_queues: HashMap<Pubkey, VecDeque<GatewayTransaction>>,

    ///
    /// The runtime to spawn transation sender worker on.
    tx_worker_rt: tokio::runtime::Handle,

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
    tasklet_meta: HashMap<Id, TokioGateawyTaskMeta>,
}

pub trait LeaderTpuInfoService {
    fn get_tpu_socket_addr(&self, leader_pubkey: Pubkey) -> Option<SocketAddr>;
}

impl LeaderTpuInfoService for ClusterTpuInfo {
    fn get_tpu_socket_addr(&self, leader_pubkey: Pubkey) -> Option<SocketAddr> {
        self.get_cluster_nodes()
            .get(&leader_pubkey)
            .and_then(|node| node.tpu)
    }
}

///
/// A transaction with destination details to be sent to a remote peer.
///
#[derive(Debug)]
pub struct GatewayTransaction {
    /// Id set by the sender to identify the transaction. Only meaningful to the sender.
    pub tx_sig: Signature,
    /// The wire format of the transaction.
    pub wire: Bytes,
    /// The pubkey of the remote peer to send the transaction to.
    pub remote_peer: Pubkey,
}

#[derive(thiserror::Error, Debug)]
enum SendTxError {
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
    pub tx_sig: Signature,
}

#[derive(Debug)]
pub struct GatewayTxFailed {
    pub remote_peer_identity: Pubkey,
    pub tx_sig: Signature,
}

#[derive(Clone, Debug, Display)]
pub enum TxDropReason {
    #[display("reached downstream transaction worker transaction queue capacity")]
    RateLimited,
    #[display("remote peer is unreachable")]
    RemotePeerUnreachable,
    #[display("tx got drop by gateway")]
    DropByGateway,
}

#[derive(Debug)]
pub struct TxDrop {
    pub remote_peer_identity: Pubkey,
    pub tx_sig: Signature,
    pub drop_reason: TxDropReason,
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
    port_range: PortRange,
    cert: Arc<QuicClientCertificate>,
    max_idle_timeout: Duration,
}

impl ConnectingTask {
    async fn run(self) -> Result<Connection, ConnectingError> {
        let remote_peer_addr = self
            .service
            .get_tpu_socket_addr(self.remote_peer_identity)
            .ok_or(ConnectingError::PeerNotInLeaderSchedule)?;

        let client_socket =
            solana_net_utils::bind_in_range(IpAddr::V4(Ipv4Addr::UNSPECIFIED), self.port_range)
                .expect("create_endpoint bind_in_range")
                .1;

        let mut endpoint = Endpoint::new(
            quinn::EndpointConfig::default(),
            None,
            client_socket,
            Arc::new(quinn::TokioRuntime),
        )
        .expect("QuicNewConnection::create_endpoint quinn::Endpoint::new");

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
            // We don't want automatic keep-alive, since we want to use it to detect inactive connections.
            res.keep_alive_interval(None);
            // We don't want fairness.
            res.send_fairness(QUIC_SEND_FAIRNESS);
            res
        };

        let mut config = ClientConfig::new(Arc::new(QuicClientConfig::try_from(crypto).unwrap()));
        config.transport_config(Arc::new(transport_config));

        endpoint.set_default_client_config(config);

        let connecting = endpoint
            .connect(remote_peer_addr, "connect")
            .map_err(ConnectingError::ConnectError)?;
        let conn = connecting.await?;
        Ok(conn)
    }
}

///
/// Transaction sender worker bound to a specific remote peer over the same connection.
///
struct QuicTxSenderWorker {
    remote_peer: Pubkey,
    max_stream_limit: u64,
    inflight_send: JoinSet<Result<SentOk, SendTxError>>,
    inflight_send_meta: HashMap<task::Id, InflightMeta>,
    connection: Arc<Connection>,
    incoming_rx: mpsc::Receiver<GatewayTransaction>,
    output_tx: mpsc::UnboundedSender<GatewayResponse>,
    tx_queue: VecDeque<GatewayTransaction>,
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
}

impl QuicTxSenderWorker {
    fn spawn_tx(&mut self, tx: GatewayTransaction) {
        let total_inflight_before = self.inflight_send.len();
        assert!(
            self.inflight_send.len() < self.max_stream_limit as usize,
            "inflight_send limit reached"
        );
        let remote_peer_identity = tx.remote_peer;
        let tx_sig = tx.tx_sig;
        let conn = Arc::clone(&self.connection);
        metrics::jet::incr_send_tx_attempt(remote_peer_identity);
        let fut = async move {
            let t = Instant::now();
            let mut uni = conn.open_uni().await?;
            uni.write_all(&tx.wire).await.map_err(|e| match e {
                WriteError::Stopped(var_int) => SendTxError::StreamStopped(var_int),
                WriteError::ConnectionLost(connection_error) => {
                    SendTxError::ConnectionError(connection_error)
                }
                WriteError::ClosedStream => SendTxError::StreamClosed,
                WriteError::ZeroRttRejected => SendTxError::ZeroRttRejected,
            })?;
            uni.finish().expect("finish uni");
            uni.stopped().await.map_err(|e| match e {
                StoppedError::ConnectionLost(connection_error) => connection_error.into(),
                StoppedError::ZeroRttRejected => SendTxError::ZeroRttRejected,
            })?;
            let elapsed = t.elapsed();
            let ok = SentOk { e2e_time: elapsed };
            Ok(ok)
        };

        let abort_handle = self.inflight_send.spawn(fut);
        tracing::debug!(
            "Sent tx: {:?} to remote peer: {:?}",
            tx.tx_sig,
            remote_peer_identity,
        );
        let meta = InflightMeta {
            tx_sig,
            prior_inflight_load: total_inflight_before,
        };
        self.inflight_send_meta.insert(abort_handle.id(), meta);
    }

    fn handle_tx_sent_result(
        &mut self,
        result: Result<(task::Id, Result<SentOk, SendTxError>), JoinError>,
    ) -> Result<(), TxSenderWorkerError> {
        match result {
            Ok((task_id, result)) => {
                let InflightMeta {
                    tx_sig,
                    prior_inflight_load,
                } = self.inflight_send_meta.remove(&task_id).unwrap();
                match result {
                    Ok(sent_ok) => {
                        tracing::debug!(
                            "Tx sent to remote peer: {} in {:?}",
                            self.remote_peer,
                            sent_ok.e2e_time
                        );
                        let resp = GatewayTxSent {
                            remote_peer_identity: self.remote_peer,
                            tx_sig,
                        };
                        let _ = self.output_tx.send(GatewayResponse::TxSent(resp));
                        metrics::jet::sts_tpu_send_inc(self.remote_peer);
                        observe_send_transaction_e2e_latency(self.remote_peer, sent_ok.e2e_time);
                        let path_stats = self.connection.stats().path;
                        let current_mut = path_stats.current_mtu;
                        set_leader_mtu(self.remote_peer, current_mut);
                        observe_leader_rtt(self.remote_peer, path_stats.rtt);
                        Ok(())
                    }
                    Err(send_err) => {
                        let resp = GatewayTxFailed {
                            remote_peer_identity: self.remote_peer,
                            tx_sig,
                        };
                        let _ = self.output_tx.send(GatewayResponse::TxFailed(resp));
                        match send_err {
                            SendTxError::ConnectionError(connection_error) => {
                                if let ConnectionError::TransportError(TransportError {
                                    code,
                                    frame: _,
                                    reason: _,
                                }) = connection_error
                                {
                                    if code == quinn_proto::TransportErrorCode::STREAM_LIMIT_ERROR {
                                        self.max_stream_limit = self
                                            .max_stream_limit
                                            .saturating_sub(1)
                                            .max(QUIC_MAX_UNSTAKED_CONCURRENT_STREAMS as u64);
                                        tracing::warn!(
                                            "Remote peer {} hit stream limit, prior load before sending this tx: {}, reducing max stream limit to {}",
                                            self.remote_peer,
                                            prior_inflight_load,
                                            self.max_stream_limit
                                        );
                                    }
                                    Ok(())
                                } else {
                                    Err(TxSenderWorkerError::ConnectionLost(connection_error))
                                }
                            }
                            SendTxError::StreamStopped(_) | SendTxError::StreamClosed => {
                                tracing::trace!(
                                    "Stream stopped or closed for tx: {:?} to remote peer: {:?}",
                                    tx_sig,
                                    self.remote_peer
                                );
                                Ok(())
                            }
                            SendTxError::ZeroRttRejected => {
                                tracing::warn!(
                                    "0-RTT rejected by remote peer: {:?} for tx: {:?}",
                                    self.remote_peer,
                                    tx_sig
                                );
                                Err(TxSenderWorkerError::ZeroRttRejected)
                            }
                        }
                    }
                }
            }
            Err(join_err) => {
                let inflight_meta = self
                    .inflight_send_meta
                    .remove(&join_err.id())
                    .expect("inflight_meta");
                let InflightMeta {
                    tx_sig,
                    prior_inflight_load: _,
                } = inflight_meta;
                tracing::error!(
                    "Join error during sending tx to {:?}: {:?} for tx_id: {}",
                    self.remote_peer,
                    join_err,
                    tx_sig
                );

                let resp = GatewayTxFailed {
                    remote_peer_identity: self.remote_peer,
                    tx_sig,
                };
                let _ = self.output_tx.send(GatewayResponse::TxFailed(resp));

                panic!(
                    "Join error during sending tx to {:?}: {:?}",
                    self.remote_peer, join_err
                );
            }
        }
    }

    fn has_capacity(&self) -> bool {
        self.inflight_send.len() < self.max_stream_limit as usize
    }

    async fn run(mut self) -> TxSenderWorkerCompleted {
        let maybe_err = loop {
            while self.has_capacity() && !self.tx_queue.is_empty() {
                if let Some(tx) = self.tx_queue.pop_front() {
                    self.spawn_tx(tx);
                }
            }

            tokio::select! {
                maybe = self.incoming_rx.recv(), if self.has_capacity() => {
                    match maybe {
                        Some(tx) => {
                            self.spawn_tx(tx);
                        }
                        None => {
                            tracing::debug!("Transaction sender inlet closed for remote peer: {:?}", self.remote_peer);
                            break None;
                        }
                    }
                }

                Some(result) = self.inflight_send.join_next_with_id() => {
                    if let Err(e) = self.handle_tx_sent_result(result) {
                        break Some(e);
                    }
                }
            }
        };

        // Properly drain, inflight transaction, since some of them may succeed.
        while let Some(result) = self.inflight_send.join_next_with_id().await {
            let _ = self.handle_tx_sent_result(result);
        }

        TxSenderWorkerCompleted {
            err: maybe_err,
            rx: self.incoming_rx,
        }
    }
}

impl TokioQuicGatewayRuntime {
    ///
    /// Spawn a new connection attempt to a remote peer.
    ///
    fn spawn_connecting(&mut self, remote_peer_identity: Pubkey, attempt: usize) {
        let service = Arc::clone(&self.leader_tpu_info_service);
        let port_range = self.config.port_range;
        let cert = Arc::clone(&self.client_certificate);
        let max_idle_timeout = self.config.max_idle_timeout;
        let fut = ConnectingTask {
            service,
            remote_peer_identity,
            port_range,
            cert,
            max_idle_timeout,
        }
        .run();
        let meta = ConnectingMeta {
            remote_peer_identity,
            connection_attempt: attempt,
        };
        let abort_handle = self.connecting_tasks.spawn(fut);
        tracing::trace!(
            "Spawning connection for remote peer: {:?}, attempt: {}",
            remote_peer_identity,
            attempt
        );
        self.connecting_remote_peers
            .insert(remote_peer_identity, abort_handle.id());
        self.connecting_meta.insert(abort_handle.id(), meta);
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
        let _ = self
            .tx_queues
            .remove(&remote_peer_identity)
            .into_iter()
            .flatten()
            .map(|tx| TxDrop {
                remote_peer_identity,
                tx_sig: tx.tx_sig,
                drop_reason: reason.clone(),
            })
            .try_for_each(|txdrop| self.response_outlet.send(GatewayResponse::TxDrop(txdrop)));
    }

    fn unreachable_peer(&mut self, remote_peer_identity: Pubkey) {
        self.drop_peer_queued_tx(remote_peer_identity, TxDropReason::RemotePeerUnreachable);
    }

    fn current_max_stream_limit(&self) -> u64 {
        let limits = self.stake_info_map.get_stake_limits(self.identity.pubkey());
        limits.max_streams
    }

    ///
    /// Installs a transaction sender worker for a remote peer with the given connection.
    ///
    fn install_worker(&mut self, remote_peer_identity: Pubkey, connection: Connection) {
        let (tx, rx) = mpsc::channel(self.config.transaction_sender_worker_channel_capacity);

        let connection = Arc::new(connection);
        let output_tx = self.response_outlet.clone();
        let max_stream_capacity = self.current_max_stream_limit();
        let worker = QuicTxSenderWorker {
            remote_peer: remote_peer_identity,
            max_stream_limit: max_stream_capacity,
            inflight_send: JoinSet::new(),
            inflight_send_meta: HashMap::new(),
            connection,
            incoming_rx: rx,
            output_tx,
            tx_queue: self
                .tx_queues
                .remove(&remote_peer_identity)
                .unwrap_or_default(),
        };

        let worker_fut = worker.run();
        let ah = self.tx_worker_set.spawn_on(worker_fut, &self.tx_worker_rt);
        assert!(
            self.tx_worker_sender_map
                .insert(remote_peer_identity, tx)
                .is_none()
        );
        self.tx_worker_meta.insert(ah.id(), remote_peer_identity);
        tracing::debug!(
            "Installed tx worker for remote peer: {remote_peer_identity} with max stream limit: {max_stream_capacity}"
        );
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
                    remote_peer_identity,
                    connection_attempt,
                } = self.connecting_meta.remove(&task_id).unwrap();
                let _ = self.connecting_remote_peers.remove(&remote_peer_identity);
                match result {
                    Ok(conn) => {
                        tracing::debug!("Connected to remote peer: {:?}", remote_peer_identity);
                        self.install_worker(remote_peer_identity, conn);
                    }
                    Err(connect_err) => match connect_err {
                        ConnectingError::ConnectError(connect_error) => {
                            tracing::error!(
                                "Failed to connect to remote peer: {remote_peer_identity}: {connect_error:?}"
                            );
                            self.unreachable_peer(remote_peer_identity);
                        }
                        ConnectingError::ConnectionError(connection_error) => {
                            tracing::error!("Connection error: {:?}", connection_error);
                            if connection_attempt < self.config.max_connection_attempts {
                                self.spawn_connecting(remote_peer_identity, connection_attempt + 1);
                            } else {
                                self.unreachable_peer(remote_peer_identity);
                            }
                        }
                        ConnectingError::PeerNotInLeaderSchedule => {
                            tracing::warn!("Connection to remote peer not in leader schedule");
                            self.unreachable_peer(remote_peer_identity);
                        }
                    },
                }
            }
            Err(join_err) => {
                let ConnectingMeta {
                    remote_peer_identity,
                    connection_attempt: _,
                } = self
                    .connecting_meta
                    .remove(&join_err.id())
                    .expect("connecting_meta");

                let _ = self.connecting_remote_peers.remove(&remote_peer_identity);
                panic!(
                    "Join error during connecting to {remote_peer_identity:?}: {:?}",
                    join_err
                );
            }
        }
    }

    ///
    /// Accepts a transaction and determines how to handle it based on the remote peer's status.
    ///
    fn accept_tx(&mut self, tx: GatewayTransaction) {
        let remote_peer_identity = tx.remote_peer;
        let tx_id = tx.tx_sig;
        if let Some(sender) = self.tx_worker_sender_map.get(&remote_peer_identity) {
            // If we have an active transaction sender worker for the remote peer,
            match sender.try_send(tx) {
                Ok(_) => {
                    tracing::trace!(
                        "Queued tx: {:?} for remote peer: {:?}",
                        tx_id,
                        remote_peer_identity
                    );
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
                            tx_sig: tx.tx_sig,
                            drop_reason: TxDropReason::RateLimited,
                        };
                        let _ = self.response_outlet.send(GatewayResponse::TxDrop(txdrop));
                    }
                    mpsc::error::TrySendError::Closed(tx) => {
                        tracing::debug!(
                            "Remote peer: {:?} tx worker is closed, enqueuing tx: {:?}",
                            remote_peer_identity,
                            tx_id
                        );
                        self.tx_queues
                            .entry(remote_peer_identity)
                            .or_default()
                            .push_back(tx);
                    }
                },
            }
        } else {
            // We don't have any active transaction sender worker for the remote peer,
            // we need to queue the transaction and try to spawn a new connection.
            self.tx_queues
                .entry(remote_peer_identity)
                .or_default()
                .push_back(tx);

            // Check if we are not already connecting to this remote peer.
            if !self
                .connecting_remote_peers
                .contains_key(&remote_peer_identity)
            {
                // If the remote peer is already being connected, just queue the tx.
                tracing::debug!(
                    "Spawning connection for remote peer: {:?}",
                    remote_peer_identity
                );
                self.spawn_connecting(remote_peer_identity, 1);
            }
        }
    }

    ///
    /// One of the transaction sender worker has completed its work.
    ///
    fn handle_worker_result(&mut self, result: Result<(Id, TxSenderWorkerCompleted), JoinError>) {
        match result {
            Ok((id, mut worker_completed)) => {
                let remote_peer_identity = self.tx_worker_meta.remove(&id).expect("tx worker meta");
                let worker_tx = self
                    .tx_worker_sender_map
                    .remove(&remote_peer_identity)
                    .expect("tx worker sender");
                drop(worker_tx);
                tracing::trace!(
                    "Tx worker for remote peer: {:?} completed",
                    remote_peer_identity
                );
                let tx_to_rescue = self.tx_queues.entry(remote_peer_identity).or_default();
                while let Ok(tx) = worker_completed.rx.try_recv() {
                    tx_to_rescue.push_back(tx);
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

                if is_peer_unreachable {
                    self.unreachable_peer(remote_peer_identity);
                } else if !tx_to_rescue.is_empty() {
                    tracing::trace!(
                        "Remote peer: {} has queued tx, wil reconnect",
                        remote_peer_identity
                    );
                    self.spawn_connecting(remote_peer_identity, 1);
                }
            }
            Err(join_err) => {
                let id = join_err.id();
                let remote_peer_identity = self.tx_worker_meta.remove(&id).expect("tx worker meta");
                let worker_tx = self
                    .tx_worker_sender_map
                    .remove(&remote_peer_identity)
                    .expect("tx worker sender");
                drop(worker_tx);
                self.drop_peer_queued_tx(remote_peer_identity, TxDropReason::DropByGateway);
                tracing::error!("Tx sender worker join error: {:?}", join_err);
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
        let mut tx_worker_meta = std::mem::take(&mut self.tx_worker_meta);
        let tx_worker_sender_map = std::mem::take(&mut self.tx_worker_sender_map);
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
            .insert(ah.id(), TokioGateawyTaskMeta::DropAllWorkers);
    }

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
        metrics::jet::quic_set_identity(self.identity.pubkey());
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
        metrics::jet::quic_set_identity(self.identity.pubkey());
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
            TokioGateawyTaskMeta::DropAllWorkers => {
                tracing::info!(
                    "finished graceful drop of all transaction workers with : {result:?}"
                );
            }
        }
    }

    pub async fn run(mut self) {
        metrics::jet::quic_set_identity(self.identity.pubkey());
        loop {
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
            }
        }

        self.schedule_graceful_drop_all_worker();
        while let Some(result) = self.tasklet.join_next_with_id().await {
            self.handle_tasklet_result(result);
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
    pub stake_info_map: StakeInfoMap,
    pub leader_tpu_info_service: Arc<dyn LeaderTpuInfoService + Send + Sync + 'static>,
    pub gateway_tx_channel_capacity: usize,
}

impl TokioQuicGatewaySpawner {
    pub fn spawn_with_default(&self, identity: Keypair) -> TokioQuicGatewaySession {
        self.spawn(identity, Default::default())
    }

    pub fn spawn(&self, identity: Keypair, config: QuicGatewayConfig) -> TokioQuicGatewaySession {
        self.spawn_on(identity, config, tokio::runtime::Handle::current())
    }

    pub fn spawn_on(
        &self,
        identity: Keypair,
        config: QuicGatewayConfig,
        gateway_rt: Handle,
    ) -> TokioQuicGatewaySession {
        let (tx_inlet, tx_outlet) = mpsc::channel(self.gateway_tx_channel_capacity);
        let (gateway_resp_tx, gateway_resp_rx) = mpsc::unbounded_channel();
        let (gateway_cnc_tx, gateway_cnc_rx) = mpsc::channel(10);

        let (certificate, privkey) = new_dummy_x509_certificate(&identity);
        let cert = Arc::new(QuicClientCertificate {
            certificate,
            key: privkey,
        });

        let gateway_runtime = TokioQuicGatewayRuntime {
            stake_info_map: self.stake_info_map.clone(),
            tx_worker_sender_map: Default::default(),
            tx_worker_meta: Default::default(),
            tx_worker_set: Default::default(),
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
}

#[async_trait::async_trait]
impl JetIdentitySyncMember for GatewayIdentityUpdater {
    async fn pause_for_identity_update(
        &self,
        new_identity: Keypair,
        barrier: Arc<tokio::sync::Barrier>,
    ) {
        let cmd = MultiStepIdentitySynchronizationCommand {
            new_identity,
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
        crate::quic_gateway::{
            GatewayCommand, GatewayIdentityUpdater, UpdateGatewayIdentityCommand,
        },
        solana_sdk::signature::Keypair,
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
}
