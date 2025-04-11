use {
    crate::{crypto_provider::crypto_provider, stake::StakeInfoMap},
    futures::channel::mpsc::SendError,
    hyper::client,
    quinn::{
        crypto::rustls::QuicClientConfig, ClientConfig, Connection, ConnectionError, Endpoint,
        IdleTimeout, TransportConfig, WriteError,
    },
    quinn_proto::TransportError,
    solana_net_utils::PortRange,
    solana_quic_client::nonblocking::quic_client::{QuicClientCertificate, SkipServerVerification},
    solana_sdk::{pubkey::Pubkey, quic::QUIC_SEND_FAIRNESS},
    solana_streamer::nonblocking::quic::ALPN_TPU_PROTOCOL_ID,
    std::{
        collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
        net::{IpAddr, Ipv4Addr, SocketAddr},
        sync::Arc,
        time::{Duration, Instant},
    },
    tokio::{
        sync::mpsc,
        task::{self, JoinError, JoinSet},
    },
    tonic::ConnectError,
    uuid::Uuid,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConnectionId([u8; 16]);

impl ConnectionId {
    pub fn new() -> Self {
        Self(Uuid::new_v4().into_bytes())
    }
}

///
/// A monotonic increasing passage of time in [`QuicGatewaySM`].
/// Each update to the state machine increase the time value.
///
/// It serves as a way to order events in the state machine and discard stale events.
///
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct StateMachineInstant(u64);

impl StateMachineInstant {
    pub const ZERO: Self = Self(0);

    pub const ONE: Self = Self(1);

    fn inc(self) -> Self {
        Self(self.0 + Self::ONE.0)
    }
}

///
/// The state of a connection in inside the [`QuicGatewaySM`] state machine.
///
struct ConnectionState {
    connection_id: ConnectionId,
    registered_at: StateMachineInstant,
    custom_max_streams: Option<u64>,
    permit_height: PermitHeight,
}

///
/// Schedule which peer transaction queue to process next.
///
#[derive(Default)]
struct PeerScheduler {
    schedule_linked_list: VecDeque<(StateMachineInstant, Pubkey)>,
    deschedule_map: HashMap<Pubkey, StateMachineInstant>,
}

impl PeerScheduler {
    ///
    /// Register a remote peer to be schedule.
    ///
    pub fn schedule(&mut self, remote_peer: Pubkey, time: StateMachineInstant) {
        self.schedule_linked_list.push_back((time, remote_peer));
    }

    ///
    /// Deschedule a remote peer from the current schedule.
    ///
    pub fn deschedule(&mut self, remote_peer: Pubkey, time: StateMachineInstant) {
        let old = self.deschedule_map.insert(remote_peer, time);
        assert!(
            old.is_none(),
            "cannot register earlier cancel time after a later one"
        );
    }

    ///
    /// Get the next remote peer to process in the schedule if any.
    ///
    pub fn get_next_remote_peer_to_process(&mut self) -> Option<Pubkey> {
        while let Some((time, remote_peer)) = self.schedule_linked_list.pop_front() {
            if let Some(deschedule_boundary) = self.deschedule_map.get(&remote_peer) {
                if time < *deschedule_boundary {
                    continue;
                } else {
                    self.deschedule_map.remove(&remote_peer);
                }
            }
            return Some(remote_peer);
        }
        None
    }
}

pub struct QuicGatewaySM {
    current_max_stream_limit_per_conn: u64,
    // scheduler: PeerScheduler,
    // Transaction that are not yet connected to a remote peer.
    // They are waiting for a connection to be established.
    // Once a connection is established, they are moved to the remote_peer_tx_queues.
    // The connection request is also added to the connection_request_queue.
    // The connection_request_queue is used to schedule the connection to the remote peer.
    unconnected_remote_peer_tx_queues: HashMap<Pubkey, VecDeque<GatewayTransaction>>,
    // Transaction queus for remote peers that are connected.
    remote_peer_tx_queues: HashMap<Pubkey, VecDeque<GatewayTransaction>>,
    // Connection state for remote peers.
    connection_map: HashMap<Pubkey, ConnectionState>,
    
    // The current time in the state machine.
    time_sequence: StateMachineInstant,

    // A queue of transactions that failed to be sent to the remote peer.
    deadletter_queue: VecDeque<GatewayTransaction>,
    // A queue of connection ids that have been deregistered by the underlying runtime.
    deregistered_connections: VecDeque<ConnectionId>,
    // A queue of remote peers that are waiting to be connected.
    connection_request_queue: VecDeque<Pubkey>,
}

impl Default for QuicGatewaySM {
    fn default() -> Self {
        Self {
            current_max_stream_limit_per_conn: 10,
            // scheduler: Default::default(),
            unconnected_remote_peer_tx_queues: Default::default(),
            remote_peer_tx_queues: Default::default(),
            connection_map: Default::default(),
            time_sequence: StateMachineInstant::ZERO,
            deadletter_queue: Default::default(),
            deregistered_connections: Default::default(),
            connection_request_queue: Default::default(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AcquireConnectionErr {
    #[error("No connection established to remote peer")]
    NoConnection,
    #[error("Reached max stream limit for connection")]
    ReachedMaxStreamLimit,
}

pub type PermitHeight = u64;

#[derive(Debug)]
pub struct Permit {
    pub remote_peer_identity: Pubkey,
    pub connection_id: ConnectionId,
    pub permit_height: PermitHeight,
    pub time: StateMachineInstant,
}

///
/// QuickGatewaySM is a state machine that manages the connection permits and schedule transactions.
///
impl QuicGatewaySM {
    pub fn tick(&mut self) -> StateMachineInstant {
        let sequence = self.time_sequence;
        self.time_sequence = sequence.inc();
        sequence
    }

    pub fn peer_stream_capacity_left(&self, remote_peer_identity: Pubkey) -> Option<u64> {
        self.connection_map.get(&remote_peer_identity).map(|state| {
            let limit = state
                .custom_max_streams
                .unwrap_or(self.current_max_stream_limit_per_conn);
            limit.saturating_sub(state.permit_height)
        })
    }

    pub fn total_peer_tx_pending_cnt(&self, peer: Pubkey) -> usize {
        let unconnected = self.unconnected_remote_peer_tx_queues.get(&peer);
        let connected = self.remote_peer_tx_queues.get(&peer);
        match (unconnected, connected) {
            (Some(unconnected), None) => unconnected.len(),
            (None, Some(connected)) => connected.len(),
            (None, None) => 0,
            _ => unreachable!(),
        }
    }

    pub fn push_front(&mut self, tx: GatewayTransaction) {
        let remote_peer_identity = tx.remote_peer;
        if self.connection_map.contains_key(&remote_peer_identity) {
            let tx_queue = self
                .remote_peer_tx_queues
                .entry(remote_peer_identity)
                .or_default();
            tx_queue.push_front(tx);
        } else {
            let tx_queue = self
                .unconnected_remote_peer_tx_queues
                .entry(remote_peer_identity)
                .or_insert_with(|| {
                    // Mark the remote peer for connection.
                    self.connection_request_queue
                        .push_back(remote_peer_identity);
                    Default::default()
                });
            tx_queue.push_front(tx);
        }
    }

    pub fn push_back(&mut self, tx: GatewayTransaction) {
        let remote_peer_identity = tx.remote_peer;
        if self.connection_map.contains_key(&remote_peer_identity) {
            let tx_queue = self
                .remote_peer_tx_queues
                .entry(remote_peer_identity)
                .or_default();
            tx_queue.push_back(tx);
        } else {
            let tx_queue = self
                .unconnected_remote_peer_tx_queues
                .entry(remote_peer_identity)
                .or_insert_with(|| {
                    // Mark the remote peer for connection.
                    self.connection_request_queue
                        .push_back(remote_peer_identity);
                    Default::default()
                });
            tx_queue.push_back(tx);
        }
    }

    pub fn register_connection(&mut self, conn: ConnectionId, remote_identity: Pubkey) {
        let now = self.tick();
        let old = self.connection_map.insert(
            remote_identity,
            ConnectionState {
                connection_id: conn,
                registered_at: now,
                custom_max_streams: None,
                permit_height: 0,
            },
        );
        assert!(old.is_none());

        if let Some(tx_queue) = self
            .unconnected_remote_peer_tx_queues
            .remove(&remote_identity)
        {
            let old = self.remote_peer_tx_queues.insert(remote_identity, tx_queue);
            assert!(old.is_none());
        }
    }

    pub fn acquire_connection_permit(
        &mut self,
        remote_peer_identity: Pubkey,
    ) -> Result<Permit, AcquireConnectionErr> {
        if let Some(state) = self.connection_map.get_mut(&remote_peer_identity) {
            let conn_id = state.connection_id;
            let limit = state
                .custom_max_streams
                .unwrap_or(self.current_max_stream_limit_per_conn);
            if state.permit_height >= limit {
                Err(AcquireConnectionErr::ReachedMaxStreamLimit)
            } else {
                state.permit_height += 1;
                Ok(Permit {
                    remote_peer_identity,
                    connection_id: conn_id,
                    permit_height: state.permit_height,
                    time: self.tick(),
                })
            }
        } else {
            Err(AcquireConnectionErr::NoConnection)
        }
    }

    pub fn release_connection_permit(&mut self, permit: Permit) {
        if let Some(state) = self.connection_map.get_mut(&permit.remote_peer_identity) {
            state.permit_height -= 1;
        }
    }

    // pub fn active_tx_queues(&mut self) -> impl Iterator<Item = (Pubkey, &mut VecDeque<GatewayTransaction>)> + '_ {
    //     self.remote_peer_tx_queues
    //         .iter_mut()
    //         .filter(|(k, _)| {
    //             let state = self.connection_map.get(k).expect("active_tx_queues");
    //             let limit = state
    //                 .custom_max_streams
    //                 .unwrap_or(self.current_max_stream_limit_per_conn);
    //             state.permit_height < limit
    //         })
    //         .map(|(k, v) | (*k, v))
    // }

    // pub fn pop_next_in_queue(&mut self, remote_peer: Pubkey) -> Option<GatewayTransaction> {
    //     let queue = self.remote_peer_tx_queues.get_mut(&remote_peer)?;
    //     queue.pop_front()
    // }

    pub fn fill_tx_batch_to_send(
        &mut self,
        buf: &mut Vec<(Permit, GatewayTransaction)>,
        limit: usize
    ) -> usize {
        let mut count = 0;
        let connected_remoted_peers = self.remote_peer_tx_queues.keys().copied().collect::<Vec<_>>();
        let mut empty_queus_detected = HashSet::new();
        let total_queues = self.remote_peer_tx_queues.len();
        
        while count < limit && empty_queus_detected.len() < total_queues {
            for remote_peer in connected_remoted_peers.iter().copied() {
                if empty_queus_detected.contains(&remote_peer) {
                    continue;
                }

                if count >= limit {
                    break;
                }

                // check if queue is not empty 
                if self.remote_peer_tx_queues.get(&remote_peer).filter(|q| !q.is_empty()).is_none() {
                    empty_queus_detected.insert(remote_peer);
                    continue;
                }

                if let Ok(permit) = self.acquire_connection_permit(remote_peer) {
                    let queue = self
                        .remote_peer_tx_queues
                        .get_mut(&remote_peer)
                        .expect("active_tx_queues");
                    let tx = queue.pop_front().expect("expect non empty queue");
                    buf.push((permit, tx));
                    count += 1;
                } else {
                    // Reached max stream limit for connection, put the tx back in the queue.
                    empty_queus_detected.insert(remote_peer);
                    continue;
                }
            }
        }
        
        count
    }

    ///
    /// Deregister a connection from the state machine.
    ///
    /// Puts any queued tx back to the "pending connection state".
    ///
    pub fn deregister_connection(
        &mut self,
        remote_identity: Pubkey,
        time: Option<StateMachineInstant>,
    ) {
        if let Some(state) = self.connection_map.remove(&remote_identity) {
            self.deregistered_connections.push_back(state.connection_id);
            if let Some(queue) = self.remote_peer_tx_queues.remove(&remote_identity) {
                self.unconnected_remote_peer_tx_queues
                    .entry(remote_identity)
                    .or_default()
                    .extend(queue);
            }
        }
    }

    pub fn set_current_max_stream_limit(&mut self, max_streams: u64) {
        self.current_max_stream_limit_per_conn = max_streams;
    }


    ///
    /// Drop any connection info and tx for a remote peer that has been abandoned.
    ///
    pub fn abandon_tx_for_remote_peer(
        &mut self,
        remote_identity: Pubkey,
        time: StateMachineInstant,
    ) {
        if let Some(state) = self.connection_map.get(&remote_identity) {
            if state.registered_at < time {
                self.deregister_connection(remote_identity, Some(time));
                if let Some(dropped_tx) = self
                    .unconnected_remote_peer_tx_queues
                    .remove(&remote_identity)
                {
                    self.deadletter_queue.extend(dropped_tx);
                }
            }
        }
    }

    pub fn get_next_peer_to_connect(&mut self) -> Option<Pubkey> {
        self.connection_request_queue.pop_front()
    }

    ///
    /// Pop the next transaction in the dead letter queue.
    ///
    pub fn pop_next_tx_in_dlq(&mut self) -> Option<GatewayTransaction> {
        self.deadletter_queue.pop_front()
    }

    pub fn drain_dlq(&mut self) -> impl IntoIterator<Item = GatewayTransaction> + '_ {
        self.deadletter_queue.drain(..)
    }

    pub fn mark_connection_failure_at_time(
        &mut self,
        remote_identity: Pubkey,
        time: StateMachineInstant,
    ) {
        if let Some(state) = self.connection_map.get(&remote_identity) {
            if state.registered_at < time {
                tracing::debug!("connection {remote_identity:?} failed at time {time:?}");
                self.deregister_connection(remote_identity, Some(time));
            }
        }
    }

    pub fn hit_rate_limit_for_remote_peer(
        &mut self,
        remote_identity: Pubkey,
        permit_height: PermitHeight,
        time: StateMachineInstant,
    ) {
        if let Some(state) = self.connection_map.get_mut(&remote_identity) {
            if state.registered_at < time {
                let min_permit = state
                    .custom_max_streams
                    .unwrap_or(permit_height)
                    .min(permit_height);
                state.custom_max_streams.replace(min_permit);
            }
        }
    }

    pub fn pop_next_deregistered_connection(&mut self) -> Option<ConnectionId> {
        self.deregistered_connections.pop_front()
    }
}

pub(crate) struct InflightMeta {
    tx_id: u64,
    permit: Permit,
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
    port_range: PortRange,
    max_idle_timeout: Option<Duration>,
    keep_alive_interval: Option<Duration>,
}

pub struct SentOk {
    pub e2e_time: Duration,
}

struct ConnectingMeta {
    remote_peer_identity: Pubkey,
    sm_instant: StateMachineInstant,
}

pub(crate) struct TokioQuicGatewayRuntime {
    sm: QuicGatewaySM,
    stake_info_map: StakeInfoMap,
    inflight_tasks: JoinSet<Result<SentOk, SendTxError>>,
    inflight_meta: HashMap<tokio::task::Id, InflightMeta>,
    connecting_tasks: JoinSet<Result<Connection, ConnectingError>>,
    connecting_meta: HashMap<tokio::task::Id, ConnectingMeta>,
    leader_tpu_info_service: Arc<dyn LeaderTpuInfoService + Send + Sync + 'static>,
    connection_map: HashMap<ConnectionId, Connection>,
    config: QuicGatewayConfig,
    client_certificate: Arc<QuicClientCertificate>,
    tx_inlet: mpsc::Receiver<GatewayTransaction>,
    response_outlet: mpsc::UnboundedSender<GatewayResponse>,
}

#[async_trait::async_trait]
pub trait LeaderTpuInfoService {
    async fn get_tpu_socket_addr(&self, leader_pubkey: Pubkey) -> Option<SocketAddr>;
}

pub struct GatewayTransaction {
    /// Id set by the sender to identify the transaction. Only meaningful to the sender.
    tx_id: u64,
    /// The wire format of the transaction.
    wire: Arc<[u8]>,
    /// The pubkey of the remote peer to send the transaction to.
    remote_peer: Pubkey,
}

#[derive(thiserror::Error, Debug)]
enum SendTxError {
    #[error(transparent)]
    ConnectionError(#[from] ConnectionError),
    #[error(transparent)]
    WriteError(#[from] WriteError),
    #[error(transparent)]
    StoppedError(#[from] quinn::StoppedError),
}

pub struct GatewayTxSent {
    pub remote_peer_identity: Pubkey,
    pub tx_id: u64,
}

pub struct GatewayTxFailed {
    pub remote_peer_identity: Pubkey,
    pub tx_id: u64,
}

pub enum GatewayResponse {
    TxSent(GatewayTxSent),
    TxFailed(GatewayTxFailed),
}

struct ConnectingTask {
    service: Arc<dyn LeaderTpuInfoService + Send + Sync + 'static>,
    remote_peer_identity: Pubkey,
    port_range: PortRange,
    cert: Arc<QuicClientCertificate>,
    max_idle_timeout: Option<Duration>,
    keep_alive_interval: Option<Duration>,
}

impl ConnectingTask {
    async fn run(self) -> Result<Connection, ConnectingError> {
        let remote_peer_addr = self.service
            .get_tpu_socket_addr(self.remote_peer_identity)
            .await
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

        let mut crypto =
            rustls::ClientConfig::builder_with_provider(Arc::new(crypto_provider()))
                .with_safe_default_protocol_versions()
                .expect("Failed to set QUIC client protocol versions")
                .dangerous()
                .with_custom_certificate_verifier(SkipServerVerification::new())
                .with_client_auth_cert(vec![self.cert.certificate.clone()], self.cert.key.clone_key())
                .expect("Failed to set QUIC client certificates");
        crypto.enable_early_data = true;
        crypto.alpn_protocols = vec![ALPN_TPU_PROTOCOL_ID.to_vec()];

        let transport_config = {
            let mut res = TransportConfig::default();

            let max_idle_timeout = self.max_idle_timeout.map(|timeout| {
                IdleTimeout::try_from(timeout).expect("Failed to set QUIC max idle timeout")
            });
            res.max_idle_timeout(max_idle_timeout);
            res.keep_alive_interval(self.keep_alive_interval);
            res.send_fairness(QUIC_SEND_FAIRNESS);

            res
        };

        let mut config =
            ClientConfig::new(Arc::new(QuicClientConfig::try_from(crypto).unwrap()));
        config.transport_config(Arc::new(transport_config));

        endpoint.set_default_client_config(config);

        let connecting = endpoint
            .connect(remote_peer_addr, "connect")
            .map_err(|e| ConnectingError::ConnectError(e))?;
        let conn = connecting.await?;
        Ok(conn)
    }
}


impl TokioQuicGatewayRuntime {
    fn spawn_connecting(&mut self, remote_peer_identity: Pubkey) {
        let service = Arc::clone(&self.leader_tpu_info_service);
        let port_range = self.config.port_range;
        let cert = Arc::clone(&self.client_certificate);
        let max_idle_timeout = self.config.max_idle_timeout;
        let keep_alive_interval = self.config.keep_alive_interval;
        let fut = ConnectingTask {
            service,
            remote_peer_identity,
            port_range,
            cert,
            max_idle_timeout,
            keep_alive_interval,
        }.run();
        let meta = ConnectingMeta {
            remote_peer_identity,
            sm_instant: self.sm.tick(),
        };
        let abort_handle = self.connecting_tasks.spawn(fut);
        self.connecting_meta.insert(abort_handle.id(), meta);
    }

    fn handle_connecting_result(
        &mut self,
        result: Result<(task::Id, Result<Connection, ConnectingError>), JoinError>,
    ) {
        match result {
            Ok((task_id, result)) => {
                let ConnectingMeta {
                    remote_peer_identity,
                    sm_instant,
                } = self.connecting_meta.remove(&task_id).unwrap();
                match result {
                    Ok(conn) => {
                        tracing::debug!("Connected to remote peer: {:?}", remote_peer_identity);
                        let conn_id = ConnectionId::new();
                        self.sm.register_connection(conn_id, remote_peer_identity);
                        assert!(self.connection_map.insert(conn_id, conn).is_none());
                    }
                    Err(connect_err) => match connect_err {
                        ConnectingError::ConnectError(connect_error) => {
                            tracing::warn!("Connect error: {:?}", connect_error);
                            self.sm
                                .mark_connection_failure_at_time(remote_peer_identity, sm_instant);
                        }
                        ConnectingError::ConnectionError(connection_error) => {
                            tracing::warn!("Connection error: {:?}", connection_error);
                            self.sm
                                .mark_connection_failure_at_time(remote_peer_identity, sm_instant);
                        }
                        ConnectingError::PeerNotInLeaderSchedule => {
                            tracing::warn!("Connection to remote peer not in leader schedule");
                            self.sm
                                .abandon_tx_for_remote_peer(remote_peer_identity, sm_instant);
                        }
                    },
                }
            }
            Err(join_err) => {
                let ConnectingMeta {
                    remote_peer_identity,
                    sm_instant: _,
                } = self
                    .connecting_meta
                    .remove(&join_err.id())
                    .expect("connecting_meta");
                panic!(
                    "Join error during connecting to {remote_peer_identity:?}: {:?}",
                    join_err
                );
            }
        }
    }

    fn handle_tx_sent_result(
        &mut self,
        result: Result<(task::Id, Result<SentOk, SendTxError>), JoinError>,
    ) {
        match result {
            Ok((task_id, result)) => {
                let InflightMeta { tx_id, permit } = self.inflight_meta.remove(&task_id).unwrap();
                let Permit {
                    connection_id,
                    remote_peer_identity,
                    permit_height,
                    time,
                } = permit;
                self.sm.release_connection_permit(permit);
                match result {
                    Ok(sent_ok) => {
                        tracing::debug!(
                            "Tx sent to remote peer: {:?} at permit height: {} in {:?}",
                            remote_peer_identity,
                            permit_height,
                            sent_ok.e2e_time
                        );
                    }
                    Err(send_err) => match send_err {
                        SendTxError::ConnectionError(connection_error) => {
                            if let ConnectionError::TransportError(TransportError {
                                code,
                                frame: _,
                                reason: _,
                            }) = connection_error
                            {
                                if code == quinn_proto::TransportErrorCode::STREAM_LIMIT_ERROR {
                                    self.sm.hit_rate_limit_for_remote_peer(
                                        remote_peer_identity,
                                        permit_height,
                                        time,
                                    );
                                }
                                let resp = GatewayTxFailed {
                                    remote_peer_identity,
                                    tx_id,
                                };
                                let _ = self.response_outlet.send(GatewayResponse::TxFailed(resp));
                            } else {
                                self.sm
                                    .mark_connection_failure_at_time(remote_peer_identity, time);
                            }
                        }
                        SendTxError::WriteError(write_error) => match write_error {
                            WriteError::Stopped(_) | WriteError::ClosedStream => {
                                let resp = GatewayTxFailed {
                                    remote_peer_identity,
                                    tx_id,
                                };
                                let _ = self.response_outlet.send(GatewayResponse::TxFailed(resp));
                            }
                            WriteError::ConnectionLost(_) | WriteError::ZeroRttRejected => {
                                self.sm
                                    .mark_connection_failure_at_time(remote_peer_identity, time);
                            }
                        },
                        SendTxError::StoppedError(stopped_error) => {
                            tracing::warn!("Connection stopped: {:?}", stopped_error);
                            self.sm
                                .mark_connection_failure_at_time(remote_peer_identity, time);
                        }
                    },
                }
            }
            Err(join_err) => {
                let inflight_meta = self
                    .inflight_meta
                    .remove(&join_err.id())
                    .expect("inflight_meta");
                let Permit {
                    remote_peer_identity,
                    connection_id,
                    permit_height,
                    time,
                } = inflight_meta.permit;
                self.sm.release_connection_permit(inflight_meta.permit);
                panic!(
                    "Join error during sending tx to {:?}: {:?}",
                    remote_peer_identity, join_err
                );
            }
        }
    }

    fn spawn_tx(&mut self, permit: Permit, tx: GatewayTransaction) {
        let Permit {
            connection_id,
            permit_height,
            time,
            remote_peer_identity,
        } = permit;
        let remote_peer_identity = tx.remote_peer;
        let tx_id = tx.tx_id;
        let conn = self
            .connection_map
            .get(&connection_id)
            .expect("connection_map")
            .clone();

        let fut = async move {
            let t = Instant::now();
            let mut uni = conn.open_uni().await?;
            uni.write_all(&tx.wire).await?;
            uni.finish().expect("finish uni");
            uni.stopped().await?;
            let elapsed = t.elapsed();
            let ok = SentOk { e2e_time: elapsed };
            Ok(ok)
        };

        let abort_handle = self.inflight_tasks.spawn(fut);
        tracing::debug!(
            "Sent tx: {:?} to remote peer: {:?} at permit height: {}",
            tx.tx_id,
            remote_peer_identity,
            permit_height
        );
        self.inflight_meta
            .insert(abort_handle.id(), InflightMeta { tx_id, permit });
    }


    pub async fn run(
        mut self,
    ) {
        let mut tx_to_send = Vec::with_capacity(1000);
        loop {
            tokio::select! {
                maybe = self.tx_inlet.recv() => {
                    match maybe {
                        Some(tx) => {
                            self.sm.push_back(tx);
                        }
                        None => {
                            tracing::debug!("Transaction gateway inlet closed");
                            break;
                        }
                    }
                }

                Some(result) = self.connecting_tasks.join_next_with_id() => {
                    self.handle_connecting_result(result);
                }

                Some(result) = self.inflight_tasks.join_next_with_id() => {
                    self.handle_tx_sent_result(result);
                }
            }

            while let Some(remote_peer) = self.sm.get_next_peer_to_connect() {
                self.spawn_connecting(remote_peer);
            }

            let count = self.sm.fill_tx_batch_to_send(&mut tx_to_send, 1000);
            tracing::debug!("Filled tx batch to send: {}", count);
            for (permit, tx) in tx_to_send.drain(..) {
                self.spawn_tx(permit, tx);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use {super::*, std::time::Duration};

    #[test]
    fn test_sm() {
        let mut sm = QuicGatewaySM::default();
        let remote_peer = Pubkey::new_unique();
        let tx = GatewayTransaction {
            tx_id: 0,
            wire: Arc::new([0u8; 10]),
            remote_peer,
        };

        sm.push_back(tx);

        let actual = sm
            .get_next_peer_to_connect()
            .expect("should have a peer to connect");

        assert_eq!(actual, remote_peer);

        let conn_id = ConnectionId::new();
        sm.register_connection(conn_id, remote_peer);

        let actual = sm.get_next_peer_to_connect();
        assert!(actual.is_none());

        let mut tx_batch = vec![];
        let cnt = sm.fill_tx_batch_to_send(&mut tx_batch, 1);
        assert_eq!(cnt, 1);

        let (permit, tx) = tx_batch.pop().expect("tx batch");

        assert_eq!(tx.tx_id, 0);
        assert_eq!(tx.wire.len(), 10);
        assert_eq!(permit.remote_peer_identity, remote_peer);
        assert_eq!(permit.connection_id, conn_id);
        assert_eq!(permit.permit_height, 1);

        sm.release_connection_permit(permit);

        // Make sure there is no more tx to process.
        let cnt = sm.fill_tx_batch_to_send(&mut tx_batch, 1);
        assert_eq!(cnt, 0);
        assert!(tx_batch.is_empty());
    }

    #[test]
    fn it_should_deschedule_tx_on_connection_failure() {
        let mut sm = QuicGatewaySM::default();
        let remote_peer = Pubkey::new_unique();
        let tx1 = GatewayTransaction {
            tx_id: 0,
            wire: Arc::new([0u8; 0]),
            remote_peer,
        };

        let tx2 = GatewayTransaction {
            tx_id: 1,
            wire: Arc::new([0u8; 0]),
            remote_peer,
        };

        sm.push_back(tx1);
        sm.push_back(tx2);

        let actual = sm
            .get_next_peer_to_connect()
            .expect("should have a peer to connect");

        assert_eq!(actual, remote_peer);

        let conn_id = ConnectionId::new();
        sm.register_connection(conn_id, remote_peer);

        let actual = sm.get_next_peer_to_connect();
        assert!(actual.is_none());

        let mut tx_batch = vec![];
        let cnt = sm.fill_tx_batch_to_send(&mut tx_batch, 1);
        assert_eq!(cnt, 1);
        assert_eq!(tx_batch.len(), 1);

        let (permit, tx) = tx_batch.pop().expect("tx batch");
        assert_eq!(tx.tx_id, 0);
        assert_eq!(permit.permit_height, 1);

        sm.mark_connection_failure_at_time(remote_peer, permit.time);
        let cnt = sm.fill_tx_batch_to_send(&mut tx_batch, 1);
        assert_eq!(cnt, 0);
        assert_eq!(tx_batch.len(), 0);
    }
}
