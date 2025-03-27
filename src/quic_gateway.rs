use std::{collections::{HashMap, VecDeque}, net::{IpAddr, Ipv4Addr, SocketAddr}, sync::Arc, time::{Duration, Instant}};

use futures::channel::mpsc::SendError;
use hyper::client;
use quinn::{crypto::rustls::QuicClientConfig, ClientConfig, Connection, ConnectionError, Endpoint, IdleTimeout, TransportConfig, WriteError};
use quinn_proto::TransportError;
use solana_net_utils::PortRange;
use solana_quic_client::nonblocking::quic_client::{QuicClientCertificate, SkipServerVerification};
use solana_sdk::{pubkey::Pubkey, quic::QUIC_SEND_FAIRNESS};
use solana_streamer::nonblocking::quic::ALPN_TPU_PROTOCOL_ID;
use tokio::{sync::mpsc, task::{self, JoinError, JoinSet}};
use tonic::ConnectError;

use crate::{crypto_provider::crypto_provider, stake::StakeInfoMap};



// struct QuicExcangeSM {
//     connection_map: HashMap<SocketAddr, QuicConnection>,
// }


type ConnectionId = usize;

///
/// A monotonic increasing passage of time in [`QuicGatewaySM`].
/// Each update to the state machine increase the time value.
/// 
/// It serves as a way to order events in the state machine and discard stale events.
/// 
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct StateMachineInstant(u64);


impl StateMachineInstant {
    fn inc(self) -> Self {
        Self(self.0 + 1)
    }
}

struct ConnectionState {
    connection_id: ConnectionId,
    registered_at: StateMachineInstant,
    custom_max_streams: Option<u64>,
    permit_height: PermitHeight,
}

struct Schedule { 
    schedule_linked_list: VecDeque<Pubkey>,
}


pub struct QuicGatewaySM {
    current_max_stream_limit_per_conn: u64,
    unconnected_remote_peer_tx_queues: HashMap<Pubkey, VecDeque<GatewayTransaction>>,
    remote_peer_tx_queues: HashMap<Pubkey, VecDeque<GatewayTransaction>>,
    connection_map: HashMap<Pubkey, ConnectionState>,
    time_sequence: StateMachineInstant,
    deadletter_queue: VecDeque<GatewayTransaction>,
    deregistered_connections: VecDeque<ConnectionId>,
}

pub enum AcquireConnectionErr {
    NoConnection,
    ReachedMaxStreamLimit,
}

pub type PermitHeight = u64;

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

    pub fn schedule_tx(&mut self, tx: GatewayTransaction) {
        let remote_peer_identity = tx.remote_peer;
        if self.connection_map.contains_key(&remote_peer_identity) {
            let tx_queue = self.remote_peer_tx_queues.entry(remote_peer_identity).or_default();
            tx_queue.push_back(tx);
        } else {
            let tx_queue = self.unconnected_remote_peer_tx_queues
                .entry(remote_peer_identity)
                .or_default();
            tx_queue.push_back(tx);
        }
    }

    pub fn register_connection(&mut self, conn: ConnectionId, remote_identity: Pubkey) {
        let now = self.tick();
        let old = self.connection_map.insert(remote_identity, ConnectionState {
            connection_id: conn,
            registered_at: now,
            custom_max_streams: None,
            permit_height: 0,
        });
        assert!(old.is_none());

        if let Some(tx_queue) = self.unconnected_remote_peer_tx_queues.remove(&remote_identity) {
            let old = self.remote_peer_tx_queues.insert(remote_identity, tx_queue);
            assert!(old.is_none());
        }
    }

    pub fn acquire_connection_permit(&mut self, remote_peer_identity: Pubkey) -> Result<Permit, AcquireConnectionErr> {
        if let Some(state) = self.connection_map.get_mut(&remote_peer_identity) {
            let conn_id = state.connection_id;
            let limit = state.custom_max_streams.unwrap_or(self.current_max_stream_limit_per_conn);
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

    pub fn deregister_connection(&mut self, remote_identity: Pubkey){
        if let Some(state)  = self.connection_map.remove(&remote_identity) {
            self.deregistered_connections.push_back(state.connection_id);
             if let Some(dropped_tx) = self.remote_peer_tx_queues.remove(&remote_identity) {
                self.deadletter_queue.extend(dropped_tx);
            }
        }
    }

    pub fn set_current_max_stream_limit(&mut self, max_streams: u64) {
        self.current_max_stream_limit_per_conn = max_streams;
    }

    pub fn process_next_tx(&mut self) -> Option<GatewayTransaction> {
        None
    }

    pub fn abandon_tx_for_remote_peer(&mut self, remote_identity: Pubkey) {
        todo!()
    }

    pub fn pop_next_remote_leader_to_connect(&mut self) -> Option<Pubkey> {
        todo!()
    }

    ///
    /// Pop the next transaction in the dead letter queue.
    /// 
    pub fn pop_next_tx_in_dlq(&mut self) -> Option<GatewayTransaction> {
        self.deadletter_queue.pop_front()
    }

    pub fn drain_dlq(&mut self) -> impl IntoIterator<Item=GatewayTransaction> + '_ {
        self.deadletter_queue.drain(..)
    }

    pub fn mark_connection_failure_at_time(&mut self, remote_identity: Pubkey, time: StateMachineInstant) {
        if let Some(state) = self.connection_map.get(&remote_identity) {
            if state.registered_at < time {
                self.deregister_connection(remote_identity);
            }
        }

        todo!()
    }

    pub fn hit_rate_limit_for_remote_peer(&mut self, remote_identity: Pubkey, permit_height: PermitHeight, time: StateMachineInstant) {
        if let Some(state) = self.connection_map.get_mut(&remote_identity) {
            if state.registered_at < time {
                let min_permit = state.custom_max_streams
                    .unwrap_or(permit_height)
                    .min(permit_height);
                state.custom_max_streams.replace(min_permit);
            }
        }
    }

    pub fn mark_connection_attempt_failure(&mut self, remote_identity: Pubkey, time: StateMachineInstant) {
        if let Some(state) = self.connection_map.get(&remote_identity) {
            if state.registered_at < time {
                self.deregister_connection(remote_identity);
            }
        }
    }
    

    pub fn pop_next_derigistered_connection(&mut self) -> Option<ConnectionId> {
        self.deregistered_connections.pop_front()
    }
}

pub(crate) struct InflightMeta {
    tx_id: u64,
    permit: Permit
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

pub(crate) struct TokioQuicGatewayDriver {
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

pub enum GatewayResponse {
    TxSent {
        remote_peer: Pubkey,
        tx_id: u64,
    },
    TxFailed {
        remote_peer: Pubkey,
        tx_id: u64,
    },
    TxDropped {
        remote_peer: Pubkey,
        tx_id: u64,
    }
}

impl TokioQuicGatewayDriver {

    fn spawn_connecting(&mut self, remote_peer_identity: Pubkey) {
        let service = Arc::clone(&self.leader_tpu_info_service);
        let port_range = self.config.port_range;
        let cert = Arc::clone(&self.client_certificate);
        let max_idle_timeout = self.config.max_idle_timeout;
        let keep_alive_interval = self.config.keep_alive_interval;
        let fut = async move {
            let remote_peer_addr = service
                .get_tpu_socket_addr(remote_peer_identity)
                .await
                .ok_or(ConnectingError::PeerNotInLeaderSchedule)?;


            let client_socket = solana_net_utils::bind_in_range(
                IpAddr::V4(Ipv4Addr::UNSPECIFIED),
                port_range,
            )
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
                    vec![cert.certificate.clone()],
                    cert.key.clone_key(),
                )
                .expect("Failed to set QUIC client certificates");
            crypto.enable_early_data = true;
            crypto.alpn_protocols = vec![ALPN_TPU_PROTOCOL_ID.to_vec()];

            let transport_config = {
                let mut res = TransportConfig::default();

                let max_idle_timeout = max_idle_timeout.map(|timeout| {
                    IdleTimeout::try_from(timeout).expect("Failed to set QUIC max idle timeout")
                });
                res.max_idle_timeout(max_idle_timeout);
                res.keep_alive_interval(keep_alive_interval);
                res.send_fairness(QUIC_SEND_FAIRNESS);

                res
            };

            let mut config = ClientConfig::new(Arc::new(QuicClientConfig::try_from(crypto).unwrap()));
            config.transport_config(Arc::new(transport_config));

            endpoint.set_default_client_config(config);


            let connecting = endpoint
                .connect(remote_peer_addr, "connect")
                .map_err(|e| {
                    ConnectingError::ConnectError(e)
                })?;
            let conn = connecting.await?;
            Ok(conn)
        };
        let meta = ConnectingMeta {
            remote_peer_identity,
            sm_instant: self.sm.tick(),
        };
        let abort_handle = self.connecting_tasks.spawn(fut);
        self.connecting_meta.insert(abort_handle.id(), meta);
    }

    fn handle_connecting_result(&mut self, result: Result<(task::Id, Result<Connection, ConnectingError>), JoinError>) {
        match result {
            Ok((task_id, result)) => {
                let ConnectingMeta { remote_peer_identity, sm_instant } = self.connecting_meta.remove(&task_id).unwrap();
                match result {
                    Ok(conn) => {
                        tracing::debug!("Connected to remote peer: {:?}", remote_peer_identity);
                        let conn_id = conn.stable_id();
                        self.sm.register_connection(conn_id, remote_peer_identity);
                        assert!(self.connection_map.insert(conn_id, conn).is_none());
                    }
                    Err(connect_err) => {
                        match connect_err {
                            ConnectingError::ConnectError(connect_error) => {
                                tracing::warn!("Connect error: {:?}", connect_error);
                                self.sm.mark_connection_attempt_failure(remote_peer_identity, sm_instant);
                            },
                            ConnectingError::ConnectionError(connection_error) => {
                                tracing::warn!("Connection error: {:?}", connection_error);
                                self.sm.mark_connection_attempt_failure(remote_peer_identity, sm_instant);
                            },
                            ConnectingError::PeerNotInLeaderSchedule => {
                                tracing::warn!("Connection to remote peer not in leader schedule");
                                self.sm.abandon_tx_for_remote_peer(remote_peer_identity)
                            },
                        }
                    }
                }
            }
            Err(join_err) => {
                let ConnectingMeta { remote_peer_identity, sm_instant: _ } = self.connecting_meta.remove(&join_err.id()).expect("connecting_meta");
                panic!("Join error during connecting to {remote_peer_identity:?}: {:?}", join_err);
            }
        }
    }


    fn handle_tx_sent_result(&mut self, result: Result<(task::Id, Result<SentOk, SendTxError>), JoinError>) {
        match result {
            Ok((task_id, result)) => {
                let InflightMeta {
                    tx_id,
                    permit,
                } = self.inflight_meta.remove(&task_id).unwrap();
                let Permit {
                    connection_id,
                    remote_peer_identity,
                    permit_height,
                    time,
                } = permit;
                self.sm.release_connection_permit(permit);
                match result {
                    Ok(sent_ok) => {
                        tracing::debug!("Tx sent to remote peer: {:?} at permit height: {} in {:?}", remote_peer_identity, permit_height, sent_ok.e2e_time);

                    }
                    Err(send_err) => {
                        match send_err {
                            SendTxError::ConnectionError(connection_error) => {
                                if let ConnectionError::TransportError(TransportError { code, frame: _, reason: _ }) = connection_error {
                                    if code == quinn_proto::TransportErrorCode::STREAM_LIMIT_ERROR {
                                        self.sm.hit_rate_limit_for_remote_peer(remote_peer_identity, permit_height, time);
                                    }
                                    todo!()
                                } else {
                                    self.sm.mark_connection_failure_at_time(remote_peer_identity, time);
                                }
                            },
                            SendTxError::WriteError(write_error) => {
                                match write_error {
                                    WriteError::Stopped(_) | WriteError::ClosedStream => {
                                        todo!()
                                    },
                                    WriteError::ConnectionLost(_) | WriteError::ZeroRttRejected => {
                                        self.sm.mark_connection_failure_at_time(remote_peer_identity, time);
                                    }
                                }
                            },
                            SendTxError::StoppedError(stopped_error) => {
                                tracing::warn!("Connection stopped: {:?}", stopped_error);
                                self.sm.mark_connection_failure_at_time(remote_peer_identity, time);
                            },
                        }
                    }
                }
            }
            Err(join_err) => {
                let inflight_meta = self.inflight_meta.remove(&join_err.id()).expect("inflight_meta");
                let Permit { 
                    remote_peer_identity,
                    connection_id,
                    permit_height,
                    time,
                } = inflight_meta.permit;
                self.sm.release_connection_permit(inflight_meta.permit);
                panic!("Join error during sending tx to {:?}: {:?}", remote_peer_identity, join_err);
            }
        }
    }

    fn spawn_tx(&mut self, permit: Permit, tx: GatewayTransaction) {
        let Permit { connection_id, permit_height, time, remote_peer_identity } = permit;
        let remote_peer_identity = tx.remote_peer;
        let tx_id = tx.tx_id;
        let conn = self.connection_map.get(&connection_id).expect("connection_map").clone();
        let fut = async move {
            let t = Instant::now();
            let mut uni = conn.open_uni().await?;
            uni.write_all(&tx.wire).await?;
            uni.finish().expect("finish uni");
            uni.stopped().await?;
            let elapsed = t.elapsed();
            let ok = SentOk {
                e2e_time: elapsed,
            };
            Ok(ok)
        };

        let abort_handle = self.inflight_tasks.spawn(fut);
        tracing::debug!("Sent tx: {:?} to remote peer: {:?} at permit height: {}", tx.tx_id, remote_peer_identity, permit_height);
        self.inflight_meta.insert(abort_handle.id(), InflightMeta {
            tx_id,
            permit,
        });
    }

    pub async fn run(mut self, 
        mut tx_queue: mpsc::Receiver<GatewayTransaction>,
        mut out: mpsc::Sender<GatewayResponse>,
    ) {
        loop {
            tokio::select! {
                maybe = tx_queue.recv() => {
                    match maybe {
                        Some(tx) => {
                            self.sm.schedule_tx(tx);
                        }
                        None => {
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

            while let Some(remote_peer) = self.sm.pop_next_remote_leader_to_connect() {
                self.spawn_connecting(remote_peer);
            }

            while let Some(tx) = self.sm.process_next_tx() {
                match self.sm.acquire_connection_permit(tx.remote_peer) {
                    Ok((permit)) => {
                        self.spawn_tx(permit, tx);
                    }
                    Err(AcquireConnectionErr::NoConnection) => {
                        unreachable!("process next should only return tx with a connection");
                    }
                    Err(AcquireConnectionErr::ReachedMaxStreamLimit) => {
                        unreachable!("process next tx should account for current max stream limit");
                    }
                }
            }
        }
    }
}
