use {
    crate::{
        cluster_tpu_info::TpuInfo,
        config::ConfigQuic,
        crypto_provider::crypto_provider,
        metrics::jet::{
            self as metrics, incr_send_tx_attempt, observe_leader_rtt,
            observe_send_transaction_e2e_latency, set_leader_mtu,
        },
        util::{PubkeySigner, ValueObserver},
    },
    lru::LruCache,
    quinn::{
        crypto::rustls::QuicClientConfig, ClientConfig, ConnectError, Connection, ConnectionError,
        Endpoint, IdleTimeout, StoppedError, TransportConfig, VarInt, WriteError,
    },
    rand::{thread_rng, Rng},
    rustls::{
        client::danger::{HandshakeSignatureValid, ServerCertVerified},
        crypto::{verify_tls12_signature, verify_tls13_signature, CryptoProvider},
        pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime},
        DigitallySignedStruct, Error,
    },
    solana_sdk::{
        pubkey::Pubkey,
        quic::QUIC_SEND_FAIRNESS,
        signature::{Keypair, Signer},
    },
    solana_streamer::{
        nonblocking::quic::ALPN_TPU_PROTOCOL_ID, tls_certificates::new_dummy_x509_certificate,
    },
    std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        sync::Arc,
        time::Duration,
    },
    tokio::{
        sync::{watch, AcquireError, Mutex, OnceCell, Semaphore},
        time::{timeout, Instant},
    },
    tracing::{debug, info},
};

///
/// Manage the quic identity of a [`ConnectionCache`].
///
/// This is useful if you want to update the identity of a [`ConectionCache`] without restarting the whole application.
pub struct ConnectionCacheIdentity {
    shared: Arc<Mutex<QuicSessionInner>>,
    identity_watcher: watch::Sender<Pubkey>,
    reactive_signer: watch::Sender<PubkeySigner>,
    identity_flusher: Box<dyn IdentityFlusher + Send + Sync + 'static>,
}

impl ConnectionCacheIdentity {
    pub async fn update_keypair(&self, keypair: &Keypair) {
        let kp = keypair.insecure_clone();
        let ps = PubkeySigner::new(kp);

        let pubkey = keypair.pubkey();
        let (certificate, privkey) = new_dummy_x509_certificate(keypair);
        let cert = Arc::new(QuicClientCertificate {
            pubkey,
            privkey,
            certificate,
        });
        let mut locked = self.shared.lock().await;

        // Connection pools actually returned owned connections, so we need to clear them AFTER we acquire the lock.
        // Otherwise, if we clear connection_pools before the lock, new connection can still be open with the old certificate.
        // Please note: since we are using Mutex + Owned connection objects this give us several important properties that we must understand:
        //  1. Previously acquire connection **won't be affected** by the change of the certificate (direct consequence of using Owned connection object through Arc)
        //  2. New connection will be created with the new certificate.
        //  3. Because of the lock, all concurrent attempt to connect will block -> this can cause deadlock if we are not cautious.
        locked.connection_pools.clear();

        metrics::quic_set_identity(cert.pubkey);
        info!("update QUIC identity: {}", cert.pubkey);
        self.identity_flusher.flush().await;
        locked.client_certificate = cert;
        self.reactive_signer.send_replace(ps);
        self.identity_watcher.send_replace(keypair.pubkey());
    }

    pub async fn get_cert(&self) -> CertificateDer {
        self.shared
            .lock()
            .await
            .client_certificate
            .certificate
            .clone()
    }

    pub async fn get_identity(&self) -> Pubkey {
        self.shared.lock().await.client_certificate.pubkey
    }

    pub fn observe_identity_change(&self) -> ValueObserver<Pubkey> {
        self.identity_watcher.subscribe().into()
    }

    pub fn observe_signer_change(&self) -> ValueObserver<PubkeySigner> {
        self.reactive_signer.subscribe().into()
    }
}

///
/// Base Trait for flushing the identity of a [`ConnectionCache`].
///
#[async_trait::async_trait]
pub trait IdentityFlusher {
    async fn flush(&self);
}

pub type BoxedIdentityFlusher = Box<dyn IdentityFlusher + Send + Sync + 'static>;

///
/// IdentityFlusher that does nothing.
pub struct NullIdentityFlusher;

#[async_trait::async_trait]
impl IdentityFlusher for NullIdentityFlusher {
    async fn flush(&self) {}
}

///
/// QUIC Session object are a managed version of raw QUIC connection where the session object
/// manage the lifecycle of each sub connection and provides caching and identity management.
///
#[derive(Clone)]
pub struct ConnectionCache {
    config: Arc<ConfigQuic>,
    shared: Arc<Mutex<QuicSessionInner>>,
}

pub struct ConnectionCacheSendPermit {
    pub tpu_info: TpuInfo,
    pub addr: SocketAddr,
    inner: Arc<QuicClient>,
}

impl ConnectionCacheSendPermit {
    pub async fn send_buffer(&self, data: &[u8]) -> Result<(), QuicError> {
        self.inner.send_buffer(data, &self.tpu_info).await
    }
}

impl ConnectionCache {
    pub fn new<F>(
        config: ConfigQuic,
        initial_identity: Keypair,
        flush_identity: F,
    ) -> (Self, ConnectionCacheIdentity)
    where
        F: IdentityFlusher + Send + Sync + 'static,
    {
        let client_certificate = Self::create_client_certificate(&initial_identity);
        let initial_signer = PubkeySigner::new(initial_identity.insecure_clone());
        metrics::quic_set_identity(client_certificate.pubkey);
        info!("generate new QUIC identity: {}", client_certificate.pubkey);

        let connection_pools = LruCache::new(config.connection_max_pools);
        let shared = Arc::new(Mutex::new(QuicSessionInner {
            client_certificate,
            connection_pools,
        }));
        let (identity_watcher, _) = watch::channel(initial_identity.pubkey());
        let (reactive_signer, _) = watch::channel(initial_signer);
        let quic_identity_man = ConnectionCacheIdentity {
            shared: Arc::clone(&shared),
            identity_watcher,
            reactive_signer,
            identity_flusher: Box::new(flush_identity),
        };
        let ret = Self {
            config: Arc::new(config),
            shared,
        };
        (ret, quic_identity_man)
    }

    fn create_client_certificate(keypair: &Keypair) -> Arc<QuicClientCertificate> {
        let pubkey = keypair.pubkey();
        let (certificate, privkey) = new_dummy_x509_certificate(keypair);
        Arc::new(QuicClientCertificate {
            pubkey,
            privkey,
            certificate,
        })
    }

    async fn get_connection(&self, addr: SocketAddr) -> Arc<QuicClient> {
        let pool = {
            let mut locked = self.shared.lock().await;
            match locked.connection_pools.get(&addr) {
                Some(pool) => Arc::clone(pool),
                None => {
                    let pool = Arc::new(Mutex::new(QuicPool {
                        endpoint: Arc::new(QuicLazyInitializedEndpoint::new(
                            Arc::clone(&locked.client_certificate),
                            Arc::clone(&self.config),
                        )),
                        connections: Vec::with_capacity(self.config.connection_pool_size),
                    }));
                    locked.connection_pools.push(addr, Arc::clone(&pool));
                    pool
                }
            }
        };

        let mut locked = pool.lock().await;
        locked.get_connection(addr)
    }

    pub async fn reserve_send_permit(
        &self,
        tpu_info: TpuInfo,
        addr: SocketAddr,
    ) -> ConnectionCacheSendPermit {
        ConnectionCacheSendPermit {
            tpu_info,
            addr,
            inner: self.get_connection(addr).await,
        }
    }

    pub async fn send_buffer(
        &self,
        addr: SocketAddr,
        data: &[u8],
        tpu_info: &TpuInfo,
    ) -> Result<(), QuicError> {
        let client = self.get_connection(addr).await;
        client.send_buffer(data, tpu_info).await
    }
}

struct QuicSessionInner {
    client_certificate: Arc<QuicClientCertificate>,
    connection_pools: LruCache<SocketAddr, Arc<Mutex<QuicPool>>>,
}

struct QuicClientCertificate {
    pubkey: Pubkey,
    privkey: PrivateKeyDer<'static>,
    certificate: CertificateDer<'static>,
}

struct QuicPool {
    endpoint: Arc<QuicLazyInitializedEndpoint>,
    connections: Vec<Arc<QuicClient>>,
}

impl QuicPool {
    fn get_connection(&mut self, addr: SocketAddr) -> Arc<QuicClient> {
        if self.connections.len() < self.connections.capacity() {
            let connection = Arc::new(QuicClient::new(
                Semaphore::new(self.endpoint.config.send_max_concurrent_streams),
                Arc::clone(&self.endpoint),
                addr,
            ));
            self.connections.push(Arc::clone(&connection));
            connection
        } else {
            let mut max_available_permits = 0;
            let mut connections = Vec::with_capacity(self.connections.len());

            for connection in self.connections.iter() {
                if connection.sem.available_permits() > max_available_permits {
                    max_available_permits = connection.sem.available_permits();
                    connections.clear();
                }
                connections.push(connection);
            }

            Arc::clone(
                connections
                    .get(thread_rng().gen_range(0..connections.len()))
                    .cloned()
                    .expect("index is within connections"),
            )
        }
    }
}

#[derive(Debug)]
struct SkipServerVerification(Arc<CryptoProvider>);

impl SkipServerVerification {
    pub fn new() -> Arc<Self> {
        Arc::new(Self(Arc::new(crypto_provider())))
    }
}

impl rustls::client::danger::ServerCertVerifier for SkipServerVerification {
    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

struct QuicLazyInitializedEndpoint {
    endpoint: OnceCell<Arc<Endpoint>>,
    client_certificate: Arc<QuicClientCertificate>,
    config: Arc<ConfigQuic>,
}

impl QuicLazyInitializedEndpoint {
    fn new(client_certificate: Arc<QuicClientCertificate>, config: Arc<ConfigQuic>) -> Self {
        Self {
            endpoint: OnceCell::<Arc<Endpoint>>::new(),
            client_certificate,
            config,
        }
    }

    fn create_endpoint(&self) -> Endpoint {
        let client_socket = solana_net_utils::bind_in_range(
            IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            self.config.endpoint_port_range,
        )
        .expect("QuicLazyInitializedEndpoint::create_endpoint bind_in_range")
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
                vec![self.client_certificate.certificate.clone()],
                self.client_certificate.privkey.clone_key(),
            )
            .expect("Failed to set QUIC client certificates");
        crypto.enable_early_data = true;
        crypto.alpn_protocols = vec![ALPN_TPU_PROTOCOL_ID.to_vec()];

        let transport_config = {
            let mut res = TransportConfig::default();

            let timeout = IdleTimeout::try_from(self.config.max_idle_timeout).unwrap();
            res.max_idle_timeout(Some(timeout));
            res.keep_alive_interval(Some(self.config.keep_alive_interval));
            res.send_fairness(QUIC_SEND_FAIRNESS);

            res
        };

        let mut config = ClientConfig::new(Arc::new(QuicClientConfig::try_from(crypto).unwrap()));
        config.transport_config(Arc::new(transport_config));

        endpoint.set_default_client_config(config);

        endpoint
    }

    async fn get_endpoint(&self) -> Arc<Endpoint> {
        Arc::clone(
            self.endpoint
                .get_or_init(|| async { Arc::new(self.create_endpoint()) })
                .await,
        )
    }
}

#[derive(thiserror::Error, Debug)]
pub enum QuicError {
    #[error(transparent)]
    ConnectError(#[from] ConnectError),
    #[error(transparent)]
    ConnectionError(#[from] ConnectionError),
    #[error(transparent)]
    WriteError(#[from] WriteError),
    #[error("loaded identity doesn't match specified in config")]
    IdentityMismatch,
    #[error("failed to send batch with {size} transactions, limit {limit}")]
    ExceedBatchLimit { size: usize, limit: usize },
    #[error(transparent)]
    AcquireFailed(#[from] AcquireError),
    #[error("0-RTT rejected")]
    ZeroRttRejected,
    #[error("stopped failed with error code {0}")]
    StopErrorCode(VarInt),
}

impl QuicError {
    pub const fn is_timedout(&self) -> bool {
        matches!(self, Self::ConnectionError(ConnectionError::TimedOut))
    }

    pub fn get_categorie(&self) -> String {
        {
            match &self {
                QuicError::ConnectError(_) => "Connect Error",
                QuicError::ConnectionError(_) => "Connection Error",
                QuicError::WriteError(_) => "Write error",
                QuicError::IdentityMismatch => "Identity Mismatch",
                QuicError::ExceedBatchLimit { .. } => "Exceed Batch Limit",
                QuicError::AcquireFailed(_) => "Acquired Failed",
                QuicError::ZeroRttRejected => "Zero RTT Rejected",
                QuicError::StopErrorCode(_) => "Stop Error Code",
            }
        }
        .to_owned()
    }
}

/// A wrapper over NewConnection with additional capability to create the endpoint as part
/// of creating a new connection.
#[derive(Clone)]
struct QuicNewConnection {
    endpoint: Arc<Endpoint>,
    connection: Arc<Connection>,
}

impl QuicNewConnection {
    /// Create a QuicNewConnection given the remote address 'addr'.
    async fn make_connection(
        endpoint: Arc<QuicLazyInitializedEndpoint>,
        addr: SocketAddr,
        connection_handshake_timeout: Duration,
    ) -> Result<Self, QuicError> {
        let endpoint = endpoint.get_endpoint().await;

        let connecting = endpoint.connect(addr, "connect")?;
        timeout(connection_handshake_timeout, connecting)
            .await
            .map_err(|_error| ConnectionError::TimedOut)?
            .map(Arc::new)
            .map(|connection| Self {
                endpoint,
                connection,
            })
            .map_err(Into::into)
    }

    // Attempts to make a faster connection by taking advantage of pre-existing key material.
    // Only works if connection to this endpoint was previously established.
    async fn make_connection_0rtt(
        &mut self,
        addr: SocketAddr,
        connection_handshake_timeout: Duration,
    ) -> Result<Arc<Connection>, QuicError> {
        self.connection = Arc::new(match self.endpoint.connect(addr, "connect")?.into_0rtt() {
            Ok((connection, zero_rtt)) => timeout(connection_handshake_timeout, zero_rtt)
                .await
                .map(|_zero_rtt| connection)
                .map_err(|_timeout| ConnectionError::TimedOut)?,
            Err(connecting) => timeout(connection_handshake_timeout, connecting)
                .await
                .map_err(|_timeout| ConnectionError::TimedOut)??,
        });
        Ok(Arc::clone(&self.connection))
    }
}

pub struct QuicClient {
    sem: Semaphore,
    endpoint: Arc<QuicLazyInitializedEndpoint>,
    connection: Mutex<Option<QuicNewConnection>>,
    addr: SocketAddr,
}

impl QuicClient {
    fn new(sem: Semaphore, endpoint: Arc<QuicLazyInitializedEndpoint>, addr: SocketAddr) -> Self {
        Self {
            sem,
            endpoint,
            connection: Mutex::new(None),
            addr,
        }
    }

    async fn send_buffer_using_conn(connection: &Connection, data: &[u8]) -> Result<(), QuicError> {
        let mut send_stream = connection.open_uni().await?;
        send_stream.write_all(data).await?;
        // Finish sets FIN bit in the last Frame, which is a signal that the stream is done sending data.
        // You must call finish before stopped underwise the reader won't ever know you finished and will make
        // `stopped` called wait forever.
        // Finish should never return an error since the stream is created and end within the same function.
        // It is impossible this function is called twice on the same stream.
        // Finish error happen if the stream has already been closed.
        send_stream.finish().expect("finish failed");
        // Make sure we the remote side has acknowledged all the data we sent
        // This stopped may timeout if the remote side is not responding after MAX_CONNECTION_IDLE_TIMEOUT
        send_stream
            .stopped()
            .await
            .map_err(|e| match e {
                StoppedError::ConnectionLost(e2) => e2.into(),
                StoppedError::ZeroRttRejected => QuicError::ZeroRttRejected,
            })
            .and_then(|maybe| {
                match maybe {
                    None => Ok(()),
                    Some(error_code) => {
                        // If we get here, it is not guaranteed that the remote side has received all the data we sent.
                        // Even if we get 0x00 (NO_ERROR) QUIC error code.
                        // NO_ERROR = the receiver close abruptly the connection.
                        // In rust, if the Recv object is dropped before reading all the data, it should return NO_ERROR.
                        Err(QuicError::StopErrorCode(error_code))
                    }
                }
            })
    }

    // Attempts to send data, connecting/reconnecting as necessary
    // On success, returns the connection used to successfully send the data
    async fn send_buffer_retry(
        &self,
        data: &[u8],
        tpu_info: &TpuInfo,
    ) -> Result<Arc<Connection>, QuicError> {
        let mut connection_try_count = 0;
        let mut last_connection_id = 0;
        let mut last_error = None;
        while connection_try_count < 2 {
            let connection = {
                let mut conn_guard = self.connection.lock().await;
                let maybe_conn = conn_guard.as_mut();
                match maybe_conn {
                    Some(conn) => {
                        // this is the problematic connection we had used before, create a new one
                        if conn.connection.stable_id() == last_connection_id {
                            let conn = conn
                                .make_connection_0rtt(
                                    self.addr,
                                    self.endpoint.config.connection_handshake_timeout,
                                )
                                .await
                                .inspect_err(|error| {
                                    debug!(
                                        tpu.leader = %tpu_info.leader,
                                        tpu.slots = ?tpu_info.slots,
                                        tpu.quic = %self.addr,
                                        error = ?error,
                                        "failed to make 0rtt connection",
                                    );
                                })?;

                            debug!(
                                tpu.leader = %tpu_info.leader,
                                tpu.slots = ?tpu_info.slots,
                                tpu.quic = %self.addr,
                                connection_id = conn.stable_id(),
                                attempt = connection_try_count,
                                "made 0rtt connection",
                            );
                            connection_try_count += 1;
                            conn
                        } else {
                            Arc::clone(&conn.connection)
                        }
                    }
                    None => {
                        let conn = QuicNewConnection::make_connection(
                            Arc::clone(&self.endpoint),
                            self.addr,
                            self.endpoint.config.connection_handshake_timeout,
                        )
                        .await
                        .inspect_err(|error| {
                            debug!(
                                tpu.leader = %tpu_info.leader,
                                tpu.slots = ?tpu_info.slots,
                                tpu.quic = %self.addr,
                                error = ?error,
                                "failed to make connection",
                            );
                        })?;
                        *conn_guard = Some(conn.clone());

                        debug!(
                            tpu.leader = %tpu_info.leader,
                            tpu.slots = ?tpu_info.slots,
                            tpu.quic = %self.addr,
                            connection_id = conn.connection.stable_id(),
                            attempt = connection_try_count,
                            "made connection",
                        );
                        connection_try_count += 1;
                        Arc::clone(&conn.connection)
                    }
                }
            };

            // no need to send packet as it is only for warming connections
            if data.is_empty() {
                return Ok(connection);
            }

            last_connection_id = connection.stable_id();
            let path_stats = connection.stats().path;
            let current_mut = path_stats.current_mtu;
            set_leader_mtu(tpu_info.leader, current_mut);
            observe_leader_rtt(tpu_info.leader, path_stats.rtt);

            let t = Instant::now();
            let quic_result = Self::send_buffer_using_conn(&connection, data).await;
            let elapsed = t.elapsed();
            observe_send_transaction_e2e_latency(tpu_info.leader, elapsed);
            incr_send_tx_attempt(tpu_info.leader);
            return match quic_result {
                Ok(()) => {
                    debug!(
                        tpu.leader = %tpu_info.leader,
                        tpu.slots = ?tpu_info.slots,
                        tpu.quic = %self.addr,
                        connection_id = connection.stable_id(),
                        datalen = data.len(),
                        "successfully sent data",
                    );
                    Ok(connection)
                }
                Err(error) => match error {
                    QuicError::ConnectionError(_) => {
                        last_error = Some(error);
                        continue;
                    }
                    _ => {
                        debug!(
                            tpu.leader = %tpu_info.leader,
                            tpu.slots = ?tpu_info.slots,
                            tpu.quic = %self.addr,
                            connection_id = connection.stable_id(),
                            error = ?error,
                            "failed to send data",
                        );
                        Err(error)
                    }
                },
            };
        }

        // If we get here but last_error is None, then we have a logic error
        // in this function, so panic here with an expect to help debugging
        Err(last_error.expect("expect to have an error after retries"))
    }

    pub async fn send_buffer(&self, data: &[u8], tpu_info: &TpuInfo) -> Result<(), QuicError> {
        let _permit = self.sem.acquire().await?;
        let _connection = self.send_buffer_retry(data, tpu_info).await?;
        Ok(())
    }
}
