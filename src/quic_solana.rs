use {
    crate::{
        cluster_tpu_info::TpuInfo,
        config::ConfigQuic,
        crypto_provider::crypto_provider,
        metrics::jet as metrics,
        util::{PubkeySigner, ValueObserver},
    },
    futures::future::try_join_all,
    lru::LruCache,
    quinn::{
        crypto::rustls::QuicClientConfig, ClientConfig, ConnectError, Connection, ConnectionError,
        Endpoint, IdleTimeout, TransportConfig, WriteError,
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
        time::timeout,
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
}

impl ConnectionCacheIdentity {
    pub async fn update_keypair(&self, keypair: &Keypair) {
        let pubkey = keypair.pubkey();
        let (certificate, privkey) = new_dummy_x509_certificate(keypair);
        let cert = Arc::new(QuicClientCertificate {
            pubkey,
            privkey,
            certificate,
        });
        let mut locked = self.shared.lock().await;
        metrics::quic_set_identity(cert.pubkey);
        info!("update QUIC identityc: {}", cert.pubkey);
        locked.connection_pools.clear(); // drop all previously created connections
        locked.client_certificate = cert;
        let kp = keypair.insecure_clone();
        let ps = PubkeySigner::new(kp);
        self.identity_watcher.send_replace(keypair.pubkey());
        self.reactive_signer.send_replace(ps);
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
/// QUIC Session object are a managed version of raw QUIC connection where the session object
/// manage the lifecycle of each sub connection and provides caching and identity management.
///
#[derive(Clone)]
pub struct ConnectionCache {
    config: Arc<ConfigQuic>,
    shared: Arc<Mutex<QuicSessionInner>>,
}

impl ConnectionCache {
    pub fn new(config: ConfigQuic, initial_identity: Keypair) -> (Self, ConnectionCacheIdentity) {
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

    pub async fn get_identity(&self) -> Pubkey {
        self.shared.lock().await.client_certificate.pubkey
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

    pub async fn send_buffer(
        &self,
        addr: SocketAddr,
        data: &[u8],
        tpu_info: &TpuInfo,
    ) -> Result<(), QuicError> {
        let client = self.get_connection(addr).await;
        client.send_buffer(data, tpu_info).await
    }

    pub async fn send_batch(
        &self,
        addr: SocketAddr,
        buffers: &[&[u8]],
        tpu_info: &TpuInfo,
    ) -> Result<(), QuicError> {
        let client = self.get_connection(addr).await;
        client.send_batch(buffers, tpu_info).await
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
        // https://github.com/anza-xyz/agave/pull/2905#issuecomment-2356530294
        // stream will be finished when dropped. Finishing here explicitly would lead to blocking.
        // send_stream.finish().await.map_err(Into::into)
        Ok(())
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

            return match Self::send_buffer_using_conn(&connection, data).await {
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

    pub async fn send_batch(&self, buffers: &[&[u8]], tpu_info: &TpuInfo) -> Result<(), QuicError> {
        if buffers.len() > self.endpoint.config.send_max_concurrent_streams {
            return Err(QuicError::ExceedBatchLimit {
                size: buffers.len(),
                limit: self.endpoint.config.send_max_concurrent_streams,
            });
        }

        // Start off by "testing" the connection by sending the first buffer
        // This will also connect to the server if not already connected
        // and reconnect and retry if the first send attempt failed
        // (for example due to a timed out connection), returning an error
        // or the connection that was used to successfully send the buffer.
        // We will use the returned connection to send the rest of the buffers in the batch
        // to avoid touching the mutex in self, and not bother reconnecting if we fail along the way
        // since testing even in the ideal GCE environment has found no cases
        // where reconnecting and retrying in the middle of a batch send
        // (i.e. we encounter a connection error in the middle of a batch send, which presumably cannot
        // be due to a timed out connection) has succeeded
        if buffers.is_empty() {
            return Ok(());
        }

        let _permit = self.sem.acquire_many(buffers.len() as u32);
        let connection = self.send_buffer_retry(buffers[0], tpu_info).await?;

        // Used to avoid dereferencing the Arc multiple times below
        // by just getting a reference to the NewConnection once
        let connection_ref: &Connection = &connection;

        try_join_all(
            buffers[1..]
                .iter()
                .map(|buffer| Self::send_buffer_using_conn(connection_ref, buffer)),
        )
        .await?;

        Ok(())
    }
}
