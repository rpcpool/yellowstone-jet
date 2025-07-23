use {
    crate::metrics::{
        jet::{
            dec_quic_server_active_connections, incr_quic_server_active_connections,
            incr_quic_server_auth_failures_total, incr_quic_server_stream_error,
            incr_quic_server_transactions_by_client_total,
            incr_quic_server_transactions_received_total, observe_quic_server_stream_duration,
        },
    },
    crate::transactions::SendTransactionRequest,
    dashmap::DashMap,
    governor::{
        clock::DefaultClock,
        middleware::NoOpMiddleware,
        state::{InMemoryState, NotKeyed},
        Quota, RateLimiter,
    },
    bytes::Bytes,
    quinn_proto::crypto::rustls::QuicServerConfig,
    rustls::{
        Error as TlsError,
        server::danger::{ClientCertVerified, ClientCertVerifier},
    },
    solana_sdk::{
        pubkey::Pubkey, transaction::VersionedTransaction,
    },
    solana_tls_utils::{
      get_pubkey_from_tls_certificate,
      new_dummy_x509_certificate,
    },
    std::{
        collections::{HashSet, HashMap},
        net::SocketAddr,
        num::NonZeroU32,
        sync::Arc,
        time::{Duration, Instant},
    },
    tokio::sync::mpsc,
    tracing::{error, info, warn},
    solana_streamer::nonblocking::quic::get_remote_pubkey,
};

const ALPN_JET_TX_PROTOCOL: &[&[u8]] = &[b"tx-quic-jet", b"solana-tpu"];

type PubkeyLimiter = RateLimiter<NotKeyed, InMemoryState, DefaultClock, NoOpMiddleware>;

#[derive(Clone)]
struct QuicRateLimiter {
    limiters: Arc<DashMap<Pubkey, Arc<PubkeyLimiter>>>,
    limits_config: Arc<HashMap<Pubkey, u32>>,
}

impl QuicRateLimiter {
    fn new(limits_config: HashMap<Pubkey, u32>) -> Self {
        Self {
            limiters: Arc::new(DashMap::new()),
            limits_config: Arc::new(limits_config),
        }
    }

    fn check(&self, pubkey: &Pubkey) -> Result<(), ()> {
        match self.limits_config.get(pubkey) {
            Some(&limit) if limit > 0 => {
                let limiter = self.limiters.entry(*pubkey).or_insert_with(|| {
                    let quota = Quota::per_second(NonZeroU32::new(limit).unwrap());
                    Arc::new(RateLimiter::direct(quota))
                });
                if limiter.check().is_err() {
                    Err(())
                } else {
                    Ok(())
                }
            }
            _ => Ok(()),
        }
    }
}

#[derive(Debug)]
struct WhitelistVerifier {
    whitelist: HashSet<solana_sdk::pubkey::Pubkey>,
    verifier: Arc<rustls::crypto::CryptoProvider>,
}

impl WhitelistVerifier {
    fn new(client_whitelist: HashSet<solana_sdk::pubkey::Pubkey>) -> Arc<Self> {
        Arc::new(WhitelistVerifier {
            whitelist: client_whitelist,
            verifier: Arc::new(rustls::crypto::ring::default_provider()),
        })
    }
}

impl ClientCertVerifier for WhitelistVerifier {
    fn root_hint_subjects(&self) -> &[rustls::DistinguishedName] {
        &[]
    }

    fn verify_client_cert(
        &self,
        end_entity: &tonic::transport::CertificateDer<'_>,
        _intermediates: &[tonic::transport::CertificateDer<'_>],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<ClientCertVerified, TlsError> {
        let pubkey =
            if let Some(pubkey) = get_pubkey_from_tls_certificate(&end_entity) {
                pubkey
            } else {
                incr_quic_server_auth_failures_total("invalid_cert_has_no_pubkey");
                return Err(TlsError::General(
                    "Failed to get pubkey from cert".to_string(),
                ));
            };

        if self.whitelist.contains(&pubkey) {
            info!("Client certificate validated for pubkey: {}", pubkey);
            Ok(ClientCertVerified::assertion())
        } else {
            incr_quic_server_auth_failures_total("not_in_whitelist");
            warn!(
                "Client certificate rejected for pubkey: {}. Not in whitelist.",
                pubkey
            );
            Err(TlsError::General(
                "Pukey is not present in whitelist".to_string(),
            ))
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &tonic::transport::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, TlsError> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.verifier.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &tonic::transport::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, TlsError> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.verifier.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.verifier
            .signature_verification_algorithms
            .supported_schemes()
    }

    fn offer_client_auth(&self) -> bool {
        true
    }

    fn client_auth_mandatory(&self) -> bool {
        self.offer_client_auth()
    }
}

pub fn spawn_quic_server(
    listen_addr: SocketAddr,
    identity: &solana_sdk::signature::Keypair,
    transaction_sink: mpsc::UnboundedSender<Arc<SendTransactionRequest>>,
    max_retries: usize,
    client_limits: HashMap<Pubkey, u32>,
) -> anyhow::Result<tokio::task::JoinHandle<()>> {
    let (cert, key) = new_dummy_x509_certificate(identity);
    
    let client_whitelist = client_limits.keys().cloned().collect();
    let rate_limiter = QuicRateLimiter::new(client_limits);

    let mut server_tls_config = rustls::ServerConfig::builder()
        .with_client_cert_verifier(WhitelistVerifier::new(client_whitelist))
        .with_single_cert(vec![cert.clone()], key)?;
    server_tls_config.alpn_protocols = ALPN_JET_TX_PROTOCOL.iter().map(|p| p.to_vec()).collect();
    let quic_server_config = QuicServerConfig::try_from(server_tls_config)?;

    let mut server_config = quinn_proto::ServerConfig::with_crypto(Arc::new(quic_server_config));

    let transport_config = Arc::get_mut(&mut server_config.transport).unwrap();
    transport_config.max_idle_timeout(Some(Duration::from_secs(30).try_into()?));
    transport_config.keep_alive_interval(Some(Duration::from_secs(10)));
    transport_config.max_concurrent_uni_streams(256_u32.into());

    let endpoint = quinn::Endpoint::server(server_config, listen_addr)?;

    let handle = tokio::spawn(async move {
        info!("QUIC server listening on {}", listen_addr);
        while let Some(conn) = endpoint.accept().await {
            let transaction_sink = transaction_sink.clone();
            let rate_limiter = rate_limiter.clone();
            tokio::spawn(async move {
                incr_quic_server_active_connections();
                match conn.await {
                    Ok(connection) => {
                        let peer_identity = get_remote_pubkey(&connection);
                        let remote_address = connection.remote_address();
                        info!("QUIC connection established from: {}", remote_address);
                        loop {
                            match connection.accept_uni().await {
                                Ok(mut stream) => {
                                    if let Some(pubkey) = peer_identity {
                                        if rate_limiter.check(&pubkey).is_err() {
                                            warn!(
                                                "Rate limit exceeded for peer: {}, ip: {}",
                                                pubkey,
                                                remote_address.ip()
                                            );
                                            let _ = stream.stop(0u32.into());
                                            continue;
                                        }
                                    }
                                    let sink = transaction_sink.clone();
                                    let pubkey = peer_identity;
                                    let start_time = Instant::now();
                                    tokio::spawn(async move {
                                        if let Err(e) =
                                            handle_stream(stream, sink, remote_address, pubkey, max_retries)
                                                .await
                                        {
                                            let reason = {
                                                let error_text = e.to_string().to_lowercase();
                                                if error_text.contains("read error") {
                                                    "read_error"
                                                } else if error_text.contains("stream too long") {
                                                    "transaction_size_too_large"
                                                } else if error_text.contains("deserialize") {
                                                    "deserialization"
                                                } else if error_text.contains("signatures") {
                                                    "no_signature"
                                                } else if error_text.contains("sink") {
                                                    "sink_send"
                                                } else {
                                                    "unknown"
                                                }
                                            };

                                            incr_quic_server_stream_error(reason);

                                            warn!(%remote_address, "Failed to handle QUIC stream: {:#}", e);
                                        }
                                        observe_quic_server_stream_duration(start_time.elapsed());
                                    });
                                }
                                Err(quinn::ConnectionError::ApplicationClosed(_)) => {
                                    break;
                                }
                                Err(e) => {
                                    error!("Error accepting QUIC stream: {}", e);
                                    break;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!("QUIC connection failed: {}", e);
                    }
                }
                dec_quic_server_active_connections();
            });
        }
    });

    Ok(handle)
}

/// Handles a single incoming QUIC stream, expecting a raw transaction.
async fn handle_stream(
    mut stream: quinn::RecvStream,
    transaction_sink: mpsc::UnboundedSender<Arc<SendTransactionRequest>>,
    remote_address: SocketAddr,
    peer_pubkey: Option<Pubkey>,
    max_retries: usize,
) -> anyhow::Result<()> {
    // It's recommended to set a max size to prevent OOM attacks.
    const MAX_TX_SIZE: usize = 1232 * 2;
    let wire_transaction_bytes = stream.read_to_end(MAX_TX_SIZE).await?;
    let wire_transaction = Bytes::from(wire_transaction_bytes);

    let transaction: VersionedTransaction = bincode::deserialize(&wire_transaction)
        .map_err(|e| anyhow::anyhow!("Failed to deserialize transaction: {}", e))?;

    let signature = transaction
        .signatures
        .first()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("Transaction has no signatures"))?;

    info!(%remote_address, %signature, "Received transaction, sending to fanout");

    let send_request = Arc::new(SendTransactionRequest {
        signature,
        transaction,
        wire_transaction,
        max_retries: Some(max_retries),    // Use scheduler's default
        policies: Vec::new(), // No policies for raw QUIC submission
    });

    transaction_sink
        .send(send_request)
        .map_err(|e| anyhow::anyhow!("Failed to send transaction to sink: {}", e))?;

    incr_quic_server_transactions_received_total();

    if let Some(pubkey) = peer_pubkey {
        incr_quic_server_transactions_by_client_total(&pubkey.to_string());
    }

    Ok(())
}