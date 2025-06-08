use {
    quinn::crypto::rustls::QuicServerConfig,
    rand::Rng,
    solana_sdk::signature::Keypair,
    solana_streamer::{
        nonblocking::quic::ALPN_TPU_PROTOCOL_ID, tls_certificates::new_dummy_x509_certificate,
    },
    std::{
        net::{SocketAddr, TcpListener},
        num::NonZeroUsize,
        sync::Arc,
        time::Duration,
    },
    yellowstone_jet::{
        config::{ConfigQuic, ConfigQuicTpuPort, ConfigSendTransactionService},
        crypto_provider::crypto_provider,
        util::CommitmentLevel,
    },
};

#[allow(dead_code)]
pub fn find_available_port() -> Option<u16> {
    let mut rng = rand::thread_rng();

    for _ in 0..100 {
        // Try up to 100 times to find an open port
        let (begin, end) = ConfigQuic::default_endpoint_port_range();
        let port = rng.gen_range(begin..=end);
        let addr = SocketAddr::from(([127, 0, 0, 1], port));

        // Try to bind to the port; if successful, port is free
        if TcpListener::bind(addr).is_ok() {
            return Some(port);
        }
    }

    None // If no port found after 100 attempts, return None
}

#[allow(dead_code)]
pub fn generate_random_local_addr() -> SocketAddr {
    let port = find_available_port().expect("port");
    SocketAddr::new("127.0.0.1".parse().expect("ipv4"), port)
}

#[allow(dead_code)]
pub fn build_random_endpoint(addr: SocketAddr) -> (quinn::Endpoint, Keypair) {
    let kp = Keypair::new();
    let (cert, priv_key) = new_dummy_x509_certificate(&kp);
    let mut crypto = rustls::ServerConfig::builder_with_provider(Arc::new(crypto_provider()))
        .with_safe_default_protocol_versions()
        .expect("server config build")
        .with_client_cert_verifier(solana_streamer::quic::SkipClientVerification::new())
        .with_single_cert(vec![cert], priv_key)
        .expect("quinn server config");
    crypto.alpn_protocols = vec![ALPN_TPU_PROTOCOL_ID.to_vec()];

    let quic_server_config = QuicServerConfig::try_from(crypto).expect("quic server config");
    let config = quinn::ServerConfig::with_crypto(Arc::new(quic_server_config));
    let endpoint = quinn::Endpoint::server(config, addr).expect("quinn server endpoint");

    (endpoint, kp)
}

#[allow(dead_code)]
pub const fn default_config_quic() -> ConfigQuic {
    ConfigQuic {
        connection_max_pools: ConfigQuic::default_connection_max_pools(),
        connection_pool_size: ConfigQuic::default_connection_pool_size(),
        send_retry_count: ConfigQuic::default_send_retry_count(),
        tpu_port: ConfigQuicTpuPort::Normal,
        connection_handshake_timeout: ConfigQuic::default_connection_handshake_timeout(),
        max_idle_timeout: ConfigQuic::default_max_idle_timeout(),
        keep_alive_interval: ConfigQuic::default_keep_alive_interval(),
        send_timeout: ConfigQuic::default_send_timeout(),
        endpoint_port_range: ConfigQuic::default_endpoint_port_range(),
        send_max_concurrent_streams: ConfigQuic::default_send_max_concurrent_streams(),
        extra_tpu_forward: vec![],
        endpoint_count: ConfigQuic::default_num_endpoints(),
    }
}

#[allow(dead_code)]
pub const fn default_config_transaction() -> ConfigSendTransactionService {
    ConfigSendTransactionService {
        default_max_retries: Some(1),
        leader_forward_count: 1,
        relay_only_mode: true,
        retry_rate: Duration::from_secs(1),
        service_max_retries: 2,
        stop_send_on_commitment: CommitmentLevel::Finalized,
    }
}

#[allow(dead_code)]
pub const fn default_config_quic_client() -> ConfigQuic {
    ConfigQuic {
        connection_max_pools: NonZeroUsize::new(256).unwrap(),
        connection_pool_size: 1,
        send_retry_count: 1,
        tpu_port: ConfigQuicTpuPort::Normal,
        connection_handshake_timeout: Duration::from_secs(2),
        max_idle_timeout: Duration::from_secs(2),
        keep_alive_interval: Duration::from_secs(1),
        send_timeout: Duration::from_secs(10),
        endpoint_port_range: (8000, 10000),
        send_max_concurrent_streams: 128,
        extra_tpu_forward: vec![],
        endpoint_count: NonZeroUsize::new(1).unwrap(),
    }
}
